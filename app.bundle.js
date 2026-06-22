var DonatBoss = (() => {
  var { useState, useEffect, useCallback, useMemo } = React;
  var sb = window.sb;

  // ─── Patch rpc untuk delete-user via Vercel function ───────────────────────
  try {
    const __rpc = sb.rpc.bind(sb);
    sb.rpc = async (fn, args) => {
      if (fn === "hapus_akun_langsung") {
        try {
          const { data: sessData } = await sb.auth.getSession();
          const token = sessData?.session?.access_token;
          if (!token) throw new Error("Owner harus login dulu.");
          const resp = await fetch("/api/delete-user", {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "Authorization": `Bearer ${token}`
            },
            body: JSON.stringify(args || {})
          });
          const text = await resp.text();
          let json = null;
          try { json = JSON.parse(text); } catch {}
          if (!resp.ok) throw new Error(json?.error || text || "Gagal hapus akun.");
          return { data: json, error: null };
        } catch (e) {
          return { data: null, error: { message: e?.message || String(e) } };
        }
      }
      return __rpc(fn, args);
    };
  } catch {}

  // ─── Helpers ───────────────────────────────────────────────────────────────
  var uid = () => {
    if (typeof crypto !== "undefined" && crypto.randomUUID) return crypto.randomUUID();
    return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
      const r = Math.random() * 16 | 0;
      const v = c === "x" ? r : r & 3 | 8;
      return v.toString(16);
    });
  };

  // ─── Store (Supabase + local cache) ────────────────────────────────────────
  var S = (() => {
    const TABLE_BY_KEY = {
      branches: "branches",
      bahanPokok: "bahanPokok",
      menuVarian: "menuVarian",
      topingTambahan: "topingTambahan",
      investors: "investors",
      profiles: "profiles",
      transactions: "transactions",
      setoranHarian: "setoranHarian",
      setoranBulanan: "setoranBulanan",
      absensi: "absensi",
      absensiBulanan: "absensiBulanan",
      editLog: "editLog",
      pengeluaranLapak: "pengeluaranLapak",
      pengeluaranOwner: "pengeluaranOwner",
      produksiCK: "produksiCK",
      distribusiCK: "distribusiCK",
      stokLapak: "stokLapak",
      danaPemeliharaan: "danaPemeliharaan"
    };
    const LOCAL_KEYS = new Set(["notified_ids", "jadwalLibur"]);
    let cache = {};
    let channels = [];
    const listeners = new Set();
    let onError = (msg) => console.warn(msg);
    const emit = () => listeners.forEach((fn) => fn());
    const deepEq = (a, b) => { try { return JSON.stringify(a) === JSON.stringify(b); } catch { return false; } };

    const get = (k, def = null) => {
      if (LOCAL_KEYS.has(k)) {
        try { const v = localStorage.getItem(k); return v ? JSON.parse(v) : def; } catch { return def; }
      }
      return k in cache ? cache[k] : def;
    };

    const setLocal = (k, v) => {
      if (LOCAL_KEYS.has(k)) {
        try { localStorage.setItem(k, JSON.stringify(v)); } catch {}
        emit(); return;
      }
      cache[k] = v; emit();
    };

    const setErrorHandler = (fn) => { onError = typeof fn === "function" ? fn : onError; };

    const loadKey = async (key) => {
      const table = TABLE_BY_KEY[key];
      if (!table) return;
      const { data, error } = await sb.from(table).select("*");
      if (error) throw error;
      cache[key] = data || [];
    };

    const loadAll = async () => {
      const keys = Object.keys(TABLE_BY_KEY).filter((k) => k !== "profiles");
      await Promise.all(keys.map((k) => loadKey(k)));
      emit();
    };

    const applyRealtime = (key, payload) => {
      const table = TABLE_BY_KEY[key];
      if (!table) return;
      const ev = payload.eventType;
      const rowNew = payload.new;
      const rowOld = payload.old;
      const id = (rowNew && rowNew.id) || (rowOld && rowOld.id);
      if (!id) return;
      const cur = cache[key] || [];
      if (ev === "DELETE") { cache[key] = cur.filter((x) => x.id !== id); emit(); return; }
      if (ev === "INSERT") { cache[key] = [...cur.filter((x) => x.id !== id), rowNew]; emit(); return; }
      if (ev === "UPDATE") { cache[key] = cur.map((x) => x.id === id ? rowNew : x); emit(); return; }
    };

    const startRealtime = () => {
      stopRealtime();
      Object.entries(TABLE_BY_KEY).forEach(([key, table]) => {
        if (LOCAL_KEYS.has(key)) return;
        const ch = sb.channel("rt:" + table)
          .on("postgres_changes", { event: "*", schema: "public", table }, (payload) => applyRealtime(key, payload))
          .subscribe();
        channels.push(ch);
      });
    };

    const stopRealtime = () => {
      channels.forEach((ch) => { try { sb.removeChannel(ch); } catch {} });
      channels = [];
    };

    const persistDiff = async (key, beforeArr, afterArr) => {
      const table = TABLE_BY_KEY[key];
      if (!table) return;
      const before = Array.isArray(beforeArr) ? beforeArr : [];
      const after = Array.isArray(afterArr) ? afterArr : [];
      const bMap = new Map(before.map((r) => [r.id, r]));
      const aMap = new Map(after.map((r) => [r.id, r]));
      const toInsert = [], toUpdate = [], toDelete = [];
      for (const [id, row] of aMap.entries()) {
        const prev = bMap.get(id);
        if (!prev) { toInsert.push(row); continue; }
        if (!deepEq(prev, row)) toUpdate.push(row);
      }
      for (const [id] of bMap.entries()) { if (!aMap.has(id)) toDelete.push(id); }
      if (toInsert.length) { const { error } = await sb.from(table).insert(toInsert); if (error) throw error; }
      if (toUpdate.length) { for (const row of toUpdate) { const { id, ...payload } = row; const { error } = await sb.from(table).update(payload).eq("id", id); if (error) throw error; } }
      if (toDelete.length) { const { error } = await sb.from(table).delete().in("id", toDelete); if (error) throw error; }
    };

    const set = (key, value) => {
      if (LOCAL_KEYS.has(key)) { setLocal(key, value); return; }
      const before = cache[key];
      cache[key] = value; emit();
      persistDiff(key, before, value).catch((e) => onError(e?.message || String(e)));
    };

    const reset = () => { stopRealtime(); cache = {}; emit(); };
    const subscribe = (fn) => { listeners.add(fn); return () => listeners.delete(fn); };
    return { get, set, setLocal, loadAll, loadKey, startRealtime, stopRealtime, reset, subscribe, setErrorHandler };
  })();

  // ─── Formatters ────────────────────────────────────────────────────────────
  var fmtRp = (n) => "Rp " + Number(n || 0).toLocaleString("id-ID");
  var today = () => { const d = new Date(); d.setHours(d.getHours() + 7); return d.toISOString().slice(0, 10); };
  var nowTs = () => new Date().toLocaleString("id-ID");
  var nowIso = () => new Date().toISOString();

  // ─── Helper: upsert stokLapak, toleran terhadap kolom lastUpdate yang mungkin belum ada ───
  var upsertStokLapak = async (branchId, menuId, newStok, existingRow) => {
    const payloadFull = { stok: newStok, lastUpdate: nowIso() };
    const payloadBasic = { stok: newStok };
    let res;
    if (existingRow) {
      res = await sb.from("stokLapak").update(payloadFull).eq("id", existingRow.id);
      if (res.error && /lastUpdate|column/i.test(res.error.message || "")) {
        res = await sb.from("stokLapak").update(payloadBasic).eq("id", existingRow.id);
      }
    } else {
      res = await sb.from("stokLapak").insert([{ id: uid(), branchId, menuId, ...payloadFull }]);
      if (res.error && /lastUpdate|column/i.test(res.error.message || "")) {
        res = await sb.from("stokLapak").insert([{ id: uid(), branchId, menuId, ...payloadBasic }]);
      }
    }
    if (res.error) throw res.error;
    return res;
  };
  // tsForDate / isoForDate: pakai TANGGAL dari date yang dipilih + JAM sekarang
  // Supaya input data tanggal lalu tetap tercatat di tanggal yang benar
  var tsForDate = (date) => {
    if (!date) return nowTs();
    const now = new Date();
    const pad = (n) => String(n).padStart(2, "0");
    // Bangun string tampilan langsung: "dd/mm/yyyy, HH.MM.SS"
    const parts = date.split("-"); // ["yyyy", "mm", "dd"]
    if (parts.length !== 3) return nowTs();
    const dd = parts[2], mm = parts[1], yyyy = parts[0];
    const HH = pad(now.getHours());
    const MM = pad(now.getMinutes());
    const SS = pad(now.getSeconds());
    return `${dd}/${mm}/${yyyy}, ${HH}.${MM}.${SS}`;
  };
  var isoForDate = (date) => {
    if (!date) return nowIso();
    const now = new Date();
    const pad = (n) => String(n).padStart(2, "0");
    const HH = pad(now.getHours());
    const MM = pad(now.getMinutes());
    const SS = pad(now.getSeconds());
    const ms = String(now.getMilliseconds()).padStart(3, "0");
    // Offset WIB +07:00
    return `${date}T${HH}:${MM}:${SS}.${ms}+07:00`;
  };
  var fmtTs = (v) => { if (!v) return "-"; try { return new Date(v).toLocaleString("id-ID"); } catch { return String(v); } };
  // fmtTxTs: tampilkan timestamp struk dengan TANGGAL dari tx.date (selalu benar)
  // + JAM dari tx.ts. Ini fix untuk data lama yang tersimpan dengan tanggal salah.
  var fmtTxTs = (tx) => {
    if (!tx) return "-";
    const date = tx.date; // "yyyy-mm-dd" — selalu benar
    const ts = tx.ts || "";  // "dd/mm/yyyy, HH.MM.SS" atau format lain
    if (!date) return ts || "-";
    // Ekstrak jam dari ts — cari pola HH.MM.SS atau HH:MM:SS
    const jamMatch = ts.match(/(\d{1,2})[.:](\d{2})[.:](\d{2})/);
    const jam = jamMatch ? `${jamMatch[1]}.${jamMatch[2]}.${jamMatch[3]}` : "";
    // Format tanggal dari date "yyyy-mm-dd" → "dd/mm/yyyy"
    const parts = date.split("-");
    if (parts.length !== 3) return ts || date;
    const tglStr = `${parts[2]}/${parts[1]}/${parts[0]}`;
    return jam ? `${tglStr}, ${jam}` : tglStr;
  };

  // ─── Branding / Assets ─────────────────────────────────────────────────────
  var getAssetBucket = () => String(window.APP_CONFIG?.SUPABASE_ASSET_BUCKET || "").trim();
  var getBrandLogo = () => { try { return localStorage.getItem("branding_logo_url") || "./logo.jpg"; } catch { return "./logo.jpg"; } };
  var setBrandLogoLocal = (url) => { try { if (url) localStorage.setItem("branding_logo_url", url); } catch {} };
  var syncBrandingFromDb = async () => {
    try {
      const { data, error } = await sb.from("app_settings").select("value").eq("key", "branding").maybeSingle();
      if (error) throw error;
      const logoUrl = data?.value?.logoUrl;
      if (logoUrl) setBrandLogoLocal(logoUrl);
      return logoUrl || null;
    } catch { return null; }
  };
  var uploadAsset = async (file, folder = "menu") => {
    if (!file) throw new Error("File belum dipilih.");
    const bucket = getAssetBucket();
    if (!bucket || bucket === "ganti_dengan_nama_bucket_kamu") throw new Error("Isi SUPABASE_ASSET_BUCKET di config.js dulu.");
    const ext = String(file.name || "file").split(".").pop()?.toLowerCase() || "jpg";
    const safeName = String(file.name || "asset").replace(/[^a-zA-Z0-9._-]/g, "-");
    const path = `${folder}/${Date.now()}-${uid()}-${safeName}`;
    const { error } = await sb.storage.from(bucket).upload(path, file, { upsert: true });
    if (error) throw error;
    const pub = sb.storage.from(bucket).getPublicUrl(path);
    return { path, url: pub?.data?.publicUrl || null };
  };

  function useStoreTick() {
    const [tick, setTick] = useState(0);
    useEffect(() => S.subscribe(() => setTick((t) => t + 1)), []);
    return tick;
  }

  // ─── REVISI #1: HPP BARU — Harga Beli Total ÷ Kapasitas Yield ─────────────
  // Struktur bahanPokok baru: { id, nama, hargaBeli, kapasitas, satuanBeli }
  // Rumus: hppPerPcs = hargaBeli / kapasitas
  //
  // Struktur resepBahanPokok di menu: [{ bahanId, jumlahPakai }]
  //   → hppAdonan = sum(bahanPokok[bahanId].hppPerPcs * jumlahPakai)
  //   (jumlahPakai default 1 = 1 pcs adonan dasar)
  //
  // Struktur resepToping di menu (varian/topping per menu):
  //   [{ nama, hargaBeli, kapasitas }]
  //   → hppToping = sum(hargaBeli / kapasitas)
  //
  // HPP satuan  = hppAdonan + hppToping
  // HPP paket   = (hppSatuan × isiBox) + boxCost

  var getBahanHppPerPcs = (bahan) => {
    const hargaBeli = parseFloat(bahan.hargaBeli || 0) || 0;
    const kapasitas = Math.max(parseInt(bahan.kapasitas || 1) || 1, 1);
    return hargaBeli / kapasitas;
  };

  var getMenuHPPBreakdown = (menu) => {
    const bahanList = S.get("bahanPokok") || [];

    // HPP adonan: jumlah semua bahan pokok yang dipakai, dihitung per pcs
    const hppAdonanPerPcs = (menu.resepBahanPokok || []).reduce((acc, r) => {
      const b = bahanList.find((x) => x.id === r.bahanId);
      if (!b) return acc;
      return acc + getBahanHppPerPcs(b) * (parseFloat(r.jumlahPakai || 1) || 1);
    }, 0);

    // HPP toping/varian per menu: masing-masing punya hargaBeli + kapasitas sendiri
    const hppTopingPerPcs = (menu.resepToping || []).reduce((acc, t) => {
      const hb = parseFloat(t.hargaBeli || 0) || 0;
      const kap = Math.max(parseInt(t.kapasitas || 1) || 1, 1);
      return acc + (hb / kap);
    }, 0);

    const hppSatuanPerPcs = Math.ceil(hppAdonanPerPcs + hppTopingPerPcs);
    const hargaJual = parseFloat(menu.hargaJual || 0) || 0;
    const omzetKotorPerPcs = Math.max(hargaJual - hppSatuanPerPcs, 0);

    // Paket/Box
    const isiBox = Math.max(parseInt(menu.isiBox || 1) || 1, 1);
    const boxCost = Math.ceil(parseFloat(menu.boxCost || 0) || 0);
    const hppPaket = Math.ceil(hppSatuanPerPcs * isiBox + boxCost);
    const marginPaket = Math.ceil(hargaJual - hppPaket);

    return {
      hppAdonanPerPcs: Math.ceil(hppAdonanPerPcs),
      hppTopingPerPcs: Math.ceil(hppTopingPerPcs),
      hppSatuanPerPcs,
      omzetKotorPerPcs,
      isiBox,
      boxCost,
      hppPaket,
      marginSatuan: Math.ceil(hargaJual - hppSatuanPerPcs),
      marginPaket
    };
  };

  var hitungHPP = (menu) => {
    const info = getMenuHPPBreakdown(menu);
    return menu?.tipe === "paket" ? info.hppPaket : info.hppSatuanPerPcs;
  };

  // ─── Owner Expense helper (dipakai di beberapa tempat) ─────────────────────
  var getOwnerExpenseSummary = (entries, branchId, activeBranchIds = []) => {
    const rows = Array.isArray(entries) ? entries : [];
    if (branchId === "all") {
      const total = rows.reduce((a, p) => a + (parseFloat(p.jumlah || 0) || 0), 0);
      return { total, direct: total, shared: 0, relevantRows: rows };
    }
    const directRows = rows.filter((p) => p.branchId === branchId);
    const globalRows = rows.filter((p) => !p.branchId);
    const count = Math.max(activeBranchIds.length || 0, 1);
    const direct = directRows.reduce((a, p) => a + (parseFloat(p.jumlah || 0) || 0), 0);
    const sharedBase = globalRows.reduce((a, p) => a + (parseFloat(p.jumlah || 0) || 0), 0);
    const shared = sharedBase / count;
    return { total: direct + shared, direct, shared, relevantRows: [...directRows, ...globalRows] };
  };

  // ─── UI Primitives ─────────────────────────────────────────────────────────
  function Modal({ title, onClose, children }) {
    return React.createElement("div", { className: "modal-backdrop", onClick: onClose },
      React.createElement("div", { className: "modal-box", onClick: (e) => e.stopPropagation() },
        React.createElement("div", { className: "modal-header" },
          React.createElement("span", null, title),
          React.createElement("button", { className: "btn-icon", onClick: onClose }, "X")
        ),
        React.createElement("div", { className: "modal-body" }, children)
      )
    );
  }

  function Notif({ msg, type, onClose }) {
    useEffect(() => { const t = setTimeout(onClose, 4000); return () => clearTimeout(t); }, [onClose]);
    return React.createElement("div", { className: "notif notif-" + type },
      React.createElement("span", { style: { flex: 1 } }, msg),
      React.createElement("button", { onClick: onClose }, "X")
    );
  }

  function BarChart({ data, height }) {
    const max = Math.max(...data.map((d) => Math.max(d.v1 || 0, d.v2 || 0)), 1);
    return React.createElement("div", { className: "bar-chart", style: { height: (height || 100) + 24 } },
      data.map((d, i) =>
        React.createElement("div", { key: i, className: "bar-col" },
          React.createElement("div", { className: "bar-wrap", style: { height: height || 100 } },
            React.createElement("div", { className: "bar-fill bar-a", style: { height: (d.v1 || 0) / max * 100 + "%" } }),
            React.createElement("div", { className: "bar-fill bar-b", style: { height: (d.v2 || 0) / max * 100 + "%" } })
          ),
          React.createElement("div", { className: "bar-label" }, d.label)
        )
      )
    );
  }

  // ─── LoginPage ─────────────────────────────────────────────────────────────
  function LoginPage() {
    const [username, setUsername] = useState("");
    const [password, setPassword] = useState("");
    const [err, setErr] = useState("");
    const [busy, setBusy] = useState(false);

    const doLogin = async () => {
      setErr("");
      const u = String(username || "").trim();
      if (!u || !password) { setErr("Masukkan nama user/email dan password."); return; }
      try {
        setBusy(true);
        const emailFormat = u.includes("@") ? u : `${u.toLowerCase()}@donatboss.local`;
        const { error } = await sb.auth.signInWithPassword({ email: emailFormat, password });
        if (error) throw error;
      } catch (ex) {
        setErr(ex?.message || String(ex));
      } finally {
        setBusy(false);
      }
    };

    return React.createElement("div", { className: "login-wrap" },
      React.createElement("div", { className: "login-card" },
        React.createElement("div", { style: { fontSize: 52, textAlign: "center" } }, "EVORA"),
        React.createElement("h1", { className: "login-title" }, "DONAT BOSS"),
        React.createElement("p", { className: "login-sub" }, "Masuk privat menggunakan Kata Sandi."),
        React.createElement("div", { className: "field-group" },
          React.createElement("label", null, "Nama User / Email"),
          React.createElement("input", { className: "inp", value: username, onChange: (e) => setUsername(e.target.value), onKeyDown: (e) => e.key === "Enter" && doLogin(), placeholder: "Ketik nama user atau email..." })
        ),
        React.createElement("div", { className: "field-group", style: { marginTop: 8 } },
          React.createElement("label", null, "Kata Sandi"),
          React.createElement("input", { className: "inp", type: "password", value: password, onChange: (e) => setPassword(e.target.value), onKeyDown: (e) => e.key === "Enter" && doLogin(), placeholder: "Masukkan kata sandi..." })
        ),
        err && React.createElement("p", { style: { color: "#ef4444", fontSize: 13, marginTop: 4 } }, err),
        React.createElement("button", { className: "btn-primary btn-full", onClick: doLogin, disabled: busy, style: { marginTop: 12 } }, busy ? "Memverifikasi..." : "Masuk"),
        React.createElement("p", { className: "login-hint" }, "Kasir & Investor cukup ketik nama pendek tanpa @")
      )
    );
  }

  // ─── PengeluaranLapak (Kasir input pengeluaran harian) ─────────────────────
  function PengeluaranLapak({ branchId, branchName, date, pushNotif }) {
    const getList = () => (S.get("pengeluaranLapak") || []).filter((p) => p.branchId === branchId && p.date === date);
    const [list, setList] = useState(getList);
    const [form, setForm] = useState({ keterangan: "", jumlah: "" });
    const refresh = () => setList(getList());
    const CHIPS = ["Kantong Plastik", "Distribusi", "Transportasi", "Tisu", "Kemasan", "Lain-lain"];

    const tambah = () => {
      if (!form.keterangan || !form.jumlah) { alert("Isi semua kolom!"); return; }
      const all = S.get("pengeluaranLapak") || [];
      S.set("pengeluaranLapak", [...all, { id: uid(), branchId, branchName, date, ts: tsForDate(date), keterangan: form.keterangan, jumlah: parseFloat(form.jumlah) }]);
      setForm({ keterangan: "", jumlah: "" });
      refresh();
      pushNotif("Pengeluaran dicatat!", "success");
    };

    const hapus = (id) => { S.set("pengeluaranLapak", (S.get("pengeluaranLapak") || []).filter((x) => x.id !== id)); refresh(); };
    const total = list.reduce((a, p) => a + p.jumlah, 0);

    return React.createElement("div", null,
      React.createElement("h3", { className: "section-title" }, "Pengeluaran Lapak - ", date),
      React.createElement("p", { className: "info-txt" }, "Catat pengeluaran harian di lapak. Dilaporkan ke Owner."),
      React.createElement("div", { className: "chips mt8" }, CHIPS.map((s) => React.createElement("button", { key: s, className: "chip", onClick: () => setForm((f) => ({ ...f, keterangan: s })) }, s))),
      React.createElement("div", { className: "form-card mt8" },
        React.createElement("div", { className: "field-group" },
          React.createElement("label", null, "Keterangan"),
          React.createElement("input", { className: "inp", value: form.keterangan, onChange: (e) => setForm((f) => ({ ...f, keterangan: e.target.value })), placeholder: "Contoh: Beli kantong plastik" })
        ),
        React.createElement("div", { className: "field-group" },
          React.createElement("label", null, "Jumlah (Rp)"),
          React.createElement("input", { className: "inp", type: "number", value: form.jumlah, onChange: (e) => setForm((f) => ({ ...f, jumlah: e.target.value })), placeholder: "5000" })
        ),
        React.createElement("button", { className: "btn-primary", onClick: tambah }, "+ Tambah")
      ),
      list.length === 0 && React.createElement("p", { className: "empty-txt mt8" }, "Belum ada pengeluaran hari ini"),
      list.length > 0 && React.createElement("div", { className: "mt8" },
        list.map((p) =>
          React.createElement("div", { key: p.id, className: "peng-row" },
            React.createElement("div", { className: "peng-info" },
              React.createElement("span", { className: "peng-ket" }, p.keterangan),
              React.createElement("span", { className: "peng-ts" }, p.ts)
            ),
            React.createElement("div", { className: "peng-right" },
              React.createElement("span", { className: "peng-jml" }, fmtRp(p.jumlah)),
              React.createElement("button", { className: "btn-danger-sm", onClick: () => hapus(p.id) }, "X")
            )
          )
        ),
        React.createElement("div", { className: "peng-total" }, "Total: ", React.createElement("strong", null, fmtRp(total)))
      )
    );
  }

  // ─── EditTxModal (dipakai HANYA dari OwnerLaporan, bukan WorkerPage kasir) ─
  function EditTxModal({ tx, onClose, onSave }) {
    const [items, setItems] = useState(tx.items.map((x) => ({ ...x })));
    const [alasan, setAlasan] = useState("");
    const changeQty = (id, qty) => {
      if (qty <= 0) { setItems((i) => i.filter((x) => x.id !== id)); return; }
      setItems((i) => i.map((x) => x.id === id ? { ...x, qty } : x));
    };
    return React.createElement(Modal, { title: "Edit Transaksi", onClose },
      React.createElement("p", { className: "info-txt" }, "Perubahan ini dicatat dan dilaporkan ke log."),
      items.map((it) =>
        React.createElement("div", { key: it.id, className: "cart-item" },
          React.createElement("span", { style: { flex: 1 } }, it.nama),
          React.createElement("input", { type: "number", min: "0", className: "inp inp-sm", style: { width: 60 }, value: it.qty, onChange: (e) => changeQty(it.id, parseInt(e.target.value) || 0) }),
          React.createElement("span", { style: { minWidth: 80, textAlign: "right" } }, fmtRp(it.hargaJual * it.qty))
        )
      ),
      React.createElement("div", { className: "field-group mt8" },
        React.createElement("label", null, "Alasan Edit (wajib)"),
        React.createElement("input", { className: "inp", value: alasan, onChange: (e) => setAlasan(e.target.value), placeholder: "Contoh: salah input qty..." })
      ),
      React.createElement("div", { className: "row-wrap mt8" },
        React.createElement("button", { className: "btn-secondary", onClick: onClose }, "Batal"),
        React.createElement("button", { className: "btn-primary", onClick: () => { if (!alasan.trim()) { alert("Wajib isi alasan!"); return; } onSave(tx.id, items, alasan); } }, "Simpan")
      )
    );
  }

  // ─── DistribusiKonfirmCard — helper untuk konfirmasi distribusi di WorkerPage
  function DistribusiKonfirmCard({ d, pushNotif }) {
    const [jumlahTerima, setJumlahTerima] = useState(String(d.jumlahKirim || ""));
    const [catatan, setCatatan] = useState("");
    const [busy, setBusy] = useState(false);
    const konfirmasi = async () => {
      const jml = parseInt(jumlahTerima);
      if (isNaN(jml) || jml < 0) { pushNotif("Jumlah tidak valid.", "warning"); return; }
      const selisih = jml - (d.jumlahKirim || 0);
      setBusy(true);
      try {
        const { error } = await sb.from("distribusiCK").update({ jumlahTerima: jml, selisih, catatanSelisih: catatan.trim(), status: "diterima", confirmedAt: nowIso() }).eq("id", d.id);
        if (error) throw error;
        await S.loadKey("distribusiCK");
        // ─── Tambah stok lapak sebesar jumlah yang BENAR-BENAR diterima ───
        if (jml > 0) {
          const stoks = S.get("stokLapak") || [];
          const existing = stoks.find((s) => s.branchId === d.branchId && s.menuId === d.menuId);
          await upsertStokLapak(d.branchId, d.menuId, (existing?.stok || 0) + jml, existing);
          await S.loadKey("stokLapak");
        }
        pushNotif("Distribusi dikonfirmasi! Stok lapak diperbarui.", "success");
      } catch(e) { pushNotif(e?.message || String(e), "warning"); }
      finally { setBusy(false); }
    };
    return React.createElement("div", { className: "form-card", style: { borderColor: "#f59e0b", background: "#1a1500", marginBottom: 8 } },
      React.createElement("div", { style: { display: "flex", justifyContent: "space-between", marginBottom: 8 } },
        React.createElement("div", null,
          React.createElement("strong", null, d.menuNama),
          React.createElement("div", { style: { fontSize: 12, color: "#9a9690" } }, d.date, " | Dikirim: ", React.createElement("strong", { style: { color: "#22c55e" } }, d.jumlahKirim, " pcs"))
        ),
        React.createElement("span", { style: { fontSize: 11, color: "#f59e0b", fontWeight: 700 } }, "PENDING")
      ),
      React.createElement("div", { className: "field-group" },
        React.createElement("label", null, "Jumlah yang benar-benar diterima (pcs)"),
        React.createElement("input", { type: "number", className: "inp", value: jumlahTerima, onChange: (e) => setJumlahTerima(e.target.value), min: 0 })
      ),
      parseInt(jumlahTerima) !== d.jumlahKirim && React.createElement("div", { className: "field-group" },
        React.createElement("label", null, "Catatan selisih (wajib jika beda)"),
        React.createElement("input", { type: "text", className: "inp", placeholder: "Contoh: 5 pcs rusak saat pengiriman...", value: catatan, onChange: (e) => setCatatan(e.target.value) })
      ),
      React.createElement("button", { className: "btn-primary", disabled: busy, onClick: konfirmasi }, busy ? "Menyimpan..." : "Konfirmasi Terima")
    );
  }

  
  // ─── REVISI #2: WorkerPage — Tombol Edit HANYA tampil di mode="owner" ──────
  function WorkerPage({ pushNotif, me, mode = "worker" }) {
    const tick = useStoreTick();
    const [tab, setTab] = useState("kasir");
    const [branches, setBranches] = useState(() => S.get("branches") || []);
    const [branchId, setBranchId] = useState(() => me?.branchId || (S.get("branches") || [{}])[0]?.id || "");
    const [menus, setMenus] = useState(() => S.get("menuVarian") || []);
    const [topings, setTopings] = useState(() => S.get("topingTambahan") || []);
    const [cart, setCart] = useState([]);
    // Worker: txDate dikunci ke hari ini, tidak bisa diubah (anti manipulasi)
    // Owner (mode="owner"): bebas pilih tanggal
    const [txDate, setTxDate] = useState(today());
    const canChangeDate = mode === "owner";
    // Kalau worker, selalu paksa ke today() setiap render
    const safeTxDate = canChangeDate ? txDate : today();
    // REVISI #2: editModal hanya dipakai kalau mode === "owner"
    const [editModal, setEditModal] = useState(null);
    const userId = me?.user_id;

    useEffect(() => {
      setBranches(S.get("branches") || []);
      setMenus(S.get("menuVarian") || []);
      setTopings(S.get("topingTambahan") || []);
      if (me?.branchId) setBranchId(me.branchId);
    }, [tick, me?.branchId]);

    const curBranch = branches.find((b) => b.id === branchId);
    const transactions = (S.get("transactions") || []).filter((t) => t.branchId === branchId && t.date === safeTxDate);
    const branchOmzet = transactions.reduce((a, t) => a + t.total, 0);
    const branchPeng = (S.get("pengeluaranLapak") || []).filter((p) => p.branchId === branchId && p.date === safeTxDate).reduce((a, p) => a + p.jumlah, 0);

    // ─── Stok Lapak real-time ───
    const stokAll = S.get("stokLapak") || [];
    const getStok = (menuId) => stokAll.find((s) => s.branchId === branchId && s.menuId === menuId)?.stok || 0;
    // Untuk paket: stok yang relevan adalah stok menu satuan dasarnya (baseMenuId)
    const getStokUntukMenu = (menu) => {
      if (menu.tipe === "paket") {
        if (!menu.baseMenuId) return null; // belum dikonfigurasi, tidak divalidasi
        const isi = menu.isiBox || 1;
        return Math.floor(getStok(menu.baseMenuId) / isi);
      }
      return getStok(menu.id);
    };

    // ─── Ringkasan Terjual & Sisa per menu (untuk hari yang sedang dilihat) ───
    const ringkasanPenjualan = useMemo(() => {
      const terjualMap = {}; // { menuId: { nama, tipe, qtyTerjual (unit asli: pcs utk satuan, box utk paket) } }
      for (const tx of transactions) {
        for (const it of tx.items || []) {
          if (it.tipe === "toping") continue;
          if (!terjualMap[it.menuId]) terjualMap[it.menuId] = { nama: it.nama, tipe: it.tipe, qtyTerjual: 0 };
          terjualMap[it.menuId].qtyTerjual += it.qty;
        }
      }
      const result = [];
      for (const m of menus) {
        if (m.tipe === "toping") continue;
        const sold = terjualMap[m.id]?.qtyTerjual || 0;
        const sisa = getStokUntukMenu(m); // null jika baseMenuId paket belum diset
        if (sold === 0 && (sisa === null || sisa === 0)) continue; // skip menu yang tidak relevan hari ini
        result.push({ menuId: m.id, nama: m.nama, tipe: m.tipe, satuan: m.tipe === "paket" ? "box" : "pcs", terjual: sold, sisa });
      }
      return result;
    }, [tick, transactions, menus, stokAll, branchId]);

    const addToCart = (menu) => setCart((c) => {
      const ex = c.find((x) => x.menuId === menu.id);
      if (ex) return c.map((x) => x.menuId === menu.id ? { ...x, qty: x.qty + 1 } : x);
      return [...c, { id: uid(), menuId: menu.id, topingId: null, nama: menu.nama, tipe: menu.tipe || "satuan", isiBox: menu.isiBox || null, hargaJual: menu.hargaJual, hpp: hitungHPP(menu), qty: 1 }];
    });

    const addToping = (tp) => setCart((c) => {
      const ex = c.find((x) => x.topingId === tp.id);
      if (ex) return c.map((x) => x.topingId === tp.id ? { ...x, qty: x.qty + 1 } : x);
      return [...c, { id: uid(), menuId: null, topingId: tp.id, nama: tp.nama + " (Toping)", tipe: "toping", hargaJual: tp.hargaJual, hpp: tp.hargaBahan || 0, qty: 1 }];
    });

    const removeCart = (id) => setCart((c) => c.filter((x) => x.id !== id));
    const totalBayar = cart.reduce((a, x) => a + x.hargaJual * x.qty, 0);

    const submitTx = async () => {
      if (!cart.length) return;
      // REVISI #6: kasir wajib checkin KECUALI mode owner
      if (mode === "worker") {
        const abs = (S.get("absensi") || []).find((a) => a.user_id === userId && a.date === safeTxDate);
        if (!abs?.checkin_ts) { alert("Silakan check-in absensi dulu sebelum input transaksi."); return; }
        // Kalau sudah checkout, tidak bisa input transaksi
        if (abs?.checkout_ts) { alert("Anda sudah Check-out hari ini. Tidak bisa input transaksi lagi."); return; }
      }

      // ─── Hitung pengurangan stok per menu satuan (pcs) ───
      const pcsKonsumsi = {}; // { menuId: totalPcsBerkurang }
      for (const item of cart) {
        if (item.tipe === "toping") continue;
        const menuDef = menus.find((m) => m.id === item.menuId);
        if (!menuDef) continue;
        if (menuDef.tipe === "paket") {
          if (!menuDef.baseMenuId) continue; // belum dikonfigurasi, skip kontrol stok
          const pcs = item.qty * (menuDef.isiBox || 1);
          pcsKonsumsi[menuDef.baseMenuId] = (pcsKonsumsi[menuDef.baseMenuId] || 0) + pcs;
        } else {
          pcsKonsumsi[item.menuId] = (pcsKonsumsi[item.menuId] || 0) + item.qty;
        }
      }

      // ─── Validasi stok cukup ───
      const stoksNow = S.get("stokLapak") || [];
      for (const [menuId, pcs] of Object.entries(pcsKonsumsi)) {
        const cur = stoksNow.find((s) => s.branchId === branchId && s.menuId === menuId);
        const sisa = cur?.stok || 0;
        if (sisa < pcs) {
          const menuNama = menus.find((m) => m.id === menuId)?.nama || menuId;
          if (!confirm(`Stok "${menuNama}" tersisa ${sisa} pcs, tapi transaksi ini butuh ${pcs} pcs. Tetap simpan transaksi? (Stok akan jadi minus)`)) return;
        }
      }

      const txs = S.get("transactions") || [];
      S.set("transactions", [...txs, { id: uid(), branchId, date: safeTxDate, ts: tsForDate(safeTxDate), items: cart.map((x) => ({ ...x })), total: totalBayar, totalHPP: cart.reduce((a, x) => a + x.hpp * x.qty, 0) }]);

      // ─── Kurangi stokLapak ───
      try {
        for (const [menuId, pcs] of Object.entries(pcsKonsumsi)) {
          const cur = stoksNow.find((s) => s.branchId === branchId && s.menuId === menuId);
          const newStok = (cur?.stok || 0) - pcs;
          await upsertStokLapak(branchId, menuId, newStok, cur);
        }
        if (Object.keys(pcsKonsumsi).length > 0) await S.loadKey("stokLapak");
      } catch (e) { pushNotif("Gagal update stok: " + (e?.message || String(e)), "warning"); }

      setCart([]);
      pushNotif("Transaksi disimpan!", "success");
    };

    // REVISI #2: saveEdit hanya dipanggil dari owner, tetap ada di sini supaya mode="owner" bisa pakai
    const saveEdit = (txId, newItems, alasan) => {
      const txs = S.get("transactions") || [];
      const old = txs.find((x) => x.id === txId);
      S.set("transactions", txs.map((t) => t.id === txId ? { ...t, items: newItems, total: newItems.reduce((a, x) => a + x.hargaJual * x.qty, 0), totalHPP: newItems.reduce((a, x) => a + x.hpp * x.qty, 0), edited: true } : t));
      const logs = S.get("editLog") || [];
      S.set("editLog", [...logs, { id: uid(), ts: tsForDate(safeTxDate), txId, branchId, branchName: curBranch?.name || branchId, alasan, before: old?.items || [], after: newItems }]);
      setEditModal(null);
      pushNotif("Transaksi diperbarui.", "warning");
    };

    const getSetoran = useCallback(() => {
      const s = S.get("setoranHarian") || [];
      return s.find((x) => x.branchId === branchId && x.date === safeTxDate) || { status: "belum" };
    }, [branchId, safeTxDate]);
    const [setoran, setSetoran] = useState(getSetoran);
    useEffect(() => setSetoran(getSetoran()), [getSetoran]);

    const doSetoran = () => {
      const s = S.get("setoranHarian") || [];
      const existing = s.find((x) => x.branchId === branchId && x.date === safeTxDate);
      const entry = { id: existing?.id || uid(), branchId, branchName: curBranch?.name || branchId, date: safeTxDate, ts: tsForDate(safeTxDate), status: "menunggu", omzet: branchOmzet, pengeluaran: branchPeng };
      S.set("setoranHarian", existing ? s.map((x) => x.id === entry.id ? entry : x) : [...s, entry]);
      setSetoran(entry);
      pushNotif("Setoran dikirim ke Owner!", "success");
    };

    const allowSetoran = mode === "worker";
    const TABS = allowSetoran ? ["kasir", "riwayat", "pengeluaran", "setoran", "absensi", "distribusi"] : ["kasir", "riwayat", "pengeluaran", "absensi", "distribusi"];
    const TAB_LABELS = { kasir: "Kasir", riwayat: "Riwayat", pengeluaran: "Pengeluaran", setoran: "Setoran", absensi: "Absensi", distribusi: "Distribusi" };

    // ─── Absensi ───
    const [absMonth, setAbsMonth] = useState(today().slice(0, 7));
    const selectedAbs = useMemo(() => {
      const all = S.get("absensi") || [];
      return all.find((a) => a.user_id === userId && a.date === safeTxDate) || null;
    }, [tick, userId, safeTxDate]);

    // REVISI #6: blokir checkin jika hari libur atau sudah checkout
    const doCheckin = async () => {
      if (!userId) return;
      const targetDate = safeTxDate || today();
      const namaHari = ["Minggu","Senin","Selasa","Rabu","Kamis","Jumat","Sabtu"][new Date(`${targetDate}T00:00:00`).getDay()];
      const jadwalLibur = S.get("jadwalLibur") || {};
      if (jadwalLibur[userId] && jadwalLibur[userId] === namaHari) {
        alert(`Akses ditolak!\n\nTanggal ${targetDate} adalah hari ${namaHari} yang merupakan jadwal libur Anda.`);
        return;
      }
      const all = S.get("absensi") || [];
      const ex = all.find((a) => a.user_id === userId && a.date === targetDate);
      if (ex?.checkin_ts) { pushNotif("Check-in untuk tanggal ini sudah ada.", "warning"); return; }
      const row = ex
        ? { ...ex, checkin_ts: isoForDate(targetDate), branchId: me?.branchId || branchId }
        : { id: uid(), user_id: userId, branchId: me?.branchId || branchId, date: targetDate, checkin_ts: isoForDate(targetDate), checkout_ts: null };
      S.set("absensi", ex ? all.map((a) => a.id === row.id ? row : a) : [...all, row]);

      // ─── Gaji harian otomatis masuk pengeluaran saat check-in ───
      try {
        const gajiHarian = parseFloat(me?.gajiHarian || 0) || 0;
        if (gajiHarian > 0) {
          const pOwnerAll = S.get("pengeluaranOwner") || [];
          const sudahAda = pOwnerAll.find((p) => p.autoGajiUserId === userId && p.date === targetDate);
          if (!sudahAda) {
            const namaPekerja = me?.displayName || me?.email || "Pekerja";
            const branchNm = branches.find((b) => b.id === (me?.branchId || branchId))?.name || "";
            const { error } = await sb.from("pengeluaranOwner").insert([{
              id: uid(), date: targetDate, ts: tsForDate(targetDate),
              keterangan: `Gaji Harian - ${namaPekerja}`, jumlah: gajiHarian,
              kategori: "gaji_pekerja", branchId: me?.branchId || branchId, branchName: branchNm,
              autoGajiUserId: userId
            }]);
            if (!error) await S.loadKey("pengeluaranOwner");
          }
        }
      } catch (e) { /* gaji auto-insert gagal, tidak blok proses checkin */ }

      pushNotif("Check-in berhasil.", "success");
    };

    // REVISI #6: setelah checkout, form absensi dinonaktifkan (handled di render)
    const doCheckout = () => {
      if (!userId) return;
      const all = S.get("absensi") || [];
      const ex = all.find((a) => a.user_id === userId && a.date === safeTxDate);
      if (!ex?.checkin_ts) { pushNotif("Belum ada check-in untuk tanggal ini.", "warning"); return; }
      if (ex?.checkout_ts) { pushNotif("Check-out sudah tercatat.", "warning"); return; }
      S.set("absensi", all.map((a) => a.id === ex.id ? { ...a, checkout_ts: isoForDate(safeTxDate) } : a));
      pushNotif("Check-out berhasil.", "success");
    };

    const myMonthRows = useMemo(() => {
      const all = S.get("absensi") || [];
      return all.filter((a) => a.user_id === userId && String(a.date || "").startsWith(absMonth));
    }, [tick, userId, absMonth]);

    const monthSnap = useMemo(() => {
      const snaps = S.get("absensiBulanan") || [];
      return snaps.find((s) => s.user_id === userId && s.bulan === absMonth && s.locked) || null;
    }, [tick, userId, absMonth]);

    const calcMonth = useMemo(() => {
      let hadir = 0, menit = 0;
      for (const r of myMonthRows) {
        if (r.checkin_ts) hadir += 1;
        if (r.checkin_ts && r.checkout_ts) {
          const a = Date.parse(r.checkin_ts), b = Date.parse(r.checkout_ts);
          if (!isNaN(a) && !isNaN(b) && b > a) menit += Math.floor((b - a) / 60000);
        }
      }
      return { hadir, menit };
    }, [myMonthRows]);

    const sudahCheckout = !!selectedAbs?.checkout_ts;

    return React.createElement("div", { className: "page" },
      // Header
      React.createElement("div", { className: "page-header" },
        React.createElement("img", { className: "page-icon", src: getBrandLogo(), style: { width: 45, height: 45, objectFit: "cover", borderRadius: 10 } }),
        React.createElement("div", null,
          React.createElement("h2", null, "Halaman Kasir"),
          React.createElement("p", { className: "page-sub" }, curBranch?.name || "\u2014", curBranch?.workers?.length ? " - " + curBranch.workers.join(", ") : "")
        )
      ),
      // Filter tanggal & cabang
      React.createElement("div", { className: "row-wrap mb8" },
        React.createElement("select", { className: "inp inp-sm", value: branchId, onChange: (e) => setBranchId(e.target.value), disabled: !!me?.branchId },
          branches.map((b) => React.createElement("option", { key: b.id, value: b.id }, b.name))
        ),
        // Owner: bisa pilih tanggal bebas | Worker: tampil hari ini saja (tidak bisa diubah)
        canChangeDate
          ? React.createElement("input", { type: "date", className: "inp inp-sm", value: txDate, onChange: (e) => { const nd = e.target.value; setTxDate(nd); setAbsMonth(String(nd || today()).slice(0, 7)); } })
          : React.createElement("div", { className: "date-locked-badge" }, "\uD83D\uDCC5 ", safeTxDate)
      ),
      // Tabs
      React.createElement("div", { className: "tabs" },
        TABS.map((t) => React.createElement("button", { key: t, className: "tab" + (tab === t ? " active" : ""), onClick: () => setTab(t) }, TAB_LABELS[t]))
      ),

      // ── Tab: Kasir ──
      tab === "kasir" && React.createElement("div", { className: "kasir-layout" },
        React.createElement("div", null,
          React.createElement("h3", { className: "section-title" }, "Menu Satuan"),
          React.createElement("div", { className: "menu-grid" },
            menus.filter((m) => m.tipe !== "paket").map((m) => {
              const stokSisa = getStokUntukMenu(m);
              const habis = stokSisa !== null && stokSisa <= 0;
              return React.createElement("button", { key: m.id, className: "menu-card" + (habis ? " menu-card-habis" : ""), onClick: () => addToCart(m), disabled: habis },
                !!m.imageUrl && React.createElement("img", { src: m.imageUrl, alt: m.nama, className: "menu-thumb" }),
                React.createElement("div", { className: "menu-name" }, m.nama),
                React.createElement("div", { className: "menu-price" }, fmtRp(m.hargaJual)),
                stokSisa !== null && React.createElement("div", { className: "menu-stok", style: { fontSize: 11, color: habis ? "#ef4444" : "#9a9690", fontWeight: habis ? 700 : 400 } }, habis ? "Stok Habis" : "Stok: " + stokSisa)
              );
            })
          ),
          React.createElement("h3", { className: "section-title mt12" }, "Box / Paket"),
          React.createElement("div", { className: "menu-grid" },
            menus.filter((m) => m.tipe === "paket").map((m) => {
              const stokSisa = getStokUntukMenu(m);
              const habis = stokSisa !== null && stokSisa <= 0;
              return React.createElement("button", { key: m.id, className: "menu-card menu-card-paket" + (habis ? " menu-card-habis" : ""), onClick: () => addToCart(m), disabled: habis },
                !!m.imageUrl && React.createElement("img", { src: m.imageUrl, alt: m.nama, className: "menu-thumb" }),
                React.createElement("div", { className: "menu-name" }, m.nama),
                React.createElement("div", { style: { fontSize: 11, opacity: 0.7 } }, "Isi ", m.isiBox, " pcs"),
                React.createElement("div", { className: "menu-price" }, fmtRp(m.hargaJual)),
                stokSisa !== null && React.createElement("div", { className: "menu-stok", style: { fontSize: 11, color: habis ? "#ef4444" : "#9a9690", fontWeight: habis ? 700 : 400 } }, habis ? "Stok Habis" : "Stok: " + stokSisa + " box")
              );
            })
          ),
          React.createElement("h3", { className: "section-title mt12" }, "Toping Tambahan"),
          React.createElement("div", { className: "menu-grid" },
            topings.map((t) =>
              React.createElement("button", { key: t.id, className: "menu-card menu-card-toping", onClick: () => addToping(t) },
                React.createElement("div", { className: "menu-name" }, t.nama),
                React.createElement("div", { className: "menu-price" }, fmtRp(t.hargaJual))
              )
            )
          )
        ),
        React.createElement("div", { className: "cart-section" },
          React.createElement("h3", { className: "section-title" }, "Keranjang"),
          cart.length === 0 && React.createElement("p", { className: "empty-txt" }, "Belum ada item"),
          cart.map((item) =>
            React.createElement("div", { key: item.id, className: "cart-item" },
              React.createElement("div", { className: "cart-item-info" },
                React.createElement("span", null, item.nama),
                React.createElement("span", { className: "cart-qty" }, "x", item.qty)
              ),
              React.createElement("div", { className: "cart-item-right" },
                React.createElement("span", null, fmtRp(item.hargaJual * item.qty)),
                React.createElement("button", { className: "btn-danger-sm", onClick: () => removeCart(item.id) }, "X")
              )
            )
          ),
          cart.length > 0 && React.createElement(React.Fragment, null,
            React.createElement("div", { className: "cart-total" }, "Total: ", React.createElement("strong", null, fmtRp(totalBayar))),
            React.createElement("div", { className: "row-wrap" },
              React.createElement("button", { className: "btn-secondary", onClick: () => setCart([]) }, "Batal"),
              React.createElement("button", { className: "btn-primary", onClick: submitTx }, "Simpan Transaksi")
            )
          ),
          React.createElement("div", { className: "omzet-box mt12" },
            React.createElement("span", null, "Omzet Hari Ini"),
            React.createElement("strong", null, fmtRp(branchOmzet))
          ),
          React.createElement("div", { className: "omzet-box", style: { borderColor: "#5a1a1a" } },
            React.createElement("span", null, "Pengeluaran"),
            React.createElement("strong", { style: { color: "#ef4444" } }, fmtRp(branchPeng))
          ),
          ringkasanPenjualan.length > 0 && React.createElement("div", { className: "form-card mt8", style: { padding: 10 } },
            React.createElement("div", { style: { fontSize: 12, fontWeight: 700, color: "#d6a354", marginBottom: 6 } }, "Terjual & Sisa Hari Ini"),
            ringkasanPenjualan.map((r) =>
              React.createElement("div", { key: r.menuId, style: { display: "flex", justifyContent: "space-between", fontSize: 12, padding: "3px 0", borderBottom: "1px solid #2a2a2e" } },
                React.createElement("span", null, r.nama),
                React.createElement("span", null,
                  "Terjual: ", React.createElement("strong", { style: { color: "#22c55e" } }, r.terjual, " ", r.satuan),
                  "  |  Sisa: ", React.createElement("strong", { style: { color: r.sisa === 0 ? "#ef4444" : "#f59e0b" } }, r.sisa === null ? "-" : r.sisa, " ", r.satuan)
                )
              )
            )
          )
        )
      ),

      // ── Tab: Riwayat — REVISI #2: tombol Edit hanya tampil kalau mode="owner" ──
      tab === "riwayat" && React.createElement("div", null,
        React.createElement("h3", { className: "section-title" }, "Riwayat - ", safeTxDate),
        transactions.length === 0 && React.createElement("p", { className: "empty-txt" }, "Belum ada transaksi"),
        [...transactions].reverse().map((tx) =>
          React.createElement("div", { key: tx.id, className: "tx-card" + (tx.edited ? " tx-edited" : "") },
            React.createElement("div", { className: "tx-header" },
              React.createElement("span", { className: "tx-id" }, "STRUK-", tx.id.slice(0, 6).toUpperCase()),
              React.createElement("span", { className: "tx-ts" }, fmtTxTs(tx)),
              tx.edited && React.createElement("span", { className: "badge-edit" }, "Diedit")
            ),
            tx.items.map((it, i) => React.createElement("div", { key: i, className: "tx-item" }, it.nama, " x", it.qty, " - ", fmtRp(it.hargaJual * it.qty))),
            React.createElement("div", { className: "tx-total" }, "Total: ", fmtRp(tx.total)),
            // REVISI #2: tombol Edit hanya untuk owner
            mode === "owner" && React.createElement("button", { className: "btn-edit-sm", onClick: () => setEditModal(tx) }, "Edit")
          )
        )
      ),

      // ── Tab: Pengeluaran ──
      tab === "pengeluaran" && React.createElement(PengeluaranLapak, { branchId, branchName: curBranch?.name || "", date: safeTxDate, pushNotif }),

      // ── Tab: Setoran (hanya worker) ──
      allowSetoran && tab === "setoran" && React.createElement("div", { className: "setoran-box-worker" },
        React.createElement("div", { className: "setoran-status setoran-" + setoran.status },
          setoran.status === "belum" && React.createElement("span", null, "Belum Setor"),
          setoran.status === "menunggu" && React.createElement("span", null, "Menunggu Konfirmasi Owner"),
          setoran.status === "selesai" && React.createElement("span", null, "Sudah Setor - Dikonfirmasi")
        ),
        React.createElement("div", { className: "setoran-omzet" }, "Omzet: ", React.createElement("strong", null, fmtRp(branchOmzet))),
        React.createElement("div", { className: "setoran-omzet" }, "Pengeluaran Lapak: ", React.createElement("strong", { style: { color: "#ef4444" } }, fmtRp(branchPeng))),
        React.createElement("div", { className: "setoran-omzet" }, "Bersih Disetor: ", React.createElement("strong", { style: { color: "#22c55e" } }, fmtRp(branchOmzet - branchPeng))),
        ringkasanPenjualan.length > 0 && React.createElement("div", { className: "form-card mt8", style: { padding: 10 } },
          React.createElement("div", { style: { fontSize: 12, fontWeight: 700, color: "#d6a354", marginBottom: 6 } }, "Rincian Terjual & Sisa per Box/Item"),
          ringkasanPenjualan.map((r) =>
            React.createElement("div", { key: r.menuId, style: { display: "flex", justifyContent: "space-between", fontSize: 12, padding: "3px 0", borderBottom: "1px solid #2a2a2e" } },
              React.createElement("span", null, r.nama),
              React.createElement("span", null,
                "Terjual: ", React.createElement("strong", { style: { color: "#22c55e" } }, r.terjual, " ", r.satuan),
                "  |  Sisa: ", React.createElement("strong", { style: { color: r.sisa === 0 ? "#ef4444" : "#f59e0b" } }, r.sisa === null ? "-" : r.sisa, " ", r.satuan)
              )
            )
          )
        ),
        setoran.status === "belum" && React.createElement("button", { className: "btn-primary btn-full", onClick: doSetoran }, "Setor Sekarang"),
        setoran.status === "menunggu" && React.createElement("p", { className: "info-txt" }, "Menunggu Owner memverifikasi setoran Anda.")
      ),

      // ── Tab: Absensi — REVISI #6: nonaktif setelah checkout ──
      tab === "absensi" && React.createElement("div", null,
        React.createElement("h3", { className: "section-title mt8" }, "Absensi"),
        sudahCheckout && React.createElement("div", { className: "form-card", style: { background: "#2a1a1a", borderColor: "#ef4444" } },
          React.createElement("p", { style: { color: "#ef4444", fontWeight: 700, textAlign: "center" } }, "Anda sudah Check-out hari ini. Form absensi dikunci.")
        ),
        !sudahCheckout && React.createElement("div", { className: "form-card" },
          React.createElement("div", { className: "row-wrap", style: { justifyContent: "space-between" } },
            React.createElement("div", null,
              React.createElement("div", { style: { fontWeight: 700 } }, "Tanggal ", safeTxDate),
              React.createElement("div", { style: { fontSize: 12, color: "#9a9690" } }, "Check-in: ", fmtTs(selectedAbs?.checkin_ts), " | Check-out: ", fmtTs(selectedAbs?.checkout_ts))
            ),
            React.createElement("div", { className: "row-wrap" },
              React.createElement("button", { className: "btn-primary btn-sm", onClick: doCheckin }, "Check-in"),
              React.createElement("button", { className: "btn-secondary btn-sm", onClick: doCheckout }, "Check-out")
            )
          )
        ),
        React.createElement("p", { className: "info-txt mt8" }, "Pilih tanggal di atas untuk input absensi hari lain yang belum tercatat."),
        React.createElement("div", { className: "field-group mt8" },
          React.createElement("label", null, "Rekap Bulan"),
          React.createElement("input", { type: "month", className: "inp inp-sm", value: absMonth, onChange: (e) => setAbsMonth(e.target.value) })
        ),
        React.createElement("div", { className: "kpi-grid" },
          React.createElement("div", { className: "kpi-card kpi-omzet" },
            React.createElement("div", { className: "kpi-label" }, "Total Hadir"),
            React.createElement("div", { className: "kpi-val" }, (monthSnap ? monthSnap.total_hadir : calcMonth.hadir), " hari")
          ),
          React.createElement("div", { className: "kpi-card kpi-profit" },
            React.createElement("div", { className: "kpi-label" }, "Total Jam"),
            React.createElement("div", { className: "kpi-val" }, Math.round(((monthSnap ? monthSnap.total_menit : calcMonth.menit) || 0) / 60 * 10) / 10, " jam")
          )
        ),
        monthSnap && React.createElement("p", { className: "info-txt mt8" }, "Rekap bulan ini sudah dikunci oleh Owner."),
        React.createElement("h3", { className: "section-title mt12" }, "Riwayat Absensi (", absMonth, ")"),
        myMonthRows.length === 0 && React.createElement("p", { className: "empty-txt" }, "Belum ada absensi."),
        [...myMonthRows].sort((a, b) => String(b.date).localeCompare(String(a.date))).map((r) =>
          React.createElement("div", { key: r.id, className: "peng-row" },
            React.createElement("div", { className: "peng-info" },
              React.createElement("span", { className: "peng-ket" }, r.date),
              React.createElement("span", { className: "peng-ts" }, "In: ", fmtTs(r.checkin_ts), " | Out: ", fmtTs(r.checkout_ts))
            )
          )
        )
      ),

      // ── Tab: Distribusi dari CK ──
      tab === "distribusi" && (() => {
        const distList = (S.get("distribusiCK") || [])
          .filter((d) => d.branchId === (me?.branchId || branchId))
          .sort((a, b) => (b.date || "").localeCompare(a.date || ""));
        const pending = distList.filter((d) => d.status === "pending");
        const confirmed = distList.filter((d) => d.status !== "pending");
        return React.createElement("div", null,
          React.createElement("h3", { className: "section-title mt8" }, "Distribusi dari Central Kitchen"),
          React.createElement("p", { className: "info-txt" }, "Daftar kiriman produk dari Central Kitchen ke lapak kamu."),
          distList.length === 0 && React.createElement("p", { className: "empty-txt mt8" }, "Belum ada distribusi masuk."),
          pending.length > 0 && React.createElement("div", null,
            React.createElement("h4", { className: "sub-title", style: { color: "#f59e0b" } }, "⏳ Menunggu Konfirmasi (", pending.length, ")"),
            pending.map((d) =>
              React.createElement(DistribusiKonfirmCard, { key: d.id, d, pushNotif })
            )
          ),
          confirmed.length > 0 && React.createElement("div", { className: "mt12" },
            React.createElement("h4", { className: "sub-title" }, "Riwayat Distribusi"),
            React.createElement("div", { className: "tbl-wrap" },
              React.createElement("table", { className: "tbl" },
                React.createElement("thead", null, React.createElement("tr", null,
                  React.createElement("th", null, "Tanggal"), React.createElement("th", null, "Produk"),
                  React.createElement("th", null, "Dikirim"), React.createElement("th", null, "Diterima"), React.createElement("th", null, "Selisih")
                )),
                React.createElement("tbody", null, confirmed.map((d) =>
                  React.createElement("tr", { key: d.id },
                    React.createElement("td", { style: { fontSize: 12 } }, d.date),
                    React.createElement("td", null, React.createElement("strong", null, d.menuNama)),
                    React.createElement("td", { style: { color: "#22c55e" } }, d.jumlahKirim, " pcs"),
                    React.createElement("td", null, d.jumlahTerima, " pcs"),
                    React.createElement("td", { style: { color: d.selisih < 0 ? "#ef4444" : d.selisih > 0 ? "#f59e0b" : "#22c55e", fontWeight: 700 } },
                      d.selisih === 0 ? "Sesuai" : (d.selisih > 0 ? "+" : "") + d.selisih,
                      d.catatanSelisih ? React.createElement("div", { style: { fontSize: 11, color: "#9a9690", fontWeight: 400 } }, d.catatanSelisih) : null
                    )
                  )
                ))
              )
            )
          )
        );
      })(),

            // Modal edit transaksi — hanya owner
      editModal && mode === "owner" && React.createElement(EditTxModal, { tx: editModal, onClose: () => setEditModal(null), onSave: saveEdit })
    );
  }

  // ─── OwnerProduksiCK — Rekap + Distribusi CK ────────────────────────────────
  function OwnerProduksiCK({ pushNotif }) {
    const tick = useStoreTick();
    const [dr, setDr] = useState({ from: today(), to: today() });
    const [subtab, setSubtab] = useState("produksi");
    const [distribForm, setDistribForm] = useState({});
    const [distribBusy, setDistribBusy] = useState({});
    const [openDay, setOpenDay] = useState({});
    const branches = (S.get("branches") || []).filter((b) => b.type !== "central_kitchen");
    const menus = S.get("menuVarian") || [];
    const produksiAll = S.get("produksiCK") || [];
    const distribAll = S.get("distribusiCK") || [];

    // ─── Form input produksi langsung dari Owner ───
    const ckBranchOwner = (S.get("branches") || []).find((b) => b.type === "central_kitchen");
    const [inputForm, setInputForm] = useState({ menuId: "", jumlah: "", keterangan: "" });
    const [inputBusy, setInputBusy] = useState(false);
    const inputProduksi = async () => {
      if (!inputForm.menuId) { pushNotif("Pilih menu dulu.", "warning"); return; }
      const jml = parseInt(inputForm.jumlah);
      if (!jml || jml <= 0) { pushNotif("Jumlah harus lebih dari 0.", "warning"); return; }
      setInputBusy(true);
      try {
        const menu = menus.find((m) => m.id === inputForm.menuId);
        const hppPerPcsProduksi = Math.ceil(getMenuHPPBreakdown(menu)?.hppSatuanPerPcs || 0);
        const tglProduksi = dr.from; // ikut tanggal filter yang sudah ditentukan Owner
        const entry = {
          id: uid(), date: tglProduksi, ts: tsForDate(tglProduksi),
          branchId: ckBranchOwner?.id || null, branchName: ckBranchOwner?.name || "Central Kitchen",
          menuId: inputForm.menuId, menuNama: menu?.nama || inputForm.menuId,
          jumlah: jml, hppPerPcs: hppPerPcsProduksi, hppTotalProduksi: hppPerPcsProduksi * jml,
          keterangan: inputForm.keterangan.trim(), createdBy: null
        };
        const { error } = await sb.from("produksiCK").insert([entry]);
        if (error) throw error;
        await S.loadKey("produksiCK");
        setInputForm({ menuId: "", jumlah: "", keterangan: "" });
        pushNotif("Produksi tercatat untuk tanggal " + tglProduksi + "!", "success");
      } catch (e) { pushNotif(e?.message || String(e), "warning"); }
      finally { setInputBusy(false); }
    };

    const filtered = produksiAll.filter((p) => p.date >= dr.from && p.date <= dr.to);
    const totalPcs = filtered.reduce((a, p) => a + (p.jumlah || 0), 0);
    const totalHppProduksi = filtered.reduce((a, p) => a + (p.hppTotalProduksi || (p.hppPerPcs || 0) * (p.jumlah || 0)), 0);
    const byMenu = {};
    filtered.forEach((p) => { const key = p.menuNama || p.menuId || "?"; byMenu[key] = (byMenu[key] || 0) + (p.jumlah || 0); });
    const byMenuArr = Object.entries(byMenu).sort((a, b) => b[1] - a[1]);
    const byDate = {};
    filtered.forEach((p) => { if (!byDate[p.date]) byDate[p.date] = []; byDate[p.date].push(p); });
    const byDateArr = Object.entries(byDate).sort((a, b) => b[0].localeCompare(a[0]));

    const sudahDistrib = (prodId) => distribAll.some((d) => d.produksiId === prodId);
    const getDistribForm = (prodId) => distribForm[prodId] || {};
    const setDistribEntry = (prodId, branchId, val) => setDistribForm((f) => ({ ...f, [prodId]: { ...(f[prodId] || {}), [branchId]: val } }));

    const kirimDistribusi = async (p) => {
      const form = getDistribForm(p.id);
      const entries = branches.map((b) => ({ branchId: b.id, jumlah: parseInt(form[b.id] || "0") || 0 })).filter((e) => e.jumlah > 0);
      if (entries.length === 0) { pushNotif("Isi minimal 1 cabang dulu.", "warning"); return; }
      const totalKirim = entries.reduce((a, e) => a + e.jumlah, 0);
      if (totalKirim > p.jumlah) { pushNotif("Total distribusi (" + totalKirim + ") melebihi jumlah produksi (" + p.jumlah + " pcs).", "warning"); return; }
      setDistribBusy((b) => ({ ...b, [p.id]: true }));
      try {
        const hppPerPcsDistrib = Math.ceil(p.hppPerPcs || getMenuHPPBreakdown(menus.find((m) => m.id === p.menuId))?.hppSatuanPerPcs || 0);
        const rows = entries.map((e) => {
          const branch = branches.find((b) => b.id === e.branchId);
          return { id: uid(), date: p.date, ts: tsForDate(p.date), produksiId: p.id, menuId: p.menuId, menuNama: p.menuNama, totalProduksi: p.jumlah, branchId: e.branchId, branchName: branch?.name || e.branchId, jumlahKirim: e.jumlah, hppPerPcs: hppPerPcsDistrib, hppTotal: hppPerPcsDistrib * e.jumlah, status: "pending" };
        });
        const { error } = await sb.from("distribusiCK").insert(rows);
        if (error) throw error;
        await S.loadKey("distribusiCK");
        setDistribForm((f) => { const c = { ...f }; delete c[p.id]; return c; });
        pushNotif("Distribusi berhasil dikirim ke " + entries.length + " cabang!", "success");
      } catch(e) { pushNotif(e?.message || String(e), "warning"); }
      finally { setDistribBusy((b) => { const c = { ...b }; delete c[p.id]; return c; }); }
    };

    const distribPending = distribAll.filter((d) => d.status === "pending");
    const distribDiterima = distribAll.filter((d) => d.status === "diterima");
    const distribSelisih = distribAll.filter((d) => d.status === "diterima" && d.selisih !== 0);
    const distribFiltered = distribAll.filter((d) => d.date >= dr.from && d.date <= dr.to);

    return React.createElement("div", null,
      React.createElement("h3", { className: "section-title mt8" }, "Produksi & Distribusi Central Kitchen"),
      React.createElement("div", { className: "tabs mb8" },
        React.createElement("button", { className: "tab" + (subtab === "produksi" ? " active" : ""), onClick: () => setSubtab("produksi") }, "Produksi"),
        React.createElement("button", { className: "tab" + (subtab === "distribusi" ? " active" : ""), onClick: () => setSubtab("distribusi") },
          "Distribusi",
          distribPending.length > 0 ? React.createElement("span", { style: { marginLeft: 4, background: "#f59e0b", color: "#000", borderRadius: 8, padding: "1px 6px", fontSize: 10, fontWeight: 700 } }, distribPending.length) : null
        )
      ),
      React.createElement("div", { className: "filter-bar mb8" },
        React.createElement("input", { type: "date", className: "inp inp-sm", value: dr.from, onChange: (e) => setDr((r) => ({ ...r, from: e.target.value })) }),
        React.createElement("span", null, "s/d"),
        React.createElement("input", { type: "date", className: "inp inp-sm", value: dr.to, onChange: (e) => setDr((r) => ({ ...r, to: e.target.value })) })
      ),
      subtab === "produksi" && React.createElement("div", null,
        React.createElement("div", { className: "form-card mb8", style: { borderColor: "var(--accent)" } },
          React.createElement("h4", null, "Input Produksi Baru"),
          React.createElement("p", { className: "info-txt", style: { fontSize: 11 } }, "Tanggal otomatis mengikuti tanggal \"Dari\" pada filter di bawah: ", React.createElement("strong", null, dr.from)),
          React.createElement("div", { className: "field-group" },
            React.createElement("label", null, "Menu"),
            React.createElement("select", { className: "inp", value: inputForm.menuId, onChange: (e) => setInputForm((f) => ({ ...f, menuId: e.target.value })) },
              React.createElement("option", { value: "" }, "-- Pilih menu --"),
              menus.filter((m) => m.tipe !== "paket" && m.tipe !== "toping").map((m) => React.createElement("option", { key: m.id, value: m.id }, m.nama))
            )
          ),
          React.createElement("div", { className: "field-group" },
            React.createElement("label", null, "Jumlah (pcs)"),
            React.createElement("input", { type: "number", className: "inp", value: inputForm.jumlah, onChange: (e) => setInputForm((f) => ({ ...f, jumlah: e.target.value })) })
          ),
          React.createElement("div", { className: "field-group" },
            React.createElement("label", null, "Keterangan (opsional)"),
            React.createElement("input", { className: "inp", value: inputForm.keterangan, onChange: (e) => setInputForm((f) => ({ ...f, keterangan: e.target.value })) })
          ),
          React.createElement("button", { className: "btn-primary btn-full", disabled: inputBusy, onClick: inputProduksi }, inputBusy ? "Menyimpan..." : "Simpan Produksi")
        ),
        React.createElement("div", { className: "kpi-grid" },
          React.createElement("div", { className: "kpi-card kpi-omzet" }, React.createElement("div", { className: "kpi-label" }, "Total Produksi"), React.createElement("div", { className: "kpi-val" }, totalPcs, " pcs")),
          React.createElement("div", { className: "kpi-card kpi-modal" }, React.createElement("div", { className: "kpi-label" }, "Jenis Produk"), React.createElement("div", { className: "kpi-val" }, byMenuArr.length, " item")),
          React.createElement("div", { className: "kpi-card kpi-cab" }, React.createElement("div", { className: "kpi-label" }, "Entri Catatan"), React.createElement("div", { className: "kpi-val" }, filtered.length, "x")),
          React.createElement("div", { className: "kpi-card kpi-modal" }, React.createElement("div", { className: "kpi-label" }, "Total HPP Produksi"), React.createElement("div", { className: "kpi-val" }, fmtRp(totalHppProduksi)))
        ),
        byMenuArr.length > 0 && React.createElement("div", { className: "mt12" },
          React.createElement("h4", { className: "sub-title" }, "Rekap Per Produk"),
          React.createElement("div", { className: "tbl-wrap" },
            React.createElement("table", { className: "tbl" },
              React.createElement("thead", null, React.createElement("tr", null, React.createElement("th", null, "Produk"), React.createElement("th", null, "Total Pcs"), React.createElement("th", null, "%"))),
              React.createElement("tbody", null,
                byMenuArr.map(([nama, jml]) => React.createElement("tr", { key: nama },
                  React.createElement("td", null, React.createElement("strong", null, nama)),
                  React.createElement("td", { style: { color: "#22c55e", fontWeight: 700 } }, jml, " pcs"),
                  React.createElement("td", { style: { color: "#9a9690" } }, totalPcs > 0 ? Math.round(jml / totalPcs * 100) : 0, "%")
                )),
                React.createElement("tr", { style: { borderTop: "2px solid var(--border)", fontWeight: 700 } }, React.createElement("td", null, "TOTAL"), React.createElement("td", { style: { color: "#22c55e" } }, totalPcs, " pcs"), React.createElement("td", null, "100%"))
              )
            )
          )
        ),
        byDateArr.length > 0 && React.createElement("div", { className: "mt12" },
          React.createElement("h4", { className: "sub-title" }, "Riwayat Harian + Distribusi"),
          byDateArr.map(([date, rows]) => {
            const dayTotal = rows.reduce((a, p) => a + (p.jumlah || 0), 0);
            return React.createElement("div", { key: date, className: "accordion-card" },
              React.createElement("div", { className: "accordion-header", onClick: () => setOpenDay((o) => ({ ...o, [date]: !o[date] })) },
                React.createElement("div", { className: "accordion-title" },
                  React.createElement("span", { style: { fontWeight: 700 } }, date),
                  React.createElement("span", { className: "accordion-omzet" }, dayTotal, " pcs total")
                ),
                React.createElement("span", { className: "accordion-arrow" }, openDay[date] ? "▲" : "▼")
              ),
              openDay[date] && React.createElement("div", { className: "accordion-body" },
                rows.map((p) =>
                  React.createElement("div", { key: p.id, style: { marginBottom: 16, paddingBottom: 12, borderBottom: "1px solid var(--border)" } },
                    React.createElement("div", { className: "peng-row" },
                      React.createElement("div", { className: "peng-info" },
                        React.createElement("span", { className: "peng-ket" }, p.menuNama),
                        React.createElement("span", { className: "peng-ts" }, p.keterangan || "-", " | ", p.ts)
                      ),
                      React.createElement("div", { className: "peng-right" },
                        React.createElement("span", { style: { color: "#22c55e", fontWeight: 700 } }, p.jumlah, " pcs"),
                        React.createElement("button", { className: "btn-danger-sm", style: { marginLeft: 8 }, onClick: () => { if (!confirm("Hapus catatan ini?")) return; S.set("produksiCK", (S.get("produksiCK") || []).filter((x) => x.id !== p.id)); pushNotif("Dihapus.", "warning"); } }, "X")
                      )
                    ),
                    sudahDistrib(p.id)
                      ? React.createElement("div", { style: { marginTop: 6, padding: "6px 10px", background: "#0a2a0a", borderRadius: 6, fontSize: 12, color: "#22c55e" } },
                          "✅ Sudah didistribusikan — ",
                          distribAll.filter((d) => d.produksiId === p.id).map((d) => d.branchName + ": " + d.jumlahKirim + " pcs").join(", ")
                        )
                      : React.createElement("div", { style: { marginTop: 8, padding: "8px 10px", background: "#111", borderRadius: 8, border: "1px solid var(--border)" } },
                          React.createElement("div", { style: { fontSize: 12, color: "#f4a227", fontWeight: 700, marginBottom: 6 } }, "🚚 Distribusi ke Cabang"),
                          branches.length === 0 && React.createElement("p", { style: { fontSize: 12, color: "#9a9690" } }, "Belum ada cabang lapak."),
                          branches.map((b) =>
                            React.createElement("div", { key: b.id, className: "row-wrap", style: { marginBottom: 4, gap: 8, alignItems: "center" } },
                              React.createElement("span", { style: { fontSize: 12, minWidth: 90 } }, b.name),
                              React.createElement("input", { type: "number", className: "inp inp-sm", style: { width: 80 }, placeholder: "0 pcs", min: 0, value: getDistribForm(p.id)[b.id] || "", onChange: (e) => setDistribEntry(p.id, b.id, e.target.value) }),
                              React.createElement("span", { style: { fontSize: 11, color: "#9a9690" } }, "pcs")
                            )
                          ),
                          React.createElement("div", { style: { marginTop: 6, fontSize: 11, color: "#9a9690" } },
                            "Total diisi: ",
                            React.createElement("strong", { style: { color: Object.values(getDistribForm(p.id)).reduce((a, v) => a + (parseInt(v) || 0), 0) > p.jumlah ? "#ef4444" : "#22c55e" } },
                              Object.values(getDistribForm(p.id)).reduce((a, v) => a + (parseInt(v) || 0), 0), " / ", p.jumlah, " pcs"
                            )
                          ),
                          React.createElement("button", { className: "btn-primary btn-sm", style: { marginTop: 8 }, disabled: !!distribBusy[p.id], onClick: () => kirimDistribusi(p) }, distribBusy[p.id] ? "Mengirim..." : "🚚 Kirim Distribusi")
                        )
                  )
                )
              )
            );
          })
        ),
        filtered.length === 0 && React.createElement("p", { className: "empty-txt mt8" }, "Belum ada data produksi untuk rentang tanggal ini.")
      ),
      subtab === "distribusi" && React.createElement("div", null,
        React.createElement("div", { className: "kpi-grid" },
          React.createElement("div", { className: "kpi-card kpi-omzet" }, React.createElement("div", { className: "kpi-label" }, "Total Dikirim"), React.createElement("div", { className: "kpi-val" }, distribFiltered.reduce((a, d) => a + (d.jumlahKirim || 0), 0), " pcs")),
          React.createElement("div", { className: "kpi-card", style: { background: "#1a1500", border: "1px solid #f59e0b" } }, React.createElement("div", { className: "kpi-label" }, "Menunggu Konfirmasi"), React.createElement("div", { className: "kpi-val", style: { color: "#f59e0b" } }, distribPending.length, "x")),
          React.createElement("div", { className: "kpi-card kpi-profit" }, React.createElement("div", { className: "kpi-label" }, "Sudah Diterima"), React.createElement("div", { className: "kpi-val", style: { color: "#22c55e" } }, distribDiterima.length, "x")),
          distribSelisih.length > 0 && React.createElement("div", { className: "kpi-card kpi-peng" }, React.createElement("div", { className: "kpi-label" }, "Ada Selisih"), React.createElement("div", { className: "kpi-val", style: { color: "#ef4444" } }, distribSelisih.length, "x")),
          React.createElement("div", { className: "kpi-card kpi-modal" }, React.createElement("div", { className: "kpi-label" }, "Total HPP Distribusi"), React.createElement("div", { className: "kpi-val" }, fmtRp(distribFiltered.reduce((a, d) => a + (d.hppTotal || 0), 0))))
        ),
        React.createElement("div", { className: "tbl-wrap mt12" },
          React.createElement("table", { className: "tbl" },
            React.createElement("thead", null, React.createElement("tr", null,
              React.createElement("th", null, "Tanggal"), React.createElement("th", null, "Produk"), React.createElement("th", null, "Cabang"),
              React.createElement("th", null, "Kirim"), React.createElement("th", null, "Terima"), React.createElement("th", null, "Selisih"), React.createElement("th", null, "HPP"), React.createElement("th", null, "Status")
            )),
            React.createElement("tbody", null,
              distribFiltered.sort((a, b) => (b.date || "").localeCompare(a.date || "")).map((d) =>
                React.createElement("tr", { key: d.id },
                  React.createElement("td", { style: { fontSize: 11 } }, d.date),
                  React.createElement("td", null, React.createElement("strong", null, d.menuNama)),
                  React.createElement("td", null, d.branchName),
                  React.createElement("td", { style: { color: "#22c55e" } }, d.jumlahKirim, " pcs"),
                  React.createElement("td", null, d.jumlahTerima != null ? d.jumlahTerima + " pcs" : "-"),
                  React.createElement("td", { style: { color: d.selisih < 0 ? "#ef4444" : d.selisih > 0 ? "#f59e0b" : "#22c55e", fontWeight: 700 } },
                    d.status === "diterima" ? (d.selisih === 0 ? "Sesuai" : (d.selisih > 0 ? "+" : "") + d.selisih) : "-",
                    d.catatanSelisih ? React.createElement("div", { style: { fontSize: 10, color: "#9a9690", fontWeight: 400 } }, d.catatanSelisih) : null
                  ),
                  React.createElement("td", { style: { fontSize: 11, color: "#9a9690" } }, fmtRp(d.hppTotal || 0)),
                  React.createElement("td", null,
                    d.status === "pending"
                      ? React.createElement("span", { style: { color: "#f59e0b", fontSize: 11, fontWeight: 700 } }, "Pending")
                      : React.createElement("span", { style: { color: "#22c55e", fontSize: 11, fontWeight: 700 } }, "Diterima")
                  )
                )
              ),
              distribFiltered.length === 0 && React.createElement("tr", null, React.createElement("td", { colSpan: 8, style: { textAlign: "center", color: "#9a9690", padding: 16 } }, "Belum ada distribusi di rentang ini."))
            )
          )
        )
      )
    );
  }
  // ─── OwnerDashboard ────────────────────────────────────────────────────────
  function OwnerDashboard() {
    const [dr, setDr] = useState({ from: today(), to: today() });
    const [selBranch, setSelBranch] = useState("all");
    const branches = S.get("branches") || [];
    const txs = S.get("transactions") || [];
    const pL = S.get("pengeluaranLapak") || [];
    const pO = S.get("pengeluaranOwner") || [];
    const fTxs = txs.filter((t) => t.date >= dr.from && t.date <= dr.to && (selBranch === "all" || t.branchId === selBranch));
    const fPL = pL.filter((p) => p.date >= dr.from && p.date <= dr.to && (selBranch === "all" || p.branchId === selBranch));
    const fPO = pO.filter((p) => p.date >= dr.from && p.date <= dr.to);
    const distribAll = S.get("distribusiCK") || [];
    const fDistrib = distribAll.filter((d) => d.date >= dr.from && d.date <= dr.to && (selBranch === "all" || d.branchId === selBranch));
    const omzet = fTxs.reduce((a, t) => a + t.total, 0);
    const hppTerjual = fTxs.reduce((a, t) => a + t.totalHPP, 0);
    const hppDistribusi = fDistrib.reduce((a, d) => a + (d.hppTotal || 0), 0);
    const hppTidakLaku = Math.max(hppDistribusi - hppTerjual, 0);
    const peng = fPL.reduce((a, p) => a + p.jumlah, 0) + fPO.reduce((a, p) => a + p.jumlah, 0);
    const modal = hppDistribusi; // HPP yang dipakai untuk Laba Bersih = HPP seluruh barang yang didistribusikan
    const laba = omzet - hppDistribusi - peng;
    const danaPemList = S.get("danaPemeliharaan") || [];
    const saldoDanaPemeliharaan = danaPemList.reduce((a, d) => a + (d.tipe === "setor" ? d.jumlah : -d.jumlah), 0);
    const menusAll = S.get("menuVarian") || [];
    const branchStats = branches
      .filter((b) => selBranch === "all" || b.id === selBranch)
      .map((b) => {
        const bTx = fTxs.filter((t) => t.branchId === b.id);
        const bPL = fPL.filter((p) => p.branchId === b.id).reduce((a, p) => a + p.jumlah, 0);
        const bO = bTx.reduce((a, t) => a + t.total, 0);
        const bHppTerjual = bTx.reduce((a, t) => a + t.totalHPP, 0);
        const bDistrib = fDistrib.filter((d) => d.branchId === b.id);
        const bHppDistrib = bDistrib.reduce((a, d) => a + (d.hppTotal || 0), 0);
        const bHppTidakLaku = Math.max(bHppDistrib - bHppTerjual, 0);
        let boxTerjual = 0, pcsTerjual = 0;
        bTx.forEach((t) => (t.items || []).forEach((it) => {
          if (it.tipe === "toping") return;
          const md = menusAll.find((m) => m.id === it.menuId);
          if (md?.tipe === "paket") boxTerjual += it.qty; else pcsTerjual += it.qty;
        }));
        return { ...b, omzet: bO, modal: bHppDistrib, hppTerjual: bHppTerjual, hppTidakLaku: bHppTidakLaku, peng: bPL, laba: bO - bHppDistrib - bPL, txCount: bTx.length, boxTerjual, pcsTerjual };
      });
    const mc = {};
    fTxs.forEach((t) => t.items.forEach((it) => { mc[it.nama] = (mc[it.nama] || 0) + it.qty; }));
    const bs = Object.entries(mc).sort((a, b) => b[1] - a[1]).slice(0, 5);
    const chart7 = [];
    for (let i = 6; i >= 0; i--) {
      const d = new Date(); d.setDate(d.getDate() - i);
      const ds = d.toISOString().slice(0, 10);
      const dO = txs.filter((t) => t.date === ds).reduce((a, t) => a + t.total, 0);
      const dP = pL.filter((p) => p.date === ds).reduce((a, p) => a + p.jumlah, 0) + pO.filter((p) => p.date === ds).reduce((a, p) => a + p.jumlah, 0);
      const dM = txs.filter((t) => t.date === ds).reduce((a, t) => a + t.totalHPP, 0);
      chart7.push({ label: ds.slice(5), v1: dO, v2: dM + dP });
    }
    const branchChart = branchStats.map((b) => ({ label: b.name.slice(0, 8), v1: b.omzet, v2: b.laba }));

    return React.createElement("div", null,
      React.createElement("div", { className: "filter-bar mb8" },
        React.createElement("input", { type: "date", className: "inp inp-sm", value: dr.from, onChange: (e) => setDr((r) => ({ ...r, from: e.target.value })) }),
        React.createElement("span", null, "s/d"),
        React.createElement("input", { type: "date", className: "inp inp-sm", value: dr.to, onChange: (e) => setDr((r) => ({ ...r, to: e.target.value })) }),
        React.createElement("select", { className: "inp inp-sm", value: selBranch, onChange: (e) => setSelBranch(e.target.value) },
          React.createElement("option", { value: "all" }, "Semua Cabang"),
          branches.map((b) => React.createElement("option", { key: b.id, value: b.id }, b.name))
        )
      ),
      React.createElement("div", { className: "kpi-grid" },
        React.createElement("div", { className: "kpi-card kpi-omzet" }, React.createElement("div", { className: "kpi-label" }, "Omzet"), React.createElement("div", { className: "kpi-val" }, fmtRp(omzet))),
        React.createElement("div", { className: "kpi-card kpi-modal" }, React.createElement("div", { className: "kpi-label" }, "HPP Bahan"), React.createElement("div", { className: "kpi-val" }, fmtRp(modal))),
        React.createElement("div", { className: "kpi-card", style: { background: "#2a1a00", border: "1px solid #f59e0b" } }, React.createElement("div", { className: "kpi-label" }, "HPP Tidak Laku"), React.createElement("div", { className: "kpi-val", style: { color: "#f59e0b" } }, fmtRp(hppTidakLaku))),
        React.createElement("div", { className: "kpi-card kpi-peng" }, React.createElement("div", { className: "kpi-label" }, "Pengeluaran"), React.createElement("div", { className: "kpi-val" }, fmtRp(peng))),
        React.createElement("div", { className: "kpi-card kpi-profit" }, React.createElement("div", { className: "kpi-label" }, "Laba Bersih"), React.createElement("div", { className: "kpi-val", style: { color: laba >= 0 ? "#22c55e" : "#ef4444" } }, fmtRp(laba))),
        React.createElement("div", { className: "kpi-card kpi-tx" }, React.createElement("div", { className: "kpi-label" }, "Transaksi"), React.createElement("div", { className: "kpi-val" }, fTxs.length, "x")),
        React.createElement("div", { className: "kpi-card", style: { background: "#0a1a14", border: "1px solid #22c55e" } }, React.createElement("div", { className: "kpi-label" }, "Saldo Dana Cadangan"), React.createElement("div", { className: "kpi-val", style: { color: "#22c55e" } }, fmtRp(saldoDanaPemeliharaan))),
        React.createElement("div", { className: "kpi-card kpi-cab" }, React.createElement("div", { className: "kpi-label" }, "Cabang"), React.createElement("div", { className: "kpi-val" }, branches.filter((b) => b.type !== "central_kitchen").length))
      ),
      React.createElement("div", { className: "two-col mt12" },
        React.createElement("div", { className: "chart-box" },
          React.createElement("h3", { className: "section-title" }, "Omzet vs Pengeluaran - 7 Hari"),
          React.createElement(BarChart, { data: chart7, height: 100 }),
          React.createElement("div", { className: "chart-legend mt8" }, React.createElement("span", { className: "leg-dot leg-a" }), React.createElement("span", null, "Omzet"), React.createElement("span", { className: "leg-dot leg-b", style: { marginLeft: 12 } }), React.createElement("span", null, "HPP+Peng"))
        ),
        React.createElement("div", { className: "chart-box" },
          React.createElement("h3", { className: "section-title" }, "Omzet Per Cabang"),
          React.createElement(BarChart, { data: branchChart, height: 100 }),
          React.createElement("div", { className: "chart-legend mt8" }, React.createElement("span", { className: "leg-dot leg-a" }), React.createElement("span", null, "Omzet"), React.createElement("span", { className: "leg-dot leg-b", style: { marginLeft: 12 } }), React.createElement("span", null, "Laba"))
        )
      ),
      React.createElement("div", { className: "two-col mt12" },
        React.createElement("div", null,
          React.createElement("h3", { className: "section-title" }, "Performa Cabang"),
          branchStats.map((b) =>
            React.createElement("div", { key: b.id, className: "branch-stat-card" },
              React.createElement("div", { className: "branch-stat-name" }, b.name, " ", React.createElement("span", { className: "badge-type " + b.type }, b.type)),
              b.workers?.length > 0 && React.createElement("div", { className: "branch-workers" }, b.workers.join(", ")),
              React.createElement("div", { className: "branch-stat-row" }, React.createElement("span", null, "Omzet"), React.createElement("strong", null, fmtRp(b.omzet))),
              React.createElement("div", { className: "branch-stat-row" }, React.createElement("span", null, "HPP"), React.createElement("strong", null, fmtRp(b.modal))),
              React.createElement("div", { className: "branch-stat-row" }, React.createElement("span", { style: { color: "#f59e0b" } }, "HPP Tidak Laku"), React.createElement("strong", { style: { color: "#f59e0b" } }, fmtRp(b.hppTidakLaku))),
              React.createElement("div", { className: "branch-stat-row" }, React.createElement("span", null, "Terjual"), React.createElement("strong", null, b.boxTerjual, " box, ", b.pcsTerjual, " pcs satuan")),
              React.createElement("div", { className: "branch-stat-row" }, React.createElement("span", null, "Pengeluaran"), React.createElement("strong", { style: { color: "#ef4444" } }, fmtRp(b.peng))),
              React.createElement("div", { className: "branch-stat-row" }, React.createElement("span", null, "Laba"), React.createElement("strong", { style: { color: "#22c55e" } }, fmtRp(b.laba))),
              React.createElement("div", { className: "branch-stat-row" }, React.createElement("span", null, "Transaksi"), React.createElement("strong", null, b.txCount, "x"))
            )
          )
        ),
        React.createElement("div", null,
          React.createElement("h3", { className: "section-title" }, "Best Seller"),
          bs.length === 0 && React.createElement("p", { className: "empty-txt" }, "Belum ada data"),
          bs.map(([nama, qty], i) =>
            React.createElement("div", { key: i, className: "bestseller-row" },
              React.createElement("span", { className: "bs-rank" }, "#", i + 1),
              React.createElement("span", { className: "bs-nama" }, nama),
              React.createElement("span", { className: "bs-qty" }, qty, " pcs")
            )
          )
        )
      )
    );
  }

  // ─── REVISI #4: PengeluaranOwner — Filter Cabang ───────────────────────────
  function PengeluaranOwner({ pushNotif }) {
    const tick = useStoreTick();
    const [date, setDate] = useState(today());
    const getList = () => S.get("pengeluaranOwner") || [];
    const [list, setList] = useState(getList);
    // REVISI #4: tambah state cabang — null = biaya global/pusat
    const [selBranch, setSelBranch] = useState("");
    const [form, setForm] = useState({ keterangan: "", jumlah: "", kategori: "gaji_pekerja" });
    const refresh = () => setList(getList());
    const branchesData = S.get("branches") || [];

    const KATEGORI = [
      { value: "gaji_pekerja", label: "Gaji Pekerja Lapak" },
      { value: "gaji_kitchen", label: "Gaji Central Kitchen" },
      { value: "bahan_baku", label: "Bahan Baku" },
      { value: "operasional", label: "Operasional" },
      { value: "sewa", label: "Sewa Tempat" },
      { value: "lainnya", label: "Lainnya" }
    ];
    const CHIPS = {
      gaji_pekerja: ["Gaji Kasir Pagi", "Gaji Kasir Siang", "Bonus Pekerja"],
      gaji_kitchen: ["Gaji Chef", "Gaji Helper", "Lembur Kitchen"],
      bahan_baku: ["Restok Tepung", "Restok Kentang", "Restok Minyak", "Restok Gas"],
      operasional: ["Listrik", "Air", "Internet"],
      sewa: ["Sewa Lapak", "Sewa Dapur"],
      lainnya: ["Lain-lain"]
    };

    const tambah = () => {
      if (!form.keterangan || !form.jumlah) { alert("Isi semua kolom!"); return; }
      S.set("pengeluaranOwner", [...(S.get("pengeluaranOwner") || []), {
        id: uid(),
        date,
        ts: tsForDate(date),
        keterangan: form.keterangan,
        jumlah: parseFloat(form.jumlah),
        kategori: form.kategori,
        branchId: selBranch || null,
        branchName: selBranch ? (branchesData.find((b) => b.id === selBranch)?.name || "") : "Pusat (Global)"
      }]);
      setForm((f) => ({ ...f, keterangan: "", jumlah: "" }));
      refresh();
      pushNotif("Pengeluaran dicatat!", "success");
    };

    const hapus = (id) => { S.set("pengeluaranOwner", (S.get("pengeluaranOwner") || []).filter((x) => x.id !== id)); refresh(); };

    const filtered = list.filter((p) => p.date === date);
    const totalHari = filtered.reduce((a, p) => a + p.jumlah, 0);
    const byKat = KATEGORI.map((k) => ({ ...k, total: filtered.filter((p) => p.kategori === k.value).reduce((a, p) => a + p.jumlah, 0) })).filter((k) => k.total > 0);
    const lapakList = (S.get("pengeluaranLapak") || []).filter((p) => p.date === date);

    return React.createElement("div", null,
      React.createElement("div", { className: "filter-bar mb8" },
        React.createElement("input", { type: "date", className: "inp inp-sm", value: date, onChange: (e) => setDate(e.target.value) })
      ),
      React.createElement("div", { className: "form-card" },
        React.createElement("h4", null, "Tambah Pengeluaran Owner"),
        // REVISI #4: Dropdown pilih cabang
        React.createElement("div", { className: "field-group" },
          React.createElement("label", null, "Alokasi ke Cabang (Opsional)"),
          React.createElement("select", { className: "inp", value: selBranch, onChange: (e) => setSelBranch(e.target.value) },
            React.createElement("option", { value: "" }, "-- Pusat / Global (dibagi rata semua cabang) --"),
            branchesData.map((b) => React.createElement("option", { key: b.id, value: b.id }, b.name))
          )
        ),
        React.createElement("div", { className: "field-group" },
          React.createElement("label", null, "Kategori"),
          React.createElement("select", { className: "inp", value: form.kategori, onChange: (e) => setForm((f) => ({ ...f, kategori: e.target.value })) },
            KATEGORI.map((k) => React.createElement("option", { key: k.value, value: k.value }, k.label))
          )
        ),
        React.createElement("div", { className: "chips" },
          (CHIPS[form.kategori] || []).map((s) => React.createElement("button", { key: s, className: "chip", onClick: () => setForm((f) => ({ ...f, keterangan: s })) }, s))
        ),
        React.createElement("div", { className: "field-group" },
          React.createElement("label", null, "Keterangan"),
          React.createElement("input", { className: "inp", value: form.keterangan, onChange: (e) => setForm((f) => ({ ...f, keterangan: e.target.value })), placeholder: "Detail pengeluaran..." })
        ),
        React.createElement("div", { className: "field-group" },
          React.createElement("label", null, "Jumlah (Rp)"),
          React.createElement("input", { className: "inp", type: "number", value: form.jumlah, onChange: (e) => setForm((f) => ({ ...f, jumlah: e.target.value })) })
        ),
        React.createElement("button", { className: "btn-primary", onClick: tambah }, "+ Tambah")
      ),
      byKat.length > 0 && React.createElement("div", { className: "kpi-grid mt8" },
        byKat.map((k) =>
          React.createElement("div", { key: k.value, className: "kpi-card kpi-peng" },
            React.createElement("div", { className: "kpi-label" }, k.label),
            React.createElement("div", { className: "kpi-val" }, fmtRp(k.total))
          )
        )
      ),
      React.createElement("h3", { className: "section-title mt8" }, "Pengeluaran Owner - ", date),
      filtered.length === 0 && React.createElement("p", { className: "empty-txt" }, "Belum ada pengeluaran"),
      filtered.map((p) =>
        React.createElement("div", { key: p.id, className: "peng-row" },
          React.createElement("div", { className: "peng-info" },
            React.createElement("span", { className: "peng-ket" }, p.keterangan),
            React.createElement("span", { className: "peng-ts" },
              KATEGORI.find((k) => k.value === p.kategori)?.label,
              " | ",
              p.branchId ? (branchesData.find((b) => b.id === p.branchId)?.name || p.branchName) : "Pusat/Global",
              " - ", p.ts
            )
          ),
          React.createElement("div", { className: "peng-right" },
            React.createElement("span", { className: "peng-jml" }, fmtRp(p.jumlah)),
            React.createElement("button", { className: "btn-danger-sm", onClick: () => hapus(p.id) }, "X")
          )
        )
      ),
      filtered.length > 0 && React.createElement("div", { className: "peng-total" }, "Total: ", React.createElement("strong", null, fmtRp(totalHari))),
      React.createElement("h3", { className: "section-title mt12" }, "Pengeluaran Lapak dari Pekerja - ", date),
      lapakList.length === 0 && React.createElement("p", { className: "empty-txt" }, "Tidak ada pengeluaran lapak"),
      lapakList.map((p) =>
        React.createElement("div", { key: p.id, className: "peng-row" },
          React.createElement("div", { className: "peng-info" },
            React.createElement("span", { className: "peng-ket" }, p.keterangan),
            React.createElement("span", { className: "peng-ts" }, branchesData.find((b) => b.id === p.branchId)?.name || p.branchName, " - ", p.ts)
          ),
          React.createElement("div", { className: "peng-right" }, React.createElement("span", { className: "peng-jml" }, fmtRp(p.jumlah)))
        )
      ),
      lapakList.length > 0 && React.createElement("div", { className: "peng-total" }, "Total Lapak: ", React.createElement("strong", null, fmtRp(lapakList.reduce((a, p) => a + p.jumlah, 0))))
    );
  }

  // ─── OwnerSetoran ──────────────────────────────────────────────────────────
  function OwnerSetoran({ pushNotif }) {
    const [tab, setTab] = useState("harian");
    const [sH, setSH] = useState(() => S.get("setoranHarian") || []);
    const [sB, setSB] = useState(() => S.get("setoranBulanan") || []);
    const [bulan, setBulan] = useState(today().slice(0, 7));
    const branches = S.get("branches") || [];
    const investors = S.get("investors") || [];
    const refresh = () => { setSH(S.get("setoranHarian") || []); setSB(S.get("setoranBulanan") || []); };

    const konfirmasi = (id) => {
      S.set("setoranHarian", (S.get("setoranHarian") || []).map((s) => s.id === id ? { ...s, status: "selesai", konfirmasiTs: nowTs() } : s));
      refresh(); pushNotif("Setoran dikonfirmasi!", "success");
    };

    const kirimBulanan = (branchId, investorId) => {
      const txs = S.get("transactions") || [];
      const mTxs = txs.filter((t) => t.branchId === branchId && t.date.startsWith(bulan));
      const omzet = mTxs.reduce((a, t) => a + t.total, 0);
      const mDistrib = (S.get("distribusiCK") || []).filter((d) => d.branchId === branchId && d.date.startsWith(bulan));
      const modal = mDistrib.reduce((a, d) => a + (d.hppTotal || 0), 0);
      const pLapak = (S.get("pengeluaranLapak") || []).filter((p) => p.branchId === branchId && p.date.startsWith(bulan)).reduce((a, p) => a + p.jumlah, 0);
      const allPO = S.get("pengeluaranOwner") || [];
      const nBranch = Math.max((S.get("branches") || []).filter((b) => b.type !== "central_kitchen").length, 1);
      // REVISI #4: gabungkan biaya langsung ke cabang + bagian global
      const directPO = allPO.filter((p) => p.branchId === branchId && p.date.startsWith(bulan)).reduce((a, p) => a + p.jumlah, 0);
      const globalPO = allPO.filter((p) => !p.branchId && p.date.startsWith(bulan)).reduce((a, p) => a + p.jumlah, 0) / nBranch;
      const pOwner = directPO + globalPO;
      const laba = omzet - modal - pLapak - pOwner;
      const inv = investors.find((i) => i.id === investorId);
      const bagian = laba * ((inv?.persenBagi || 0) / 100);
      const all = S.get("setoranBulanan") || [];
      const ex = all.find((s) => s.branchId === branchId && s.bulan === bulan && s.investorId === investorId);
      const entry = { id: ex?.id || uid(), branchId, investorId, bulan, omzet, modal, pLapak, pOwner, laba, bagianInvestor: bagian, persen: inv?.persenBagi || 0, status: "menunggu", ts: nowTs() };
      S.set("setoranBulanan", ex ? all.map((s) => s.id === entry.id ? entry : s) : [...all, entry]);
      refresh(); pushNotif("Laporan bulanan dikirim!", "success");
    };

    const konfirmBulanan = (id) => {
      S.set("setoranBulanan", (S.get("setoranBulanan") || []).map((s) => s.id === id ? { ...s, status: "selesai", konfirmasiTs: nowTs(), confirmedBy: "owner" } : s));
      refresh(); pushNotif("Laporan bulanan dikonfirmasi!", "success");
    };

    return React.createElement("div", null,
      React.createElement("div", { className: "tabs" },
        React.createElement("button", { className: "tab" + (tab === "harian" ? " active" : ""), onClick: () => setTab("harian") }, "Harian (Pekerja ke Owner)"),
        React.createElement("button", { className: "tab" + (tab === "bulanan" ? " active" : ""), onClick: () => setTab("bulanan") }, "Bulanan (Owner ke Investor)")
      ),
      tab === "harian" && React.createElement("div", null,
        React.createElement("h3", { className: "section-title mt8" }, "Status Setoran Harian"),
        sH.length === 0 && React.createElement("p", { className: "empty-txt" }, "Belum ada setoran masuk"),
        [...sH].reverse().map((s) => {
          const b = branches.find((x) => x.id === s.branchId);
          return React.createElement("div", { key: s.id, className: "setoran-card" + (s.status === "menunggu" ? " setoran-card-menunggu" : s.status === "selesai" ? " setoran-card-selesai" : "") },
            React.createElement("div", { className: "setoran-card-header" },
              React.createElement("span", null, b?.name || s.branchName || s.branchId),
              React.createElement("span", { className: "setoran-date" }, s.date)
            ),
            React.createElement("div", { style: { fontSize: 13, color: "#9a9690" } }, "Omzet: ", fmtRp(s.omzet), " | Peng: ", fmtRp(s.pengeluaran || 0), " | Bersih: ", fmtRp((s.omzet || 0) - (s.pengeluaran || 0))),
            React.createElement("div", { className: "setoran-card-status" },
              s.status === "menunggu" && React.createElement(React.Fragment, null,
                React.createElement("span", { className: "badge-warn" }, "Menunggu"),
                React.createElement("button", { className: "btn-primary btn-sm", onClick: () => konfirmasi(s.id) }, "Konfirmasi")
              ),
              s.status === "selesai" && React.createElement("span", { className: "badge-ok" }, "Dikonfirmasi - ", s.konfirmasiTs)
            )
          );
        })
      ),
      tab === "bulanan" && React.createElement("div", null,
        React.createElement("div", { className: "field-group mt8" },
          React.createElement("label", null, "Pilih Bulan"),
          React.createElement("input", { type: "month", className: "inp inp-sm", value: bulan, onChange: (e) => setBulan(e.target.value) })
        ),
        React.createElement("h3", { className: "section-title mt8" }, "Cabang Investasi"),
        branches.filter((b) => b.type === "investasi").length === 0 && React.createElement("p", { className: "empty-txt" }, "Belum ada cabang investasi."),
        branches.filter((b) => b.type === "investasi").map((b) => {
          const inv = investors.find((i) => i.id === b.investorId);
          const ex = sB.find((s) => s.branchId === b.id && s.bulan === bulan && s.investorId === b.investorId);
          return React.createElement("div", { key: b.id, className: "setoran-card" },
            React.createElement("div", { className: "setoran-card-header" },
              React.createElement("span", null, b.name),
              React.createElement("span", null, "Investor: ", inv?.nama || "-", " (", inv?.persenBagi || 0, "%)")
            ),
            ex && React.createElement("div", { style: { fontSize: 13, color: "#9a9690" } }, "Omzet: ", fmtRp(ex.omzet), " | HPP: ", fmtRp(ex.modal), " | Laba: ", fmtRp(ex.laba), " | ", React.createElement("strong", { style: { color: "#f4a227" } }, "Bagian Investor: ", fmtRp(ex.bagianInvestor))),
            React.createElement("div", { className: "setoran-card-status" },
              !ex && React.createElement("button", { className: "btn-primary btn-sm", onClick: () => kirimBulanan(b.id, b.investorId) }, "Kirim Laporan"),
              ex?.status === "menunggu" && React.createElement(React.Fragment, null,
                React.createElement("span", { className: "badge-warn" }, "Menunggu Investor"),
                React.createElement("button", { className: "btn-secondary btn-sm", onClick: () => konfirmBulanan(ex.id) }, "Tandai Selesai (Manual)")
              ),
              ex?.status === "selesai" && React.createElement("span", { className: "badge-ok" }, "Dikonfirmasi", ex.confirmedBy ? ` (${ex.confirmedBy})` : "", " - ", ex.konfirmasiTs)
            )
          );
        })
      )
    );
  }

  // ─── REVISI #3: OwnerLaporan — Accordion per Cabang + tombol Edit ──────────
  function OwnerLaporan({ pushNotif }) {
    const tick = useStoreTick();
    const [date, setDate] = useState(today());
    const [selBranch, setSelBranch] = useState("all");
    // State accordion: { [branchId]: bool }
    const [openBranches, setOpenBranches] = useState({});
    const [editModal, setEditModal] = useState(null);
    const branches = S.get("branches") || [];

    const txsAll = (S.get("transactions") || []).filter((t) => t.date === date && (selBranch === "all" || t.branchId === selBranch));
    const pL = (S.get("pengeluaranLapak") || []).filter((p) => p.date === date && (selBranch === "all" || p.branchId === selBranch));
    const pO = (S.get("pengeluaranOwner") || []).filter((p) => p.date === date);
    const distribAllRpt = (S.get("distribusiCK") || []).filter((d) => d.date === date && (selBranch === "all" || d.branchId === selBranch));
    const editLogs = (S.get("editLog") || []).filter((l) => selBranch === "all" || l.branchId === selBranch);
    const omzet = txsAll.reduce((a, t) => a + t.total, 0);
    const hppTerjualRpt = txsAll.reduce((a, t) => a + t.totalHPP, 0);
    const modal = distribAllRpt.reduce((a, d) => a + (d.hppTotal || 0), 0);
    const hppTidakLakuRpt = Math.max(modal - hppTerjualRpt, 0);
    const tPL = pL.reduce((a, p) => a + p.jumlah, 0);
    const tPO = pO.reduce((a, p) => a + p.jumlah, 0);
    const laba = omzet - modal - tPL - tPO;

    const saveEdit = (txId, newItems, alasan) => {
      const txs = S.get("transactions") || [];
      const old = txs.find((x) => x.id === txId);
      const branchId = old?.branchId;
      const branchName = branches.find((b) => b.id === branchId)?.name || branchId;
      S.set("transactions", txs.map((t) => t.id === txId ? { ...t, items: newItems, total: newItems.reduce((a, x) => a + x.hargaJual * x.qty, 0), totalHPP: newItems.reduce((a, x) => a + x.hpp * x.qty, 0), edited: true } : t));
      const logs = S.get("editLog") || [];
      S.set("editLog", [...logs, { id: uid(), ts: tsForDate(date), txId, branchId, branchName, alasan, before: old?.items || [], after: newItems }]);
      setEditModal(null);
      pushNotif?.("Transaksi diperbarui.", "warning");
    };

    const toggleBranch = (id) => setOpenBranches((o) => ({ ...o, [id]: !o[id] }));

    // KPI per cabang untuk accordion
    const nBranchAll = Math.max(branches.filter((b) => b.type !== "central_kitchen").length, 1);
    const pOGlobalTotal = pO.filter((p) => !p.branchId).reduce((a, p) => a + p.jumlah, 0);
    const branchSummaries = branches
      .filter((b) => selBranch === "all" || b.id === selBranch)
      .map((b) => {
        const bTxs = txsAll.filter((t) => t.branchId === b.id);
        const bPL = pL.filter((p) => p.branchId === b.id).reduce((a, p) => a + p.jumlah, 0);
        const bPODirect = pO.filter((p) => p.branchId === b.id).reduce((a, p) => a + p.jumlah, 0);
        const bPOGlobal = pOGlobalTotal / nBranchAll;
        const bPO = bPODirect + bPOGlobal;
        const bO = bTxs.reduce((a, t) => a + t.total, 0);
        const bHppTerjual = bTxs.reduce((a, t) => a + t.totalHPP, 0);
        const bM = distribAllRpt.filter((d) => d.branchId === b.id).reduce((a, d) => a + (d.hppTotal || 0), 0);
        const bHppTidakLaku = Math.max(bM - bHppTerjual, 0);
        return { ...b, txs: bTxs, omzet: bO, modal: bM, hppTerjual: bHppTerjual, hppTidakLaku: bHppTidakLaku, pLapak: bPL, pOwner: bPO, laba: bO - bM - bPL - bPO };
      });

    return React.createElement("div", null,
      React.createElement("div", { className: "filter-bar mb8" },
        React.createElement("input", { type: "date", className: "inp inp-sm", value: date, onChange: (e) => setDate(e.target.value) }),
        React.createElement("select", { className: "inp inp-sm", value: selBranch, onChange: (e) => setSelBranch(e.target.value) },
          React.createElement("option", { value: "all" }, "Semua Cabang"),
          branches.map((b) => React.createElement("option", { key: b.id, value: b.id }, b.name))
        )
      ),
      // KPI ringkasan total
      React.createElement("div", { className: "kpi-grid" },
        React.createElement("div", { className: "kpi-card kpi-omzet" }, React.createElement("div", { className: "kpi-label" }, "Omzet"), React.createElement("div", { className: "kpi-val" }, fmtRp(omzet))),
        React.createElement("div", { className: "kpi-card kpi-modal" }, React.createElement("div", { className: "kpi-label" }, "HPP Bahan"), React.createElement("div", { className: "kpi-val" }, fmtRp(modal))),
        React.createElement("div", { className: "kpi-card", style: { background: "#2a1a00", border: "1px solid #f59e0b" } }, React.createElement("div", { className: "kpi-label" }, "HPP Tidak Laku"), React.createElement("div", { className: "kpi-val", style: { color: "#f59e0b" } }, fmtRp(hppTidakLakuRpt))),
        React.createElement("div", { className: "kpi-card kpi-peng" }, React.createElement("div", { className: "kpi-label" }, "Peng. Lapak"), React.createElement("div", { className: "kpi-val" }, fmtRp(tPL))),
        React.createElement("div", { className: "kpi-card kpi-peng" }, React.createElement("div", { className: "kpi-label" }, "Peng. Operasional"), React.createElement("div", { className: "kpi-val" }, fmtRp(tPO))),
        React.createElement("div", { className: "kpi-card kpi-profit" }, React.createElement("div", { className: "kpi-label" }, "Laba Bersih"), React.createElement("div", { className: "kpi-val" }, fmtRp(laba)))
      ),
      // Log edit
      editLogs.length > 0 && React.createElement("div", { className: "mt8" },
        React.createElement("h3", { className: "section-title" }, "Log Perubahan Kasir"),
        editLogs.map((log) =>
          React.createElement("div", { key: log.id, className: "log-card" },
            React.createElement("div", { className: "log-header" },
              React.createElement("span", null, log.ts),
              React.createElement("span", { className: "badge-warn" }, "Diedit"),
              React.createElement("span", { className: "badge-branch" }, log.branchName || log.branchId)
            ),
            React.createElement("div", { className: "log-detail" }, "STRUK-", log.txId.slice(0, 6).toUpperCase(), ' - Alasan: "', log.alasan, '"'),
            React.createElement("div", { style: { fontSize: 12, color: "#9a9690", marginTop: 4 } },
              "Sebelum: ", (log.before || []).map((x) => x.nama + " x" + x.qty).join(", "),
              " - Sesudah: ", (log.after || []).map((x) => x.nama + " x" + x.qty).join(", ")
            )
          )
        )
      ),
      // REVISI #3: Accordion per Cabang
      React.createElement("h3", { className: "section-title mt8" }, "Detail Transaksi per Cabang"),
      branchSummaries.length === 0 && React.createElement("p", { className: "empty-txt" }, "Belum ada data"),
      branchSummaries.map((b) =>
        React.createElement("div", { key: b.id, className: "accordion-card" },
          // Header accordion — klik untuk buka/tutup
          React.createElement("div", { className: "accordion-header", onClick: () => toggleBranch(b.id) },
            React.createElement("div", { className: "accordion-title" },
              React.createElement("span", { className: "badge-branch" }, b.name),
              React.createElement("span", { className: "accordion-omzet" }, "Omzet: ", fmtRp(b.omzet), " | HPP Tidak Laku: ", fmtRp(b.hppTidakLaku), " | Laba: ", fmtRp(b.laba))
            ),
            React.createElement("span", { className: "accordion-arrow" }, openBranches[b.id] ? "▲" : "▼")
          ),
          // Body accordion — hanya tampil kalau open
          openBranches[b.id] && React.createElement("div", { className: "accordion-body" },
            b.txs.length === 0 && React.createElement("p", { className: "empty-txt" }, "Belum ada transaksi"),
            b.txs.map((tx) =>
              React.createElement("div", { key: tx.id, className: "tx-card" + (tx.edited ? " tx-edited" : "") },
                React.createElement("div", { className: "tx-header" },
                  React.createElement("span", { className: "tx-id" }, "STRUK-", tx.id.slice(0, 6).toUpperCase()),
                  React.createElement("span", { className: "tx-ts" }, fmtTxTs(tx)),
                  tx.edited && React.createElement("span", { className: "badge-warn" }, "Diedit")
                ),
                tx.items.map((it, i) => React.createElement("div", { key: i, className: "tx-item" }, it.nama, " x", it.qty, " = ", fmtRp(it.hargaJual * it.qty), " (HPP: ", fmtRp(it.hpp * it.qty), ")")),
                React.createElement("div", { className: "tx-total" }, "Omzet: ", fmtRp(tx.total), " | HPP: ", fmtRp(tx.totalHPP), " | Laba: ", fmtRp(tx.total - tx.totalHPP)),
                // Tombol Edit ada di sini (OwnerLaporan)
                React.createElement("button", { className: "btn-edit-sm mt4", onClick: () => setEditModal(tx) }, "Edit Transaksi")
              )
            ),
            // Pengeluaran lapak cabang ini
            pL.filter((p) => p.branchId === b.id).length > 0 && React.createElement("div", { className: "mt8" },
              React.createElement("h4", { className: "sub-title" }, "Pengeluaran Lapak"),
              pL.filter((p) => p.branchId === b.id).map((p) =>
                React.createElement("div", { key: p.id, className: "peng-row" },
                  React.createElement("div", { className: "peng-info" },
                    React.createElement("span", { className: "peng-ket" }, p.keterangan),
                    React.createElement("span", { className: "peng-ts" }, p.ts)
                  ),
                  React.createElement("div", { className: "peng-right" },
                    React.createElement("span", { className: "peng-jml" }, fmtRp(p.jumlah)),
                    // Owner bisa edit pengeluaran lapak
                    React.createElement("button", { className: "btn-edit-sm", style: { marginLeft: 6 }, onClick: () => {
                      const ket = prompt("Edit keterangan:", p.keterangan);
                      if (ket === null) return;
                      const jml = parseFloat(prompt("Edit jumlah (Rp):", p.jumlah));
                      if (isNaN(jml)) return;
                      S.set("pengeluaranLapak", (S.get("pengeluaranLapak") || []).map((x) => x.id === p.id ? { ...x, keterangan: ket, jumlah: jml } : x));
                    } }, "Edit")
                  )
                )
              )
            ),
            // Pengeluaran Operasional owner yang dialokasikan ke cabang ini
            (() => {
              const pOCabang = pO.filter((p) => p.branchId === b.id);
              const pOGlobal = pO.filter((p) => !p.branchId);
              const nBranch = nBranchAll;
              if (pOCabang.length === 0 && pOGlobal.length === 0) return null;
              return React.createElement("div", { className: "mt8" },
                React.createElement("h4", { className: "sub-title" }, "Pengeluaran Operasional"),
                pOCabang.map((p) =>
                  React.createElement("div", { key: p.id, className: "peng-row" },
                    React.createElement("div", { className: "peng-info" },
                      React.createElement("span", { className: "peng-ket" }, p.keterangan),
                      React.createElement("span", { className: "peng-ts" },
                        (["gaji_pekerja","gaji_kitchen","bahan_baku","operasional","sewa","lainnya"].find(k => k === p.kategori) ? {gaji_pekerja:"Gaji Pekerja",gaji_kitchen:"Gaji Kitchen",bahan_baku:"Bahan Baku",operasional:"Operasional",sewa:"Sewa",lainnya:"Lainnya"}[p.kategori] : p.kategori || "-"),
                        " | Langsung - ", p.ts
                      )
                    ),
                    React.createElement("div", { className: "peng-right" }, React.createElement("span", { className: "peng-jml" }, fmtRp(p.jumlah)))
                  )
                ),
                pOGlobal.map((p) =>
                  React.createElement("div", { key: p.id, className: "peng-row" },
                    React.createElement("div", { className: "peng-info" },
                      React.createElement("span", { className: "peng-ket" }, p.keterangan),
                      React.createElement("span", { className: "peng-ts" },
                        (["gaji_pekerja","gaji_kitchen","bahan_baku","operasional","sewa","lainnya"].find(k => k === p.kategori) ? {gaji_pekerja:"Gaji Pekerja",gaji_kitchen:"Gaji Kitchen",bahan_baku:"Bahan Baku",operasional:"Operasional",sewa:"Sewa",lainnya:"Lainnya"}[p.kategori] : p.kategori || "-"),
                        " | Global ÷ ", nBranch, " cabang = ", fmtRp(p.jumlah / nBranch), " - ", p.ts
                      )
                    ),
                    React.createElement("div", { className: "peng-right" }, React.createElement("span", { className: "peng-jml" }, fmtRp(p.jumlah / nBranch)))
                  )
                )
              );
            })()
          )
        )
      ),
      // ── Section Produksi Central Kitchen ──
      (() => {
        const ckBranch = branches.find((b) => b.type === "central_kitchen");
        if (!ckBranch) return null;
        const ckList = (S.get("produksiCK") || []).filter((p) => p.date === date);
        const totalCK = ckList.reduce((a, p) => a + (p.jumlah || 0), 0);
        // Rekap per menu
        const byMenu = {};
        ckList.forEach((p) => {
          if (!byMenu[p.menuId]) byMenu[p.menuId] = { nama: p.menuNama, total: 0 };
          byMenu[p.menuId].total += p.jumlah || 0;
        });
        return React.createElement("div", { className: "mt12" },
          React.createElement("h3", { className: "section-title" }, "\uD83C\uDF73 Produksi Central Kitchen"),
          React.createElement("div", { className: "kpi-grid" },
            React.createElement("div", { className: "kpi-card kpi-omzet" },
              React.createElement("div", { className: "kpi-label" }, "Total Produksi"),
              React.createElement("div", { className: "kpi-val", style: { color: "#22c55e" } }, totalCK, " pcs")
            ),
            React.createElement("div", { className: "kpi-card kpi-modal" },
              React.createElement("div", { className: "kpi-label" }, "Jenis Produk"),
              React.createElement("div", { className: "kpi-val" }, Object.keys(byMenu).length, " item")
            )
          ),
          ckList.length === 0
            ? React.createElement("p", { className: "empty-txt mt8" }, "Belum ada catatan produksi CK hari ini.")
            : React.createElement("div", null,
                React.createElement("div", { className: "tbl-wrap mt8" },
                  React.createElement("table", { className: "tbl" },
                    React.createElement("thead", null,
                      React.createElement("tr", null,
                        React.createElement("th", null, "Produk"),
                        React.createElement("th", null, "Jumlah"),
                        React.createElement("th", null, "Keterangan"),
                        React.createElement("th", null, "Jam")
                      )
                    ),
                    React.createElement("tbody", null,
                      ckList.map((p) =>
                        React.createElement("tr", { key: p.id },
                          React.createElement("td", null, React.createElement("strong", null, p.menuNama)),
                          React.createElement("td", { style: { color: "#22c55e", fontWeight: 700 } }, p.jumlah, " pcs"),
                          React.createElement("td", { style: { color: "#9a9690" } }, p.keterangan || "-"),
                          React.createElement("td", { style: { color: "#9a9690", fontSize: 12 } }, p.ts || "-")
                        )
                      ),
                      React.createElement("tr", { style: { borderTop: "2px solid var(--border)", fontWeight: 700 } },
                        React.createElement("td", null, "TOTAL"),
                        React.createElement("td", { style: { color: "#22c55e" } }, totalCK, " pcs"),
                        React.createElement("td", { colSpan: 2 })
                      )
                    )
                  )
                ),
                Object.keys(byMenu).length > 1 && React.createElement("div", { className: "mt8" },
                  React.createElement("h4", { className: "sub-title" }, "Rekap per Produk"),
                  Object.values(byMenu).map((m, i) =>
                    React.createElement("div", { key: i, className: "branch-stat-row" },
                      React.createElement("span", null, m.nama),
                      React.createElement("strong", { style: { color: "#22c55e" } }, m.total, " pcs")
                    )
                  )
                )
            )
        );
      })(),
      // Modal edit transaksi (Owner Laporan)
      editModal && React.createElement(EditTxModal, { tx: editModal, onClose: () => setEditModal(null), onSave: saveEdit })
    );
  }

  // ─── OwnerAbsensi ──────────────────────────────────────────────────────────
  function OwnerAbsensi({ pushNotif }) {
    const tick = useStoreTick();
    const [month, setMonth] = useState(today().slice(0, 7));
    const [selBranch, setSelBranch] = useState("all");
    const branches = S.get("branches") || [];
    const profiles = S.get("profiles") || [];
    const absensi = S.get("absensi") || [];
    const snaps = S.get("absensiBulanan") || [];
    const pengeluaranOwner = S.get("pengeluaranOwner") || [];
    const workers = profiles.filter((p) => p.role === "worker").filter((p) => selBranch === "all" || p.branchId === selBranch);
    // Gaji = gajiHarian × jumlah hari hadir bulan ini
    // gajiHarian diinput owner di tab Akun per pekerja
    const gajiMap = useMemo(() => {
      const map = {};
      workers.forEach((w) => {
        const gajiHarian = parseFloat(w.gajiHarian || 0) || 0;
        if (!gajiHarian) return;
        const hadirBulan = absensi.filter((a) => a.user_id === w.user_id && String(a.date || "").startsWith(month) && a.checkin_ts).length;
        map[w.user_id] = { gajiHarian, hadir: hadirBulan, total: gajiHarian * hadirBulan };
      });
      return map;
    }, [workers, absensi, month]);
    const totalGaji = Object.values(gajiMap).reduce((a, v) => a + v.total, 0);

    const calcUserMonth = useCallback((userId) => {
      const rows2 = absensi.filter((a) => a.user_id === userId && String(a.date || "").startsWith(month));
      let hadir = 0, menit = 0;
      for (const r of rows2) {
        if (r.checkin_ts) hadir += 1;
        if (r.checkin_ts && r.checkout_ts) {
          const a = Date.parse(r.checkin_ts), b = Date.parse(r.checkout_ts);
          if (!isNaN(a) && !isNaN(b) && b > a) menit += Math.floor((b - a) / 60000);
        }
      }
      rows2.sort((a, b) => (a.date || "").localeCompare(b.date || ""));
      return { hadir, menit, history: rows2 };
    }, [absensi, month]);

    const rows = useMemo(() => workers.map((w) => {
      const snap = snaps.find((s) => s.user_id === w.user_id && s.bulan === month && s.locked) || null;
      const calc = calcUserMonth(w.user_id);
      return { w, locked: !!snap, hadir: snap ? snap.total_hadir : calc.hadir, menit: snap ? snap.total_menit : calc.menit, history: calc.history };
    }), [workers, snaps, month, calcUserMonth, tick]);

    const lockMonth = async () => {
      try {
        if (rows.length === 0) { pushNotif("Tidak ada pekerja untuk direkap.", "warning"); return; }
        const entries = rows.map((r) => ({ user_id: r.w.user_id, branchId: r.w.branchId, bulan: month, total_hadir: r.hadir, total_menit: r.menit, locked: true, generated_at: nowIso() }));
        const { error } = await sb.from("absensiBulanan").upsert(entries, { onConflict: "user_id,bulan" });
        if (error) throw error;
        await S.loadKey("absensiBulanan");
        pushNotif("Rekap absensi bulan ini dikunci.", "success");
      } catch (e) { pushNotif(e?.message || String(e), "warning"); }
    };

    const unlockMonth = async () => {
      try {
        for (const r of rows) {
          const { error } = await sb.from("absensiBulanan").update({ locked: false, generated_at: nowIso() }).eq("user_id", r.w.user_id).eq("bulan", month);
          if (error) throw error;
        }
        await S.loadKey("absensiBulanan");
        pushNotif("Kunci rekap dibuka.", "success");
      } catch (e) { pushNotif(e?.message || String(e), "warning"); }
    };

    const totalHadir = rows.reduce((a, r) => a + (r.hadir || 0), 0);
    const totalMenit = rows.reduce((a, r) => a + (r.menit || 0), 0);
    const getJam = (ts) => ts ? (ts.split(" ")[1] || ts.split("T")[1]?.slice(0, 5) || ts) : "-";

    return React.createElement("div", null,
      React.createElement("div", { className: "filter-bar mb8" },
        React.createElement("input", { type: "month", className: "inp inp-sm", value: month, onChange: (e) => setMonth(e.target.value) }),
        React.createElement("select", { className: "inp inp-sm", value: selBranch, onChange: (e) => setSelBranch(e.target.value) },
          React.createElement("option", { value: "all" }, "Semua Cabang"),
          branches.map((b) => React.createElement("option", { key: b.id, value: b.id }, b.name))
        ),
        React.createElement("button", { className: "btn-primary btn-sm", onClick: lockMonth }, "Kunci Rekap"),
        React.createElement("button", { className: "btn-secondary btn-sm", onClick: unlockMonth }, "Buka Kunci")
      ),
      React.createElement("div", { className: "kpi-grid" },
        React.createElement("div", { className: "kpi-card kpi-omzet" }, React.createElement("div", { className: "kpi-label" }, "Total Hadir"), React.createElement("div", { className: "kpi-val" }, totalHadir, " hari")),
        React.createElement("div", { className: "kpi-card kpi-profit" }, React.createElement("div", { className: "kpi-label" }, "Total Jam"), React.createElement("div", { className: "kpi-val" }, Math.round(totalMenit / 60 * 10) / 10, " jam")),
        React.createElement("div", { className: "kpi-card kpi-cab" }, React.createElement("div", { className: "kpi-label" }, "Pekerja"), React.createElement("div", { className: "kpi-val" }, rows.length)),
        React.createElement("div", { className: "kpi-card kpi-peng" }, React.createElement("div", { className: "kpi-label" }, "Total Gaji Bulan Ini"), React.createElement("div", { className: "kpi-val" }, fmtRp(totalGaji)))
      ),
      React.createElement("p", { className: "info-txt mt4" }, "Gaji dihitung otomatis: Gaji Harian × Hadir. Set gaji harian per pekerja di tab Seting → Akun."),
      React.createElement("h3", { className: "section-title mt12" }, "Detail Absensi Bulanan"),
      rows.length === 0 && React.createElement("p", { className: "empty-txt" }, "Belum ada pekerja / data profiles belum termuat."),
      rows.map((r) =>
        React.createElement("div", { key: r.w.user_id, className: "peng-row", style: { flexDirection: "column", alignItems: "flex-start" } },
          React.createElement("div", { className: "peng-info", style: { width: "100%", marginBottom: 4 } },
            React.createElement("span", { className: "peng-ket" }, r.w.display_name || r.w.email || r.w.user_id.slice(0, 6)),
            React.createElement("span", { className: "peng-ts" }, "Cabang: ", branches.find((b) => b.id === r.w.branchId)?.name || "-", " | Hadir: ", r.hadir, " hari | Jam: ", Math.round(r.menit / 60 * 10) / 10, " | Gaji: ", fmtRp(gajiMap[r.w.user_id]?.total || 0), gajiMap[r.w.user_id]?.gajiHarian ? ` (${fmtRp(gajiMap[r.w.user_id].gajiHarian)}/hari × ${gajiMap[r.w.user_id].hadir} hari)` : " (gaji belum diset)", r.locked ? " | (Terkunci)" : "")
          ),
          React.createElement("div", { style: { paddingLeft: 8, borderLeft: "2px solid #555", width: "100%", marginTop: 4 } },
            r.history.length === 0
              ? React.createElement("div", { style: { fontSize: 12, color: "#888" } }, "Belum ada riwayat absen")
              : r.history.map((h) =>
                  React.createElement("div", { key: h.id, style: { fontSize: 12, color: "#ccc", marginBottom: 2 } },
                    "• ", h.date, " : Masuk ", getJam(h.checkin_ts), " - Keluar ", h.checkout_ts ? getJam(h.checkout_ts) : "Belum"
                  )
                )
          )
        )
      )
    );
  }


  // ─── REVISI #1: EditMenuModal baru — resep pakai jumlahPakai (berapa pcs dari bahan ini) ──
  function EditMenuModal({ menu, bahan, isPaket, menuSatuanList, onSave, onClose }) {
    const [m, setM] = useState({
      ...menu,
      tipe: isPaket ? "paket" : "satuan",
      resepBahanPokok: menu.resepBahanPokok || [],
      resepToping: menu.resepToping || [],
      imageUrl: menu.imageUrl || "",
      imagePath: menu.imagePath || "",
      boxCost: parseFloat(menu.boxCost || 0) || 0,
      isiBox: parseInt(menu.isiBox || 3) || 3
    });
    // Bahan pokok baru: pilih bahan + jumlahPakai (berapa pcs adonan dasar yang dipakai, default 1)
    const [nRB, setNRB] = useState({ bahanId: bahan[0]?.id || "", jumlahPakai: "1" });
    // Toping menu: nama, hargaBeli, kapasitas (per menu varian)
    const [nRT, setNRT] = useState({ nama: "", hargaBeli: "", kapasitas: "" });
    const [uploading, setUploading] = useState(false);

    const addRB = () => {
      if (!nRB.bahanId || !nRB.jumlahPakai) return;
      setM((p) => ({ ...p, resepBahanPokok: [...p.resepBahanPokok, { bahanId: nRB.bahanId, jumlahPakai: parseFloat(nRB.jumlahPakai) || 1 }] }));
      setNRB((x) => ({ ...x, jumlahPakai: "1" }));
    };
    const delRB = (i) => setM((p) => ({ ...p, resepBahanPokok: p.resepBahanPokok.filter((_, idx) => idx !== i) }));

    const addRT = () => {
      if (!nRT.nama || !nRT.hargaBeli || !nRT.kapasitas) return;
      setM((p) => ({ ...p, resepToping: [...p.resepToping, { nama: nRT.nama, hargaBeli: parseFloat(nRT.hargaBeli), kapasitas: parseInt(nRT.kapasitas) }] }));
      setNRT({ nama: "", hargaBeli: "", kapasitas: "" });
    };
    const delRT = (i) => setM((p) => ({ ...p, resepToping: p.resepToping.filter((_, idx) => idx !== i) }));

    const doUploadImage = async (e) => {
      const file = e.target.files?.[0]; if (!file) return;
      try {
        setUploading(true);
        const uploaded = await uploadAsset(file, "menu");
        setM((prev) => ({ ...prev, imageUrl: uploaded.url || prev.imageUrl, imagePath: uploaded.path || prev.imagePath }));
      } catch (err) { alert(err?.message || String(err)); } finally { setUploading(false); }
    };

    const info = getMenuHPPBreakdown(m);

    return React.createElement(Modal, { title: (isPaket ? "Box - " : "Menu - ") + (m.id ? "Edit" : "Tambah"), onClose },
      // Nama
      React.createElement("div", { className: "field-group" },
        React.createElement("label", null, "Nama"),
        React.createElement("input", { className: "inp", value: m.nama, onChange: (e) => setM((x) => ({ ...x, nama: e.target.value })) })
      ),
      // Upload gambar
      React.createElement("div", { className: "field-group" },
        React.createElement("label", null, "Gambar Menu"),
        m.imageUrl && React.createElement("img", { src: m.imageUrl, alt: m.nama || "Menu", className: "brand-preview", style: { width: 120, height: 120, margin: "0 auto" } }),
        React.createElement("input", { className: "inp", type: "file", accept: "image/*", onChange: doUploadImage, disabled: uploading }),
        React.createElement("input", { className: "inp", value: m.imageUrl || "", onChange: (e) => setM((x) => ({ ...x, imageUrl: e.target.value })), placeholder: "Atau tempel URL gambar" })
      ),
      // Isi box (paket)
      isPaket && React.createElement("div", { className: "field-group" },
        React.createElement("label", null, "Isi Box (pcs)"),
        React.createElement("input", { className: "inp", type: "number", value: m.isiBox || 3, onChange: (e) => setM((x) => ({ ...x, isiBox: parseInt(e.target.value) || 3 })) })
      ),
      // Menu satuan dasar (untuk pengurangan stok otomatis)
      isPaket && React.createElement("div", { className: "field-group" },
        React.createElement("label", null, "Menu Satuan Dasar (untuk potong stok)"),
        React.createElement("select", { className: "inp", value: m.baseMenuId || "", onChange: (e) => setM((x) => ({ ...x, baseMenuId: e.target.value || null })) },
          React.createElement("option", { value: "" }, "— Pilih menu satuan —"),
          (menuSatuanList || []).map((ms) => React.createElement("option", { key: ms.id, value: ms.id }, ms.nama))
        ),
        React.createElement("p", { className: "info-txt", style: { fontSize: 11 } }, "Saat paket ini terjual, stok menu satuan dasar akan otomatis berkurang sebanyak Isi Box.")
      ),
      // Harga kardus (paket)
      isPaket && React.createElement("div", { className: "field-group" },
        React.createElement("label", null, "Harga Kardus/Box (Rp)"),
        React.createElement("input", { className: "inp", type: "number", value: m.boxCost || 0, onChange: (e) => setM((x) => ({ ...x, boxCost: parseFloat(e.target.value) || 0 })) })
      ),
      // Harga jual
      React.createElement("div", { className: "field-group" },
        React.createElement("label", null, "Harga Jual (Rp)"),
        React.createElement("input", { className: "inp", type: "number", value: m.hargaJual, onChange: (e) => setM((x) => ({ ...x, hargaJual: parseFloat(e.target.value) || 0 })) })
      ),
      // Resep Bahan Pokok
      React.createElement("h4", { className: "sub-title" }, "Bahan Baku Adonan", isPaket ? " (untuk " + (m.isiBox || 1) + " pcs box)" : ""),
      React.createElement("p", { className: "info-txt" }, "Pilih bahan dari daftar bahan baku. HPP dihitung otomatis dari harga beli ÷ kapasitas."),
      m.resepBahanPokok.map((r, i) => {
        const b = bahan.find((x) => x.id === r.bahanId);
        const hpp = b ? getBahanHppPerPcs(b) * (r.jumlahPakai || 1) : 0;
        return React.createElement("div", { key: i, className: "resep-row" },
          b?.nama || "?",
          " × ", r.jumlahPakai || 1, " pcs",
          " → HPP: ", fmtRp(Math.ceil(hpp)),
          " ",
          React.createElement("button", { className: "btn-danger-sm", onClick: () => delRB(i) }, "X")
        );
      }),
      React.createElement("div", { className: "add-row" },
        React.createElement("select", { className: "inp inp-sm", value: nRB.bahanId, onChange: (e) => setNRB((x) => ({ ...x, bahanId: e.target.value })) },
          bahan.length === 0 && React.createElement("option", null, "-- Tambah bahan dulu --"),
          bahan.map((b) => React.createElement("option", { key: b.id, value: b.id }, b.nama, " (HPP: ", fmtRp(Math.ceil(getBahanHppPerPcs(b))), "/pcs)"))
        ),
        React.createElement("input", { className: "inp inp-sm", type: "number", placeholder: "Pcs pakai", value: nRB.jumlahPakai, onChange: (e) => setNRB((x) => ({ ...x, jumlahPakai: e.target.value })), style: { width: 80 } }),
        React.createElement("button", { className: "btn-primary btn-sm", onClick: addRB }, "+")
      ),
      // Toping/Varian Menu
      React.createElement("h4", { className: "sub-title" }, "Toping / Varian Menu"),
      React.createElement("p", { className: "info-txt" }, "Tambah toping spesifik untuk menu ini. Isi harga beli toping + kapasitas (jadi berapa pcs)."),
      m.resepToping.map((t, i) => {
        const hpp = Math.ceil((t.hargaBeli || 0) / Math.max(t.kapasitas || 1, 1));
        return React.createElement("div", { key: i, className: "resep-row" },
          t.nama, " → HPP: ", fmtRp(hpp), "/pcs",
          " ",
          React.createElement("button", { className: "btn-danger-sm", onClick: () => delRT(i) }, "X")
        );
      }),
      React.createElement("div", { className: "add-row" },
        React.createElement("input", { className: "inp inp-sm", placeholder: "Nama toping/varian", value: nRT.nama, onChange: (e) => setNRT((x) => ({ ...x, nama: e.target.value })) }),
        React.createElement("input", { className: "inp inp-sm", type: "number", placeholder: "Harga Beli Total", value: nRT.hargaBeli, onChange: (e) => setNRT((x) => ({ ...x, hargaBeli: e.target.value })), style: { width: 120 } }),
        React.createElement("input", { className: "inp inp-sm", type: "number", placeholder: "Jadi (pcs)", value: nRT.kapasitas, onChange: (e) => setNRT((x) => ({ ...x, kapasitas: e.target.value })), style: { width: 80 } }),
        React.createElement("button", { className: "btn-primary btn-sm", onClick: addRT }, "+")
      ),
      // Preview HPP
      React.createElement("div", { className: "hpp-preview" },
        "HPP Adonan/pcs: ", React.createElement("strong", null, fmtRp(info.hppAdonanPerPcs)),
        " | HPP Toping/pcs: ", React.createElement("strong", null, fmtRp(info.hppTopingPerPcs)),
        " | HPP Produk/pcs: ", React.createElement("strong", { style: { color: "#f4a227" } }, fmtRp(info.hppSatuanPerPcs)),
        isPaket
          ? React.createElement(React.Fragment, null, " | HPP Paket: ", React.createElement("strong", { style: { color: "#f4a227" } }, fmtRp(info.hppPaket)), " | Margin Paket: ", React.createElement("strong", { style: { color: "#22c55e" } }, fmtRp(info.marginPaket)))
          : React.createElement(React.Fragment, null, " | Omzet Kotor/pcs: ", React.createElement("strong", { style: { color: "#22c55e" } }, fmtRp(info.omzetKotorPerPcs)))
      ),
      React.createElement("div", { className: "row-wrap mt8" },
        React.createElement("button", { className: "btn-secondary", onClick: onClose }, "Batal"),
        React.createElement("button", { className: "btn-primary", onClick: () => { if (!m.nama) { alert("Isi nama!"); return; } onSave(m); }, disabled: uploading }, uploading ? "Upload..." : "Simpan")
      )
    );
  }

  // ─── REVISI #1: SettingHPP baru — input Harga Beli + Kapasitas ─────────────
  function SettingHPP({ pushNotif }) {
    const tick = useStoreTick();
    const [sub, setSub] = useState("bahan");
    const [bahan, setBahan] = useState(() => S.get("bahanPokok") || []);
    const [menus, setMenus] = useState(() => (S.get("menuVarian") || []).filter((m) => m.tipe !== "paket"));
    const [topings, setTopings] = useState(() => S.get("topingTambahan") || []);
    const [editMenu, setEditMenu] = useState(null);
    // Bahan Pokok baru: nama, hargaBeli (total), kapasitas (yield pcs), satuanBeli (opsional keterangan)
    const [nB, setNB] = useState({ nama: "", hargaBeli: "", kapasitas: "", satuanBeli: "" });
    // Toping tambahan tetap: nama, hargaJual, plus untuk HPP: hargaBeli, kapasitas
    const [nT, setNT] = useState({ nama: "", hargaBeli: "", kapasitas: "", hargaJual: "" });

    useEffect(() => {
      setBahan(S.get("bahanPokok") || []);
      setMenus((S.get("menuVarian") || []).filter((m) => m.tipe !== "paket"));
      setTopings(S.get("topingTambahan") || []);
    }, [tick]);

    const saveB = () => {
      if (!nB.nama || !nB.hargaBeli || !nB.kapasitas) { alert("Isi nama, harga beli, dan kapasitas!"); return; }
      const hppPerPcs = parseFloat(nB.hargaBeli) / Math.max(parseInt(nB.kapasitas), 1);
      const u = [...bahan, { id: uid(), nama: nB.nama, hargaBeli: parseFloat(nB.hargaBeli), kapasitas: parseInt(nB.kapasitas), satuanBeli: nB.satuanBeli || "", satuan: nB.satuanBeli || "pcs", hppPerPcs: Math.ceil(hppPerPcs) }];
      S.set("bahanPokok", u); setBahan(u);
      setNB({ nama: "", hargaBeli: "", kapasitas: "", satuanBeli: "" });
      pushNotif("Bahan ditambah!", "success");
    };

    const delB = (id) => { const u = bahan.filter((x) => x.id !== id); S.set("bahanPokok", u); setBahan(u); };

    const saveMenu = (m) => {
      const all = S.get("menuVarian") || [];
      const u = all.find((x) => x.id === m.id) ? all.map((x) => x.id === m.id ? m : x) : [...all, { ...m, id: uid() }];
      S.set("menuVarian", u); setMenus(u.filter((x) => x.tipe !== "paket"));
      setEditMenu(null); pushNotif("Menu disimpan!", "success");
    };

    const delMenu = (id) => { const u = (S.get("menuVarian") || []).filter((x) => x.id !== id); S.set("menuVarian", u); setMenus(u.filter((x) => x.tipe !== "paket")); };

    const saveT = () => {
      if (!nT.nama || !nT.hargaBeli || !nT.kapasitas || !nT.hargaJual) { alert("Isi semua kolom toping!"); return; }
      const u = [...topings, { id: uid(), nama: nT.nama, hargaBeli: parseFloat(nT.hargaBeli), kapasitas: parseInt(nT.kapasitas), hargaJual: parseFloat(nT.hargaJual) }];
      S.set("topingTambahan", u); setTopings(u);
      setNT({ nama: "", hargaBeli: "", kapasitas: "", hargaJual: "" });
      pushNotif("Toping ditambah!", "success");
    };

    const delT = (id) => { const u = topings.filter((x) => x.id !== id); S.set("topingTambahan", u); setTopings(u); };

    const SUB_TABS = ["bahan", "menu", "toping"];
    const SUB_LABEL = { bahan: "Bahan Pokok", menu: "Varian Menu", toping: "Toping Tambahan" };

    return React.createElement("div", null,
      React.createElement("div", { className: "tabs tabs-sm" },
        SUB_TABS.map((t) => React.createElement("button", { key: t, className: "tab" + (sub === t ? " active" : ""), onClick: () => setSub(t) }, SUB_LABEL[t]))
      ),

      // ── Sub: Bahan Pokok ──
      sub === "bahan" && React.createElement("div", null,
        React.createElement("h3", { className: "section-title mt8" }, "Bahan Baku Pokok"),
        React.createElement("p", { className: "info-txt" }, "Masukkan total harga beli dan hasil jadi (kapasitas/yield). Contoh: Tepung 1kg Rp 10.000 → 10 pcs adonan → HPP/pcs = Rp 1.000."),
        React.createElement("table", { className: "tbl mt8" },
          React.createElement("thead", null,
            React.createElement("tr", null,
              React.createElement("th", null, "Nama Bahan"),
              React.createElement("th", null, "Harga Beli Total"),
              React.createElement("th", null, "Kapasitas (pcs)"),
              React.createElement("th", null, "HPP/pcs"),
              React.createElement("th", null)
            )
          ),
          React.createElement("tbody", null,
            bahan.map((b) =>
              React.createElement("tr", { key: b.id },
                React.createElement("td", null, b.nama, b.satuanBeli ? React.createElement("span", { style: { fontSize: 11, color: "#888", marginLeft: 4 } }, "(", b.satuanBeli, ")") : null),
                React.createElement("td", null, fmtRp(b.hargaBeli)),
                React.createElement("td", null, b.kapasitas, " pcs"),
                React.createElement("td", { style: { color: "#f4a227", fontWeight: 700 } }, fmtRp(Math.ceil(b.hargaBeli / Math.max(b.kapasitas, 1)))),
                React.createElement("td", null, React.createElement("button", { className: "btn-danger-sm", onClick: () => delB(b.id) }, "Hapus"))
              )
            )
          )
        ),
        React.createElement("div", { className: "form-card mt8" },
          React.createElement("h4", null, "Tambah Bahan Baku"),
          React.createElement("div", { className: "field-group" },
            React.createElement("label", null, "Nama Bahan & Ukuran Beli"),
            React.createElement("input", { className: "inp", placeholder: "Contoh: Tepung 1kg, Kentang 2kg", value: nB.nama, onChange: (e) => setNB((x) => ({ ...x, nama: e.target.value })) })
          ),
          React.createElement("div", { className: "field-group" },
            React.createElement("label", null, "Harga Beli Total (Rp)"),
            React.createElement("input", { className: "inp", type: "number", placeholder: "Contoh: 10000", value: nB.hargaBeli, onChange: (e) => setNB((x) => ({ ...x, hargaBeli: e.target.value })) })
          ),
          React.createElement("div", { className: "field-group" },
            React.createElement("label", null, "Kapasitas / Yield (bisa jadi berapa pcs)"),
            React.createElement("input", { className: "inp", type: "number", placeholder: "Contoh: 10", value: nB.kapasitas, onChange: (e) => setNB((x) => ({ ...x, kapasitas: e.target.value })) })
          ),
          nB.hargaBeli && nB.kapasitas && React.createElement("div", { className: "hpp-preview" },
            "HPP per pcs = ", React.createElement("strong", null, fmtRp(Math.ceil(parseFloat(nB.hargaBeli || 0) / Math.max(parseInt(nB.kapasitas || 1), 1))))
          ),
          React.createElement("button", { className: "btn-primary mt8", onClick: saveB }, "+ Tambah Bahan")
        )
      ),

      // ── Sub: Varian Menu ──
      sub === "menu" && React.createElement("div", null,
        React.createElement("h3", { className: "section-title mt8" }, "Varian Menu Satuan"),
        menus.map((m) => {
          const info = getMenuHPPBreakdown(m);
          return React.createElement("div", { key: m.id, className: "menu-setting-card" },
            React.createElement("div", { className: "menu-setting-row" },
              React.createElement("strong", null, m.nama),
              React.createElement("span", null, "Jual: ", fmtRp(m.hargaJual))
            ),
            React.createElement("div", { style: { fontSize: 12, color: "#9a9690", marginTop: 2 } },
              "HPP Adonan: ", fmtRp(info.hppAdonanPerPcs),
              " + Toping: ", fmtRp(info.hppTopingPerPcs),
              " = HPP: ", React.createElement("strong", { style: { color: "#f4a227" } }, fmtRp(info.hppSatuanPerPcs)),
              " | Omzet Kotor: ", React.createElement("strong", { style: { color: "#22c55e" } }, fmtRp(info.omzetKotorPerPcs))
            ),
            React.createElement("div", { className: "menu-setting-actions" },
              React.createElement("button", { className: "btn-secondary btn-sm", onClick: () => setEditMenu({ ...m }) }, "Edit"),
              React.createElement("button", { className: "btn-danger-sm", onClick: () => delMenu(m.id) }, "Hapus")
            )
          );
        }),
        React.createElement("button", { className: "btn-primary mt8", onClick: () => setEditMenu({ id: null, nama: "", tipe: "satuan", hargaJual: "", resepBahanPokok: [], resepToping: [] }) }, "+ Tambah Menu"),
        editMenu && React.createElement(EditMenuModal, { menu: editMenu, bahan, onSave: saveMenu, onClose: () => setEditMenu(null) })
      ),

      // ── Sub: Toping Tambahan ──
      sub === "toping" && React.createElement("div", null,
        React.createElement("h3", { className: "section-title mt8" }, "Toping Tambahan"),
        React.createElement("p", { className: "info-txt" }, "HPP toping dihitung dari: Harga Beli Toping ÷ Kapasitas (jadi berapa pcs). Contoh: Glaze 1kg Rp 40.000 → 10 pcs → HPP = Rp 4.000/pcs."),
        React.createElement("table", { className: "tbl mt8" },
          React.createElement("thead", null,
            React.createElement("tr", null,
              React.createElement("th", null, "Nama"),
              React.createElement("th", null, "Harga Beli"),
              React.createElement("th", null, "Kapasitas"),
              React.createElement("th", null, "HPP/pcs"),
              React.createElement("th", null, "Harga Jual"),
              React.createElement("th", null)
            )
          ),
          React.createElement("tbody", null,
            topings.map((t) =>
              React.createElement("tr", { key: t.id },
                React.createElement("td", null, t.nama),
                React.createElement("td", null, fmtRp(t.hargaBeli)),
                React.createElement("td", null, t.kapasitas, " pcs"),
                React.createElement("td", { style: { color: "#f4a227", fontWeight: 700 } }, fmtRp(Math.ceil((t.hargaBeli || 0) / Math.max(t.kapasitas || 1, 1)))),
                React.createElement("td", null, fmtRp(t.hargaJual)),
                React.createElement("td", null, React.createElement("button", { className: "btn-danger-sm", onClick: () => delT(t.id) }, "Hapus"))
              )
            )
          )
        ),
        React.createElement("div", { className: "form-card mt8" },
          React.createElement("h4", null, "Tambah Toping"),
          React.createElement("div", { className: "field-group" },
            React.createElement("label", null, "Nama Toping & Ukuran Beli"),
            React.createElement("input", { className: "inp inp-sm", placeholder: "Contoh: Glaze 1kg", value: nT.nama, onChange: (e) => setNT((x) => ({ ...x, nama: e.target.value })) })
          ),
          React.createElement("div", { className: "field-group" },
            React.createElement("label", null, "Harga Beli Total (Rp)"),
            React.createElement("input", { className: "inp inp-sm", type: "number", placeholder: "Contoh: 40000", value: nT.hargaBeli, onChange: (e) => setNT((x) => ({ ...x, hargaBeli: e.target.value })) })
          ),
          React.createElement("div", { className: "field-group" },
            React.createElement("label", null, "Kapasitas (jadi berapa pcs)"),
            React.createElement("input", { className: "inp inp-sm", type: "number", placeholder: "Contoh: 10", value: nT.kapasitas, onChange: (e) => setNT((x) => ({ ...x, kapasitas: e.target.value })) })
          ),
          React.createElement("div", { className: "field-group" },
            React.createElement("label", null, "Harga Jual per Toping (Rp)"),
            React.createElement("input", { className: "inp inp-sm", type: "number", placeholder: "Contoh: 10000", value: nT.hargaJual, onChange: (e) => setNT((x) => ({ ...x, hargaJual: e.target.value })) })
          ),
          nT.hargaBeli && nT.kapasitas && React.createElement("div", { className: "hpp-preview" },
            "HPP/pcs = ", React.createElement("strong", null, fmtRp(Math.ceil(parseFloat(nT.hargaBeli || 0) / Math.max(parseInt(nT.kapasitas || 1), 1)))),
            " | Omzet Kotor = ", React.createElement("strong", { style: { color: "#22c55e" } }, fmtRp(Math.max(parseFloat(nT.hargaJual || 0) - Math.ceil(parseFloat(nT.hargaBeli || 0) / Math.max(parseInt(nT.kapasitas || 1), 1)), 0)))
          ),
          React.createElement("button", { className: "btn-primary mt8", onClick: saveT }, "+ Tambah Toping")
        )
      )
    );
  }

  // ─── SettingPaket ──────────────────────────────────────────────────────────
  function SettingPaket({ pushNotif }) {
    const tick = useStoreTick();
    const [pakets, setPakets] = useState(() => (S.get("menuVarian") || []).filter((m) => m.tipe === "paket"));
    const [bahan] = useState(() => S.get("bahanPokok") || []);
    const [editP, setEditP] = useState(null);

    useEffect(() => { setPakets((S.get("menuVarian") || []).filter((m) => m.tipe === "paket")); }, [tick]);

    const save = (m) => {
      const all = S.get("menuVarian") || [];
      const u = all.find((x) => x.id === m.id) ? all.map((x) => x.id === m.id ? m : x) : [...all, { ...m, id: uid() }];
      S.set("menuVarian", u); setPakets(u.filter((x) => x.tipe === "paket"));
      setEditP(null); pushNotif("Box disimpan!", "success");
    };

    const del = (id) => { const u = (S.get("menuVarian") || []).filter((x) => x.id !== id); S.set("menuVarian", u); setPakets(u.filter((x) => x.tipe === "paket")); };

    return React.createElement("div", null,
      React.createElement("h3", { className: "section-title mt8" }, "Box / Paket"),
      React.createElement("p", { className: "info-txt" }, "HPP Paket = (HPP menu satuan × isi box) + harga kardus."),
      pakets.map((p) => {
        const info = getMenuHPPBreakdown(p);
        return React.createElement("div", { key: p.id, className: "menu-setting-card" },
          React.createElement("div", { className: "menu-setting-row" },
            React.createElement("strong", null, p.nama),
            React.createElement("span", { className: "badge-paket" }, "Isi ", p.isiBox, " pcs"),
            React.createElement("span", null, "Jual: ", fmtRp(p.hargaJual))
          ),
          React.createElement("div", { style: { fontSize: 12, color: "#9a9690", marginTop: 2 } },
            "HPP satuan: ", fmtRp(info.hppSatuanPerPcs),
            " × ", p.isiBox, " + Kardus: ", fmtRp(info.boxCost),
            " = HPP Paket: ", React.createElement("strong", { style: { color: "#f4a227" } }, fmtRp(info.hppPaket)),
            " | Margin: ", React.createElement("strong", { style: { color: "#22c55e" } }, fmtRp(info.marginPaket))
          ),
          React.createElement("div", { className: "menu-setting-actions" },
            React.createElement("button", { className: "btn-secondary btn-sm", onClick: () => setEditP({ ...p }) }, "Edit"),
            React.createElement("button", { className: "btn-danger-sm", onClick: () => del(p.id) }, "Hapus")
          )
        );
      }),
      React.createElement("button", { className: "btn-primary mt8", onClick: () => setEditP({ id: null, nama: "", tipe: "paket", isiBox: 3, hargaJual: "", boxCost: 0, resepBahanPokok: [], resepToping: [] }) }, "+ Tambah Box"),
      editP && React.createElement(EditMenuModal, { menu: editP, bahan, isPaket: true, menuSatuanList: (S.get("menuVarian") || []).filter((m) => m.tipe !== "paket"), onSave: save, onClose: () => setEditP(null) })
    );
  }

  // ─── SettingCabang ─────────────────────────────────────────────────────────
  function SettingCabang({ pushNotif }) {
    const [branches, setBranches] = useState(() => S.get("branches") || []);
    const investors = S.get("investors") || [];
    const [form, setForm] = useState({ nama: "", type: "mandiri", investorId: "", workers: "" });
    const [editB, setEditB] = useState(null);

    const add = () => {
      if (!form.nama) return;
      const wArr = form.workers.split(",").map((s) => s.trim()).filter(Boolean);
      const u = [...branches, { id: uid(), name: form.nama, type: form.type, investorId: form.type === "investasi" ? form.investorId : null, workers: wArr }];
      S.set("branches", u); setBranches(u);
      setForm({ nama: "", type: "mandiri", investorId: "", workers: "" });
      pushNotif("Cabang ditambahkan!", "success");
    };

    const saveEdit = () => {
      const wArr = editB.ws.split(",").map((s) => s.trim()).filter(Boolean);
      const u = branches.map((b) => b.id === editB.id ? { ...b, name: editB.name, workers: wArr, type: editB.type, investorId: editB.type === "investasi" ? editB.investorId : null } : b);
      S.set("branches", u); setBranches(u); setEditB(null);
      pushNotif("Cabang diperbarui!", "success");
    };

    const del = (id) => { const u = branches.filter((x) => x.id !== id); S.set("branches", u); setBranches(u); };

    return React.createElement("div", null,
      React.createElement("h3", { className: "section-title mt8" }, "Kelola Cabang"),
      branches.map((b) =>
        React.createElement("div", { key: b.id, className: "branch-row" },
          React.createElement("div", { style: { flex: 1 } },
            React.createElement("strong", null, b.name), " ",
            React.createElement("span", { className: "badge-type " + b.type }, b.type),
            b.workers?.length > 0 && React.createElement("div", { className: "branch-workers" }, b.workers.join(", ")),
            b.type === "investasi" && React.createElement("div", { style: { fontSize: 12, color: "#9a9690" } }, "Investor: ", investors.find((i) => i.id === b.investorId)?.nama || "-")
          ),
          React.createElement("button", { className: "btn-secondary btn-sm", onClick: () => setEditB({ ...b, ws: (b.workers || []).join(", ") }) }, "Edit"),
          React.createElement("button", { className: "btn-danger-sm", onClick: () => del(b.id) }, "Hapus")
        )
      ),
      editB && React.createElement(Modal, { title: "Edit Cabang", onClose: () => setEditB(null) },
        React.createElement("div", { className: "field-group" }, React.createElement("label", null, "Nama Cabang"), React.createElement("input", { className: "inp", value: editB.name, onChange: (e) => setEditB((x) => ({ ...x, name: e.target.value })) })),
        React.createElement("div", { className: "field-group" }, React.createElement("label", null, "Nama Pekerja (pisah koma)"), React.createElement("input", { className: "inp", value: editB.ws, onChange: (e) => setEditB((x) => ({ ...x, ws: e.target.value })), placeholder: "Andi, Sari, Budi" })),
        React.createElement("div", { className: "field-group" },
          React.createElement("label", null, "Tipe"),
          React.createElement("div", { className: "role-tabs" },
            React.createElement("button", { className: "role-tab" + (editB.type === "mandiri" ? " active" : ""), onClick: () => setEditB((x) => ({ ...x, type: "mandiri" })) }, "Mandiri"),
            React.createElement("button", { className: "role-tab" + (editB.type === "investasi" ? " active" : ""), onClick: () => setEditB((x) => ({ ...x, type: "investasi" })) }, "Investasi"),
            React.createElement("button", { className: "role-tab" + (editB.type === "central_kitchen" ? " active" : ""), onClick: () => setEditB((x) => ({ ...x, type: "central_kitchen" })) }, "Central Kitchen"),
            React.createElement("button", { className: "role-tab" + (editB.type === "central_kitchen" ? " active" : ""), onClick: () => setEditB((x) => ({ ...x, type: "central_kitchen" })) }, "Central Kitchen")
          )
        ),
        editB.type === "investasi" && React.createElement("div", { className: "field-group" },
          React.createElement("label", null, "Investor"),
          React.createElement("select", { className: "inp", value: editB.investorId, onChange: (e) => setEditB((x) => ({ ...x, investorId: e.target.value })) },
            React.createElement("option", { value: "" }, "-- Pilih --"),
            investors.map((i) => React.createElement("option", { key: i.id, value: i.id }, i.nama, " (", i.persenBagi, "%)"))
          )
        ),
        React.createElement("div", { className: "row-wrap mt8" },
          React.createElement("button", { className: "btn-secondary", onClick: () => setEditB(null) }, "Batal"),
          React.createElement("button", { className: "btn-primary", onClick: saveEdit }, "Simpan")
        )
      ),
      React.createElement("div", { className: "form-card mt12" },
        React.createElement("h4", null, "Tambah Cabang Baru"),
        React.createElement("div", { className: "field-group" }, React.createElement("label", null, "Nama Cabang"), React.createElement("input", { className: "inp", value: form.nama, onChange: (e) => setForm((x) => ({ ...x, nama: e.target.value })) })),
        React.createElement("div", { className: "field-group" }, React.createElement("label", null, "Nama Pekerja (pisah koma)"), React.createElement("input", { className: "inp", value: form.workers, onChange: (e) => setForm((x) => ({ ...x, workers: e.target.value })), placeholder: "Andi, Sari" })),
        React.createElement("div", { className: "field-group" },
          React.createElement("label", null, "Tipe"),
          React.createElement("div", { className: "role-tabs" },
            React.createElement("button", { className: "role-tab" + (form.type === "mandiri" ? " active" : ""), onClick: () => setForm((x) => ({ ...x, type: "mandiri" })) }, "Mandiri"),
            React.createElement("button", { className: "role-tab" + (form.type === "investasi" ? " active" : ""), onClick: () => setForm((x) => ({ ...x, type: "investasi" })) }, "Investasi"),
            React.createElement("button", { className: "role-tab" + (form.type === "central_kitchen" ? " active" : ""), onClick: () => setForm((x) => ({ ...x, type: "central_kitchen" })) }, "Central Kitchen"),
            React.createElement("button", { className: "role-tab" + (form.type === "central_kitchen" ? " active" : ""), onClick: () => setForm((x) => ({ ...x, type: "central_kitchen" })) }, "Central Kitchen")
          )
        ),
        form.type === "investasi" && React.createElement("div", { className: "field-group" },
          React.createElement("label", null, "Investor"),
          React.createElement("select", { className: "inp", value: form.investorId, onChange: (e) => setForm((x) => ({ ...x, investorId: e.target.value })) },
            React.createElement("option", { value: "" }, "-- Pilih --"),
            investors.map((i) => React.createElement("option", { key: i.id, value: i.id }, i.nama, " (", i.persenBagi, "%)"))
          )
        ),
        React.createElement("button", { className: "btn-primary", onClick: add }, "+ Tambah Cabang")
      )
    );
  }

  // ─── SettingAkun ───────────────────────────────────────────────────────────
  function SettingAkun({ pushNotif }) {
    const tick = useStoreTick();
    const branches = S.get("branches") || [];
    const investors = S.get("investors") || [];
    const profiles = S.get("profiles") || [];
    const [invites, setInvites] = useState([]);
    const [loading, setLoading] = useState(false);
    const [showPassword, setShowPassword] = useState(false);
    const [deletedIds, setDeletedIds] = useState([]);
    const [jadwalLibur, setJadwalLibur] = useState(() => S.get("jadwalLibur") || {});
    const [form, setForm] = useState({ role: "worker", email: "", password: "", displayName: "", branchId: branches[0]?.id || "", investorId: investors[0]?.id || "", gajiHarian: "" });

    useEffect(() => {
      if (!form.branchId && branches[0]?.id) setForm((f) => ({ ...f, branchId: branches[0].id }));
      if (!form.investorId && investors[0]?.id) setForm((f) => ({ ...f, investorId: investors[0].id }));
    }, [tick]);

    const refreshInvites = async () => {
      setLoading(true);
      try { const { data, error } = await sb.from("invites").select("*").order("created_at", { ascending: false }); if (error) throw error; setInvites(data || []); }
      catch (e) { pushNotif(e?.message || String(e), "warning"); } finally { setLoading(false); }
    };

    useEffect(() => { refreshInvites(); }, []);

    const createInvite = async () => {
      const emailInput = String(form.email || "").trim();
      const pwdInput = String(form.password || "").trim();
      if (!emailInput) { alert("Username / Email tidak boleh kosong."); return; }
      if (!pwdInput || pwdInput.length < 6) { alert("Password wajib diisi (minimal 6 karakter)."); return; }
      if (form.role === "worker" && !form.branchId) { alert("Pilih cabang untuk pekerja."); return; }
      if (form.role === "investor" && !form.investorId) { alert("Pilih investor."); return; }
      const emailFormat = emailInput.includes("@") ? emailInput : `${emailInput.toLowerCase()}@donatboss.local`;
      try {
        const { data: sessData } = await sb.auth.getSession();
        const token = sessData?.session?.access_token;
        if (!token) throw new Error("Owner harus login dulu.");
        const resp = await fetch("/api/create-user", {
          method: "POST",
          headers: { "Content-Type": "application/json", "Authorization": `Bearer ${token}` },
          body: JSON.stringify({ emailOrUsername: emailFormat, password: pwdInput, role: form.role, displayName: String(form.displayName || "").trim() || null, branchId: form.role === "worker" ? form.branchId : null, investorId: form.role === "investor" ? form.investorId : null, gajiHarian: form.role === "worker" && form.gajiHarian ? parseFloat(form.gajiHarian) : null })
        });
        const text = await resp.text(); let json = null; try { json = JSON.parse(text); } catch {}
        if (!resp.ok) throw new Error(json?.error || text || "Gagal membuat user.");
        pushNotif("Akun Berhasil Dibuat!", "success");
        setForm((f) => ({ ...f, email: "", password: "", displayName: "" }));
        refreshInvites();
      } catch (e) { pushNotif(e?.message || String(e), "warning"); }
    };

    const deleteInvite = async (id) => {
      if (!confirm("Hapus invite ini?")) return;
      try { const { error } = await sb.from("invites").delete().eq("id", id); if (error) throw error; refreshInvites(); }
      catch (e) { pushNotif(e?.message || String(e), "warning"); }
    };

    const updateLibur = (userId, hari) => {
      const baru = { ...jadwalLibur, [userId]: hari };
      S.set("jadwalLibur", baru); setJadwalLibur(baru);
      pushNotif("Jadwal libur diset ke " + (hari || "Tidak Ada"), "success");
    };

    const filteredProfiles = profiles.filter((p) => !deletedIds.includes(p.user_id));

    return React.createElement("div", null,
      React.createElement("h3", { className: "section-title mt8" }, "Akun & Invite"),
      React.createElement("p", { className: "info-txt" }, "Kelola akun pekerja dan investor langsung dari aplikasi."),
      React.createElement("div", { className: "form-card mt8" },
        React.createElement("h4", null, "Buat Akun Baru"),
        React.createElement("div", { className: "field-group" },
          React.createElement("label", null, "Role"),
          React.createElement("div", { className: "role-tabs" },
            React.createElement("button", { className: "role-tab" + (form.role === "worker" ? " active" : ""), onClick: () => setForm((f) => ({ ...f, role: "worker" })) }, "Pekerja"),
            React.createElement("button", { className: "role-tab" + (form.role === "investor" ? " active" : ""), onClick: () => setForm((f) => ({ ...f, role: "investor" })) }, "Investor"),
            React.createElement("button", { className: "role-tab" + (form.role === "owner" ? " active" : ""), onClick: () => setForm((f) => ({ ...f, role: "owner" })) }, "Owner")
          )
        ),
        React.createElement("div", { className: "field-group" }, React.createElement("label", null, "Username / Email"), React.createElement("input", { className: "inp", value: form.email, onChange: (e) => setForm((f) => ({ ...f, email: e.target.value })), placeholder: "Ketik nama user (misal: satria)" })),
        React.createElement("div", { className: "field-group", style: { marginTop: 4 } },
          React.createElement("label", null, "Kata Sandi"),
          React.createElement("div", { style: { position: "relative", display: "flex", alignItems: "center" } },
            React.createElement("input", { className: "inp", type: showPassword ? "text" : "password", value: form.password, onChange: (e) => setForm((f) => ({ ...f, password: e.target.value })), placeholder: "Minimal 6 karakter..." }),
            React.createElement("button", { type: "button", style: { position: "absolute", right: 10, background: "none", border: "none", color: "var(--text2)", cursor: "pointer", fontSize: 11, fontWeight: "700" }, onClick: () => setShowPassword(!showPassword) }, showPassword ? "SEMBUNYIKAN" : "LIHAT")
          )
        ),
        React.createElement("div", { className: "field-group", style: { marginTop: 4 } }, React.createElement("label", null, "Nama Tampilan (opsional)"), React.createElement("input", { className: "inp", value: form.displayName, onChange: (e) => setForm((f) => ({ ...f, displayName: e.target.value })), placeholder: "Nama asli kasir..." })),
        form.role === "worker" && React.createElement("div", { className: "field-group" }, React.createElement("label", null, "Cabang"), React.createElement("select", { className: "inp", value: form.branchId, onChange: (e) => setForm((f) => ({ ...f, branchId: e.target.value })) }, React.createElement("option", { value: "" }, "-- Pilih --"), branches.map((b) => React.createElement("option", { key: b.id, value: b.id }, b.name)))),
        form.role === "worker" && React.createElement("div", { className: "field-group" }, React.createElement("label", null, "Gaji Harian (Rp) — opsional"), React.createElement("input", { className: "inp", type: "number", value: form.gajiHarian, onChange: (e) => setForm((f) => ({ ...f, gajiHarian: e.target.value })), placeholder: "Contoh: 50000" })),
        form.role === "investor" && React.createElement("div", { className: "field-group" }, React.createElement("label", null, "Pilih Investor"), React.createElement("select", { className: "inp", value: form.investorId, onChange: (e) => setForm((f) => ({ ...f, investorId: e.target.value })) }, React.createElement("option", { value: "" }, "-- Pilih --"), investors.map((i) => React.createElement("option", { key: i.id, value: i.id }, i.nama, " (", i.persenBagi, "%)"))), investors.length === 0 && React.createElement("p", { className: "info-txt mt8" }, "Belum ada investor. Buat dulu di tab Investor.")),
        React.createElement("button", { className: "btn-primary", onClick: createInvite }, "+ Buat Akun Langsung")
      ),
      React.createElement("h3", { className: "section-title mt12" }, "Daftar Antrean Akun"),
      loading && React.createElement("p", { className: "info-txt" }, "Memuat..."),
      !loading && invites.length === 0 && React.createElement("p", { className: "empty-txt" }, "Belum ada antrean."),
      !loading && invites.map((iv) =>
        React.createElement("div", { key: iv.id, className: "investor-row" },
          React.createElement("div", { style: { flex: 1 } },
            React.createElement("strong", null, iv.email),
            React.createElement("div", { style: { fontSize: 12, color: "#9a9690" } }, "Role: ", iv.role, iv.branchId ? ` | Cabang: ${branches.find((b) => b.id === iv.branchId)?.name || iv.branchId}` : "", iv.investorId ? ` | Investor: ${investors.find((i) => i.id === iv.investorId)?.nama || iv.investorId}` : "")
          ),
          React.createElement("button", { className: "btn-danger-sm", onClick: () => deleteInvite(iv.id) }, "Hapus")
        )
      ),
      React.createElement("h3", { className: "section-title mt12" }, "Akun Aktif Terdaftar"),
      filteredProfiles.length === 0 && React.createElement("p", { className: "empty-txt" }, "Belum ada data profiles."),
      filteredProfiles.map((p) =>
        React.createElement("div", { key: p.user_id, className: "branch-row", style: { alignItems: "flex-start" } },
          React.createElement("div", { style: { flex: 1 } },
            React.createElement("strong", null, p.display_name || p.email || p.user_id.slice(0, 8)),
            React.createElement("div", { style: { fontSize: 12, color: "#9a9690" } }, "Role: ", p.role, p.branchId ? ` | Cabang: ${branches.find((b) => b.id === p.branchId)?.name || p.branchId}` : "", p.gajiHarian ? ` | Gaji: ${fmtRp(p.gajiHarian)}/hari` : ""),
            p.role === "worker" && React.createElement("div", { style: { marginTop: 6, display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" } },
              React.createElement("span", { style: { fontSize: 12 } }, "Gaji/hari:"),
              React.createElement("input", { type: "number", className: "inp inp-sm", style: { width: 100, padding: "2px 6px", fontSize: 12 }, defaultValue: p.gajiHarian || "", placeholder: "0", onBlur: async (e) => {
                const val = parseFloat(e.target.value) || 0;
                const { error } = await sb.from("profiles").update({ gajiHarian: val }).eq("user_id", p.user_id);
                if (error) pushNotif(error.message, "warning");
                else { await S.loadKey("profiles"); pushNotif("Gaji diperbarui!", "success"); }
              } }),
              React.createElement("span", { style: { fontSize: 12, marginLeft: 8 } }, "Libur:"),
              React.createElement("select", { className: "inp inp-sm", style: { width: "auto", display: "inline-block", padding: "2px 6px", fontSize: 12 }, value: jadwalLibur[p.user_id] || "", onChange: (e) => updateLibur(p.user_id, e.target.value) },
                React.createElement("option", { value: "" }, "-- Tidak Libur --"),
                ["Minggu","Senin","Selasa","Rabu","Kamis","Jumat","Sabtu"].map((h) => React.createElement("option", { key: h, value: h }, h))
              )
            )
          ),
          p.role !== "owner" && React.createElement("button", { className: "btn-danger-sm", onClick: async () => {
            if (!confirm(`Hapus total akun ${p.email || p.display_name}?`)) return;
            try {
              const { error } = await sb.rpc("hapus_akun_langsung", { target_user_id: p.user_id, target_email: p.email });
              if (error) throw error;
              setDeletedIds((prev) => [...prev, p.user_id]);
              pushNotif("Akun berhasil dihapus!", "success");
            } catch (err) { pushNotif(err?.message || String(err), "warning"); }
          } }, "Hapus")
        )
      )
    );
  }

  // ─── SettingInvestor ───────────────────────────────────────────────────────
  function SettingInvestor({ pushNotif }) {
    const [investors, setInvestors] = useState(() => S.get("investors") || []);
    const [form, setForm] = useState({ nama: "", persenBagi: "" });
    const add = () => {
      if (!form.nama || !form.persenBagi) return;
      const u = [...investors, { id: uid(), nama: form.nama, persenBagi: parseFloat(form.persenBagi) }];
      S.set("investors", u); setInvestors(u); setForm({ nama: "", persenBagi: "" });
      pushNotif("Investor ditambahkan!", "success");
    };
    const del = (id) => { const u = investors.filter((x) => x.id !== id); S.set("investors", u); setInvestors(u); };
    const upP = (id, p) => { const u = investors.map((x) => x.id === id ? { ...x, persenBagi: parseFloat(p) || 0 } : x); S.set("investors", u); setInvestors(u); };

    return React.createElement("div", null,
      React.createElement("h3", { className: "section-title mt8" }, "Kelola Investor"),
      investors.map((inv) =>
        React.createElement("div", { key: inv.id, className: "investor-row" },
          React.createElement("strong", null, inv.nama),
          React.createElement("div", { className: "row-wrap" },
            React.createElement("input", { className: "inp inp-sm", type: "number", value: inv.persenBagi, onChange: (e) => upP(inv.id, e.target.value), style: { width: 70 } }),
            React.createElement("span", null, "%"),
            React.createElement("button", { className: "btn-danger-sm", onClick: () => del(inv.id) }, "Hapus")
          )
        )
      ),
      React.createElement("div", { className: "form-card mt12" },
        React.createElement("h4", null, "Tambah Investor"),
        React.createElement("div", { className: "field-group" }, React.createElement("label", null, "Nama"), React.createElement("input", { className: "inp", value: form.nama, onChange: (e) => setForm((x) => ({ ...x, nama: e.target.value })) })),
        React.createElement("div", { className: "field-group" }, React.createElement("label", null, "% Bagi Hasil"), React.createElement("input", { className: "inp", type: "number", value: form.persenBagi, onChange: (e) => setForm((x) => ({ ...x, persenBagi: e.target.value })) })),
        React.createElement("button", { className: "btn-primary", onClick: add }, "+ Tambah")
      )
    );
  }

  // ─── BrandingSetting ────────────────────────────────────────────────────────
  function BrandingSetting({ pushNotif }) {
    const [logoUrl, setLogoUrl] = useState(getBrandLogo());
    const [busy, setBusy] = useState(false);
    const saveToDb = async (nextUrl) => {
      const value = { logoUrl: nextUrl, updatedAt: nowIso() };
      const { error } = await sb.from("app_settings").upsert({ key: "branding", value });
      if (error) throw error;
      setBrandLogoLocal(nextUrl);
    };
    const doUpload = async (e) => {
      const file = e.target.files?.[0]; if (!file) return;
      try {
        setBusy(true);
        const uploaded = await uploadAsset(file, "branding");
        const nextUrl = uploaded.url || logoUrl;
        setLogoUrl(nextUrl);
        await saveToDb(nextUrl);
        pushNotif("Logo berhasil diperbarui.", "success");
      } catch (err) { pushNotif(err?.message || String(err), "warning"); } finally { setBusy(false); }
    };
    const saveManual = async () => {
      if (!logoUrl) { pushNotif("Isi URL logo dulu.", "warning"); return; }
      try { setBusy(true); await saveToDb(logoUrl); pushNotif("Logo berhasil disimpan.", "success"); }
      catch (err) { pushNotif(err?.message || String(err), "warning"); } finally { setBusy(false); }
    };
    return React.createElement("div", null,
      React.createElement("h3", { className: "section-title mt8" }, "Branding Logo"),
      React.createElement("p", { className: "info-txt" }, "Upload logo ke Supabase Storage, atau isi URL manual."),
      React.createElement("div", { className: "form-card mt8" },
        React.createElement("div", { style: { display: "flex", justifyContent: "center" } },
          React.createElement("img", { src: logoUrl || getBrandLogo(), alt: "Logo bisnis", className: "brand-preview" })
        ),
        React.createElement("div", { className: "field-group" },
          React.createElement("label", null, "Upload Logo"),
          React.createElement("input", { className: "inp", type: "file", accept: "image/*", onChange: doUpload, disabled: busy })
        ),
        React.createElement("div", { className: "field-group" },
          React.createElement("label", null, "URL Logo"),
          React.createElement("input", { className: "inp", value: logoUrl, onChange: (e) => setLogoUrl(e.target.value), placeholder: "https://... atau public URL Supabase" })
        ),
        React.createElement("button", { className: "btn-primary", onClick: saveManual, disabled: busy }, busy ? "Menyimpan..." : "Simpan Logo")
      )
    );
  }

  function SettingStokLapak({ pushNotif }) {
    const tick = useStoreTick();
    const branches = (S.get("branches") || []).filter((b) => b.type !== "central_kitchen");
    const menus = S.get("menuVarian") || [];
    const stoks = S.get("stokLapak") || [];
    const [editVal, setEditVal] = useState({});
    const [busy, setBusy] = useState({});

    const getMenuNama = (id) => menus.find((m) => m.id === id)?.nama || id;
    const getBranchNama = (id) => branches.find((b) => b.id === id)?.name || id;

    const saveEdit = async (row) => {
      const val = parseFloat(editVal[row.id]);
      if (isNaN(val) || val < 0) { pushNotif("Nilai stok tidak valid.", "warning"); return; }
      setBusy((b) => ({ ...b, [row.id]: true }));
      try {
        await upsertStokLapak(row.branchId, row.menuId, val, row);
        await S.loadKey("stokLapak");
        setEditVal((e) => { const c = { ...e }; delete c[row.id]; return c; });
        pushNotif("Stok diperbarui.", "success");
      } catch (e) { pushNotif(e?.message || String(e), "warning"); }
      finally { setBusy((b) => { const c = { ...b }; delete c[row.id]; return c; }); }
    };

    const hapusRow = async (row) => {
      if (!confirm(`Hapus data stok "${getMenuNama(row.menuId)}" di "${getBranchNama(row.branchId)}"?`)) return;
      setBusy((b) => ({ ...b, [row.id]: true }));
      try {
        const { error } = await sb.from("stokLapak").delete().eq("id", row.id);
        if (error) throw error;
        await S.loadKey("stokLapak");
        pushNotif("Data stok dihapus.", "success");
      } catch (e) { pushNotif(e?.message || String(e), "warning"); }
      finally { setBusy((b) => { const c = { ...b }; delete c[row.id]; return c; }); }
    };

    return React.createElement("div", null,
      React.createElement("h3", { className: "section-title mt8" }, "Stok Lapak (Real-time)"),
      React.createElement("p", { className: "info-txt" }, "Stok ini otomatis bertambah saat kasir konfirmasi terima distribusi, dan berkurang saat ada penjualan. Bisa dikoreksi manual di sini bila perlu."),
      stoks.length === 0 && React.createElement("p", { className: "empty-txt mt8" }, "Belum ada data stok lapak."),
      branches.map((b) => {
        const rows = stoks.filter((s) => s.branchId === b.id);
        if (rows.length === 0) return null;
        return React.createElement("div", { key: b.id, className: "mt12" },
          React.createElement("h4", { className: "sub-title" }, b.name),
          rows.map((row) =>
            React.createElement("div", { key: row.id, className: "row-wrap", style: { gap: 8, alignItems: "center", marginBottom: 6 } },
              React.createElement("span", { style: { flex: 1 } }, getMenuNama(row.menuId)),
              React.createElement("input", {
                type: "number", className: "inp inp-sm", style: { width: 80 }, min: 0,
                value: editVal[row.id] !== undefined ? editVal[row.id] : row.stok,
                onChange: (e) => setEditVal((v) => ({ ...v, [row.id]: e.target.value }))
              }),
              React.createElement("span", { style: { fontSize: 12, color: "#9a9690" } }, "pcs"),
              React.createElement("button", { className: "btn-secondary btn-sm", disabled: !!busy[row.id], onClick: () => saveEdit(row) }, "Simpan"),
              React.createElement("button", { className: "btn-danger-sm", disabled: !!busy[row.id], onClick: () => hapusRow(row) }, "Hapus")
            )
          )
        );
      })
    );
  }


  // ─── SettingData ───────────────────────────────────────────────────────────
  function SettingData({ pushNotif }) {
    const [busy, setBusy] = useState(false);
    const branches = S.get("branches") || [];
    const [selBranch, setSelBranch] = useState("");
    const [selDate, setSelDate] = useState("");
    const doClear = async (label, tables, keysToReload) => {
      const branchName = selBranch ? branches.find((b) => b.id === selBranch)?.name : "SEMUA CABANG";
      const dateName = selDate ? selDate : "SEMUA TANGGAL";
      if (!confirm(`⚠️ YAKIN HAPUS DATA: ${label}?\n\nCabang: ${branchName}\nTanggal: ${dateName}\n\nTidak bisa dibatalkan!`)) return;
      setBusy(true);
      try {
        for (const t of tables) {
          let query = sb.from(t).delete().neq("id", "00000000-0000-0000-0000-000000000000");
          if (selBranch && t !== "pengeluaranOwner") query = query.eq("branchId", selBranch);
          if (selDate) {
            if (["transactions", "pengeluaranLapak", "pengeluaranOwner", "setoranHarian", "absensi"].includes(t)) query = query.eq("date", selDate);
            else if (["setoranBulanan", "absensiBulanan"].includes(t)) query = query.eq("bulan", selDate.slice(0, 7));
          }
          const { error } = await query; if (error) throw error;
        }
        for (const k of keysToReload) await S.loadKey(k).catch(() => {});
        pushNotif(`Berhasil hapus: ${label}`, "success");
      } catch (e) { pushNotif(e?.message || String(e), "warning"); } finally { setBusy(false); }
    };
    return React.createElement("div", null,
      React.createElement("h3", { className: "section-title mt8" }, "Bersihkan Data"),
      React.createElement("p", { className: "info-txt" }, "Pilih cabang dan tanggal untuk hapus data spesifik. Kosong = hapus SEMUA."),
      React.createElement("div", { className: "filter-bar mb8" },
        React.createElement("select", { className: "inp inp-sm", value: selBranch, onChange: (e) => setSelBranch(e.target.value) },
          React.createElement("option", { value: "" }, "-- Semua Cabang --"),
          branches.map((b) => React.createElement("option", { key: b.id, value: b.id }, b.name))
        ),
        React.createElement("input", { type: "date", className: "inp inp-sm", value: selDate, onChange: (e) => setSelDate(e.target.value) })
      ),
      React.createElement("div", { className: "form-card mt8" },
        React.createElement("h4", null, "Eksekusi Hapus"),
        React.createElement("p", { className: "info-txt", style: { fontSize: 11 } }, "Catatan: untuk \"Produksi & Distribusi CK\", filter Cabang tidak berlaku untuk Produksi (selalu hapus semua produksi sesuai tanggal). Untuk Distribusi, filter Cabang berlaku normal."),
        React.createElement("div", { className: "row-wrap", style: { gap: 8, flexWrap: "wrap", marginTop: 8 } },
          React.createElement("button", { className: "btn-danger-sm", disabled: busy, onClick: () => doClear("Transaksi", ["transactions"], ["transactions"]) }, "Hapus Transaksi"),
          React.createElement("button", { className: "btn-danger-sm", disabled: busy, onClick: () => doClear("Pengeluaran", ["pengeluaranLapak", "pengeluaranOwner"], ["pengeluaranLapak", "pengeluaranOwner"]) }, "Hapus Pengeluaran"),
          React.createElement("button", { className: "btn-danger-sm", disabled: busy, onClick: () => doClear("Setoran", ["setoranHarian", "setoranBulanan"], ["setoranHarian", "setoranBulanan"]) }, "Hapus Setoran"),
          React.createElement("button", { className: "btn-danger-sm", disabled: busy, onClick: () => doClear("Absensi", ["absensi", "absensiBulanan"], ["absensi", "absensiBulanan"]) }, "Hapus Absensi"),
          React.createElement("button", { className: "btn-danger-sm", disabled: busy, onClick: () => doClear("Edit Log", ["editLog"], ["editLog"]) }, "Hapus Edit Log"),
          React.createElement("button", { className: "btn-danger-sm", disabled: busy, onClick: () => doClear("Stok Lapak", ["stokLapak"], ["stokLapak"]) }, "Hapus Stok Lapak"),
          React.createElement("button", { className: "btn-danger-sm", disabled: busy, onClick: () => doClear("Produksi & Distribusi CK", ["produksiCK", "distribusiCK"], ["produksiCK", "distribusiCK"]) }, "Hapus Produksi & Distribusi CK")
        )
      )
    );
  }

  // ─── OwnerSetting ──────────────────────────────────────────────────────────
  function OwnerSetting({ stab, setStab, pushNotif }) {
    const TABS = ["hpp", "paket", "cabang", "akun", "investor", "branding", "stok", "dana", "data"];
    const TLABEL = { hpp: "Menu HPP", paket: "Box/Paket", cabang: "Cabang", akun: "Akun", investor: "Investor", branding: "Logo", stok: "Stok Lapak", dana: "Dana Cadangan", data: "Data" };
    return React.createElement("div", null,
      React.createElement("div", { className: "tabs tabs-sm" },
        TABS.map((t) => React.createElement("button", { key: t, className: "tab" + (stab === t ? " active" : ""), onClick: () => setStab(t) }, TLABEL[t]))
      ),
      stab === "hpp"      && React.createElement(SettingHPP, { pushNotif }),
      stab === "paket"    && React.createElement(SettingPaket, { pushNotif }),
      stab === "cabang"   && React.createElement(SettingCabang, { pushNotif }),
      stab === "akun"     && React.createElement(SettingAkun, { pushNotif }),
      stab === "investor" && React.createElement(SettingInvestor, { pushNotif }),
      stab === "branding" && React.createElement(BrandingSetting, { pushNotif }),
      stab === "stok"     && React.createElement(SettingStokLapak, { pushNotif }),
      stab === "dana"     && React.createElement(SettingDanaPemeliharaan, { pushNotif }),
      stab === "data"     && React.createElement(SettingData, { pushNotif })
    );
  }

  // ─── SettingDanaPemeliharaan — kas cadangan untuk perbaikan/pemeliharaan ──
  function SettingDanaPemeliharaan({ pushNotif }) {
    const tick = useStoreTick();
    const list = (S.get("danaPemeliharaan") || []).slice().sort((a, b) => (b.ts || "").localeCompare(a.ts || ""));
    const [form, setForm] = useState({ tipe: "setor", keterangan: "", jumlah: "" });
    const [busy, setBusy] = useState(false);

    const saldo = list.reduce((a, d) => a + (d.tipe === "setor" ? d.jumlah : -d.jumlah), 0);

    const tambah = async () => {
      if (!form.keterangan || !form.jumlah) { alert("Isi keterangan dan jumlah!"); return; }
      const jml = parseFloat(form.jumlah);
      if (form.tipe === "pakai" && jml > saldo) {
        if (!confirm(`Saldo dana cadangan hanya ${fmtRp(saldo)}, tapi mau pakai ${fmtRp(jml)}. Tetap lanjut? (saldo akan minus)`)) return;
      }
      setBusy(true);
      try {
        const { error } = await sb.from("danaPemeliharaan").insert([{
          id: uid(), date: today(), ts: nowIso(), tipe: form.tipe, keterangan: form.keterangan, jumlah: jml
        }]);
        if (error) throw error;
        await S.loadKey("danaPemeliharaan");
        setForm({ tipe: form.tipe, keterangan: "", jumlah: "" });
        pushNotif("Tercatat!", "success");
      } catch (e) { pushNotif(e?.message || String(e), "warning"); }
      finally { setBusy(false); }
    };

    const hapus = async (id) => {
      if (!confirm("Hapus entri ini?")) return;
      const { error } = await sb.from("danaPemeliharaan").delete().eq("id", id);
      if (!error) await S.loadKey("danaPemeliharaan");
    };

    return React.createElement("div", null,
      React.createElement("h3", { className: "section-title mt8" }, "Dana Pemeliharaan & Perbaikan"),
      React.createElement("p", { className: "info-txt" }, "Kas cadangan untuk perbaikan atau kondisi tak terduga. \"Setor\" = menyisihkan dana dari kas operasional ke cadangan. \"Pakai\" = dana cadangan dipakai untuk perbaikan/pemeliharaan."),
      React.createElement("div", { className: "kpi-card kpi-modal mt8", style: { maxWidth: 280 } },
        React.createElement("div", { className: "kpi-label" }, "Saldo Dana Cadangan"),
        React.createElement("div", { className: "kpi-val", style: { color: saldo >= 0 ? "#22c55e" : "#ef4444" } }, fmtRp(saldo))
      ),
      React.createElement("div", { className: "form-card mt8" },
        React.createElement("div", { className: "row-wrap mb8" },
          React.createElement("button", { className: "tab" + (form.tipe === "setor" ? " active" : ""), onClick: () => setForm((f) => ({ ...f, tipe: "setor" })) }, "Setor Dana"),
          React.createElement("button", { className: "tab" + (form.tipe === "pakai" ? " active" : ""), onClick: () => setForm((f) => ({ ...f, tipe: "pakai" })) }, "Pakai Dana")
        ),
        React.createElement("div", { className: "field-group" },
          React.createElement("label", null, "Keterangan"),
          React.createElement("input", { className: "inp", value: form.keterangan, onChange: (e) => setForm((f) => ({ ...f, keterangan: e.target.value })), placeholder: form.tipe === "setor" ? "Contoh: Sisihkan dana bulan ini" : "Contoh: Ganti kompor rusak" })
        ),
        React.createElement("div", { className: "field-group" },
          React.createElement("label", null, "Jumlah (Rp)"),
          React.createElement("input", { type: "number", className: "inp", value: form.jumlah, onChange: (e) => setForm((f) => ({ ...f, jumlah: e.target.value })) })
        ),
        React.createElement("button", { className: "btn-primary btn-full", disabled: busy, onClick: tambah }, busy ? "Menyimpan..." : (form.tipe === "setor" ? "Setor ke Dana Cadangan" : "Catat Pemakaian Dana"))
      ),
      React.createElement("h4", { className: "sub-title mt12" }, "Riwayat"),
      list.length === 0 && React.createElement("p", { className: "empty-txt" }, "Belum ada riwayat."),
      list.map((d) =>
        React.createElement("div", { key: d.id, className: "row-wrap", style: { justifyContent: "space-between", alignItems: "center", padding: "6px 0", borderBottom: "1px solid #2a2a2e" } },
          React.createElement("div", null,
            React.createElement("div", { style: { fontSize: 13 } }, d.keterangan),
            React.createElement("div", { style: { fontSize: 11, color: "#9a9690" } }, d.date, " · ", d.tipe === "setor" ? "Setor" : "Pakai")
          ),
          React.createElement("div", { style: { display: "flex", alignItems: "center", gap: 8 } },
            React.createElement("strong", { style: { color: d.tipe === "setor" ? "#22c55e" : "#ef4444" } }, d.tipe === "setor" ? "+" : "-", fmtRp(d.jumlah)),
            React.createElement("button", { className: "btn-danger-sm", onClick: () => hapus(d.id) }, "Hapus")
          )
        )
      )
    );
  }


  // ─── SettingStokLapak — lihat, edit manual, hapus per-baris stok lapak ────
  // ─── OwnerPage ─────────────────────────────────────────────────────────────
  function OwnerPage({ pushNotif, me }) {
    const tick = useStoreTick();
    const [tab, setTab] = useState("dashboard");
    const [stab, setStab] = useState("hpp");

    useEffect(() => {
      const iv = setInterval(() => {
        const list = S.get("setoranHarian") || [];
        const pending = list.filter((s) => s.status === "menunggu");
        if (pending.length) {
          const noted = S.get("notified_ids") || [];
          const fresh = pending.filter((s) => !noted.includes(s.id));
          if (fresh.length) {
            pushNotif(fresh.length + " setoran menunggu konfirmasi!", "warning");
            S.set("notified_ids", [...noted, ...fresh.map((s) => s.id)]);
          }
        }
      }, 5000);
      return () => clearInterval(iv);
    }, [pushNotif]);

    const TABS = ["dashboard", "kasir", "setoran", "laporan", "absensi", "pengeluaran", "produksiCK", "setting"];
    const TLABEL = { dashboard: "Dashboard", kasir: "Kasir", setoran: "Setoran", laporan: "Laporan", absensi: "Absensi", pengeluaran: "Pengeluaran", produksiCK: "Produksi CK", setting: "Seting" };

    return React.createElement("div", { className: "page" },
      React.createElement("div", { className: "page-header" },
        React.createElement("img", { className: "page-icon", src: getBrandLogo(), style: { width: 45, height: 45, objectFit: "cover", borderRadius: 10 } }),
        React.createElement("div", null,
          React.createElement("h2", null, "Panel Owner"),
          React.createElement("p", { className: "page-sub" }, "Kontrol penuh bisnis Anda")
        )
      ),
      React.createElement("div", { className: "tabs tabs-scroll" },
        TABS.map((t) => React.createElement("button", { key: t, className: "tab" + (tab === t ? " active" : ""), onClick: () => setTab(t) }, TLABEL[t]))
      ),
      tab === "dashboard"   && React.createElement(OwnerDashboard, null),
      tab === "kasir"       && React.createElement(WorkerPage, { pushNotif, me, mode: "owner" }),
      tab === "setoran"     && React.createElement(OwnerSetoran, { pushNotif }),
      tab === "laporan"     && React.createElement(OwnerLaporan, { pushNotif }),
      tab === "absensi"     && React.createElement(OwnerAbsensi, { pushNotif }),
      tab === "pengeluaran" && React.createElement(PengeluaranOwner, { pushNotif }),
      tab === "produksiCK"  && React.createElement(OwnerProduksiCK, { pushNotif }),
      tab === "setting"     && React.createElement(OwnerSetting, { stab, setStab, pushNotif })
    );
  }

  // ─── InvestorPage — Laporan Harian/Bulanan/Tahunan Akumulatif ───────────────
  function InvestorPage({ investorId, pushNotif, me }) {
    const tick = useStoreTick();
    const [tab, setTab] = useState("harian");
    const [selDate, setSelDate] = useState(today());
    const [month, setMonth] = useState(today().slice(0, 7));
    const [year, setYear] = useState(today().slice(0, 4));
    const [openTx, setOpenTx] = useState({});
    // Filter cabang: "" = semua cabang investasi milik investor ini, atau id spesifik
    const [selBranch, setSelBranch] = useState("");
    const toggleTx = (id) => setOpenTx((o) => ({ ...o, [id]: !o[id] }));

    const investors = S.get("investors") || [];
    const invMe = investors.find((i) => i.id === investorId);
    const allInvBranches = (S.get("branches") || []).filter((b) => b.type === "investasi" && (!investorId || b.investorId === investorId));
    const branches = selBranch ? allInvBranches.filter((b) => b.id === selBranch) : allInvBranches;
    const txs = S.get("transactions") || [];
    const pLapakAll = S.get("pengeluaranLapak") || [];
    const pOwnerAll = S.get("pengeluaranOwner") || [];
    const setoranBulAll = (S.get("setoranBulanan") || []).filter((s) => !investorId || s.investorId === investorId);
    const nBranchTotal = Math.max((S.get("branches") || []).filter((b) => b.type !== "central_kitchen").length, 1);

    const konfirmBulananInvestor = (id) => {
      const all = S.get("setoranBulanan") || [];
      S.set("setoranBulanan", all.map((s) => s.id === id ? { ...s, status: "selesai", konfirmasiTs: nowTs(), confirmedBy: "investor" } : s));
      pushNotif?.("Laporan bulanan dikonfirmasi.", "success");
    };

    const distribAllInv = S.get("distribusiCK") || [];

    // Helper: hitung akumulasi untuk rentang cabang + tanggal tertentu langsung dari transaksi
    const calcAccum = (branchIds, dateFilter) => {
      const bTxs = txs.filter((t) => branchIds.includes(t.branchId) && dateFilter(t.date));
      const bPL = pLapakAll.filter((p) => branchIds.includes(p.branchId) && dateFilter(p.date));
      const bPODirect = pOwnerAll.filter((p) => p.branchId && branchIds.includes(p.branchId) && dateFilter(p.date));
      const bPOGlobal = pOwnerAll.filter((p) => !p.branchId && dateFilter(p.date));
      const bDistrib = distribAllInv.filter((d) => branchIds.includes(d.branchId) && dateFilter(d.date));
      const omzet = bTxs.reduce((a, t) => a + t.total, 0);
      // HPP = total HPP dari barang yang DIDISTRIBUSIKAN ke cabang investor ini (bukan nunggu laku)
      const modal = bDistrib.reduce((a, d) => a + (d.hppTotal || 0), 0);
      const pLapak = bPL.reduce((a, p) => a + p.jumlah, 0);
      const pOwnerD = bPODirect.reduce((a, p) => a + p.jumlah, 0);
      const pOwnerG = bPOGlobal.reduce((a, p) => a + p.jumlah, 0) / nBranchTotal;
      const pOwner = pOwnerD + pOwnerG;
      const laba = omzet - modal - pLapak - pOwner;
      return { omzet, modal, pLapak, pOwner, laba, txCount: bTxs.length };
    };

    const branchIds = branches.map((b) => b.id);

    // Data harian langsung dari transaksi (real-time akumulatif hari dipilih)
    const harian = calcAccum(branchIds, (d) => d === selDate);

    // Data bulanan: akumulasi semua hari dalam bulan
    const bulanan = calcAccum(branchIds, (d) => d && d.startsWith(month));

    // Data tahunan: akumulasi semua hari dalam tahun
    const tahunan = calcAccum(branchIds, (d) => d && d.startsWith(year));

    // Rincian per bulan dalam tahun (untuk tabel tahunan)
    const bulanList = Array.from({ length: 12 }, (_, i) => {
      const m = String(i + 1).padStart(2, "0");
      const key = `${year}-${m}`;
      const acc = calcAccum(branchIds, (d) => d && d.startsWith(key));
      return { key, label: ["Jan","Feb","Mar","Apr","Mei","Jun","Jul","Agu","Sep","Okt","Nov","Des"][i], ...acc };
    });

    // Chart 7 hari
    const chart7 = Array.from({ length: 7 }, (_, i) => {
      const d = new Date(); d.setDate(d.getDate() - (6 - i));
      const ds = d.toISOString().slice(0, 10);
      const acc = calcAccum(branchIds, (dt) => dt === ds);
      return { label: ds.slice(5), v1: acc.omzet, v2: acc.modal + acc.pLapak + acc.pOwner };
    });

    const inv = invMe;
    const persen = inv?.persenBagi || 0;

    const KpiRow = ({ data, label }) => React.createElement("div", null,
      React.createElement("h4", { className: "sub-title mt8" }, label),
      React.createElement("div", { className: "investor-kpi-grid" },
        React.createElement("div", { className: "inv-kpi kpi-card kpi-omzet" }, React.createElement("div", { className: "kpi-label" }, "Omzet"), React.createElement("div", { className: "kpi-val-sm", style: { color: "#22c55e" } }, fmtRp(data.omzet))),
        React.createElement("div", { className: "inv-kpi kpi-card kpi-modal" }, React.createElement("div", { className: "kpi-label" }, "HPP"), React.createElement("div", { className: "kpi-val-sm", style: { color: "#ef4444" } }, fmtRp(data.modal))),
        React.createElement("div", { className: "inv-kpi kpi-card kpi-peng" }, React.createElement("div", { className: "kpi-label" }, "Peng. Lapak"), React.createElement("div", { className: "kpi-val-sm", style: { color: "#ef4444" } }, fmtRp(data.pLapak))),
        React.createElement("div", { className: "inv-kpi kpi-card kpi-peng" }, React.createElement("div", { className: "kpi-label" }, "Peng. Pusat"), React.createElement("div", { className: "kpi-val-sm", style: { color: "#ef4444" } }, fmtRp(data.pOwner))),
        React.createElement("div", { className: "inv-kpi kpi-card kpi-profit" }, React.createElement("div", { className: "kpi-label" }, "Laba"), React.createElement("div", { className: "kpi-val-sm", style: { color: data.laba >= 0 ? "#22c55e" : "#ef4444" } }, fmtRp(data.laba))),
        React.createElement("div", { className: "inv-kpi kpi-card", style: { gridColumn: "1/-1", borderColor: "var(--accent)", background: "#1a1000" } },
          React.createElement("div", { className: "kpi-label" }, "Est. Bagian Anda (", persen, "%)"),
          React.createElement("div", { className: "kpi-val-sm", style: { color: "var(--accent)", fontSize: 18 } }, fmtRp(data.laba * persen / 100))
        )
      )
    );

    return React.createElement("div", { className: "page" },
      React.createElement("div", { className: "page-header" },
        React.createElement("img", { className: "page-icon", src: getBrandLogo(), style: { width: 45, height: 45, objectFit: "cover", borderRadius: 10 } }),
        React.createElement("div", null,
          React.createElement("h2", null, "Portal Investor"),
          React.createElement("p", { className: "page-sub" }, inv?.nama ? `Akun: ${inv.nama} (${persen}%)` : "Cabang Investasi")
        )
      ),
      // Filter cabang
      React.createElement("div", { className: "filter-bar mb8" },
        React.createElement("button", { className: "chip" + (!selBranch ? " chip-active" : ""), onClick: () => setSelBranch("") }, "Semua Cabang"),
        allInvBranches.map((b) => React.createElement("button", { key: b.id, className: "chip" + (selBranch === b.id ? " chip-active" : ""), onClick: () => setSelBranch(b.id) }, b.name))
      ),
      // Tabs
      React.createElement("div", { className: "tabs" },
        React.createElement("button", { className: "tab" + (tab === "harian" ? " active" : ""), onClick: () => setTab("harian") }, "Harian"),
        React.createElement("button", { className: "tab" + (tab === "bulanan" ? " active" : ""), onClick: () => setTab("bulanan") }, "Bulanan"),
        React.createElement("button", { className: "tab" + (tab === "tahunan" ? " active" : ""), onClick: () => setTab("tahunan") }, "Tahunan")
      ),

      // ── Tab Harian ──
      tab === "harian" && React.createElement("div", null,
        React.createElement("div", { className: "field-group mt8" },
          React.createElement("label", null, "Pilih Tanggal"),
          React.createElement("input", { type: "date", className: "inp inp-sm", value: selDate, onChange: (e) => setSelDate(e.target.value) })
        ),
        React.createElement(KpiRow, { data: harian, label: "Laporan Harian - " + selDate }),
        // Rincian pengeluaran lapak
        branches.map((b) => {
          const dayTxs = txs.filter((t) => t.branchId === b.id && t.date === selDate);
          const pLapakRinci = pLapakAll.filter((p) => p.branchId === b.id && p.date === selDate);
          const pODirect = pOwnerAll.filter((p) => p.branchId === b.id && p.date === selDate);
          const pOGlobal = pOwnerAll.filter((p) => !p.branchId && p.date === selDate);
          if (dayTxs.length === 0 && pLapakRinci.length === 0) return null;
          return React.createElement("div", { key: b.id, className: "investor-report-card mt8" },
            React.createElement("div", { className: "investor-report-header" },
              React.createElement("h3", null, b.name),
              React.createElement("span", { className: "badge-type investasi" }, dayTxs.length + "x transaksi")
            ),
            // Transaksi accordion
            dayTxs.map((tx) =>
              React.createElement("div", { key: tx.id, className: "accordion-card" },
                React.createElement("div", { className: "accordion-header", onClick: () => toggleTx(tx.id) },
                  React.createElement("div", { className: "accordion-title" },
                    React.createElement("span", { className: "tx-id" }, "STRUK-", tx.id.slice(0, 6).toUpperCase()),
                    React.createElement("span", { className: "accordion-omzet" }, fmtTxTs(tx), " — ", fmtRp(tx.total))
                  ),
                  React.createElement("span", { className: "accordion-arrow" }, openTx[tx.id] ? "▲" : "▼")
                ),
                openTx[tx.id] && React.createElement("div", { className: "accordion-body" },
                  tx.items.map((it, i) => React.createElement("div", { key: i, className: "tx-item" }, it.nama, " x", it.qty, " = ", fmtRp(it.hargaJual * it.qty))),
                  React.createElement("div", { className: "tx-total" }, "Total: ", fmtRp(tx.total))
                )
              )
            ),
            // Pengeluaran lapak
            pLapakRinci.length > 0 && React.createElement("div", { className: "mt4" },
              React.createElement("h4", { className: "sub-title" }, "Pengeluaran Lapak"),
              pLapakRinci.map((p) => React.createElement("div", { key: p.id, className: "peng-row" },
                React.createElement("div", { className: "peng-info" }, React.createElement("span", { className: "peng-ket" }, p.keterangan), React.createElement("span", { className: "peng-ts" }, p.ts)),
                React.createElement("div", { className: "peng-right" }, React.createElement("span", { className: "peng-jml" }, fmtRp(p.jumlah)))
              ))
            ),
            // Pengeluaran owner
            (pODirect.length > 0 || pOGlobal.length > 0) && React.createElement("div", { className: "mt4" },
              React.createElement("h4", { className: "sub-title" }, "Pengeluaran Operasional"),
              pODirect.map((p) => React.createElement("div", { key: p.id, className: "peng-row" },
                React.createElement("div", { className: "peng-info" }, React.createElement("span", { className: "peng-ket" }, p.keterangan, " (Langsung)")),
                React.createElement("div", { className: "peng-right" }, React.createElement("span", { className: "peng-jml" }, fmtRp(p.jumlah)))
              )),
              pOGlobal.map((p) => React.createElement("div", { key: p.id, className: "peng-row" },
                React.createElement("div", { className: "peng-info" }, React.createElement("span", { className: "peng-ket" }, p.keterangan, " (Global÷", nBranchTotal, ")")),
                React.createElement("div", { className: "peng-right" }, React.createElement("span", { className: "peng-jml" }, fmtRp(p.jumlah / nBranchTotal)))
              ))
            )
          );
        }),
        React.createElement("div", { className: "chart-box mt12" },
          React.createElement("h3", { className: "section-title" }, "Omzet 7 Hari Terakhir"),
          React.createElement(BarChart, { data: chart7, height: 90 }),
          React.createElement("div", { className: "chart-legend mt4" },
            React.createElement("span", { className: "leg-dot leg-a" }), React.createElement("span", null, "Omzet"),
            React.createElement("span", { className: "leg-dot leg-b", style: { marginLeft: 12 } }), React.createElement("span", null, "HPP+Peng")
          )
        )
      ),

      // ── Tab Bulanan ──
      tab === "bulanan" && React.createElement("div", null,
        React.createElement("div", { className: "field-group mt8" },
          React.createElement("label", null, "Pilih Bulan"),
          React.createElement("input", { type: "month", className: "inp inp-sm", value: month, onChange: (e) => setMonth(e.target.value) })
        ),
        React.createElement(KpiRow, { data: bulanan, label: "Akumulasi Bulanan - " + month }),
        React.createElement("p", { className: "info-txt mt4" }, "Data dihitung langsung dari semua transaksi harian dalam bulan ini (real-time)."),
        // Laporan resmi dari owner (jika sudah dikirim)
        branches.map((b) => {
          const laporan = setoranBulAll.find((s) => s.branchId === b.id && s.bulan === month);
          if (!laporan) return React.createElement("div", { key: b.id, className: "investor-report-card mt8" },
            React.createElement("div", { className: "investor-report-header" }, React.createElement("h3", null, b.name), React.createElement("span", { className: "badge-type investasi" }, "Investasi")),
            React.createElement("p", { className: "info-txt" }, "Laporan resmi bulan ini belum dikirim Owner. Data di atas adalah estimasi real-time.")
          );
          return React.createElement("div", { key: b.id, className: "investor-report-card mt8" },
            React.createElement("div", { className: "investor-report-header" }, React.createElement("h3", null, b.name), React.createElement("span", { className: "badge-type investasi" }, "Investasi")),
            React.createElement("h4", { className: "sub-title" }, "Laporan Resmi dari Owner"),
            React.createElement("div", { className: "investor-kpi-grid" },
              React.createElement("div", { className: "inv-kpi kpi-card kpi-omzet" }, React.createElement("div", { className: "kpi-label" }, "Omzet"), React.createElement("div", { className: "kpi-val-sm", style: { color: "#22c55e" } }, fmtRp(laporan.omzet))),
              React.createElement("div", { className: "inv-kpi kpi-card kpi-modal" }, React.createElement("div", { className: "kpi-label" }, "HPP"), React.createElement("div", { className: "kpi-val-sm", style: { color: "#ef4444" } }, fmtRp(laporan.modal))),
              React.createElement("div", { className: "inv-kpi kpi-card kpi-peng" }, React.createElement("div", { className: "kpi-label" }, "Peng. Lapak"), React.createElement("div", { className: "kpi-val-sm", style: { color: "#ef4444" } }, fmtRp(laporan.pLapak || 0))),
              React.createElement("div", { className: "inv-kpi kpi-card kpi-peng" }, React.createElement("div", { className: "kpi-label" }, "Peng. Pusat"), React.createElement("div", { className: "kpi-val-sm", style: { color: "#ef4444" } }, fmtRp(laporan.pOwner || 0))),
              React.createElement("div", { className: "inv-kpi kpi-card kpi-profit" }, React.createElement("div", { className: "kpi-label" }, "Laba Bersih"), React.createElement("div", { className: "kpi-val-sm", style: { color: laporan.laba >= 0 ? "#22c55e" : "#ef4444" } }, fmtRp(laporan.laba))),
              React.createElement("div", { className: "inv-kpi kpi-card", style: { gridColumn: "1/-1", borderColor: "var(--accent)", background: "#1a1000" } },
                React.createElement("div", { className: "kpi-label" }, "Bagian Anda (", laporan.persen, "%)"),
                React.createElement("div", { className: "kpi-val-sm", style: { color: "var(--accent)", fontSize: 18 } }, fmtRp(laporan.bagianInvestor))
              )
            ),
            React.createElement("div", { className: "setoran-status setoran-" + laporan.status, style: { marginTop: 10 } },
              laporan.status === "menunggu" && React.createElement(React.Fragment, null,
                React.createElement("span", null, "Menunggu konfirmasi Anda"),
                React.createElement("button", { className: "btn-primary btn-sm", onClick: () => konfirmBulananInvestor(laporan.id) }, "Konfirmasi")
              ),
              laporan.status === "selesai" && React.createElement("span", null, "✓ Dikonfirmasi - ", laporan.konfirmasiTs)
            )
          );
        })
      ),

      // ── Tab Tahunan ──
      tab === "tahunan" && React.createElement("div", null,
        React.createElement("div", { className: "field-group mt8" },
          React.createElement("label", null, "Pilih Tahun"),
          React.createElement("input", { type: "number", className: "inp inp-sm", style: { width: 100 }, value: year, min: "2020", max: "2099", onChange: (e) => setYear(e.target.value) })
        ),
        React.createElement(KpiRow, { data: tahunan, label: "Akumulasi Tahunan - " + year }),
        React.createElement("p", { className: "info-txt mt4" }, "Dihitung dari akumulasi semua transaksi sepanjang tahun ", year, " (real-time)."),
        // Tabel per bulan
        React.createElement("h3", { className: "section-title mt12" }, "Rincian Per Bulan"),
        React.createElement("div", { className: "tbl-wrap mt8" },
          React.createElement("table", { className: "tbl" },
            React.createElement("thead", null,
              React.createElement("tr", null,
                React.createElement("th", null, "Bulan"),
                React.createElement("th", null, "Omzet"),
                React.createElement("th", null, "Laba"),
                React.createElement("th", null, "Bagian Anda")
              )
            ),
            React.createElement("tbody", null,
              bulanList.map((m) =>
                React.createElement("tr", { key: m.key, style: m.omzet > 0 ? { fontWeight: 600 } : { opacity: 0.4 } },
                  React.createElement("td", null, m.label, " ", year),
                  React.createElement("td", { style: { color: "var(--kpi-omzet, #f4a227)" } }, fmtRp(m.omzet)),
                  React.createElement("td", { style: { color: m.laba >= 0 ? "#22c55e" : "#ef4444" } }, fmtRp(m.laba)),
                  React.createElement("td", { style: { color: "var(--accent)" } }, fmtRp(m.laba * persen / 100))
                )
              ),
              // Baris total
              React.createElement("tr", { style: { borderTop: "2px solid var(--border)", fontWeight: 700 } },
                React.createElement("td", null, "TOTAL"),
                React.createElement("td", { style: { color: "var(--kpi-omzet, #f4a227)" } }, fmtRp(tahunan.omzet)),
                React.createElement("td", { style: { color: tahunan.laba >= 0 ? "#22c55e" : "#ef4444" } }, fmtRp(tahunan.laba)),
                React.createElement("td", { style: { color: "var(--accent)" } }, fmtRp(tahunan.laba * persen / 100))
              )
            )
          )
        )
      )
    );
  }

  // ─── KitchenPage — Halaman Pekerja Central Kitchen ────────────────────────
  function KitchenPage({ pushNotif, me }) {
    const tick = useStoreTick();
    const [tab, setTab] = useState("produksi");
    const [date, setDate] = useState(today());
    const [form, setForm] = useState({ menuId: "", jumlah: "", keterangan: "" });
    const [busy, setBusy] = useState(false);
    const [absMonth, setAbsMonth] = useState(today().slice(0, 7));

    const branches = S.get("branches") || [];
    const menus = S.get("menuVarian") || [];
    const ckBranch = branches.find((b) => b.type === "central_kitchen");
    const branchId = ckBranch?.id || me?.branchId || "";
    const branchName = ckBranch?.name || "Central Kitchen";
    const userId = me?.user_id;

    const produksiList = (S.get("produksiCK") || [])
      .filter((p) => p.date === date)
      .sort((a, b) => (b.ts || "").localeCompare(a.ts || ""));

    const totalPcs = produksiList.reduce((a, p) => a + (p.jumlah || 0), 0);

    const simpan = async () => {
      if (!form.menuId) { pushNotif("Pilih menu dulu.", "warning"); return; }
      const jml = parseInt(form.jumlah);
      if (!jml || jml <= 0) { pushNotif("Jumlah harus lebih dari 0.", "warning"); return; }
      setBusy(true);
      try {
        const menu = menus.find((m) => m.id === form.menuId);
        const hppPerPcsProduksi = Math.ceil(getMenuHPPBreakdown(menu)?.hppSatuanPerPcs || 0);
        const entry = {
          id: uid(),
          date,
          ts: tsForDate(date),
          branchId,
          branchName,
          menuId: form.menuId,
          menuNama: menu?.nama || form.menuId,
          jumlah: jml,
          hppPerPcs: hppPerPcsProduksi,
          hppTotalProduksi: hppPerPcsProduksi * jml,
          keterangan: form.keterangan.trim(),
          createdBy: me?.user_id || null,
        };
        S.set("produksiCK", [...(S.get("produksiCK") || []), entry]);
        setForm((f) => ({ ...f, jumlah: "", keterangan: "" }));
        pushNotif("Produksi tercatat!", "success");
      } finally {
        setBusy(false);
      }
    };

    const hapus = (id) => {
      if (!confirm("Hapus catatan produksi ini?")) return;
      S.set("produksiCK", (S.get("produksiCK") || []).filter((x) => x.id !== id));
      pushNotif("Dihapus.", "warning");
    };

    // Absensi logic (sama seperti WorkerPage)
    const selectedAbs = useMemo(() => {
      const all = S.get("absensi") || [];
      return all.find((a) => a.user_id === userId && a.date === today()) || null;
    }, [tick, userId]);

    const sudahCheckout = !!selectedAbs?.checkout_ts;

    const doCheckin = () => {
      if (!userId) return;
      const targetDate = today(); // CK selalu hari ini, tidak bisa mundur
      const jadwalLibur = S.get("jadwalLibur") || {};
      const namaHari = ["Minggu","Senin","Selasa","Rabu","Kamis","Jumat","Sabtu"][new Date().getDay()];
      if (jadwalLibur[userId] && jadwalLibur[userId] === namaHari) {
        alert(`Hari ${namaHari} adalah jadwal libur Anda.`); return;
      }
      const all = S.get("absensi") || [];
      const ex = all.find((a) => a.user_id === userId && a.date === targetDate);
      if (ex?.checkin_ts) { pushNotif("Check-in sudah ada.", "warning"); return; }
      const row = ex
        ? { ...ex, checkin_ts: isoForDate(targetDate), branchId }
        : { id: uid(), user_id: userId, branchId, date: targetDate, checkin_ts: isoForDate(targetDate), checkout_ts: null };
      S.set("absensi", ex ? all.map((a) => a.id === row.id ? row : a) : [...all, row]);
      pushNotif("Check-in berhasil!", "success");
    };

    const doCheckout = () => {
      if (!userId) return;
      const targetDate = today();
      const all = S.get("absensi") || [];
      const ex = all.find((a) => a.user_id === userId && a.date === targetDate);
      if (!ex?.checkin_ts) { pushNotif("Belum check-in hari ini.", "warning"); return; }
      if (ex?.checkout_ts) { pushNotif("Sudah check-out.", "warning"); return; }
      S.set("absensi", all.map((a) => a.id === ex.id ? { ...a, checkout_ts: isoForDate(targetDate) } : a));
      pushNotif("Check-out berhasil!", "success");
    };

    const myMonthRows = useMemo(() => {
      const all = S.get("absensi") || [];
      return all.filter((a) => a.user_id === userId && String(a.date || "").startsWith(absMonth))
                .sort((a, b) => String(b.date).localeCompare(String(a.date)));
    }, [tick, userId, absMonth]);

    const calcMonth = useMemo(() => {
      let hadir = 0, menit = 0;
      for (const r of myMonthRows) {
        if (r.checkin_ts) hadir++;
        if (r.checkin_ts && r.checkout_ts) {
          const a = Date.parse(r.checkin_ts), b = Date.parse(r.checkout_ts);
          if (!isNaN(a) && !isNaN(b) && b > a) menit += Math.floor((b - a) / 60000);
        }
      }
      return { hadir, menit };
    }, [myMonthRows]);

    // Estimasi gaji dari profil
    const gajiHarian = parseFloat(me?.gajiHarian || 0) || 0;
    const estGaji = gajiHarian * calcMonth.hadir;

    return React.createElement("div", { className: "page" },
      React.createElement("div", { className: "page-header" },
        React.createElement("div", { className: "page-icon" }, "\uD83C\uDF73"),
        React.createElement("div", null,
          React.createElement("h2", null, "Central Kitchen"),
          React.createElement("p", { className: "page-sub" }, branchName, " \u2014 Catat Produksi Harian")
        )
      ),
      // Tabs
      React.createElement("div", { className: "tabs" },
        React.createElement("button", { className: "tab" + (tab === "produksi" ? " active" : ""), onClick: () => setTab("produksi") }, "Produksi"),
        React.createElement("button", { className: "tab" + (tab === "absensi" ? " active" : ""), onClick: () => setTab("absensi") }, "Absensi")
      ),

      // ── Tab Produksi ──
      tab === "produksi" && React.createElement("div", null,
      // Filter tanggal
      React.createElement("div", { className: "filter-bar mb8" },
        React.createElement("input", { type: "date", className: "inp inp-sm", value: date, onChange: (e) => setDate(e.target.value) })
      ),

      // Form input produksi
      React.createElement("div", { className: "form-card" },
        React.createElement("h4", null, "\u2795 Catat Produksi"),
        React.createElement("div", { className: "field-group" },
          React.createElement("label", null, "Produk"),
          React.createElement("select", { className: "inp", value: form.menuId, onChange: (e) => setForm((f) => ({ ...f, menuId: e.target.value })) },
            React.createElement("option", { value: "" }, "-- Pilih Menu --"),
            menus.map((m) => React.createElement("option", { key: m.id, value: m.id }, m.nama))
          )
        ),
        React.createElement("div", { className: "field-group" },
          React.createElement("label", null, "Jumlah Diproduksi (pcs)"),
          React.createElement("input", { type: "number", className: "inp", placeholder: "Contoh: 120", min: 1, value: form.jumlah, onChange: (e) => setForm((f) => ({ ...f, jumlah: e.target.value })) })
        ),
        React.createElement("div", { className: "field-group" },
          React.createElement("label", null, "Keterangan (opsional)"),
          React.createElement("input", { type: "text", className: "inp", placeholder: "Contoh: Batch pagi, Batch sore...", value: form.keterangan, onChange: (e) => setForm((f) => ({ ...f, keterangan: e.target.value })) })
        ),
        React.createElement("button", { className: "btn-primary", disabled: busy, onClick: simpan }, busy ? "Menyimpan..." : "Simpan Produksi")
      ),

      // Rekap hari ini
      React.createElement("div", { className: "kpi-grid" },
        React.createElement("div", { className: "kpi-card kpi-omzet" },
          React.createElement("div", { className: "kpi-label" }, "Total Produksi Hari Ini"),
          React.createElement("div", { className: "kpi-val", style: { color: "#22c55e" } }, totalPcs, " pcs")
        ),
        React.createElement("div", { className: "kpi-card kpi-modal" },
          React.createElement("div", { className: "kpi-label" }, "Jenis Produk"),
          React.createElement("div", { className: "kpi-val" }, new Set(produksiList.map((p) => p.menuId)).size, " item")
        )
      ),

      // Tabel produksi
      React.createElement("h3", { className: "section-title" }, "Catatan Produksi - ", date),
      produksiList.length === 0
        ? React.createElement("p", { className: "empty-txt" }, "Belum ada catatan produksi hari ini.")
        : React.createElement("div", { className: "tbl-wrap" },
            React.createElement("table", { className: "tbl" },
              React.createElement("thead", null,
                React.createElement("tr", null,
                  React.createElement("th", null, "Produk"),
                  React.createElement("th", null, "Jumlah"),
                  React.createElement("th", null, "Keterangan"),
                  React.createElement("th", null, "Jam"),
                  React.createElement("th", null, "")
                )
              ),
              React.createElement("tbody", null,
                produksiList.map((p) =>
                  React.createElement("tr", { key: p.id },
                    React.createElement("td", null, React.createElement("strong", null, p.menuNama)),
                    React.createElement("td", { style: { color: "#22c55e", fontWeight: 700 } }, p.jumlah, " pcs"),
                    React.createElement("td", { style: { color: "#9a9690" } }, p.keterangan || "-"),
                    React.createElement("td", { style: { color: "#9a9690", fontSize: 12 } }, p.ts || "-"),
                    React.createElement("td", null,
                      React.createElement("button", { className: "btn-danger-sm", onClick: () => hapus(p.id) }, "Hapus")
                    )
                  )
                ),
                React.createElement("tr", { style: { borderTop: "2px solid var(--border)", fontWeight: 700 } },
                  React.createElement("td", null, "TOTAL"),
                  React.createElement("td", { style: { color: "#22c55e" } }, totalPcs, " pcs"),
                  React.createElement("td", { colSpan: 3 })
                )
              )
            )
          )
      ), // end tab produksi

      // ── Tab Absensi CK ──
      tab === "absensi" && React.createElement("div", null,
        React.createElement("h3", { className: "section-title mt8" }, "Absensi Hari Ini"),
        sudahCheckout
          ? React.createElement("div", { className: "form-card", style: { background: "#2a1a1a", borderColor: "#ef4444" } },
              React.createElement("p", { style: { color: "#ef4444", fontWeight: 700, textAlign: "center" } }, "Anda sudah Check-out hari ini. Form absensi dikunci.")
            )
          : React.createElement("div", { className: "form-card" },
              React.createElement("div", { style: { fontWeight: 700 } }, "Tanggal: ", today()),
              React.createElement("div", { style: { fontSize: 12, color: "#9a9690", marginBottom: 10 } },
                "Check-in: ", fmtTs(selectedAbs?.checkin_ts), " | Check-out: ", fmtTs(selectedAbs?.checkout_ts)
              ),
              React.createElement("div", { className: "row-wrap" },
                React.createElement("button", { className: "btn-primary btn-sm", onClick: doCheckin }, "Check-in"),
                React.createElement("button", { className: "btn-secondary btn-sm", onClick: doCheckout }, "Check-out")
              )
            ),
        // KPI bulan ini
        React.createElement("div", { className: "field-group mt12" },
          React.createElement("label", null, "Rekap Bulan"),
          React.createElement("input", { type: "month", className: "inp inp-sm", value: absMonth, onChange: (e) => setAbsMonth(e.target.value) })
        ),
        React.createElement("div", { className: "kpi-grid mt8" },
          React.createElement("div", { className: "kpi-card kpi-omzet" },
            React.createElement("div", { className: "kpi-label" }, "Total Hadir"),
            React.createElement("div", { className: "kpi-val" }, calcMonth.hadir, " hari")
          ),
          React.createElement("div", { className: "kpi-card kpi-profit" },
            React.createElement("div", { className: "kpi-label" }, "Total Jam"),
            React.createElement("div", { className: "kpi-val" }, Math.round(calcMonth.menit / 60 * 10) / 10, " jam")
          ),
          gajiHarian > 0 && React.createElement("div", { className: "kpi-card kpi-peng", style: { gridColumn: "1/-1" } },
            React.createElement("div", { className: "kpi-label" }, "Est. Gaji Bulan Ini"),
            React.createElement("div", { className: "kpi-val" },
              fmtRp(estGaji),
              React.createElement("span", { style: { fontSize: 11, color: "#9a9690", marginLeft: 6 } },
                "(", fmtRp(gajiHarian), "/hari × ", calcMonth.hadir, " hari)"
              )
            )
          )
        ),
        // Riwayat absensi
        React.createElement("h3", { className: "section-title mt12" }, "Riwayat Absensi (", absMonth, ")"),
        myMonthRows.length === 0
          ? React.createElement("p", { className: "empty-txt" }, "Belum ada absensi.")
          : myMonthRows.map((r) =>
              React.createElement("div", { key: r.id, className: "peng-row" },
                React.createElement("div", { className: "peng-info" },
                  React.createElement("span", { className: "peng-ket" }, r.date),
                  React.createElement("span", { className: "peng-ts" },
                    "Masuk: ", fmtTs(r.checkin_ts), " | Keluar: ", r.checkout_ts ? fmtTs(r.checkout_ts) : "Belum"
                  )
                ),
                r.checkin_ts && r.checkout_ts && React.createElement("div", { className: "peng-right" },
                  React.createElement("span", { style: { fontSize: 12, color: "#22c55e" } },
                    Math.round((Date.parse(r.checkout_ts) - Date.parse(r.checkin_ts)) / 60000), " menit"
                  )
                )
              )
            )
      ) // end tab absensi
    );
  }

  // ─── App Root ──────────────────────────────────────────────────────────────
  function App() {
    const [authSession, setAuthSession] = useState(null);
    const [profile, setProfile] = useState(null);
    const [loading, setLoading] = useState(true);
    const [notifs, setNotifs] = useState([]);

    const pushNotif = useCallback((msg, type = "success") => {
      const id = uid();
      setNotifs((n) => [...n, { id, msg, type }]);
    }, []);

    const removeNotif = useCallback((id) => setNotifs((n) => n.filter((x) => x.id !== id)), []);

    useEffect(() => { S.setErrorHandler((msg) => pushNotif(String(msg), "warning")); }, [pushNotif]);

    const syncAfterLogin = useCallback(async (session) => {
      setAuthSession(session);
      if (!session) { S.reset(); setProfile(null); setLoading(false); return; }
      setLoading(true);
      try {
        const { data: prof, error } = await sb.from("profiles").select("*").eq("user_id", session.user.id).single();
        if (error) throw error;
        if (!prof || prof.role === "none") {
          pushNotif("Akun kamu belum diundang oleh Owner (akses ditolak).", "warning");
          await sb.auth.signOut(); return;
        }
        setProfile(prof);
        await syncBrandingFromDb().catch(() => {});
        await S.loadAll();
        if (prof.role === "owner") await S.loadKey("profiles").catch(() => {});
        S.startRealtime();
      } catch (ex) {
        pushNotif(ex?.message || String(ex), "warning");
      } finally {
        setLoading(false);
      }
    }, [pushNotif]);

    useEffect(() => {
      let unsub = null;
      sb.auth.getSession().then(({ data }) => syncAfterLogin(data?.session || null));
      const { data } = sb.auth.onAuthStateChange((_event, session) => syncAfterLogin(session));
      unsub = data?.subscription;
      return () => { try { unsub?.unsubscribe(); } catch {} };
    }, [syncAfterLogin]);

    return React.createElement(React.Fragment, null,
      !authSession
        ? React.createElement(LoginPage, null)
        : React.createElement("div", { className: "app-wrap" },
            React.createElement("nav", { className: "top-nav" },
              React.createElement("span", { className: "nav-brand" }, "Evora"),
              React.createElement("span", { className: "nav-role" }, profile?.role === "owner" ? "Owner" : profile?.role === "worker" ? (() => { const br = (S.get("branches") || []).find((b) => b.id === profile.branchId); return br?.type === "central_kitchen" ? "Pekerja CK" : "Pekerja"; })() : profile?.role === "investor" ? "Investor" : "\u2014"),
              React.createElement("button", { className: "btn-logout", onClick: () => sb.auth.signOut() }, "Keluar")
            ),
            React.createElement("div", { className: "content-wrap" },
              loading && React.createElement("p", { className: "info-txt" }, "Memuat data..."),
              !loading && profile?.role === "worker" && (() => {
                const branches = S.get("branches") || [];
                const myBranch = branches.find((b) => b.id === profile.branchId);
                return myBranch?.type === "central_kitchen"
                  ? React.createElement(KitchenPage, { pushNotif, me: profile })
                  : React.createElement(WorkerPage, { pushNotif, me: profile });
              })(),
              !loading && profile?.role === "owner"    && React.createElement(OwnerPage, { pushNotif, me: profile }),
              !loading && profile?.role === "investor" && React.createElement(InvestorPage, { investorId: profile.investorId, pushNotif, me: profile })
            )
          ),
      React.createElement("div", { className: "notif-stack" },
        notifs.map((n) => React.createElement(Notif, { key: n.id, msg: n.msg, type: n.type, onClose: () => removeNotif(n.id) }))
      )
    );
  }

  var root = ReactDOM.createRoot(document.getElementById("root"));
  root.render(React.createElement(App, null));
})();
