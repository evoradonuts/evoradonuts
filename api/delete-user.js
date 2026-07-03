// Vercel Serverless Function
// Simpan sebagai: /api/delete-user.js
//
// Fungsi: Owner bisa menonaktifkan (SOFT-DELETE) akun WORKER / INVESTOR (bukan owner).
// Akun TIDAK dihapus dari Supabase Auth — hanya di-ban dari login, dan ditandai
// status="deleted" di tabel profiles. Histori transaksi bisnis tetap utuh.
// Aman: validasi token owner + cek role owner di tabel profiles + cek role target bukan owner.
//
// PRASYARAT: tabel `profiles` harus punya kolom: status (text), deleted_at (timestamptz),
// deleted_by (uuid), deleted_reason (text). Kalau belum ada, jalankan dulu:
//   ALTER TABLE profiles ADD COLUMN IF NOT EXISTS status text DEFAULT 'active';
//   ALTER TABLE profiles ADD COLUMN IF NOT EXISTS deleted_at timestamptz;
//   ALTER TABLE profiles ADD COLUMN IF NOT EXISTS deleted_by uuid;
//   ALTER TABLE profiles ADD COLUMN IF NOT EXISTS deleted_reason text;

module.exports = async (req, res) => {
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  const SUPABASE_URL = process.env.SUPABASE_URL;
  const ANON = process.env.SUPABASE_ANON_KEY;
  const SERVICE = process.env.SUPABASE_SERVICE_ROLE_KEY;

  try {
    if (!SUPABASE_URL || !ANON || !SERVICE) {
      return res.status(500).json({ error: "Env di Vercel belum lengkap (SUPABASE_URL/ANON/SERVICE)." });
    }

    const authHeader = req.headers.authorization || "";
    const token = authHeader.startsWith("Bearer ") ? authHeader.slice("Bearer ".length) : null;
    if (!token) return res.status(401).json({ error: "Butuh Authorization Bearer token (owner harus login)." });

    const body = typeof req.body === "string" ? JSON.parse(req.body) : (req.body || {});
    const target_user_id = body?.target_user_id;
    const target_email = body?.target_email || null;
    const reason = String(body?.reason || "").trim();

    if (!target_user_id) return res.status(400).json({ error: "target_user_id wajib." });

    // 1) Validasi token owner -> ambil user id
    const userResp = await fetch(`${SUPABASE_URL}/auth/v1/user`, {
      headers: { apikey: ANON, Authorization: `Bearer ${token}` },
    });
    const userJson = await userResp.json();
    if (!userResp.ok) return res.status(401).json({ error: userJson?.msg || userJson?.error || "Token owner tidak valid." });
    const ownerId = userJson?.id;
    if (!ownerId) return res.status(401).json({ error: "Tidak bisa membaca owner id." });

    // 2) Cek role owner dari tabel profiles (pakai service role)
    const profResp = await fetch(
      `${SUPABASE_URL}/rest/v1/profiles?select=role&user_id=eq.${ownerId}&limit=1`,
      { headers: { apikey: SERVICE, Authorization: `Bearer ${SERVICE}` } }
    );
    const profJson = await profResp.json();
    const ownerRole = Array.isArray(profJson) && profJson[0]?.role;
    if (ownerRole !== "owner") return res.status(403).json({ error: "Hanya owner yang boleh menghapus akun." });

    // 3) Cek role target supaya tidak bisa hapus owner
    const targetResp = await fetch(
      `${SUPABASE_URL}/rest/v1/profiles?select=role,email&user_id=eq.${target_user_id}&limit=1`,
      { headers: { apikey: SERVICE, Authorization: `Bearer ${SERVICE}` } }
    );
    const targetJson = await targetResp.json();
    const targetRole = Array.isArray(targetJson) && targetJson[0]?.role;
    const targetEmailFromDb = Array.isArray(targetJson) ? targetJson[0]?.email : null;
    if (!targetRole) return res.status(404).json({ error: "Target user tidak ditemukan di profiles." });
    if (targetRole === "owner") return res.status(403).json({ error: "Akun owner tidak boleh dihapus dari aplikasi." });

    // 4) SOFT-DELETE: cabut akses login (ban ~100 tahun), TIDAK hapus user dari Auth.
    //    "banned_until" dipakai GoTrue untuk menolak login & refresh token baru.
    //    Sesi/JWT yang sedang aktif akan tetap valid sampai kedaluwarsa (biasanya
    //    1 jam), lalu otomatis tidak bisa refresh lagi.
    const banResp = await fetch(`${SUPABASE_URL}/auth/v1/admin/users/${target_user_id}`, {
      method: "PUT",
      headers: {
        apikey: SERVICE,
        Authorization: `Bearer ${SERVICE}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ ban_duration: "876000h" }), // ~100 tahun
    });
    if (!banResp.ok) {
      const t = await banResp.text();
      return res.status(400).json({ error: `Gagal mencabut akses login: ${t}` });
    }

    // 5) Tandai status di tabel profiles (histori & data bisnis TETAP ada, tidak dihapus)
    const patchResp = await fetch(
      `${SUPABASE_URL}/rest/v1/profiles?user_id=eq.${target_user_id}`,
      {
        method: "PATCH",
        headers: {
          apikey: SERVICE,
          Authorization: `Bearer ${SERVICE}`,
          "Content-Type": "application/json",
          Prefer: "return=minimal",
        },
        body: JSON.stringify({
          status: "deleted",
          deleted_at: new Date().toISOString(),
          deleted_by: ownerId,
          deleted_reason: reason || null,
        }),
      }
    );
    if (!patchResp.ok) {
      const t = await patchResp.text();
      // Login sudah dicabut tapi profil gagal ditandai — tetap laporkan sebagai error
      // supaya owner tahu perlu retry, daripada status jadi tidak konsisten diam-diam.
      return res.status(400).json({ error: `Akses login sudah dicabut, tapi gagal update status profil: ${t}` });
    }

    // 6) Bersihkan invites pending jika email diketahui (opsional, tidak mempengaruhi histori)
    const finalEmail = target_email || targetEmailFromDb;
    if (finalEmail) {
      await fetch(`${SUPABASE_URL}/rest/v1/invites?email=eq.${encodeURIComponent(finalEmail)}`, {
        method: "DELETE",
        headers: { apikey: SERVICE, Authorization: `Bearer ${SERVICE}` },
      }).catch(() => {});
    }

    return res.json({ ok: true, userId: target_user_id, softDeleted: true });
  } catch (e) {
    return res.status(500).json({ error: e?.message || String(e) });
  }
};

