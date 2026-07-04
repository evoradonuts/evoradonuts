// Vercel Serverless Function
// Simpan sebagai: /api/delete-user.js
//
// PENTING: Ini SOFT-DEACTIVATE, bukan hard delete.
// - Akses login user di-ban (bukan dihapus) via ban_duration di Supabase Auth.
// - Baris profil TETAP ADA, cuma role diubah jadi "none" (dianggap nonaktif
//   oleh isActiveProfile() dan semua filter role==='worker' di frontend).
// - Email & data profil TIDAK diubah/diacak, supaya kalau owner mau
//   mengaktifkan lagi, tinggal buka ban + kembalikan role — bukan buat ulang
//   akun dari nol.

module.exports = async (req, res) => {
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  const SUPABASE_URL = process.env.SUPABASE_URL;
  const ANON = process.env.SUPABASE_ANON_KEY;
  const SERVICE = process.env.SUPABASE_SERVICE_ROLE_KEY;

  const parseBody = (rawBody) => {
    if (typeof rawBody !== "string") return rawBody || {};
    try { return JSON.parse(rawBody); }
    catch { throw new Error("Body request tidak valid."); }
  };

  const getJsonOrText = async (resp) => {
    const text = await resp.text();
    if (!text) return null;
    try { return JSON.parse(text); }
    catch { return text; }
  };

  try {
    if (!SUPABASE_URL || !ANON || !SERVICE) {
      return res.status(500).json({ error: "Env di Vercel belum lengkap (SUPABASE_URL/ANON/SERVICE)." });
    }

    const authHeader = req.headers.authorization || "";
    const token = authHeader.startsWith("Bearer ") ? authHeader.slice("Bearer ".length) : null;
    if (!token) return res.status(401).json({ error: "Butuh Authorization Bearer token (owner harus login)." });

    const body = parseBody(req.body);
    const target_user_id = String(body?.target_user_id || "").trim();
    const reason = String(body?.reason || "").trim() || null;

    if (!target_user_id) return res.status(400).json({ error: "target_user_id wajib." });

    // 1) Validasi token owner -> ambil user id
    const userResp = await fetch(`${SUPABASE_URL}/auth/v1/user`, {
      headers: { apikey: ANON, Authorization: `Bearer ${token}` },
    });
    const userJson = await getJsonOrText(userResp);
    if (!userResp.ok) {
      return res.status(401).json({ error: userJson?.msg || userJson?.error || "Token owner tidak valid." });
    }
    const ownerId = userJson?.id;
    if (!ownerId) return res.status(401).json({ error: "Tidak bisa membaca owner id." });
    if (ownerId === target_user_id) {
      return res.status(403).json({ error: "Owner tidak boleh menonaktifkan akunnya sendiri dari aplikasi." });
    }

    // 2) Cek role owner dari tabel profiles (pakai service role)
    const profResp = await fetch(
      `${SUPABASE_URL}/rest/v1/profiles?select=role&user_id=eq.${ownerId}&limit=1`,
      { headers: { apikey: SERVICE, Authorization: `Bearer ${SERVICE}` } }
    );
    const profJson = await getJsonOrText(profResp);
    const ownerRole = Array.isArray(profJson) && profJson[0]?.role;
    if (ownerRole !== "owner") {
      return res.status(403).json({ error: "Hanya owner yang boleh menonaktifkan akun." });
    }

    // 3) Cek role target supaya tidak bisa nonaktifkan owner, dan supaya
    //    tidak nonaktifkan akun yang sudah nonaktif (idempoten & jelas pesannya)
    const targetResp = await fetch(
      `${SUPABASE_URL}/rest/v1/profiles?select=user_id,role,email,display_name,branchId,investorId&user_id=eq.${target_user_id}&limit=1`,
      { headers: { apikey: SERVICE, Authorization: `Bearer ${SERVICE}` } }
    );
    const targetJson = await getJsonOrText(targetResp);
    const targetProfile = Array.isArray(targetJson) ? targetJson[0] : null;
    const targetRole = targetProfile?.role;

    if (!targetRole) return res.status(404).json({ error: "Target user tidak ditemukan di profiles." });
    if (targetRole === "owner") return res.status(403).json({ error: "Akun owner tidak boleh dinonaktifkan dari aplikasi." });
    if (targetRole === "none") return res.status(409).json({ error: "Akun ini sudah nonaktif sebelumnya." });

    // 4) BAN akses login (BUKAN hapus). ban_duration ~100 tahun = efektif
    //    permanen sampai owner cabut manual. Baris auth.users TETAP ADA,
    //    jadi bisa di-unban kapan saja tanpa buat akun baru.
    const banResp = await fetch(`${SUPABASE_URL}/auth/v1/admin/users/${target_user_id}`, {
      method: "PUT",
      headers: {
        apikey: SERVICE,
        Authorization: `Bearer ${SERVICE}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ ban_duration: "876000h" }),
    });
    if (!banResp.ok) {
      const banErr = await getJsonOrText(banResp);
      return res.status(400).json({ error: `Gagal mencabut akses login: ${banErr?.msg || banErr?.error || banErr || "unknown error"}` });
    }

    // 5) Tandai profil nonaktif. Sengaja TIDAK mengubah email/branchId/
    //    investorId/display_name — data asli dipertahankan supaya histori
    //    (transaksi, absensi, gaji) tetap nyambung dan akun bisa
    //    diaktifkan lagi persis seperti semula kalau perlu.
    const profileUpdateResp = await fetch(`${SUPABASE_URL}/rest/v1/profiles?user_id=eq.${target_user_id}`, {
      method: "PATCH",
      headers: {
        apikey: SERVICE,
        Authorization: `Bearer ${SERVICE}`,
        "Content-Type": "application/json",
        Prefer: "return=minimal",
      },
      body: JSON.stringify({ role: "none" }),
    });

    const warnings = [];
    if (!profileUpdateResp.ok) {
      // Login sudah keburu diban di langkah 4. Kalau ini gagal, laporkan
      // dengan jelas supaya owner tahu harus cek manual — jangan diam-diam.
      const patchErr = await getJsonOrText(profileUpdateResp);
      warnings.push(`Akses login sudah dicabut, TAPI status profil gagal diupdate: ${patchErr?.message || patchErr || "unknown error"}. Cek manual di Supabase.`);
    }

    return res.json({
      ok: true,
      userId: target_user_id,
      deactivated: true,
      previousRole: targetRole,
      reason,
      deactivatedAt: new Date().toISOString(),
      warnings,
    });
  } catch (e) {
    return res.status(500).json({ error: e?.message || String(e) });
  }
};
