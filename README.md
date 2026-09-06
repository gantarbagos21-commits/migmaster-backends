# MigMaster Cloudflare Backend — FINAL v2 Anti-Macet Vote

Versi FINAL v2 ini dibuat khusus untuk **anti-macet vote** sambil mempertahankan **Join Room** dan seluruh perbaikan backend sebelumnya.

## Anti-macet vote — inti perubahan

- `room.kick` dikirim **langsung** melalui WebSocket.
- Backend **tidak pernah menunggu** `room.kick.result`, `room.kick.queued`, atau `job.get` sebelum mengirim vote berikutnya.
- Delay vote hanya menjadi **jarak pengiriman** dan dikendalikan frontend.
- Mendukung alias frontend: `delayMs`, `voteDelayMs`, `socketDelayMs`, `loopDelayMs`.
- Maksimum 10 target dan maksimum 100 loop tetap dipertahankan.
- Pola 10 WebSocket × 10 target tetap didukung selama akun online, `session.ready`, sudah terkonfirmasi Join Room, dan memiliki `rooms.kick`.
- Satu run Kick All/Auto Kick aktif pada satu waktu. Klik/run kedua ditolak agar dua rangkaian vote tidak saling menimpa.
- Metadata vote untuk pencatatan hasil async dibatasi agar respons upstream yang terlambat/tidak datang tidak membuat memori bertumbuh tanpa batas.

## Join Room tetap dipertahankan

- `joinAll` tetap mengirim `room.join` ke 10 akun yang sudah `session.ready`.
- `room.join.result` tetap menjadi konfirmasi membership.
- `joinedRooms` dan `requestedRooms` tetap dikirim saat dashboard reconnect.
- Kick tetap memeriksa membership room yang sudah dikonfirmasi sebelum `room.kick` dikirim.

## Login / session

Urutan login tetap mengikuti protokol Mig33:

`auth.required` → `developer.login` → `session.ready`

`session.ready` adalah tanda login sukses. Tidak ada auto-relogin hanya karena dashboard/UI reconnect.

## Keep-alive

Backend tetap mengirim application-level:

```json
{"type":"ping"}
```

dengan interval 50 detik.

## Dashboard reconnect

Menutup/reconnect dashboard **tidak menutup WebSocket akun Mig33**. Logout/Disconnect eksplisit tetap menutup socket akun.

## Outbound WebSocket Cloudflare

Koneksi akun ke upstream menggunakan pola Cloudflare Workers:

`fetch(MIG_WS_URL, { headers: { Upgrade: "websocket" } })`

Jika upstream tidak mengembalikan HTTP 101, backend menampilkan status HTTP yang diterima (termasuk 523) agar diagnosis lebih jelas.

## Environment

- `MIG_WS_URL` = `wss://developer.mig33.id/developer/ws`
- `DASHBOARD_TOKEN` = token dashboard jika digunakan

## Deploy

```bash
npm install
npx wrangler deploy --config ./wrangler.json
```

## Endpoint frontend

Jika Worker tetap memakai domain yang sama:

`wss://migmaster-backends.gantarbagos21.workers.dev/ws`

## Catatan penting

Mode anti-macet di sini berarti **backend tidak memblokir pengiriman vote berdasarkan hasil vote/job sebelumnya**. Delay frontend tetap dapat digunakan untuk mengatur kecepatan pengiriman.

Jika upstream tetap mengembalikan HTTP 523, itu adalah masalah keterjangkauan endpoint upstream dari jaringan Cloudflare dan bukan antrean vote lokal.
