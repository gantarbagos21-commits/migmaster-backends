# MigMaster FINAL v3 — Node.js Persistent Backend

Backend khusus Node.js persistent untuk MigMaster. Versi ini mempertahankan fix Join Room, login/session, dashboard reconnect, keep-alive, participant list, wallet, Auto Kick, dan Kick All anti-macet dari versi V2, tetapi koneksi akun ke Mig33 dilakukan langsung dari runtime Node.js.

## Kenapa Node.js persistent

Versi Cloudflare sebelumnya gagal pada koneksi keluar ke `wss://developer.mig33.id/developer/ws` dengan pesan `Fetch API cannot load`. V3 menghilangkan lapisan outbound WebSocket Cloudflare dan menggunakan package `ws` langsung dari Node.js.

## Anti-macet vote

Kick All menggunakan mode `anti-stall-direct-dispatch-v3`:
- payload tetap `room.kick` sesuai API;
- tidak menunggu `room.kick.queued`;
- tidak menunggu `room.kick.result`;
- tidak menunggu `job.get`;
- vote berikutnya dikirim langsung setelah `socket.send()` berhasil;
- delay hanya menjadi pembatas kecepatan pengiriman dan dikontrol frontend;
- maksimal 10 target dan 10 akun per loop;
- hanya satu Kick run aktif pada satu waktu agar dua klik/proses tidak saling menimpa;
- metadata dispatch dibatasi agar response upstream yang macet tidak membuat memori terus bertambah.

## Join Room tetap

Join All tetap mengirim `room.join` ke akun yang sudah `session.ready`. `room.join.result` tetap digunakan sebagai konfirmasi membership. Kick hanya dikirim dari akun yang sudah terkonfirmasi masuk room dan memiliki `rooms.kick` bila permission tersedia.

## Login dan keep-alive

- login: `auth.required` → `developer.login` → `session.ready`;
- `session.ready` adalah indikator login sukses;
- client mengirim JSON `{"type":"ping"}` setiap 50 detik;
- API tidak memiliki command logout khusus, sehingga logout dilakukan dengan menutup WebSocket;
- dashboard disconnect tidak memutus WebSocket akun dan tidak memicu relogin otomatis.

## Struktur

```text
backend/
├── src/
│   └── index.js
├── package.json
└── README.md
```

## Environment Variables

```text
PORT=3000
DASHBOARD_TOKEN=isi_token_yang_sama_dengan_frontend
MIG_WS_URL=wss://developer.mig33.id/developer/ws
```

`PORT` dibaca dari environment provider. Server bind ke `0.0.0.0`.

## Jalankan lokal

```bash
npm install
npm start
```

Health check:

```text
GET /health
```

Dashboard WebSocket:

```text
wss://DOMAIN-BACKEND-ANDA/ws?token=TOKEN
```

Jika frontend tidak menggunakan token, kosongkan `DASHBOARD_TOKEN`.

## Deploy Railway / host Node.js lain

1. Upload folder ini ke GitHub.
2. Buat service Node.js dari repository tersebut.
3. Set environment variables `DASHBOARD_TOKEN` dan `MIG_WS_URL`.
4. Start command: `npm start`.
5. Pastikan service mendapatkan public HTTPS/WSS domain.
6. Di frontend isi URL backend WebSocket dengan:
   `wss://DOMAIN-BACKEND-ANDA/ws?token=TOKEN`
7. Buka Log dan pastikan akun menunjukkan `WebSocket OPEN`, `auth.required`, lalu `session.ready`.
8. Jalankan Enter Room — All dan tunggu `JOIN BERHASIL`.
9. Setelah room confirmed, jalankan Kick All.

## Penting

Jangan deploy V3 sebagai Cloudflare Worker. V3 memang ditujukan untuk runtime Node.js persistent. Railway, VPS, Render, atau host Node.js persistent lain dapat digunakan.

## Pemeriksaan

```bash
npm install
npm run check
```

Tidak ada konfigurasi Durable Object/Wrangler di V3.
