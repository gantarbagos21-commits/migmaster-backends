# MigMaster Backend

Node.js WebSocket backend for MigMaster Real Queue.

## Run locally

```bash
npm install
npm start
```

Health check:

`GET /health`

Dashboard WebSocket:

`ws://localhost:3000/ws`

## Environment variables

Copy `.env.example` to `.env` when running locally and set values as needed.

- `PORT` — listening port; hosting providers usually provide this automatically.
- `DASHBOARD_TOKEN` — optional token required as `?token=...` on `/ws`.
- `MIG_WS_URL` — upstream Mig33 developer WebSocket endpoint.

## Important hosting note

This backend uses a persistent Node.js HTTP server and WebSocket connections. It should be deployed on a long-running Node.js service such as Render, Railway, Fly.io, or a VPS. Vercel's normal serverless functions are not suitable for this persistent WebSocket server.
