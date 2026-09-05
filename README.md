# MigMaster Cloudflare Backend

Backend WebSocket bridge for the MigReborn Developer API, packaged for Cloudflare Workers + Durable Objects.

## Deploy from Cloudflare Dashboard

1. Create a Cloudflare account and open **Workers & Pages**.
2. Create a Worker project and connect this GitHub repository, or upload this project through your normal Git workflow.
3. Build/deploy command: `npx wrangler deploy`.
4. The Worker exposes:
   - `GET /health`
   - `wss://YOUR-WORKER.workers.dev/ws`
5. Optional secret: `DASHBOARD_TOKEN`.

## Deploy with Wrangler

```bash
npm install
npx wrangler login
npx wrangler deploy
```

Cloudflare Durable Objects are used for the dashboard WebSocket session. The Worker opens separate upstream WebSockets for the configured accounts and forwards API events to the dashboard.

## Important

- Do not put Mig33 usernames/passwords into this repository.
- Credentials are supplied at runtime by the dashboard and kept in the active session only.
- The upstream endpoint defaults to `wss://developer.mig33.id/developer/ws`.
- Cloudflare's WebSocket/Durable Objects runtime has platform limits; for long-lived outbound WebSockets the Durable Object may need to reconnect.
