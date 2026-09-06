MigMaster Integrated Kick 10x10

KICK FLOW
- WebSocket/account #1..#10 are the voters.
- Targets #1..#10 are dispatched across every eligible voter.
- 10 voters x 10 targets = 100 room.kick dispatches per loop when all 10 accounts are eligible.
- loopCount is controlled by the frontend.
- Delay antar vote is controlled by the frontend and sent to backend as delayMs/voteDelayMs/socketDelayMs.
- Backend does not wait for room.kick.result or job.get before dispatching the next vote.
- loopDelayMs is also accepted from the frontend for compatibility, but the primary inter-vote delay is delayMs/voteDelayMs.

FILES
backend/  -> Cloudflare Worker/Durable Object backend
frontend/ -> Vercel frontend
