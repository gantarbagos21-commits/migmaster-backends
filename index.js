import { DurableObject } from "cloudflare:workers";

const DEFAULT_MIG_WS_URL = "wss://developer.mig33.id/developer/ws";
const ROOM_TEXT_EVENT_TYPES = new Set(["room.text", "room.text.received", "room.message.received", "room.message"]);
const VOTE_KICK_TEXT_RE = /\ba\s*vote\s+to\s+kick\b/i;

function safeString(v, max = 300) { return String(v ?? "").slice(0, max); }
function payloadOf(data) { return data?.data ?? data?.result ?? data ?? {}; }
function errorOf(data) {
  const p = payloadOf(data);
  const status = String(p?.status || p?.state || data?.status || "").toLowerCase();
  const err = p?.error || data?.error;
  if (err || ["error", "failed", "failure", "rejected", "denied"].includes(status)) {
    return safeString(p?.message || data?.message || err || status);
  }
  return "";
}
function roomMatches(a, b) { return String(a || "").trim().toLowerCase() === String(b || "").trim().toLowerCase(); }
function clampMs(v, fallback = 0) {
  const n = Number(v); return Math.max(0, Math.min(60000, Number.isFinite(n) ? n : fallback));
}
function terminal(status) { return ["completed","complete","success","succeeded","done","finished","failed","failure","error","cancelled","canceled","rejected"].includes(status); }
function jobIdOf(data) { const p = payloadOf(data); return String(p?.job?.job_id || p?.job?.id || p?.job_id || data?.job_id || "").trim(); }
function jobStatusOf(data) { const p = payloadOf(data); return String(p?.status || p?.state || p?.job?.status || data?.status || "").trim().toLowerCase(); }

function normalizeUsers(value) {
  const out = [], seen = new Set();
  const add = v => {
    if (typeof v !== "string") return;
    const x = v.trim();
    if (!x || seen.has(x) || /^usr_[A-Za-z0-9_-]+$/.test(x)) return;
    if (/^(participants|users|members|people|list|data|result|status|success|room)$/i.test(x)) return;
    seen.add(x); out.push(x);
  };
  const walk = (node, depth = 0) => {
    if (node == null || depth > 20) return;
    if (Array.isArray(node)) return node.forEach(x => walk(x, depth + 1));
    if (typeof node !== "object") return;
    for (const k of ["username","user_name","userName","nickname","nick","display_name","displayName","handle","login"]) add(node[k]);
    for (const k of ["user","member","participant","account","profile"]) if (node[k] && typeof node[k] === "object") walk(node[k], depth + 1);
    for (const [k, child] of Object.entries(node)) {
      if (child && typeof child === "object") walk(child, depth + 1);
      if (/^(participants|users|members|people|items|list)$/i.test(k) && child && typeof child === "object" && !Array.isArray(child)) Object.keys(child).forEach(add);
    }
  };
  walk(value); return out.slice(0, 500);
}
function eventRoom(data) {
  const p = payloadOf(data);
  const c = [data?.room,data?.room_name,data?.roomName,data?.room_id,data?.roomId,data?.record?.room,data?.record?.room_name,
    data?.data?.room,data?.data?.room_name,data?.data?.roomName,data?.data?.room_id,data?.data?.roomId,
    data?.result?.room,data?.result?.room_name,data?.result?.roomName,data?.result?.room_id,data?.result?.roomId,
    p?.room,p?.room_name,p?.roomName,p?.room_id,p?.roomId,p?.record?.room];
  return c.find(x => typeof x === "string" && x.trim())?.trim() || "";
}
function textCandidates(data) {
  const out = [], seen = new Set(), add = v => { if (typeof v === "string" && v.trim() && !seen.has(v)) { seen.add(v); out.push(v.trim()); } };
  const walk = (n, d = 0) => {
    if (!n || typeof n !== "object" || d > 8) return;
    if (Array.isArray(n)) return n.forEach(x => walk(x, d + 1));
    ["message","text","content","body","caption","msg","room_message","roomMessage","chat_message","chatMessage"].forEach(k => add(n[k]));
    ["record","data","result","event","payload","message","message_data","messageData"].forEach(k => { if (n[k] && typeof n[k] === "object") walk(n[k], d + 1); });
  };
  walk(data); return out;
}
function senderCandidates(data) {
  const out = [], seen = new Set(), add = v => { if (typeof v === "string" && v.trim() && !seen.has(v.trim())) { seen.add(v.trim()); out.push(v.trim()); } };
  const walk = (n, d = 0) => {
    if (!n || typeof n !== "object" || d > 8) return;
    if (Array.isArray(n)) return n.forEach(x => walk(x, d + 1));
    for (const k of ["sender","from","author","source","origin","actor","created_by","createdBy","sender_type","senderType","sender_name","senderName","from_type","fromType","source_type","sourceType"]) {
      const s = n[k];
      if (typeof s === "string") add(s);
      else if (s && typeof s === "object") { ["type","name","username","user_name","userName","display_name","displayName","role","id","kind","category","sender_type","senderType","entity_type","entityType"].forEach(x => add(s[x])); walk(s, d + 1); }
    }
    ["record","data","result","event","payload","message"].forEach(k => { if (n[k] && typeof n[k] === "object") walk(n[k], d + 1); });
  };
  walk(data); return out;
}
function isRoomSender(data, room) {
  const rn = String(room || "").trim().toLowerCase(), senders = senderCandidates(data);
  if (senders.some(s => ["room",rn,`room:${rn}`,`room/${rn}`].includes(s.toLowerCase()))) return true;
  if (senders.length) return false;
  return ROOM_TEXT_EVENT_TYPES.has(String(data?.type || "").trim().toLowerCase());
}
function detectVoteKick(data) {
  const room = eventRoom(data); if (!room || !isRoomSender(data, room)) return null;
  for (const text of textCandidates(data)) if (VOTE_KICK_TEXT_RE.test(text)) {
    const target = /\ba\s*vote\s+to\s+kick\s+(.+?)(?:\s+has\s+been\s+started\s+by\b|[.!?]|$)/i.exec(text)?.[1]?.trim() || "";
    const startedBy = /\bhas\s+been\s+started\s+by\s+(.+?)(?:,|\.|$)/i.exec(text)?.[1]?.trim() || "";
    const p = payloadOf(data);
    return { room, targetUsername: target, startedBy, sender: "room", message: text,
      eventId: String(data?.event_id || data?.eventId || data?.message_id || data?.messageId || data?.data?.event_id || p?.event_id || p?.message_id || "").trim() };
  }
  return null;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/health") return Response.json({ ok: true, service: "migmaster-cloudflare-backend", websocket: "/ws", upstream: env.MIG_WS_URL || DEFAULT_MIG_WS_URL });
    if (url.pathname !== "/ws") return new Response("MigMaster Cloudflare Backend\nUse /ws for WebSocket or /health for health check.", { status: 404 });
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") return new Response("Expected WebSocket", { status: 426 });
    if (env.DASHBOARD_TOKEN && url.searchParams.get("token") !== env.DASHBOARD_TOKEN) return new Response("Unauthorized", { status: 401 });
    const id = env.MIGMASTER_SESSIONS.idFromName(crypto.randomUUID());
    return env.MIGMASTER_SESSIONS.get(id).fetch(request);
  }
};

export class MigMasterSession extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env); this.env = env; this.dashboard = null; this.accounts = Array.from({ length: 10 }, () => this.newAccount());
    this.commandQueueRunning = false; this.voteKickDetectorIndex = 0; this.autoKick = { enabled:false, room:"", thresholdMs:30000, countdownMs:60000, remainingMs:60000, targets:[], targetDelaysMs:[], loopCount:1, socketDelayMs:0, sequentialMode:false, source:null, countdownEndAt:0, timer:null, triggered:false };
  }
  newAccount() { return { ws:null, username:"", password:"", permissions:[], ready:false, joined:new Set(), requested:new Set(), pendingJoin:"", pingTimer:null, lastHeartbeat:0, pollTimer:null, pendingJobs:new Map(), reconnectTimer:null, reconnectAttempt:0, authFailed:false, manual:false }; }
  send(obj) { if (this.dashboard?.readyState === WebSocket.OPEN) try { this.dashboard.send(JSON.stringify(obj)); } catch {} }
  status(i, status, extra={}) { this.send({type:"status", index:i, status, ...extra}); }
  async fetch(request) {
    const pair = new WebSocketPair(); const client = pair[0], server = pair[1]; server.accept(); this.dashboard = server;
    this.send({type:"dashboard.ready", accounts:10});
    server.addEventListener("message", e => this.onDashboardMessage(e.data));
    server.addEventListener("close", () => { this.stopAutoKick("dashboard-closed"); for (let i=0;i<10;i++) this.closeAccount(i,true); this.dashboard=null; });
    return new Response(null, { status:101, webSocket:client });
  }
  requiredPermission(p) { return ({"room.join":"rooms.join","room.leave":"rooms.leave","room.participants":"rooms.read","room.kick":"rooms.kick","room.send_message":"messaging.send","wallet.balance":"wallet.read"})[p?.type] || ""; }
  canSend(i,p) {
    const a=this.accounts[i]; if (!a.ws || a.ws.readyState!==WebSocket.OPEN || !a.ready) return false;
    const perm=this.requiredPermission(p); return !(perm && a.permissions.length && !a.permissions.includes(perm));
  }
  sendAccount(i,p) { if(!this.canSend(i,p)) { this.send({type:"log",index:i,message:"Command dilewati: session/permission belum siap"}); return false; } this.accounts[i].ws.send(JSON.stringify(p)); return true; }
  stopPing(i) { const a=this.accounts[i]; if(a.pingTimer) clearInterval(a.pingTimer); a.pingTimer=null; }
  stopPoll(i) { const a=this.accounts[i]; if(a.pollTimer) clearTimeout(a.pollTimer); a.pollTimer=null; a.pendingJobs.clear(); }
  startPing(i) {
    const a=this.accounts[i]; this.stopPing(i); a.lastHeartbeat=Date.now();
    a.pingTimer=setInterval(()=>{ if(!a.ready||!a.ws||a.ws.readyState!==WebSocket.OPEN)return; if(Date.now()-a.lastHeartbeat>=60000){ try{a.ws.close();}catch{} return; } try{a.ws.send(JSON.stringify({type:"ping"})); this.send({type:"log",index:i,message:"PING keep-alive dikirim"});}catch{} },50000);
  }
  schedulePoll(i) {
    const a=this.accounts[i]; if(a.pollTimer||!a.pendingJobs.size)return;
    a.pollTimer=setTimeout(()=>{ a.pollTimer=null; if(!a.ready||!a.ws||a.ws.readyState!==WebSocket.OPEN)return; const [id,j]=a.pendingJobs.entries().next().value||[]; if(!id||!j)return; j.attempts++; if(j.attempts>120){a.pendingJobs.delete(id);this.send({type:"log",index:i,message:`JOB ${id} timeout`});}else this.sendAccount(i,{type:"job.get",job_id:id}); if(a.pendingJobs.size)this.schedulePoll(i); },500);
  }
  trackJob(i,data) { const id=jobIdOf(data); if(!id)return; const p=payloadOf(data), a=this.accounts[i]; a.pendingJobs.set(id,{attempts:0}); this.send({type:"log",index:i,message:`JOB ${id} diantrikan`}); this.schedulePoll(i); }
  handleJob(i,data) { const id=jobIdOf(data), a=this.accounts[i], st=jobStatusOf(data); if(!id||!a.pendingJobs.has(id)||!terminal(st))return; a.pendingJobs.delete(id); const ok=["completed","complete","success","succeeded","done","finished"].includes(st); this.send({type:"log",index:i,message:`JOB ${id} ${ok?"selesai":`gagal (${st})`}${!ok&&errorOf(data)?`: ${errorOf(data)}`:""}`}); if(a.pendingJobs.size)this.schedulePoll(i); }
  closeAccount(i,manual=true) { const a=this.accounts[i]; a.manual=manual; if(a.reconnectTimer)clearTimeout(a.reconnectTimer); a.reconnectTimer=null; this.stopPing(i); this.stopPoll(i); a.ready=false; a.joined.clear(); a.requested.clear(); a.pendingJoin=""; if(a.ws)try{a.ws.close();}catch{} a.ws=null; this.status(i,"offline"); }
  scheduleReconnect(i) { const a=this.accounts[i]; if(a.manual||a.authFailed||!a.username||!a.password)return; const ms=Math.min(15000,Math.round(2000*Math.pow(1.5,Math.min(a.reconnectAttempt++,5)))); this.send({type:"log",index:i,message:`RELOGIN otomatis dalam ${ms} ms`}); a.reconnectTimer=setTimeout(()=>this.connectAccount(i,true),ms); }
  connectAccount(i,reset=false) {
    const a=this.accounts[i]; if(reset)a.reconnectAttempt=0; if(a.reconnectTimer)clearTimeout(a.reconnectTimer); a.reconnectTimer=null; a.manual=false; a.authFailed=false; this.stopPing(i); this.stopPoll(i); if(!a.username||!a.password){this.status(i,"error",{message:"Username dan password wajib diisi"});return;}
    try{if(a.ws)a.ws.close();}catch{} this.status(i,"connecting");
    const ws=new WebSocket(this.env.MIG_WS_URL||DEFAULT_MIG_WS_URL); a.ws=ws;
    ws.addEventListener("open",()=>this.send({type:"log",index:i,message:"WebSocket OPEN; menunggu auth.required"}));
    ws.addEventListener("message",e=>this.onUpstream(i,ws,e.data));
    ws.addEventListener("error",e=>this.send({type:"log",index:i,message:`WebSocket ERROR: ${safeString(e?.message||"upstream error")}`}));
    ws.addEventListener("close",e=>{if(a.ws!==ws)return;this.stopPing(i);this.stopPoll(i);a.ready=false;a.ws=null;const auth=a.authFailed;this.status(i,auth?"error":"offline",{code:e.code,reason:safeString(e.reason||"-"),authFailed:auth});if(!auth)this.scheduleReconnect(i);});
  }
  onUpstream(i,ws,raw) {
    const a=this.accounts[i]; if(a.ws!==ws)return; let data; try{data=JSON.parse(typeof raw==="string"?raw:new TextDecoder().decode(raw));}catch{return;}
    a.lastHeartbeat=Date.now(); this.send({type:"api",index:i,data});
    if(String(data?.type||"").toLowerCase()==="ping"){try{ws.send(JSON.stringify({type:"pong"}));}catch{}}
    if(String(data?.type||"").endsWith(".queued"))this.trackJob(i,data); if(["job.status.result","job.get.result","job.status"].includes(data?.type))this.handleJob(i,data);
    if(data.type==="room.participants.result"||data.type==="room.participants"){const p=payloadOf(data),users=normalizeUsers(p),room=p?.room||data?.room||a.pendingJoin||"";this.send({type:"participants",index:i,room,users});this.send({type:"participants.raw",index:i,data:JSON.stringify(data).slice(0,8000)});}
    if(data.type==="auth.required"){this.send({type:"log",index:i,message:"auth.required diterima"});try{ws.send(JSON.stringify({type:"developer.login",username:a.username,password:a.password}));}catch{}this.status(i,"auth");return;}
    if(data.type==="session.ready"){a.ready=true;a.authFailed=false;a.reconnectAttempt=0;a.permissions=Array.isArray(data?.data?.developer?.permissions||data?.data?.permissions)?(data?.data?.developer?.permissions||data?.data?.permissions):[];const w=data?.data?.wallet||data?.data?.developer?.wallet;this.status(i,"online",{permissions:a.permissions,balance:w?.label||"-"});this.send({type:"log",index:i,message:"LOGIN BERHASIL; keep-alive aktif"});this.startPing(i);this.rejoin(i);return;}
    if(data.type==="error"){const err=errorOf(data);this.send({type:"log",index:i,message:`API ERROR: ${err||"Unknown error"}`});if(/auth|credential|password|username|login|invalid/i.test(err)&&!a.ready){a.authFailed=true;this.status(i,"error",{authFailed:true,message:err});try{ws.close();}catch{}}}
    if(data.type==="session.replaced"){a.ready=false;this.stopPing(i);try{ws.close();}catch{}this.status(i,"error",{message:"Session replaced"});}
    if(data.type==="room.join.result"){const p=payloadOf(data),room=String(p?.room||p?.room_name||p?.roomName||data?.room||a.pendingJoin||"").trim(),st=String(p?.status||p?.state||data?.status||"").toLowerCase(),fail=p?.success===false||data?.success===false||["error","failed","failure","rejected","denied"].includes(st)||!!errorOf(data);if(!fail&&room){a.joined.add(room);a.requested.add(room);a.pendingJoin="";this.status(i,"online",{room});this.send({type:"log",index:i,message:`JOIN BERHASIL: ${room}`});}}
    if(data.type==="room.leave.result"){const p=payloadOf(data),room=String(p?.room||p?.room_name||data?.room||"").trim();if(room&&!errorOf(data)){for(const x of a.joined)if(roomMatches(x,room))a.joined.delete(x);for(const x of a.requested)if(roomMatches(x,room))a.requested.delete(x);}}
    if(data.type==="wallet.balance.result"){const w=data?.data?.wallet;if(w)this.send({type:"balance",index:i,balance:w.label||String(w.balance_cr||"-")});}
    if(i===this.voteKickDetectorIndex){ const det=detectVoteKick(data); if(det)this.startAutoKick(i,det); }
  }
  rejoin(i){const a=this.accounts[i];[...a.requested].filter(r=>![...a.joined].some(x=>roomMatches(x,r))).forEach((room,n)=>setTimeout(()=>{if(this.canSend(i,{type:"room.join",room})){a.pendingJoin=room;this.sendAccount(i,{type:"room.join",room});}},n*600));}
  async joinAll(room){for(let i=0;i<10;i++){const a=this.accounts[i];if(a.ready&&a.ws?.readyState===WebSocket.OPEN){a.pendingJoin=room;if(this.sendAccount(i,{type:"room.join",room}))a.requested.add(room);}await new Promise(r=>setTimeout(r,600));}}
  async kickQueue(room,targets,delayMs,loops,source,opts={}) {
    if(!room||!targets.length||this.commandQueueRunning)return; this.commandQueueRunning=true; const delays=targets.map((_,n)=>clampMs(opts.targetDelaysMs?.[n],delayMs)),total=targets.length*10*loops; let no=0,sent=0,skipped=0;this.send({type:"kickQueue.start",room,targets,targetDelaysMs:delays,socketDelayMs:clampMs(opts.socketDelayMs),sequentialMode:opts.sequentialMode===true,loopCount:loops,total,source});
    try{for(let loop=1;loop<=loops;loop++)for(let ti=0;ti<targets.length;ti++){for(let i=0;i<10;i++){no++;const a=this.accounts[i],ok=a.ready&&a.ws?.readyState===WebSocket.OPEN&&[...a.joined].some(x=>roomMatches(x,room));if(ok&&this.sendAccount(i,{type:"room.kick",room,target_username:targets[ti]}))sent++;else skipped++;this.send({type:"kickQueue.step",loop,targetIndex:ti+1,target:targets[ti],accountIndex:i+1,delayMs:delays[ti],actionNo:no,total,sent:ok,status:ok?"queued":"skipped"});if(opts.sequentialMode&&i<9)await new Promise(r=>setTimeout(r,clampMs(opts.socketDelayMs)));}await new Promise(r=>setTimeout(r,delays[ti]));}}
    finally{this.commandQueueRunning=false;}this.send({type:"kickQueue.done",total,sent,skipped,source});
  }
  beginAutoKick(index,det,source="detected"){if(this.autoKick.timer||!this.autoKick.room)return;const initial=this.autoKick.remainingMs>0&&this.autoKick.remainingMs<60000?this.autoKick.remainingMs:60000;this.autoKick.countdownMs=60000;this.autoKick.remainingMs=initial;this.autoKick.countdownEndAt=Date.now()+initial;this.autoKick.source=source;this.autoKick.triggered=false;this.send({type:source==="detected"?"autoKick.detected":"autoKick.manual.started",index,room:det.room,countdownMs:60000,thresholdMs:this.autoKick.thresholdMs,targetUsername:det.targetUsername||"",startedBy:det.startedBy||"",eventId:det.eventId||"",message:det.message||""});let lastBroadcast=0;this.autoKick.timer=setInterval(()=>{const rem=Math.max(0,this.autoKick.countdownEndAt-Date.now());this.autoKick.remainingMs=rem;const now=Date.now();if(now-lastBroadcast>=50||rem===0){lastBroadcast=now;this.send({type:"autoKick.state",status:this.autoKick.triggered?"triggering":"countdown",enabled:this.autoKick.enabled,room:this.autoKick.room,source:this.autoKick.source,thresholdMs:this.autoKick.thresholdMs,countdownMs:60000,remainingMs:rem});}if(!this.autoKick.triggered&&rem<=this.autoKick.thresholdMs){this.autoKick.triggered=true;this.send({type:"autoKick.triggered",room:this.autoKick.room,thresholdMs:this.autoKick.thresholdMs,remainingMs:rem});if(this.autoKick.targets.length&&!this.commandQueueRunning)this.kickQueue(this.autoKick.room,this.autoKick.targets,0,this.autoKick.loopCount,"autoKick",this.autoKick);}if(rem===0){clearInterval(this.autoKick.timer);this.autoKick.timer=null;this.autoKick.countdownEndAt=0;this.autoKick.remainingMs=0;this.autoKick.triggered=false;this.autoKick.source=null;this.send({type:"autoKick.state",status:this.autoKick.enabled?"armed":"disabled",enabled:this.autoKick.enabled,room:this.autoKick.room,remainingMs:0});}},1);}
  startAutoKick(i,det){if(!this.autoKick.enabled||!this.autoKick.room||!roomMatches(det.room,this.autoKick.room))return;this.beginAutoKick(i,det,"detected");}
  stopAutoKick(reason="stop"){if(this.autoKick.timer){this.autoKick.remainingMs=Math.max(0,this.autoKick.countdownEndAt-Date.now());clearInterval(this.autoKick.timer);}this.autoKick.timer=null;this.autoKick.countdownEndAt=0;this.autoKick.triggered=false;this.autoKick.source=null;if(reason==="autoKick.reset"||reason==="reset"){this.autoKick.countdownMs=60000;this.autoKick.remainingMs=60000;}this.send({type:"autoKick.state",status:this.autoKick.enabled?"armed":"disabled",enabled:this.autoKick.enabled,room:this.autoKick.room,reason,remainingMs:this.autoKick.remainingMs,countdownMs:60000});}
  async onDashboardMessage(raw){let m;try{m=JSON.parse(typeof raw==="string"?raw:new TextDecoder().decode(raw));}catch{return;}const i=Number(m.index);
    if(m.action==="login"&&Number.isInteger(i)&&i>=0&&i<10){const a=this.accounts[i];a.username=String(m.username||"").trim();a.password=String(m.password||"");this.connectAccount(i,true);return;}
    if(m.action==="disconnect"&&Number.isInteger(i)&&i>=0&&i<10){this.closeAccount(i,true);return;}
    if(m.action==="loginAll"){for(let n=0;n<10;n++){if(m.accounts?.[n]?.username&&m.accounts?.[n]?.password){this.accounts[n].username=String(m.accounts[n].username).trim();this.accounts[n].password=String(m.accounts[n].password);setTimeout(()=>this.connectAccount(n,true),n*500);}}return;}
    if(m.action==="disconnectAll"||m.action==="logoutAll"){for(let n=0;n<10;n++)this.closeAccount(n,true);this.send({type:"logout.done"});return;}
    if(m.action==="joinAll"){const room=String(m.room||"").trim();if(room)this.joinAll(room);return;}
    if(m.action==="leaveAll"){const room=String(m.room||"").trim();for(let n=0;n<10;n++)if(this.sendAccount(n,{type:"room.leave",room}))for(const x of this.accounts[n].requested)if(roomMatches(x,room))this.accounts[n].requested.delete(x);return;}
    if(m.action==="balanceAll"){for(let n=0;n<10;n++)this.sendAccount(n,{type:"wallet.balance"});return;}
    if(m.action==="messageAll"){const room=String(m.room||"").trim(),message=String(m.message||"");if(!room||!message)return;for(let n=0;n<10;n++)if([...this.accounts[n].joined].some(x=>roomMatches(x,room)))this.sendAccount(n,{type:"room.send_message",room,message});return;}
    if(m.action==="participants"){const room=String(m.room||"").trim();const n=this.accounts.findIndex(a=>a.ready&&[...a.joined].some(x=>roomMatches(x,room)));if(n<0){this.send({type:"error",index:0,message:`Tidak ada ID Online yang masuk room ${room}.`});return;}this.sendAccount(n,{type:"room.participants",room});this.send({type:"participants.source",index:n,room});return;}
    if(m.action==="autoKick.configure"){Object.assign(this.autoKick,{enabled:m.enabled!==false,room:String(m.room||"").trim(),thresholdMs:clampMs(m.thresholdMs),targets:Array.isArray(m.targets)?[...new Set(m.targets.map(x=>String(x||"").trim()).filter(Boolean))].slice(0,10):[],targetDelaysMs:Array.isArray(m.targetDelaysMs)?m.targetDelaysMs.slice(0,10).map(x=>clampMs(x)):[],loopCount:Math.max(1,Math.min(100,Number(m.loopCount)||1)),socketDelayMs:clampMs(m.socketDelayMs),sequentialMode:m.sequentialMode===true});this.autoKick.countdownMs=60000;if(this.autoKick.remainingMs<=0)this.autoKick.remainingMs=60000;if(!this.autoKick.enabled)this.stopAutoKick("disabled");else this.send({type:"autoKick.state",status:this.autoKick.countdownEndAt?"countdown":"armed",enabled:true,room:this.autoKick.room,remainingMs:Math.max(0,this.autoKick.countdownEndAt-Date.now())});return;}
    if(m.action==="autoKick.start"){if(this.autoKick.room&&!this.autoKick.timer)this.beginAutoKick(0,{room:this.autoKick.room},"manual");return;}
    if(m.action==="autoKick.stop"){this.stopAutoKick("stop");return;} if(m.action==="autoKick.reset"){this.stopAutoKick("reset");return;}
    if(m.action==="kickQueue"){const room=String(m.room||"").trim(),targets=Array.isArray(m.targets)?[...new Set(m.targets.map(x=>String(x||"").trim()).filter(Boolean))].slice(0,10):[];if(room&&targets.length)this.kickQueue(room,targets,clampMs(m.delayMs),Math.max(1,Math.min(100,Number(m.loopCount)||1)),"kickAll",{socketDelayMs:clampMs(m.socketDelayMs),sequentialMode:m.sequentialMode===true,targetDelaysMs:m.targetDelaysMs});}
  }
}
