import { DurableObject } from "cloudflare:workers";

const DEFAULT_MIG_WS_URL = "wss://developer.mig33.id/developer/ws";
let MIG_WS_URL = DEFAULT_MIG_WS_URL;
let DASHBOARD_TOKEN = "";

// Small compatibility layer so the proven V5 backend logic can run on the
// Cloudflare Workers WebSocket API without the Node `ws` package.
class CFWebSocket {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;

  constructor(urlOrSocket) {
    this._handlers = new Map();
    this._onceHandlers = new Map();
    this._socket = typeof urlOrSocket === "string"
      ? new globalThis.WebSocket(urlOrSocket)
      : urlOrSocket;
    this._bind();
  }

  get readyState() { return this._socket.readyState; }

  _emit(name, event) {
    const set = this._handlers.get(name);
    if (set) for (const fn of [...set]) { try { fn(...event); } catch {} }
    const once = this._onceHandlers.get(name);
    if (once) {
      this._onceHandlers.delete(name);
      try { once(...event); } catch {}
    }
  }

  _bind() {
    this._socket.addEventListener("open", event => this._emit("open", [event]));
    this._socket.addEventListener("message", async event => {
      let data = event.data;
      if (data instanceof ArrayBuffer) data = new TextDecoder().decode(data);
      else if (typeof Blob !== "undefined" && data instanceof Blob) data = await data.text();
      this._emit("message", [data]);
    });
    this._socket.addEventListener("error", event => this._emit("error", [event]));
    this._socket.addEventListener("close", event => {
      this._emit("close", [event.code, event.reason || ""]);
    });
  }

  on(name, fn) {
    if (!this._handlers.has(name)) this._handlers.set(name, new Set());
    this._handlers.get(name).add(fn);
    return this;
  }

  once(name, fn) {
    this._onceHandlers.set(name, fn);
    return this;
  }

  send(data) { return this._socket.send(data); }
  close(code = 1000, reason = "") { try { this._socket.close(code, reason); } catch {} }
  terminate() { try { this._socket.close(1000, "terminated"); } catch {} }
  ping() { /* Browser/Workers WebSocket has no exposed protocol ping API. */ }
}

const WebSocket = CFWebSocket;

const wss = {
  _connectionHandler: null,
  on(event, handler) {
    if (event === "connection") this._connectionHandler = handler;
    return this;
  },
  emitConnection(socket) {
    if (this._connectionHandler) this._connectionHandler(socket);
  }
};

function jsonResponse(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" }
  });
}

function normalizeUsers(value) {
  const out = [];
  const seen = new Set();
  const add = (v) => {
    if (typeof v !== "string") return;
    const x = v.trim();
    if (!x || seen.has(x)) return;
    // Participant identity must come from an explicit username-like field.
    // Do not display user IDs (usr_...) as usernames.
    if (/^usr_[A-Za-z0-9_-]+$/.test(x)) return;
    if (/^(participants|users|members|people|list|data|result|status|success|room)$/i.test(x)) return;
    seen.add(x);
    out.push(x);
  };

  const walk = (node, depth = 0) => {
    if (node == null || depth > 20) return;
    if (Array.isArray(node)) {
      for (const item of node) walk(item, depth + 1);
      return;
    }
    if (typeof node !== "object") return;

    // The actual participant payload observed in the API contains objects such as:
    // { user_id: "usr_...", username: "silent_killer8", ... }
    if (typeof node.username === "string") add(node.username);
    if (typeof node.user_name === "string") add(node.user_name);
    if (typeof node.userName === "string") add(node.userName);
    if (typeof node.nickname === "string") add(node.nickname);
    if (typeof node.nick === "string") add(node.nick);
    if (typeof node.display_name === "string") add(node.display_name);
    if (typeof node.displayName === "string") add(node.displayName);
    if (typeof node.handle === "string") add(node.handle);
    if (typeof node.login === "string") add(node.login);

    // Nested participant/user records.
    for (const key of ["user", "member", "participant", "account", "profile"]) {
      if (node[key] && typeof node[key] === "object") walk(node[key], depth + 1);
    }

    // Recurse through containers such as data/result/participants/users.
    for (const [key, child] of Object.entries(node)) {
      if (child && typeof child === "object") walk(child, depth + 1);
      // Handle a map keyed by username: { participants: { alice: {...} } }.
      if (/^(participants|users|members|people|items|list)$/i.test(key) && child && typeof child === "object" && !Array.isArray(child)) {
        for (const mapKey of Object.keys(child)) add(mapKey);
      }
    }
  };

  walk(value);
  return [...new Set(out)].slice(0, 500);
}

function safeSend(ws, obj) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return false;
  try {
    ws.send(JSON.stringify(obj));
    return true;
  } catch {
    return false;
  }
}

function createOutboundScheduler({
  canEnqueue,
  isAvailable,
  send,
  onQueueFull = () => {},
  maxQueue = 5000,
  now = Date.now,
  schedule = setTimeout,
  cancel = clearTimeout
} = {}) {
  if (typeof canEnqueue !== "function") throw new TypeError("canEnqueue is required");
  if (typeof isAvailable !== "function") throw new TypeError("isAvailable is required");
  if (typeof send !== "function") throw new TypeError("send is required");

  let queue = [];
  let timer = null;
  let current = null;
  let nextOutboundAt = 0;
  let stopped = false;

  function resolveItem(item, value) {
    if (item?.resolve) item.resolve(value);
  }

  function stop() {
    stopped = true;
    if (timer !== null) cancel(timer);
    timer = null;
    resolveItem(current, false);
    current = null;
    for (const item of queue.splice(0)) resolveItem(item, false);
    nextOutboundAt = 0;
  }

  function pump() {
    if (stopped || timer !== null || current || !queue.length) return;

    current = queue.shift();
    const waitMs = Math.max(0, nextOutboundAt - now());
    timer = schedule(() => {
      timer = null;
      const pending = current;
      current = null;

      if (!pending) {
        pump();
        return;
      }

      if (stopped || !isAvailable(pending.payload)) {
        resolveItem(pending, false);
        pump();
        return;
      }

      try {
        send(pending.payload);
        nextOutboundAt = now() + pending.spacingMs;
        resolveItem(pending, true);
      } catch {
        resolveItem(pending, false);
      }
      pump();
    }, waitMs);
  }

  function enqueue(payload, options = {}) {
    if (stopped || !canEnqueue(payload)) return false;
    if (queue.length >= maxQueue) {
      onQueueFull(payload);
      return false;
    }

    const item = {
      payload,
      spacingMs: Math.max(0, Math.min(5000, Number(options.spacingMs) || 0)),
      resolve: () => {}
    };
    // Keep-alive always goes ahead of pending kick payloads.
    if (options.priority) queue.unshift(item);
    else queue.push(item);
    pump();
    return true;
  }

  return {
    enqueue,
    stop,
    pendingCount: () => queue.length + (current ? 1 : 0)
  };
}

function publicError(message) {
  return String(message || "Unknown error").slice(0, 300);
}

function responsePayload(data) {
  return data?.data ?? data?.result ?? data ?? {};
}

function responseError(data) {
  const payload = responsePayload(data);
  const status = String(payload?.status || payload?.state || data?.status || "").toLowerCase();
  const errorCode = payload?.error || data?.error;
  if (errorCode || ["error", "failed", "failure", "rejected", "denied"].includes(status)) {
    return publicError(payload?.message || data?.message || errorCode || status);
  }
  return "";
}

function roomMatches(left, right) {
  return String(left || "").trim().toLowerCase() === String(right || "").trim().toLowerCase();
}

function hasJoinedRoom(account, room) {
  return [...account.joined].some(joinedRoom => roomMatches(joinedRoom, room));
}

function requiredPermissionFor(payload) {
  switch (payload?.type) {
    case "room.join": return "rooms.join";
    case "room.leave": return "rooms.leave";
    case "room.participants": return "rooms.read";
    case "room.kick": return "rooms.kick";
    case "room.send_message": return "messaging.send";
    case "wallet.balance": return "wallet.read";
    default: return "";
  }
}

function queuedJobId(data) {
  const payload = responsePayload(data);
  return String(
    payload?.job?.job_id ||
    payload?.job?.id ||
    payload?.job_id ||
    data?.job_id ||
    ""
  ).trim();
}

function jobStatus(data) {
  const payload = responsePayload(data);
  return String(
    payload?.status ||
    payload?.state ||
    payload?.job?.status ||
    data?.status ||
    ""
  ).trim().toLowerCase();
}

function isTerminalJobStatus(status) {
  return [
    "completed",
    "complete",
    "success",
    "succeeded",
    "done",
    "finished",
    "failed",
    "failure",
    "error",
    "cancelled",
    "canceled",
    "rejected"
  ].includes(status);
}

function clampDelayMs(value, fallback = 0) {
  const parsed = Number(value);
  const resolved = Number.isFinite(parsed) ? parsed : fallback;
  return Math.max(0, Math.min(60000, resolved));
}


function accountState() {
  return {
    ws: null,
    username: "",
    password: "",
    permissions: [],
    ready: false,
    joined: new Set(),
    requestedRooms: new Set(),
    pendingJoinRoom: "",
    pingTimer: null,
    lastHeartbeatAt: 0,
    jobPollTimer: null,
    pendingJobs: new Map(),
    pendingKickDispatches: [],
    kickRunIds: new Set(),
    completedKickActions: new Set(),
    outboundScheduler: null,
    authTimer: null,
    authFailed: false,
    manuallyClosed: false,
    lastRoomCommand: null
  };
}

let dashboardClient = null;
const accounts = Array.from({length: 10}, accountState);
let commandQueueRunning = false;
const autoKick = {
  enabled: false,
  room: "",
  thresholdMs: 30000,
  countdownMs: 60000,
  targets: [],
  targetDelaysMs: [],
  loopCount: 1,
  socketDelayMs: 0,
  sequentialMode: false,
  source: null,
  countdownEndAt: 0,
  countdownInterval: null,
  countdownTriggered: false
};

wss.on("connection", dashboard => {
  dashboardClient = dashboard;

  safeSend(dashboardClient, {type: "dashboard.ready", accounts: 10, backendVersion: "persistent-account-sockets-kickall-v5-cloudflare-2026-09-06"});

  function dashboardStatus(i, status, extra = {}) {
    safeSend(dashboardClient, {type: "status", index: i, status, ...extra});
  }

  // Rebind the UI to the existing account sockets after a dashboard reconnect.
  // No new developer.login is performed here.
  function sendDashboardSnapshot() {
    for (let i = 0; i < accounts.length; i++) {
      const a = accounts[i];
      if (!a.username && !a.ws && !a.ready && !a.joined.size) continue;
      const status = a.ws && a.ws.readyState === WebSocket.OPEN && a.ready
        ? "online"
        : (a.ws ? "connecting" : "offline");
      safeSend(dashboardClient, {
        type: "status",
        index: i,
        status,
        username: a.username,
        balance: a.balance || "-",
        joinedRooms: [...a.joined],
        requestedRooms: [...a.requestedRooms]
      });
      safeSend(dashboardClient, {
        type: "account.snapshot",
        index: i,
        username: a.username,
        status,
        ready: !!a.ready,
        socketOpen: !!(a.ws && a.ws.readyState === WebSocket.OPEN),
        joinedRooms: [...a.joined],
        requestedRooms: [...a.requestedRooms]
      });
    }
  }
  sendDashboardSnapshot();

  // Room audit: record only commands/events actually observed by this client.
  // This does not infer undocumented server behaviour.
  function auditRoom(i, direction, type, room, extra = {}) {
    const a = accounts[i];
    const entry = {
      at: new Date().toISOString(),
      direction,
      type,
      room: String(room || ""),
      ...extra
    };
    a.lastRoomCommand = entry;
    safeSend(dashboardClient, {
      type: "room.audit",
      index: i,
      audit: entry
    });
    safeSend(dashboardClient, {
      type: "log",
      index: i,
      message: `[ROOM AUDIT] ${direction} ${type}${room ? ` room=${room}` : ""}${extra.target ? ` target=${extra.target}` : ""}${extra.code != null ? ` code=${extra.code}` : ""}${extra.reason ? ` reason=${extra.reason}` : ""}`
    });
  }

  function autoKickState(status = "idle", extra = {}) {
    const remainingMs = autoKick.countdownEndAt
      ? Math.max(0, autoKick.countdownEndAt - Date.now())
      : null;
    safeSend(dashboardClient, {
      type: "autoKick.state",
      status,
      enabled: autoKick.enabled,
      room: autoKick.room,
      source: autoKick.source,
      thresholdMs: autoKick.thresholdMs,
      countdownMs: autoKick.countdownMs,
      deadlineAt: autoKick.countdownEndAt || null,
      remainingMs,
      ...extra
    });
  }

  function stopAutoKickCountdown(reason = "reset", statusOverride = null, remainingOverride = 0) {
    if (autoKick.countdownInterval) clearInterval(autoKick.countdownInterval);
    autoKick.countdownInterval = null;
    autoKick.countdownEndAt = 0;
    autoKick.countdownTriggered = false;
    autoKick.source = null;
    autoKickState(
      statusOverride || (autoKick.enabled ? "armed" : "disabled"),
      {reason, remainingMs: remainingOverride}
    );
  }

  function beginAutoKickCountdown(index, detection, source = "manual") {
    const detectedRoom = String(detection?.room || "").trim();
    if (autoKick.countdownEndAt) return;

    autoKick.countdownEndAt = Date.now() + 60000;
    autoKick.countdownTriggered = false;
    autoKick.source = "manual";
    safeSend(dashboardClient, {
      type: "autoKick.manual.started",
      room: detectedRoom,
      countdownMs: autoKick.countdownMs,
      thresholdMs: autoKick.thresholdMs
    });
    safeSend(dashboardClient, {
      type: "log",
      index: 0,
      message: `Timer auto kick dimulai manual untuk room ${detectedRoom} dari ${autoKick.countdownMs} ms.`
    });
    autoKickState("countdown", {source: "manual"});

    autoKick.countdownInterval = setInterval(() => {
      const remainingMs = Math.max(0, autoKick.countdownEndAt - Date.now());
      autoKickState(autoKick.countdownTriggered ? "triggering" : "countdown", {remainingMs});

      if (!autoKick.countdownTriggered && remainingMs <= autoKick.thresholdMs) {
        autoKick.countdownTriggered = true;
        safeSend(dashboardClient, {
          type: "autoKick.triggered",
          room: autoKick.room,
          thresholdMs: autoKick.thresholdMs,
          remainingMs
        });

        if (!autoKick.targets.length) {
          safeSend(dashboardClient, {
            type: "error",
            index: 0,
            message: "Timer selesai, tetapi belum ada target kick yang dipilih."
          });
        } else if (commandQueueRunning) {
          safeSend(dashboardClient, {
            type: "error",
            index: 0,
            message: "Auto Kick tidak dijalankan karena antrian perintah masih berjalan."
          });
        } else {
          autoKickState("triggering", {remainingMs});
          runKickQueue(
            autoKick.room,
            autoKick.targets,
            0,
            autoKick.loopCount,
            "autoKick",
            {
              socketDelayMs: autoKick.socketDelayMs,
              sequentialMode: autoKick.sequentialMode,
              loopDelayMs: autoKick.delayMs
            }
          ).finally(() => {
            if (autoKick.countdownEndAt) {
              autoKickState("triggering", {
                remainingMs: Math.max(0, autoKick.countdownEndAt - Date.now())
              });
            } else {
              autoKickState(autoKick.enabled ? "armed" : "disabled", {remainingMs: 0});
            }
          });
        }
      }

      if (remainingMs <= 0) {
        clearInterval(autoKick.countdownInterval);
        autoKick.countdownInterval = null;
        autoKick.countdownEndAt = 0;
        autoKick.countdownTriggered = false;
        autoKick.source = null;
        autoKickState(autoKick.enabled ? "armed" : "disabled", {
          reason: "countdown-complete",
          remainingMs: 0
        });
      }
    }, 100);
    return true;
  }

  function startManualAutoKickCountdown() {
    if (!autoKick.room) return false;
    if (autoKick.countdownEndAt) return false;
    return beginAutoKickCountdown(
      0,
      {
        room: autoKick.room,
        targetUsername: "",
        startedBy: "",
        eventId: "",
        message: ""
      },
      "manual"
    );
  }

  function stopPing(i) {
    const a = accounts[i];
    if (a.pingTimer) clearInterval(a.pingTimer);
    a.pingTimer = null;
    a.lastHeartbeatAt = 0;
  }

  function stopOutbound(i) {
    const a = accounts[i];
    if (a.outboundScheduler) a.outboundScheduler.stop();
    a.outboundScheduler = null;
  }

  function stopJobPolling(i) {
    const a = accounts[i];
    if (a.jobPollTimer) clearTimeout(a.jobPollTimer);
    a.jobPollTimer = null;
    a.pendingJobs.clear();
    a.pendingKickDispatches.length = 0;
  }

  function scheduleJobPoll(i) {
    const a = accounts[i];
    if (a.jobPollTimer || !a.pendingJobs.size) return;
    a.jobPollTimer = setTimeout(() => {
      a.jobPollTimer = null;
      if (!a.ready || !a.ws || a.ws.readyState !== WebSocket.OPEN) return;

      const [jobId, job] = a.pendingJobs.entries().next().value || [];
      if (!jobId || !job) return;

      job.attempts += 1;
      if (job.attempts > 120) {
        a.pendingJobs.delete(jobId);
        safeSend(dashboardClient, {
          type: "log",
          index: i,
          message: `JOB ${jobId} tidak selesai setelah 60 detik; polling dihentikan`
        });
      } else {
        sendToAccount(i, {type: "job.get", job_id: jobId});
      }

      if (a.pendingJobs.size) scheduleJobPoll(i);
    }, 1000);
  }

  function trackQueuedJob(i, data) {
    const jobId = queuedJobId(data);
    if (!jobId) {
      safeSend(dashboardClient, {
        type: "log",
        index: i,
        message: `API mengembalikan ${data?.type || "queued"} tanpa job_id`
      });
      return;
    }

    const payload = responsePayload(data);
    const command = String(data?.type || "").replace(/\.queued$/, "");
    const pendingKick = command === "room.kick" ? accounts[i].pendingKickDispatches.shift() : null;
    accounts[i].pendingJobs.set(jobId, {
      command,
      room: pendingKick?.room || payload?.room || data?.room || "",
      target: pendingKick?.target || payload?.target_username || data?.target_username || "",
      runId: pendingKick?.runId || "",
      loop: pendingKick?.loop || 0,
      targetIndex: pendingKick?.targetIndex || 0,
      actionNo: pendingKick?.actionNo || 0,
      attempts: 0
    });
    safeSend(dashboardClient, {
      type: "log",
      index: i,
      message: `JOB ${jobId} diantrikan (${String(data?.type || "command").replace(/\.queued$/, "")}) target=${pendingKick?.target || "-"}`
    });
    if (command === "room.kick") {
      safeSend(dashboardClient, {
        type: "kick.queued",
        index: i,
        jobId,
        room: pendingKick?.room || payload?.room || data?.room || "",
        target: pendingKick?.target || payload?.target_username || data?.target_username || ""
      });
    }
    scheduleJobPoll(i);
  }

  function handleJobStatus(i, data) {
    const jobId = queuedJobId(data);
    if (!jobId) return;

    const a = accounts[i];
    const job = a.pendingJobs.get(jobId);
    const status = jobStatus(data);
    if (!job || !status) return;

    if (!isTerminalJobStatus(status)) return;
    a.pendingJobs.delete(jobId);
    const ok = ["completed", "complete", "success", "succeeded", "done", "finished"].includes(status);
    const jobMessage = `JOB ${jobId} ${ok ? "selesai" : `gagal (${status})`}${responseError(data) && !ok ? `: ${responseError(data)}` : ""}`;
    safeSend(dashboardClient, {
      type: "log",
      index: i,
      message: jobMessage
    });

    // room.kick is an API job: queued is not success. Only a terminal
    // job.get/job.status response is treated as authoritative completion.
    if (job.command === "room.kick") {
      safeSend(dashboardClient, {
        type: "kick.result",
        index: i,
        jobId,
        room: job.room,
        target: job.target,
        status,
        success: ok,
        error: ok ? "" : responseError(data),
        runId: job.runId || "",
        loop: job.loop || 0,
        targetIndex: job.targetIndex || 0,
        actionNo: job.actionNo || 0
      });
      if (job.runId) {
        // Mark this exact dispatch as terminal before the queue advances to
        // the next target. This prevents multiple room.kick jobs for the
        // same account from being mixed across targets.
        if (Number.isFinite(Number(job.actionNo)) && Number(job.actionNo) > 0) {
          a.completedKickActions.add(Number(job.actionNo));
        }
        safeSend(dashboardClient, {
          type: "kickQueue.job",
          runId: job.runId,
          index: i,
          jobId,
          target: job.target,
          success: ok,
          status,
          terminal: true
        });
      }
    }

    const payload = responsePayload(data);
    const wallet = payload?.wallet || payload?.data?.wallet;
    if (wallet) {
      safeSend(dashboardClient, {
        type: "balance",
        index: i,
        balance: wallet.label || String(wallet.balance_cr || "-")
      });
    }
    if (a.pendingJobs.size) scheduleJobPoll(i);
  }

  function startPing(i) {
    const a = accounts[i];
    stopPing(i);
    // The API documentation allows application-level ping every 30–50 seconds
    // and closes idle sessions after 60 seconds. Use the documented maximum.
    a.lastHeartbeatAt = Date.now();
    a.pingTimer = setInterval(() => {
      if (!a.ws || a.ws.readyState !== WebSocket.OPEN || !a.ready) return;
      if (a.lastHeartbeatAt && Date.now() - a.lastHeartbeatAt >= 60000) {
        safeSend(dashboardClient, {
          type: "log",
          index: i,
          message: "PING timeout; socket ditutup"
        });
        try { a.ws.terminate(); } catch {}
        return;
      }
      enqueueOutbound(i, {type: "ping"}, {priority: true});
      safeSend(dashboardClient, {type: "log", index: i, message: "PING keep-alive dikirim"});
    }, 50000);
  }

  function closeAccount(i, manual = false) {
    const a = accounts[i];
    if (!a) return Promise.resolve();

    a.manuallyClosed = manual === true;
    a.ready = false;
    a.authFailed = false;
    a.joined.clear();
    a.pendingJoinRoom = "";

    stopPing(i);
    stopJobPolling(i);
    stopOutbound(i);
    if (a.authTimer) clearTimeout(a.authTimer);
    a.authTimer = null;

    const ws = a.ws;
    if (!ws) {
      dashboardStatus(i, "offline");
      return Promise.resolve();
    }

    // The MigReborn API has no logout command. Closing the WebSocket is the
    // protocol-level disconnect. For re-login, wait for the old socket to
    // actually emit `close` before opening a replacement connection. This
    // prevents the new login from racing the old active WebSocket.
    return new Promise(resolve => {
      let settled = false;
      const finish = () => {
        if (settled) return;
        settled = true;
        if (a.ws === ws) a.ws = null;
        dashboardStatus(i, "offline");
        resolve();
      };

      if (ws.readyState === WebSocket.CLOSED) {
        finish();
        return;
      }

      ws.once("close", finish);
      try {
        if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
          ws.close(1000, manual ? "manual disconnect" : "relogin");
        } else {
          finish();
        }
      } catch {
        try { ws.terminate(); } catch {}
        finish();
      }
    });
  }

  function connectAccount(i, options = {}) {
    const a = accounts[i];
    stopPing(i);
    stopJobPolling(i);
    stopOutbound(i);
    a.authFailed = false;
    if (a.authTimer) clearTimeout(a.authTimer);
    a.authTimer = null;
    a.manuallyClosed = false;
    a.ready = false;
    a.joined.clear();
    a.pendingJoinRoom = "";

    if (!a.username || !a.password) {
      dashboardStatus(i, "error", {message: "Username dan password wajib diisi"});
      return;
    }

    dashboardStatus(i, "connecting");

    const ws = new WebSocket(MIG_WS_URL);
    a.ws = ws;
    ws.on("pong", () => {
      a.lastHeartbeatAt = Date.now();
      safeSend(dashboardClient, {type: "log", index: i, message: "PONG keep-alive diterima"});
    });

    ws.on("open", () => {
      safeSend(dashboardClient, {type: "log", index: i, message: "WebSocket OPEN; menunggu auth.required"});
    });

    ws.on("message", raw => {
      if (a.ws !== ws) return;

      let data;
      try { data = JSON.parse(raw.toString()); }
      catch {
        safeSend(dashboardClient, {type: "raw", index: i, data: raw.toString().slice(0, 1000)});
        return;
      }

      if (["pong", "ping.result", "heartbeat", "heartbeat.result"].includes(String(data?.type || "").toLowerCase())) {
        a.lastHeartbeatAt = Date.now();
      }
      if (String(data?.type || "").toLowerCase() === "ping") {
        safeSend(ws, {type: "pong"});
      }
      safeSend(dashboardClient, {type: "api", index: i, data});
      if (String(data?.type || "").endsWith(".queued")) trackQueuedJob(i, data);
      // The API documentation defines job.get by request shape and job fields,
      // but does not require one single response event name. Only process a
      // status response when its job_id matches a job this account actually
      // received from room.kick. This avoids guessing an undocumented event type.
      const responseJobId = queuedJobId(data);
      if (responseJobId && accounts[i].pendingJobs.has(responseJobId)) {
        handleJobStatus(i, data);
      }
      if (data.type === "room.participants" || data.type === "room.participants.result") {
        safeSend(dashboardClient, {type: "participants.raw", index: i, data: JSON.stringify(data).slice(0, 8000)});
      }


      if (data.type === "auth.required") {
        // Once session.ready has been received, a late/duplicate auth.required
        // must never downgrade a successful account back to AUTH.
        if (a.ready) return;
        if (a.authTimer) clearTimeout(a.authTimer);
        a.authTimer = setTimeout(() => {
          if (a.ws !== ws || a.ready || a.authFailed) return;
          a.authFailed = true;
          safeSend(dashboardClient, {type:"log", index:i, message:"AUTH timeout: server tidak menerima session.ready setelah login"});
          dashboardStatus(i, "error", {authFailed:true, message:"AUTH timeout"});
          try { ws.close(1000, "authentication timeout"); } catch {}
        }, 60000);
        safeSend(dashboardClient, {type: "log", index: i, message: "auth.required diterima"});
        safeSend(ws, {
          type: "developer.login",
          username: a.username,
          password: a.password
        });
        dashboardStatus(i, "auth");
        return;
      }

      // Login success is authoritative only when the API sends session.ready.
      // Do not infer success from generic result/status fields.

      if (data.type === "session.ready") {
        if (a.authTimer) clearTimeout(a.authTimer);
        a.authTimer = null;
        a.ready = true;
        a.authFailed = false;
        a.lastHeartbeatAt = Date.now();
        const permissions =
          data?.data?.developer?.permissions ||
          data?.data?.permissions ||
          [];
        a.permissions = Array.isArray(permissions) ? permissions : [];
        const wallet = data?.data?.wallet || data?.data?.developer?.wallet || null;
        const initialBalance = wallet?.label || (wallet?.balance_cr != null ? `${wallet.balance_cr} CR` : "-");

        dashboardStatus(i, "online", {
          permissions,
          balance: initialBalance
        });
        safeSend(dashboardClient, {type: "log", index: i, message: "LOGIN BERHASIL; keep-alive aktif"});

        // session.ready normally contains wallet data, but the API explicitly
        // provides wallet.balance as the authoritative way to refresh the
        // current balance. Request it immediately after authentication so an
        // account (especially the last/multi-ID connection) cannot remain at
        // "Saldo -" simply because its session.ready payload had no wallet.
        if (a.permissions.includes("wallet.read")) {
          enqueueOutbound(i, {type: "wallet.balance"}, {priority: true});
        }

        startPing(i);
        return;
      }

      if (data.type === "error") {
        const err = responseError(data);
        safeSend(dashboardClient, {type: "log", index: i, message: `API ERROR: ${publicError(err)}`});

        // Authentication failures must never enter a reconnect loop.
        // `developer already has an active websocket` is a connection-state
        // error, not a credential error; surface it and close this attempt.
        if (/active websocket/i.test(String(err)) && !a.ready) {
          a.authFailed = true;
          a.ready = false;
          dashboardStatus(i, "error", {authFailed: true, message: publicError(err)});
          try { ws.close(1000, "active websocket"); } catch {}
          return;
        }

        if (
          /auth|credential|password|username|login|invalid/i.test(String(err)) &&
          !a.ready
        ) {
          a.authFailed = true;
          a.ready = false;
          dashboardStatus(i, "error", {
            authFailed: true,
            message: publicError(err)
          });
          try { ws.close(1000, "authentication failed"); } catch {}
          return;
        }

        safeSend(dashboardClient, {type: "error", index: i, message: publicError(err)});
        return;
      }

      if (data.type === "session.replaced") {
        auditRoom(i, "IN", "session.replaced", "", {reason: "logged_in_elsewhere"});
        a.ready = false;
        a.joined.clear();
        a.pendingJoinRoom = "";
        stopPing(i);
        dashboardStatus(i, "error", {message: "Session replaced"});
      try { ws.close(4001, "session replaced"); } catch {}
      }

      if (data.type === "room.join.result") {
        const payload = responsePayload(data);
        const status = String(payload?.status ?? payload?.state ?? data?.status ?? "").toLowerCase();
        const joinedRoom = String(
          payload?.room || payload?.room_name || payload?.roomName ||
          data?.room || a.pendingJoinRoom || ""
        ).trim();
        const errorText = responseError(data);
        const failed =
          payload?.success === false ||
          data?.success === false ||
          ["error", "failed", "failure", "rejected", "denied"].includes(status) ||
          !!errorText;

        const ok =
          !failed &&
          (["joined","success","ok"].includes(status) ||
           payload?.joined === true ||
           payload?.success === true ||
           (!!joinedRoom && !status));

        if (ok && joinedRoom) {
          auditRoom(i, "IN", "room.join.result", joinedRoom, {status: status || "success"});
          a.joined.add(joinedRoom);
          a.requestedRooms.add(joinedRoom);
          a.pendingJoinRoom = "";
          // room.join.result confirms room membership only; it must never be
          // used as the login-status signal. session.ready is authoritative.
          safeSend(dashboardClient, {type: "log", index: i, message: `JOIN BERHASIL: ${joinedRoom}`});
        } else {
          safeSend(dashboardClient, {
            type: "log",
            index: i,
            message: `JOIN response: ${publicError(errorText || status || "tidak sukses")}`
          });
        }
      }

      if (data.type === "room.leave.result") {
        const payload = responsePayload(data);
        const leftRoom = String(
          payload?.room || payload?.room_name || data?.room || ""
        ).trim();
        const status = String(payload?.status ?? payload?.state ?? data?.status ?? "").toLowerCase();
        const failed = !!responseError(data) || ["error", "failed", "failure", "rejected", "denied"].includes(status);
        if (leftRoom && !failed) {
          auditRoom(i, "IN", "room.leave.result", leftRoom, {status: status || "success"});
          for (const joinedRoom of a.joined) {
            if (roomMatches(joinedRoom, leftRoom)) a.joined.delete(joinedRoom);
          }
          for (const requestedRoom of a.requestedRooms) {
            if (roomMatches(requestedRoom, leftRoom)) a.requestedRooms.delete(requestedRoom);
          }
          if (roomMatches(a.pendingJoinRoom, leftRoom)) a.pendingJoinRoom = "";
        }
      }

      if (data.type === "wallet.balance.result") {
        const payload = responsePayload(data);
        const wallet = payload?.wallet || data?.data?.wallet || null;
        if (wallet) {
          const balance = wallet.label || (wallet.balance_cr != null ? `${wallet.balance_cr} CR` : "-");
          safeSend(dashboardClient, {type: "balance", index: i, balance});
          safeSend(dashboardClient, {type: "log", index: i, message: `SALDO diperbarui: ${balance}`});
        } else {
          safeSend(dashboardClient, {type: "log", index: i, message: "wallet.balance.result diterima tetapi data wallet kosong"});
        }
      }
    });

    ws.on("error", err => {
      safeSend(dashboardClient, {
        type: "log",
        index: i,
        message: `WebSocket ERROR: ${publicError(err?.message || err)}`
      });
    });

    ws.on("close", (code, reasonBuf) => {
      if (a.ws !== ws) return;

      stopPing(i);
      stopOutbound(i);
      if (a.authTimer) clearTimeout(a.authTimer);
      a.authTimer = null;
      const reason = reasonBuf?.toString() || "-";
      const wasAuthFailure = a.authFailed || reason.includes("authentication failed");
      auditRoom(i, "SOCKET", "close", "", {code, reason});
      a.ready = false;
      a.ws = null;

      dashboardStatus(i, wasAuthFailure ? "error" : "offline", {
        code,
        reason,
        authFailed: wasAuthFailure
      });
      safeSend(dashboardClient, {
        type: "log",
        index: i,
        message: `CLOSED code=${code} reason=${reason}`
      });

    });
  }

  function canSendToAccount(i, payload) {
    const a = accounts[i];
    if (!a.ws || a.ws.readyState !== WebSocket.OPEN || !a.ready) {
      safeSend(dashboardClient, {type: "log", index: i, message: "Belum session.ready; command dilewati"});
      return false;
    }
    const permission = requiredPermissionFor(payload);
    if (permission && a.permissions.length && !a.permissions.includes(permission)) {
      safeSend(dashboardClient, {
        type: "log",
        index: i,
        message: `Command ${payload.type} dilewati: permission ${permission} tidak tersedia`
      });
      return false;
    }
    return true;
  }

  function outboundSchedulerFor(i) {
    const a = accounts[i];
    if (a.outboundScheduler) return a.outboundScheduler;

    a.outboundScheduler = createOutboundScheduler({
      canEnqueue: payload => canSendToAccount(i, payload),
      isAvailable: () => Boolean(
        a.ws &&
        a.ws.readyState === WebSocket.OPEN &&
        a.ready
      ),
      send: payload => a.ws.send(JSON.stringify(payload)),
      onQueueFull: payload => safeSend(dashboardClient, {
        type: "log",
        index: i,
        message: `Antrean outbound penuh; ${payload.type} dilewati`
      })
    });
    return a.outboundScheduler;
  }

  function enqueueOutbound(i, payload, options = {}) {
    return outboundSchedulerFor(i).enqueue(payload, options);
  }

  function sendToAccount(i, payload) {
    const a = accounts[i];
    if (!canSendToAccount(i, payload)) return false;
    a.ws.send(JSON.stringify(payload));
    if (payload?.type === "room.join" || payload?.type === "room.leave" || payload?.type === "room.participants") {
      auditRoom(i, "OUT", payload.type, payload.room, {
        source: payload.type === "room.leave" ? "leaveAll" : payload.type === "room.join" ? "joinAll" : "participants"
      });
    }
    return true;
  }

  function sendKickToAccount(i, payload, meta = {}) {
    const a = accounts[i];
    if (!a || !a.ws || a.ws.readyState !== WebSocket.OPEN || !a.ready) return false;
    if (!hasJoinedRoom(a, payload.room)) {
      safeSend(dashboardClient, {
        type: "log",
        index: i,
        message: `KICK dilewati: ID #${i + 1} belum terkonfirmasi masuk room ${payload.room}`
      });
      return false;
    }
    if (!canSendToAccount(i, payload)) return false;
    a.pendingKickDispatches.push({
      room: String(payload?.room || ""),
      target: String(payload?.target_username || ""),
      runId: String(meta.runId || ""),
      loop: Number(meta.loop || 0),
      targetIndex: Number(meta.targetIndex || 0),
      actionNo: Number(meta.actionNo || 0)
    });
    try {
      // room.kick is a documented queued API command. We send the exact
      // protocol payload and use job_id/job.get for authoritative completion.
      a.ws.send(JSON.stringify(payload));
      auditRoom(i, "OUT", "room.kick", payload.room, {
        target: payload.target_username,
        source: "kickQueue",
        runId: meta.runId || "",
        actionNo: meta.actionNo || 0
      });
      return true;
    } catch {
      a.pendingKickDispatches.pop();
      return false;
    }
  }

  async function runKickQueue(room, targets, delayMs, loopCount, source = "kickAll") {
    if (!room || !targets.length) return {started: false, sent: 0, skipped: 0, total: 0};
    if (commandQueueRunning) {
      safeSend(dashboardClient, {type: "error", index: 0, message: "Kick All masih berjalan. Tunggu sampai selesai sebelum menjalankan lagi."});
      return {started: false, sent: 0, skipped: 0, total: 0};
    }

    const loopDelayMs = clampDelayMs(delayMs);
    const kickQueueId = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const uniqueTargets = [...new Set(targets.map(v => String(v || "").trim()).filter(Boolean))].slice(0, 10);
    const preflight = uniqueTargets.map(target => ({
      target,
      eligible: accounts.map((a, index) => ({a, index}))
        .filter(({a}) => a.ready && a.ws?.readyState === WebSocket.OPEN && hasJoinedRoom(a, room) && (!a.permissions.length || a.permissions.includes("rooms.kick")))
        .map(({index}) => index + 1)
    }));
    const voterCount = [...new Set(preflight.flatMap(item => item.eligible))].length;
    const total = uniqueTargets.length * voterCount * loopCount;

    safeSend(dashboardClient, {
      type: "kickQueue.preflight",
      room,
      targets: uniqueTargets,
      voters: [...new Set(preflight.flatMap(item => item.eligible))],
      detail: preflight,
      total,
      loopCount,
      source,
      kickQueueId,
      mode: "sequential-voter-and-target-waves"
    });

    if (!voterCount) {
      safeSend(dashboardClient, {
        type: "kickQueue.error",
        total: 0,
        done: 0,
        sent: 0,
        skipped: uniqueTargets.length,
        source,
        message: `Tidak ada akun ONLINE yang terkonfirmasi masuk room ${room} dan memiliki permission rooms.kick.`
      });
      return {started: false, sent: 0, skipped: uniqueTargets.length, total: 0};
    }

    commandQueueRunning = true;
    accounts.forEach(a => a.completedKickActions.clear());
    let actionNo = 0;
    let sent = 0;
    let skipped = 0;
    let completed = 0;

    safeSend(dashboardClient, {
      type: "kickQueue.start",
      room,
      targets: uniqueTargets,
      loopDelayMs,
      loopCount,
      total,
      source,
      kickQueueId,
      mode: "sequential-voter-and-target-waves"
    });

    const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

    async function waitForActions(actionNos, timeoutMs = 70000) {
      const wanted = new Set(actionNos);
      const startedAt = Date.now();
      while (wanted.size && Date.now() - startedAt < timeoutMs) {
        for (const a of accounts) {
          for (const action of wanted) {
            if (a.completedKickActions.has(action)) wanted.delete(action);
          }
        }
        if (!wanted.size) return true;
        await sleep(250);
      }
      return wanted.size === 0;
    }

    try {
      for (let loop = 1; loop <= loopCount; loop++) {
        for (let targetIndex = 0; targetIndex < uniqueTargets.length; targetIndex++) {
          const target = uniqueTargets[targetIndex];
          const eligible = accounts.map((a, index) => ({a, index}))
            .filter(({a}) => a.ready && a.ws?.readyState === WebSocket.OPEN && hasJoinedRoom(a, room) && (!a.permissions.length || a.permissions.includes("rooms.kick")))
            .map(({index}) => index);

          safeSend(dashboardClient, {
            type: "kickQueue.target",
            runId: kickQueueId,
            loop,
            targetIndex: targetIndex + 1,
            target,
            eligibleAccounts: eligible.map(index => index + 1),
            eligibleCount: eligible.length,
            mode: "sequential-voter-and-target-waves"
          });

          if (!eligible.length) {
            skipped += voterCount;
            continue;
          }

          // V5 reliability fix: process ONE voter job at a time, globally.
          // The API's room.kick command is asynchronous and returns a job_id.
          // Sending ten vote jobs at once can make it impossible to distinguish
          // queue acceptance from the actual vote result. A voter must finish
          // (terminal job state) before the next voter is dispatched. After all
          // voters for target A finish, target B starts. This is intentionally
          // slower, but removes concurrency as a source of missed targets.
          for (const accountIndex of eligible) {
            actionNo++;
            const currentAction = actionNo;
            const didSend = sendKickToAccount(accountIndex, {
              type: "room.kick",
              room,
              target_username: target
            }, {
              runId: kickQueueId,
              loop,
              targetIndex: targetIndex + 1,
              actionNo: currentAction
            });
            if (didSend) sent++;
            else skipped++;

            safeSend(dashboardClient, {
              type: "kickQueue.step",
              loop,
              targetIndex: targetIndex + 1,
              target,
              accountIndex: accountIndex + 1,
              actionNo: currentAction,
              total,
              sent,
              skipped,
              status: didSend ? "sent" : "skipped",
              mode: "sequential-voter-and-target-waves"
            });

            if (didSend) {
              const finished = await waitForActions([currentAction], 70000);
              if (finished && accounts.some(a => a.completedKickActions.has(currentAction))) {
                completed = Math.min(total, completed + 1);
              } else {
                safeSend(dashboardClient, {
                  type: "log",
                  index: accountIndex,
                  message: `KICK target ${target}: voter #${accountIndex + 1} belum mencapai status terminal setelah timeout; voter berikutnya tetap dilanjutkan.`
                });
              }
            }
          }

          safeSend(dashboardClient, {
            type: "kickQueue.progress",
            done: Math.min(total, actionNo),
            completed,
            total,
            sent,
            skipped,
            loop,
            targetIndex: targetIndex + 1,
            target,
            source,
            mode: "sequential-voter-and-target-waves"
          });
        }

        if (loop < loopCount && loopDelayMs > 0) await sleep(loopDelayMs);
      }
    } catch (err) {
      commandQueueRunning = false;
      safeSend(dashboardClient, {type: "kickQueue.error", total, done: actionNo, sent, skipped, source, message: err?.message || String(err)});
      return {started: true, sent, skipped, total, error: err?.message || String(err)};
    }

    commandQueueRunning = false;
    safeSend(dashboardClient, {
      type: "kickQueue.done",
      total,
      done: actionNo,
      completed,
      sent,
      skipped,
      source,
      mode: "sequential-voter-and-target-waves",
      dispatched: true,
      note: "Setiap voter diproses satu per satu; target berikutnya baru dimulai setelah seluruh voter target sebelumnya selesai atau timeout."
    });
    return {started: true, sent, skipped, total};
  }

  dashboard.on("message", async raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); }
    catch { return; }

    const i = Number(msg.index);

    if (msg.action === "dashboard.sync") {
      sendDashboardSnapshot();
      safeSend(dashboardClient, {
        type: "dashboard.sync.done",
        accounts: accounts.length,
        connectedAccounts: accounts.filter(a => a.ready && a.ws?.readyState === WebSocket.OPEN).length,
        joinedAccountCount: accounts.filter(a => a.joined.size > 0).length
      });
      return;
    }

    if (msg.action === "login" && Number.isInteger(i) && i >= 0 && i < 10) {
      accounts[i].username = String(msg.username || "").trim();
      accounts[i].password = String(msg.password || "");

      // The API documents one WebSocket per username. Never let a relogin
      // race an existing local socket; close it first, then create the new one.
      for (let n = 0; n < 10; n++) {
        if (n === i) continue;
        if (accounts[n].username && accounts[n].username === accounts[i].username) {
          if (accounts[n].ws || accounts[n].ready) {
            safeSend(dashboardClient, {type: "log", index: i, message: `LOGIN dibatalkan: username ${accounts[i].username} sudah dipakai koneksi #${n + 1}`});
            dashboardStatus(i, "error", {message: "Username sudah memiliki koneksi WebSocket lain"});
            return;
          }
        }
      }

      closeAccount(i, true).then(() => connectAccount(i, {resetBackoff: true}));
      return;
    }

    if (msg.action === "disconnect" && Number.isInteger(i) && i >= 0 && i < 10) {
      closeAccount(i, true);
      return;
    }

    if (msg.action === "loginAll") {
      const requested = new Map();
      for (let n = 0; n < 10; n++) {
        const username = String(msg.accounts?.[n]?.username || "").trim();
        const password = String(msg.accounts?.[n]?.password || "");
        if (!username || !password) continue;
        if (requested.has(username)) {
          safeSend(dashboardClient, {type: "log", index: n, message: `Login All dibatalkan untuk #${n + 1}: username ${username} duplikat pada #${requested.get(username) + 1}`});
          dashboardStatus(n, "error", {message: "Username duplikat; satu username hanya satu koneksi"});
          continue;
        }
        requested.set(username, n);
        accounts[n].username = username;
        accounts[n].password = password;
      }

      for (let n = 0; n < 10; n++) {
        if (!requested.has(accounts[n].username) || !accounts[n].password) continue;
        if (accounts[n].ready && accounts[n].ws?.readyState === WebSocket.OPEN) {
          safeSend(dashboardClient, {type: "log", index: n, message: "Login All: sudah Online, login ulang dilewati"});
          continue;
        }
        closeAccount(n, true).then(() => connectAccount(n, {resetBackoff: true}));
      }
      return;
    }

    if (msg.action === "disconnectAll") {
      for (let n = 0; n < 10; n++) closeAccount(n, true);
      safeSend(dashboardClient, {type: "logout.done"});
      return;
    }

    if (msg.action === "logoutAll") {
      // Disconnect every account first, then acknowledge the dashboard.
      // The API has no separate logout command; closing each WebSocket is
      // the actual protocol-level disconnect.
      for (let n = 0; n < 10; n++) {
        try { closeAccount(n, true); } catch (err) {
          safeSend(dashboardClient, {type: "log", index: n, message: `Logout cleanup error: ${publicError(err)}`});
          dashboardStatus(n, "offline");
        }
      }
      safeSend(dashboardClient, {type: "logout.done"});
      return;
    }

    if (msg.action === "joinAll") {
      const room = String(msg.room || "").trim();
      if (!room) { safeSend(dashboardClient, {type:"error", index:0, message:"Nama room wajib diisi."}); return; }
      joinAll(room);
      return;
    }

    if (msg.action === "leaveAll") {
      const room = String(msg.room || "").trim();
      for (let n = 0; n < 10; n++) {
        if (sendToAccount(n, {type: "room.leave", room})) {
          for (const requestedRoom of accounts[n].requestedRooms) {
            if (roomMatches(requestedRoom, room)) accounts[n].requestedRooms.delete(requestedRoom);
          }
        }
      }
      return;
    }

    if (msg.action === "balanceAll") {
      for (let n = 0; n < 10; n++) sendToAccount(n, {type: "wallet.balance"});
      return;
    }

    if (msg.action === "messageAll") {
      const room = String(msg.room || "").trim();
      const message = String(msg.message || "");
      if (!room || !message) return;
      for (let n = 0; n < 10; n++) {
        if (hasJoinedRoom(accounts[n], room)) {
          sendToAccount(n, {type: "room.send_message", room, message});
        } else {
          safeSend(dashboardClient, {
            type: "log",
            index: n,
            message: `SEND dilewati: belum terkonfirmasi masuk room ${room}`
          });
        }
      }
      return;
    }

    if (msg.action === "participants") {
      const room = String(msg.room || "").trim();
      if (!room) return;

      // List User Room intentionally uses ONE already-joined websocket only.
      // The first ready account that has confirmed the room is used as the
      // participant-list source, so the API is not queried 10 times and the
      // dashboard receives one authoritative participant response.
      let source = -1;
      for (let n = 0; n < 10; n++) {
        const a = accounts[n];
        if (a.ready && hasJoinedRoom(a, room) && a.ws?.readyState === WebSocket.OPEN) {
          source = n;
          break;
        }
      }

      if (source < 0) {
        safeSend(dashboardClient, {
          type: "error",
          index: 0,
          message: `Tidak ada ID Online yang tercatat masuk room ${room}. Tekan Enter Room — All terlebih dahulu.`
        });
        return;
      }

      const ok = sendToAccount(source, {type: "room.participants", room});
      if (ok) {
        safeSend(dashboardClient, {
          type: "participants.source",
          index: source,
          room
        });
        safeSend(dashboardClient, {
          type: "log",
          index: source,
          message: `LIST USER ${room} dikirim melalui 1 WebSocket saja (ID #${source + 1})`
        });
      }
      return;
    }

    if (msg.action === "autoKick.configure") {
      autoKick.enabled = msg.enabled !== false;
      autoKick.room = String(msg.room || "").trim();
      autoKick.thresholdMs = Math.max(0, Math.min(60000, Number(msg.thresholdMs) || 0));
      autoKick.targets = Array.isArray(msg.targets)
        ? [...new Set(msg.targets.map(v => String(v || "").trim()).filter(Boolean))].slice(0, 10)
        : [];
      autoKick.targetDelaysMs = Array.isArray(msg.targetDelaysMs)
        ? msg.targetDelaysMs.slice(0, autoKick.targets.length).map(value => clampDelayMs(value))
        : [];
      autoKick.loopCount = Math.max(1, Math.min(100, Number(msg.loopCount) || 1));
      autoKick.socketDelayMs = Math.max(0, Math.min(60000, Number(msg.socketDelayMs) || 0));
      autoKick.delayMs = clampDelayMs(msg.delayMs);
      autoKick.sequentialMode = msg.sequentialMode === true;
      if (!autoKick.enabled) stopAutoKickCountdown("disabled");
      else autoKickState(autoKick.countdownEndAt ? "countdown" : "armed");
      return;
    }

    if (msg.action === "autoKick.reset") {
      stopAutoKickCountdown("manual-reset", null, autoKick.countdownMs);
      safeSend(dashboardClient, {type: "log", index: 0, message: "Timer auto kick di-reset."});
      return;
    }

    if (msg.action === "autoKick.start") {
      const started = startManualAutoKickCountdown();
      if (!started) {
        safeSend(dashboardClient, {
          type: "error",
          index: 0,
          message: autoKick.countdownEndAt
            ? "Timer auto kick sudah berjalan."
            : "Timer tidak dapat dimulai. Pastikan nama room sudah diisi."
        });
      }
      return;
    }

    if (msg.action === "autoKick.stop") {
      if (autoKick.countdownEndAt) {
        stopAutoKickCountdown("manual-stop", "stopped", 0);
        safeSend(dashboardClient, {type: "log", index: 0, message: "Timer auto kick dihentikan manual."});
      } else {
        autoKickState("stopped", {reason: "manual-stop", remainingMs: 0});
      }
      return;
    }

    if (msg.action === "kickQueue") {
      const room = String(msg.room || "").trim();
      const targets = Array.isArray(msg.targets)
        ? [...new Set(msg.targets.map(v => String(v || "").trim()).filter(Boolean))].slice(0, 10)
        : [];
      const delayMs = clampDelayMs(msg.delayMs);
      const loopCount = Math.max(1, Math.min(100, Number(msg.loopCount) || 1));
      if (!room || !targets.length) return;
      runKickQueue(room, targets, delayMs, loopCount, "kickAll");
      return;
    }

  });

  dashboard.on("close", () => {
    if (dashboardClient === dashboard) dashboardClient = null;
    // Dashboard/UI disconnect is not an account logout. Keep all Mig33
    // account WebSockets alive; explicit Logout/Logout All closes them.
    console.log("Dashboard disconnected; Mig33 account sockets kept alive; no relogin triggered");
  });
});


export class MigMasterSession extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname !== "/ws") {
      return jsonResponse({
        ok: true,
        service: "migmaster-backend",
        websocket: "/ws",
        mig33Endpoint: MIG_WS_URL,
        backendVersion: "persistent-account-sockets-kickall-v5-cloudflare-2026-09-06"
      });
    }

    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    DASHBOARD_TOKEN = env?.DASHBOARD_TOKEN || "";
    MIG_WS_URL = env?.MIG_WS_URL || DEFAULT_MIG_WS_URL;
    if (DASHBOARD_TOKEN && url.searchParams.get("token") !== DASHBOARD_TOKEN) {
      return new Response("Unauthorized", { status: 401 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    server.accept();
    const dashboard = new CFWebSocket(server);
    wss.emitConnection(dashboard);
    return new Response(null, { status: 101, webSocket: client });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/health") {
      return jsonResponse({
        ok: true,
        service: "migmaster-backend",
        websocket: "/ws",
        mig33Endpoint: env?.MIG_WS_URL || DEFAULT_MIG_WS_URL,
        backendVersion: "persistent-account-sockets-kickall-v5-cloudflare-2026-09-06"
      });
    }

    if (url.pathname !== "/ws") {
      return jsonResponse({
        ok: true,
        service: "migmaster-backend",
        websocket: "/ws",
        backendVersion: "persistent-account-sockets-kickall-v5-cloudflare-2026-09-06"
      });
    }

    const id = env.MIGMASTER_SESSION.idFromName("migmaster-main");
    return env.MIGMASTER_SESSION.get(id).fetch(request);
  }
};
