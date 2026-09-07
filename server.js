const http = require("http");
const WebSocket = require("ws");

const PORT = Number(process.env.PORT || 3000);
const DASHBOARD_TOKEN = process.env.DASHBOARD_TOKEN || "";
const MIG_WS_URL = process.env.MIG_WS_URL || "wss://developer.mig33.id/developer/ws";
// The Android app starts its auto-kick flow from the room caption emitted
// when a vote-kick is opened, not from room.kick.queued (which is our own
// command acknowledgement). The sender check below is intentional: only the
// room/system message may arm the countdown.
const VOTE_KICK_TEXT_RE = /\ba\s*vote\s+to\s+kick\b/i;
const ROOM_TEXT_EVENT_TYPES = new Set([
  "room.text",
  "room.text.received",
  "room.message.received",
  "room.message"
]);
const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
  if (req.method !== "GET") {
    res.writeHead(405, {"Content-Type": "text/plain; charset=utf-8"});
    return res.end("Method not allowed");
  }
  if (url.pathname === "/" || url.pathname === "/health") {
    res.writeHead(200, {"Content-Type": "application/json; charset=utf-8"});
    return res.end(JSON.stringify({
      ok: true,
      service: "migmaster-backend",
      websocket: "/ws",
      mig33Endpoint: MIG_WS_URL
    }));
  }
  res.writeHead(404, {"Content-Type": "text/plain; charset=utf-8"});
  return res.end("Not found");
});

const wss = new WebSocket.Server({ noServer: true });

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

server.on("upgrade", (req, socket, head) => {
  const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);

  if (url.pathname !== "/ws") {
    socket.write("HTTP/1.1 404 Not Found\r\n\r\n");
    socket.destroy();
    return;
  }

  if (DASHBOARD_TOKEN && url.searchParams.get("token") !== DASHBOARD_TOKEN) {
    socket.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, ws => wss.emit("connection", ws, req));
});

function safeSend(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
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

function eventRoom(data) {
  const payload = responsePayload(data);
  const candidates = [
    data?.room,
    data?.room_name,
    data?.roomName,
    data?.room_id,
    data?.roomId,
    data?.record?.room,
    data?.record?.room_name,
    data?.record?.roomName,
    data?.record?.room_id,
    data?.record?.roomId,
    data?.data?.room,
    data?.data?.room_name,
    data?.data?.roomName,
    data?.data?.room_id,
    data?.data?.roomId,
    data?.data?.record?.room,
    data?.data?.record?.room_name,
    data?.data?.record?.roomName,
    data?.data?.record?.room_id,
    data?.data?.record?.roomId,
    data?.result?.room,
    data?.result?.room_name,
    data?.result?.roomName,
    data?.result?.room_id,
    data?.result?.roomId,
    data?.result?.record?.room,
    data?.result?.record?.room_name,
    data?.result?.record?.roomName,
    data?.result?.record?.room_id,
    data?.result?.record?.roomId,
    payload?.room,
    payload?.room_name,
    payload?.roomName,
    payload?.room_id,
    payload?.roomId,
    payload?.record?.room
  ];
  return candidates.find(value => typeof value === "string" && value.trim())?.trim() || "";
}

function roomTextCandidates(data) {
  const values = [];
  const seen = new Set();
  const add = value => {
    if (typeof value !== "string" || !value.trim() || seen.has(value)) return;
    seen.add(value);
    values.push(value.trim());
  };
  const walk = (node, depth = 0) => {
    if (node == null || depth > 8) return;
    if (Array.isArray(node)) {
      node.forEach(item => walk(item, depth + 1));
      return;
    }
    if (typeof node !== "object") return;
    for (const key of [
      "message",
      "text",
      "content",
      "body",
      "caption",
      "msg",
      "room_message",
      "roomMessage",
      "chat_message",
      "chatMessage"
    ]) {
      add(node[key]);
    }
    for (const key of [
      "record",
      "data",
      "result",
      "event",
      "payload",
      "message",
      "message_data",
      "messageData"
    ]) {
      if (node[key] && typeof node[key] === "object") walk(node[key], depth + 1);
    }
  };
  walk(data);
  return values;
}

function roomSenderCandidates(data) {
  const values = [];
  const seen = new Set();
  const add = value => {
    if (typeof value !== "string") return;
    const normalized = value.trim();
    if (!normalized || seen.has(normalized)) return;
    seen.add(normalized);
    values.push(normalized);
  };

  const walk = (node, depth = 0) => {
    if (node == null || depth > 8) return;
    if (Array.isArray(node)) {
      node.forEach(item => walk(item, depth + 1));
      return;
    }
    if (typeof node !== "object") return;

    for (const key of [
      "sender",
      "from",
      "author",
      "source",
      "origin",
      "actor",
      "created_by",
      "createdBy",
      "sender_type",
      "senderType",
      "sender_name",
      "senderName",
      "from_type",
      "fromType",
      "source_type",
      "sourceType"
    ]) {
      const sender = node[key];
      if (typeof sender === "string") {
        add(sender);
      } else if (sender && typeof sender === "object") {
        for (const nameKey of [
          "type",
          "name",
          "username",
          "user_name",
          "userName",
          "display_name",
          "displayName",
          "role",
          "id",
          "kind",
          "category",
          "sender_type",
          "senderType",
          "entity_type",
          "entityType"
        ]) {
          add(sender[nameKey]);
        }
        walk(sender, depth + 1);
      }
    }

    for (const key of ["record", "data", "result", "event", "payload", "message"]) {
      if (node[key] && typeof node[key] === "object") {
        walk(node[key], depth + 1);
      }
    }
  };

  walk(data);
  return values;
}

function isRoomSender(data, room) {
  const roomName = String(room || "").trim().toLowerCase();
  const senderCandidates = roomSenderCandidates(data);
  const explicitSender = senderCandidates.some(sender => {
    const normalized = sender.toLowerCase();
    return (
      normalized === "room" ||
      (roomName && normalized === roomName) ||
      (roomName && normalized === `room:${roomName}`) ||
      (roomName && normalized === `room/${roomName}`)
    );
  });
  if (explicitSender) return true;
  if (senderCandidates.length) return false;

  // Mig33's live room.text event identifies its origin in the event type
  // and may omit a sender field entirely. This is still an explicit room
  // source, unlike a generic message event without sender metadata.
  return ROOM_TEXT_EVENT_TYPES.has(String(data?.type || "").trim().toLowerCase());
}

function voteKickTarget(text) {
  const match = /\ba\s*vote\s+to\s+kick\s+(.+?)(?:\s+has\s+been\s+started\s+by\b|[.!?]|$)/i.exec(text);
  return String(match?.[1] || "").trim();
}

function voteKickStartedBy(text) {
  const match = /\bhas\s+been\s+started\s+by\s+(.+?)(?:,|\.|$)/i.exec(text);
  return String(match?.[1] || "").trim();
}

function detectVoteKickTimer(data) {
  const type = String(data?.type || "").toLowerCase();

  const room = eventRoom(data);
  if (!room || !isRoomSender(data, room)) return null;

  for (const text of roomTextCandidates(data)) {
    if (!VOTE_KICK_TEXT_RE.test(text)) continue;
    const payload = responsePayload(data);
    return {
      room,
      targetUsername: voteKickTarget(text),
      startedBy: voteKickStartedBy(text),
      sender: "room",
      message: text,
      eventId: String(
        data?.event_id ||
        data?.eventId ||
        data?.message_id ||
        data?.messageId ||
        data?.data?.event_id ||
        data?.data?.eventId ||
        data?.data?.message_id ||
        data?.data?.messageId ||
        payload?.event_id ||
        payload?.eventId ||
        payload?.message_id ||
        payload?.messageId ||
        ""
      ).trim()
    };
  }
  return null;
}

function clampDelayMs(value, fallback = 0) {
  const parsed = Number(value);
  const resolved = Number.isFinite(parsed) ? parsed : fallback;
  return Math.max(0, Math.min(60000, resolved));
}

function normalizeTargetDelays(targets, targetDelaysMs, fallbackDelayMs = 0) {
  const fallback = clampDelayMs(fallbackDelayMs);
  return targets.map((_, index) => clampDelayMs(
    Array.isArray(targetDelaysMs) ? targetDelaysMs[index] : undefined,
    fallback
  ));
}

function buildKickSequence(targets, loopCount, targetDelaysMs, fallbackDelayMs = 0) {
  const delays = normalizeTargetDelays(targets, targetDelaysMs, fallbackDelayMs);
  const sequence = [];
  for (let loop = 1; loop <= loopCount; loop++) {
    for (let targetIndex = 0; targetIndex < targets.length; targetIndex++) {
      sequence.push({
        loop,
        target: targets[targetIndex],
        targetIndex: targetIndex + 1,
        delayMs: delays[targetIndex]
      });
    }
  }
  return sequence;
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
    subscribedRooms: new Set(),
    pendingJoinRoom: "",
    pingTimer: null,
    lastHeartbeatAt: 0,
    jobPollTimer: null,
    pendingJobs: new Map(),
    outboundScheduler: null,
    reconnectTimer: null,
    reconnectAttempt: 0,
    authTimer: null,
    authFailed: false,
    manuallyClosed: false
  };
}

wss.on("connection", dashboard => {
  dashboard.isAlive = true;
  dashboard.on("pong", () => { dashboard.isAlive = true; });
  const dashboardHeartbeat = setInterval(() => {
    if (dashboard.readyState !== WebSocket.OPEN) return;
    if (dashboard.isAlive === false) {
      try { dashboard.terminate(); } catch {}
      return;
    }
    dashboard.isAlive = false;
    try { dashboard.ping(); } catch {}
  }, 25000);

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
  safeSend(dashboard, {type: "dashboard.ready", accounts: 10});

  function dashboardStatus(i, status, extra = {}) {
    safeSend(dashboard, {type: "status", index: i, status, ...extra});
  }

  function autoKickState(status = "idle", extra = {}) {
    const remainingMs = autoKick.countdownEndAt
      ? Math.max(0, autoKick.countdownEndAt - Date.now())
      : null;
    safeSend(dashboard, {
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

  function beginAutoKickCountdown(index, detection, source = "detected") {
    const detectedRoom = detection.room;
    if (autoKick.countdownEndAt) return;

    autoKick.countdownEndAt = Date.now() + 60000;
    autoKick.countdownTriggered = false;
    autoKick.source = source;
    if (source === "detected") {
      safeSend(dashboard, {
        type: "autoKick.detected",
        index,
        room: detectedRoom,
        targetUsername: detection.targetUsername,
        startedBy: detection.startedBy,
        eventId: detection.eventId,
        message: detection.message,
        countdownMs: autoKick.countdownMs,
        thresholdMs: autoKick.thresholdMs
      });
    } else {
      safeSend(dashboard, {
        type: "autoKick.manual.started",
        room: detectedRoom,
        countdownMs: autoKick.countdownMs,
        thresholdMs: autoKick.thresholdMs
      });
    }
    safeSend(dashboard, {
      type: "log",
      index,
      message: source === "detected"
        ? `Deteksi vote-kick di room ${detectedRoom}: target ${detection.targetUsername}. Countdown auto kick dimulai dari ${autoKick.countdownMs} ms.`
        : `Timer auto kick dimulai manual untuk room ${detectedRoom} dari ${autoKick.countdownMs} ms.`
    });
    autoKickState("countdown", {source});

    autoKick.countdownInterval = setInterval(() => {
      const remainingMs = Math.max(0, autoKick.countdownEndAt - Date.now());
      autoKickState(autoKick.countdownTriggered ? "triggering" : "countdown", {remainingMs});

      if (!autoKick.countdownTriggered && remainingMs <= autoKick.thresholdMs) {
        autoKick.countdownTriggered = true;
        safeSend(dashboard, {
          type: "autoKick.triggered",
          room: autoKick.room,
          thresholdMs: autoKick.thresholdMs,
          remainingMs
        });

        if (!autoKick.targets.length) {
          safeSend(dashboard, {
            type: "error",
            index: 0,
            message: "Timer selesai, tetapi belum ada target kick yang dipilih."
          });
        } else if (commandQueueRunning) {
          safeSend(dashboard, {
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
              targetDelaysMs: autoKick.targetDelaysMs
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

  function startAutoKickCountdown(index, data) {
    const detection = detectVoteKickTimer(data);
    if (!detection) return false;
    if (!autoKick.enabled || !autoKick.room) return false;
    if (!roomMatches(detection.room, autoKick.room)) return false;
    return beginAutoKickCountdown(index, detection, "detected");
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
        safeSend(dashboard, {
          type: "log",
          index: i,
          message: `JOB ${jobId} tidak selesai setelah 60 detik; polling dihentikan`
        });
      } else {
        sendToAccount(i, {type: "job.get", job_id: jobId});
      }

      if (a.pendingJobs.size) scheduleJobPoll(i);
    }, 500);
  }

  function trackQueuedJob(i, data) {
    const jobId = queuedJobId(data);
    if (!jobId) {
      safeSend(dashboard, {
        type: "log",
        index: i,
        message: `API mengembalikan ${data?.type || "queued"} tanpa job_id`
      });
      return;
    }

    const payload = responsePayload(data);
    accounts[i].pendingJobs.set(jobId, {
      command: String(data?.type || "").replace(/\.queued$/, ""),
      room: payload?.room || data?.room || "",
      target: payload?.target_username || data?.target_username || "",
      attempts: 0
    });
    safeSend(dashboard, {
      type: "log",
      index: i,
      message: `JOB ${jobId} diantrikan (${String(data?.type || "command").replace(/\.queued$/, "")})`
    });
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
    safeSend(dashboard, {
      type: "log",
      index: i,
      message: `JOB ${jobId} ${ok ? "selesai" : `gagal (${status})`}${responseError(data) && !ok ? `: ${responseError(data)}` : ""}`
    });

    const payload = responsePayload(data);
    const wallet = payload?.wallet || payload?.data?.wallet;
    if (wallet) {
      safeSend(dashboard, {
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
        safeSend(dashboard, {
          type: "log",
          index: i,
          message: "PING timeout; socket ditutup agar relogin otomatis berjalan"
        });
        try { a.ws.terminate(); } catch {}
        return;
      }
      try { a.ws.ping(); } catch {}
      enqueueOutbound(i, {type: "ping"}, {priority: true});
      safeSend(dashboard, {type: "log", index: i, message: "PING keep-alive dikirim"});
    }, 50000);
  }

  function clearReconnect(i) {
    const a = accounts[i];
    if (a.reconnectTimer) clearTimeout(a.reconnectTimer);
    a.reconnectTimer = null;
  }

  function closeAccount(i, manual = true) {
    const a = accounts[i];
    // Mark manual logout BEFORE touching the socket so the close handler
    // can never schedule an automatic reconnect.
    a.manuallyClosed = manual;
    clearReconnect(i);
    stopPing(i);
    stopJobPolling(i);
    stopOutbound(i);
    if (a.authTimer) clearTimeout(a.authTimer);
    a.authTimer = null;
    a.ready = false;
    a.authFailed = false;
    a.joined.clear();
    a.requestedRooms.clear();
    a.subscribedRooms.clear();
    a.pendingJoinRoom = "";
    const ws = a.ws;
    // Detach first so a late close event from the old socket cannot change
    // the account back to ERROR/ONLINE or trigger reconnect logic.
    a.ws = null;
    if (ws) {
      try {
        if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
          ws.terminate();
        }
      } catch {}
    }
    dashboardStatus(i, "offline");
  }

  function scheduleReconnect(i) {
    const a = accounts[i];
    if (a.manuallyClosed || a.authFailed || !a.username || !a.password) return;
    clearReconnect(i);
    const delayMs = Math.min(15000, Math.round(2000 * Math.pow(1.5, Math.min(a.reconnectAttempt, 5))));
    a.reconnectAttempt += 1;
    safeSend(dashboard, {
      type: "log",
      index: i,
      message: `RELOGIN otomatis dalam ${delayMs} ms (percobaan ${a.reconnectAttempt})`
    });
    a.reconnectTimer = setTimeout(() => connectAccount(i), delayMs);
  }

  function rejoinRequestedRooms(i) {
    const a = accounts[i];
    const rooms = [...a.requestedRooms].filter(room => !hasJoinedRoom(a, room));
    rooms.forEach((room, roomIndex) => {
      setTimeout(() => {
        if (
          !a.ready ||
          !a.ws ||
          a.ws.readyState !== WebSocket.OPEN ||
          hasJoinedRoom(a, room)
        ) return;

        a.pendingJoinRoom = room;
        safeSend(dashboard, {
          type: "log",
          index: i,
          message: `REJOIN ${room} dikirim setelah koneksi pulih`
        });
        a.ws.send(JSON.stringify({type: "room.join", room}));
      }, roomIndex * 600);
    });
  }

  function connectAccount(i, options = {}) {
    const a = accounts[i];
    if (options.resetBackoff) a.reconnectAttempt = 0;
    clearReconnect(i);
    stopPing(i);
    stopJobPolling(i);
    stopOutbound(i);
    a.authFailed = false;
    if (a.authTimer) clearTimeout(a.authTimer);
    a.authTimer = null;
    a.manuallyClosed = false;
    a.ready = false;
    a.joined.clear();
    a.subscribedRooms.clear();
    a.pendingJoinRoom = "";

    if (!a.username || !a.password) {
      dashboardStatus(i, "error", {message: "Username dan password wajib diisi"});
      return;
    }

    try { if (a.ws) a.ws.close(); } catch {}
    dashboardStatus(i, "connecting");

    const ws = new WebSocket(MIG_WS_URL);
    a.ws = ws;
    ws.on("pong", () => {
      a.lastHeartbeatAt = Date.now();
      safeSend(dashboard, {type: "log", index: i, message: "PONG keep-alive diterima"});
    });

    ws.on("open", () => {
      safeSend(dashboard, {type: "log", index: i, message: "WebSocket OPEN; menunggu auth.required"});
    });

    ws.on("message", raw => {
      if (a.ws !== ws) return;

      let data;
      try { data = JSON.parse(raw.toString()); }
      catch {
        safeSend(dashboard, {type: "raw", index: i, data: raw.toString().slice(0, 1000)});
        return;
      }

      if (["pong", "ping.result", "heartbeat", "heartbeat.result"].includes(String(data?.type || "").toLowerCase())) {
        a.lastHeartbeatAt = Date.now();
      }
      if (String(data?.type || "").toLowerCase() === "ping") {
        safeSend(ws, {type: "pong"});
      }
      safeSend(dashboard, {type: "api", index: i, data});
      if (String(data?.type || "").endsWith(".queued")) trackQueuedJob(i, data);
      if (["job.status.result", "job.get.result", "job.status"].includes(data?.type)) {
        handleJobStatus(i, data);
      }
      if (data.type === "room.participants" || data.type === "room.participants.result") {
        safeSend(dashboard, {type: "participants.raw", index: i, data: JSON.stringify(data).slice(0, 8000)});
      }

      if (data.type === "room.participants.result" || data.type === "room.participants") {
        const payload = data?.data ?? data?.result ?? data;
        const users = normalizeUsers(payload);
        const room = payload?.room || data?.room || data?.data?.room || accounts[i].pendingJoinRoom || "";
        safeSend(dashboard, {type: "participants", index: i, room, users});
        safeSend(dashboard, {type: "log", index: i, message: `Participants response: ${users.length} username terdeteksi${room ? ` untuk room ${room}` : ""}`});
        if (!users.length) safeSend(dashboard, {type: "log", index: i, message: `Participants raw: ${JSON.stringify(data).slice(0, 8000)}`});
      }

      const voteKickDetection = detectVoteKickTimer(data);
      if (voteKickDetection) {
        const started = startAutoKickCountdown(i, data);
        if (!started) {
          safeSend(dashboard, {
            type: "log",
            index: i,
            message: `Vote-kick terdeteksi tetapi timer tidak di-arm: room=${voteKickDetection.room}, konfigurasi room=${autoKick.room || "-"}.`
          });
        }
      } else if (roomTextCandidates(data).some(text => VOTE_KICK_TEXT_RE.test(text))) {
        safeSend(dashboard, {
          type: "log",
          index: i,
          message: `Kandidat vote-kick diterima tetapi sender bukan room: room=${eventRoom(data) || "-"}, sender=${roomSenderCandidates(data).join(", ") || "-"}.`
        });
      }

      if (data.type === "auth.required") {
        // Once session.ready has been received, a late/duplicate auth.required
        // must never downgrade a successful account back to AUTH.
        if (a.ready) return;
        if (a.authTimer) clearTimeout(a.authTimer);
        a.authTimer = setTimeout(() => {
          if (a.ws !== ws || a.ready || a.authFailed) return;
          a.authFailed = true;
          safeSend(dashboard, {type:"log", index:i, message:"AUTH timeout: server tidak menerima session.ready setelah login"});
          dashboardStatus(i, "error", {authFailed:true, message:"AUTH timeout"});
          try { ws.close(1000, "authentication timeout"); } catch {}
        }, 15000);
        safeSend(dashboard, {type: "log", index: i, message: "auth.required diterima"});
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
        a.reconnectAttempt = 0;
        a.lastHeartbeatAt = Date.now();
        const permissions =
          data?.data?.developer?.permissions ||
          data?.data?.permissions ||
          [];
        a.permissions = Array.isArray(permissions) ? permissions : [];
        const wallet = data?.data?.wallet || data?.data?.developer?.wallet || null;

        dashboardStatus(i, "online", {
          permissions,
          balance: wallet?.label || "-"
        });
        safeSend(dashboard, {type: "log", index: i, message: "LOGIN BERHASIL; keep-alive aktif"});
        startPing(i);
        rejoinRequestedRooms(i);
        return;
      }

      if (data.type === "error") {
        const err = responseError(data);
        safeSend(dashboard, {type: "log", index: i, message: `API ERROR: ${publicError(err)}`});

        // Authentication failures must never enter a reconnect loop.
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

        safeSend(dashboard, {type: "error", index: i, message: publicError(err)});
        return;
      }

      if (data.type === "session.replaced") {
        a.ready = false;
        a.joined.clear();
        a.pendingJoinRoom = "";
        stopPing(i);
        dashboardStatus(i, "error", {message: "Session replaced"});
      try { ws.close(4001, "session replaced"); } catch { scheduleReconnect(i); }
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
          a.joined.add(joinedRoom);
          a.requestedRooms.add(joinedRoom);
          a.pendingJoinRoom = "";
          dashboardStatus(i, "online", {room: joinedRoom});
          safeSend(dashboard, {type: "log", index: i, message: `JOIN BERHASIL: ${joinedRoom}`});
          subscribeRoomText(i, joinedRoom);
        } else {
          safeSend(dashboard, {
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
          for (const joinedRoom of a.joined) {
            if (roomMatches(joinedRoom, leftRoom)) a.joined.delete(joinedRoom);
          }
          for (const requestedRoom of a.requestedRooms) {
            if (roomMatches(requestedRoom, leftRoom)) a.requestedRooms.delete(requestedRoom);
          }
          unsubscribeRoomText(i, leftRoom);
          if (roomMatches(a.pendingJoinRoom, leftRoom)) a.pendingJoinRoom = "";
        }
      }

      if (data.type === "wallet.balance.result") {
        const wallet = data?.data?.wallet;
        if (wallet) safeSend(dashboard, {
          type: "balance",
          index: i,
          balance: wallet.label || String(wallet.balance_cr || "-")
        });
      }
    });

    ws.on("error", err => {
      safeSend(dashboard, {
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
      a.ready = false;
      a.ws = null;

      dashboardStatus(i, wasAuthFailure ? "error" : "offline", {
        code,
        reason,
        authFailed: wasAuthFailure
      });
      safeSend(dashboard, {
        type: "log",
        index: i,
        message: `CLOSED code=${code} reason=${reason}`
      });

      if (!wasAuthFailure) scheduleReconnect(i);
    });
  }

  function canSendToAccount(i, payload) {
    const a = accounts[i];
    if (!a.ws || a.ws.readyState !== WebSocket.OPEN || !a.ready) {
      safeSend(dashboard, {type: "log", index: i, message: "Belum session.ready; command dilewati"});
      return false;
    }
    const permission = requiredPermissionFor(payload);
    if (permission && a.permissions.length && !a.permissions.includes(permission)) {
      safeSend(dashboard, {
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
      onQueueFull: payload => safeSend(dashboard, {
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
    return true;
  }

  function sendKickToAccount(i, payload) {
    // Kick traffic is throttled and queued so it cannot monopolize the
    // same WebSocket used by the application-level keep-alive.
    return enqueueOutbound(i, payload, {spacingMs: 100});
  }

  function subscribeRoomText(i, room) {
    const name = String(room || "").trim();
    const a = accounts[i];
    if (!name || a.subscribedRooms.has(name)) return false;
    a.subscribedRooms.add(name);
    safeSend(dashboard, {
      type: "log",
      index: i,
      message: `Listener room.text aktif setelah JOIN untuk ${name}`
    });
    return true;
  }

  function unsubscribeRoomText(i, room) {
    const name = String(room || "").trim();
    const a = accounts[i];
    if (!name || !a.subscribedRooms.has(name)) return false;
    a.subscribedRooms.delete(name);
    return true;
  }

  async function runKickQueue(room, targets, delayMs, loopCount, source = "kickAll", options = {}) {
    if (!room || !targets.length) return {started: false, sent: 0, skipped: 0, total: 0};
    if (commandQueueRunning) {
      safeSend(dashboard, {
        type: "error",
        index: 0,
        message: "Kick All masih berjalan. Tunggu sampai selesai sebelum menjalankan lagi."
      });
      return {started: false, sent: 0, skipped: 0, total: targets.length * 10 * loopCount};
    }

    const total = targets.length * 10 * loopCount;
    const targetDelaysMs = normalizeTargetDelays(
      targets,
      options.targetDelaysMs,
      options.batchDelayMs ?? delayMs
    );
    const socketDelayMs = Math.max(0, Math.min(60000, Number(options.socketDelayMs) || 0));
    const sequentialMode = options.sequentialMode === true;
    safeSend(dashboard, {
      type: "kickQueue.start",
      room,
      targets,
      targetDelaysMs,
      socketDelayMs,
      sequentialMode,
      loopCount,
      total,
      source
    });

    commandQueueRunning = true;
    let actionNo = 0;
    let sent = 0;
    let skipped = 0;
    safeSend(dashboard, {
      type: "kickQueue.progress",
      done: 0,
      total,
      sent: 0,
      skipped: 0,
      source
    });
    try {
      for (const step of buildKickSequence(targets, loopCount, targetDelaysMs)) {
        const {loop, targetIndex, target, delayMs: targetDelayMs} = step;
        for (let n = 0; n < 10; n++) {
          actionNo++;
          const account = accounts[n];
          const canKick =
            account.ready &&
            account.ws?.readyState === WebSocket.OPEN &&
            hasJoinedRoom(account, room);
          const didSend = canKick && sendKickToAccount(n, {
            type: "room.kick",
            room,
            target_username: target
          });

          if (didSend) {
            sent++;
          } else {
            skipped++;
            safeSend(dashboard, {
              type: "log",
              index: n,
              message: `KICK dilewati: ID #${n + 1} belum siap atau belum terkonfirmasi masuk room ${room}`
            });
          }

          safeSend(dashboard, {
            type: "kickQueue.step",
            loop,
            targetIndex: targetIndex + 1,
            target,
            accountIndex: n + 1,
            delayMs: targetDelayMs,
            actionNo,
            total,
            sent: didSend,
            status: didSend ? "queued" : "skipped"
          });
          safeSend(dashboard, {
            type: "kickQueue.progress",
            done: actionNo,
            total,
            sent,
            skipped,
            loop,
            targetIndex: targetIndex + 1,
            source
          });

          if (sequentialMode && n < 9) {
            if (socketDelayMs > 0) {
              await new Promise(resolve => setTimeout(resolve, socketDelayMs));
            } else {
              await new Promise(resolve => setImmediate(resolve));
            }
          }
        }
        if (targetDelayMs > 0) {
          await new Promise(resolve => setTimeout(resolve, targetDelayMs));
        } else {
          await new Promise(resolve => setImmediate(resolve));
        }
      }
    } catch (err) {
      safeSend(dashboard, {
        type: "kickQueue.error",
        total,
        done: actionNo,
        sent,
        skipped,
        source,
        message: err?.message || String(err)
      });
      safeSend(dashboard, {
        type: "kickQueue.done",
        total,
        done: actionNo,
        sent,
        skipped,
        source,
        error: true
      });
      return {started: true, sent, skipped, total, error: err?.message || String(err)};
    } finally {
      commandQueueRunning = false;
    }

    safeSend(dashboard, {type: "kickQueue.done", total, done: total, sent, skipped, source});
    return {started: true, sent, skipped, total};
  }

  async function joinAll(room) {
    const name = String(room || "").trim();
    if (!name) return;
    for (let i = 0; i < 10; i++) {
      const a = accounts[i];
      if (!a.ws || a.ws.readyState !== WebSocket.OPEN || !a.ready) continue;

      dashboardStatus(i, "joining", {room: name});
      safeSend(dashboard, {type: "log", index: i, message: `JOIN ${name} dikirim`});
      const didSend = sendToAccount(i, {type: "room.join", room: name});
      if (didSend) {
        a.pendingJoinRoom = name;
        a.requestedRooms.add(name);
      }

      // Mark the room only after room.join.result confirms success.
    }
  }

  dashboard.on("message", async raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); }
    catch { return; }

    const i = Number(msg.index);

    if (msg.action === "login" && Number.isInteger(i) && i >= 0 && i < 10) {
      accounts[i].username = String(msg.username || "").trim();
      accounts[i].password = String(msg.password || "");
      connectAccount(i, {resetBackoff: true});
      return;
    }

    if (msg.action === "disconnect" && Number.isInteger(i) && i >= 0 && i < 10) {
      closeAccount(i, true);
      return;
    }

    if (msg.action === "loginAll") {
      for (let n = 0; n < 10; n++) {
        if (msg.accounts?.[n]?.username && msg.accounts?.[n]?.password) {
          accounts[n].username = String(msg.accounts[n].username).trim();
          accounts[n].password = String(msg.accounts[n].password);

          if (accounts[n].ready && accounts[n].ws?.readyState === WebSocket.OPEN) {
            safeSend(dashboard, {type: "log", index: n, message: "Login All: sudah Online, login ulang dilewati"});
            continue;
          }

          // If an earlier attempt is stuck in CONNECTING/AUTH, discard that
          // socket and start a fresh connection immediately. No artificial delay.
          if (accounts[n].ws) closeAccount(n, true);
          connectAccount(n, {resetBackoff: true});
        }
      }
      return;
    }

    if (msg.action === "disconnectAll") {
      for (let n = 0; n < 10; n++) closeAccount(n, true);
      safeSend(dashboard, {type: "logout.done"});
      return;
    }

    if (msg.action === "logoutAll") {
      // Acknowledge immediately so the UI can never wait for socket close events.
      safeSend(dashboard, {type: "logout.done"});
      for (let n = 0; n < 10; n++) {
        try { closeAccount(n, true); } catch (err) {
          safeSend(dashboard, {type: "log", index: n, message: `Logout cleanup error: ${publicError(err)}`});
          dashboardStatus(n, "offline");
        }
      }
      return;
    }

    if (msg.action === "joinAll") {
      const room = String(msg.room || "").trim();
      if (!room) { safeSend(dashboard, {type:"error", index:0, message:"Nama room wajib diisi."}); return; }
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
          safeSend(dashboard, {
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
        safeSend(dashboard, {
          type: "error",
          index: 0,
          message: `Tidak ada ID Online yang tercatat masuk room ${room}. Tekan Enter Room — All terlebih dahulu.`
        });
        return;
      }

      const ok = sendToAccount(source, {type: "room.participants", room});
      if (ok) {
        safeSend(dashboard, {
          type: "participants.source",
          index: source,
          room
        });
        safeSend(dashboard, {
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
      autoKick.sequentialMode = msg.sequentialMode === true;
      if (!autoKick.enabled) stopAutoKickCountdown("disabled");
      else autoKickState(autoKick.countdownEndAt ? "countdown" : "armed");
      return;
    }

    if (msg.action === "autoKick.reset") {
      stopAutoKickCountdown("manual-reset", null, autoKick.countdownMs);
      safeSend(dashboard, {type: "log", index: 0, message: "Timer auto kick di-reset."});
      return;
    }

    if (msg.action === "autoKick.start") {
      const started = startManualAutoKickCountdown();
      if (!started) {
        safeSend(dashboard, {
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
        safeSend(dashboard, {type: "log", index: 0, message: "Timer auto kick dihentikan manual."});
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
      const targetDelaysMs = Array.isArray(msg.targetDelaysMs)
        ? msg.targetDelaysMs.slice(0, targets.length).map(value => clampDelayMs(value))
        : [];
      const loopCount = Math.max(1, Math.min(100, Number(msg.loopCount) || 1));
      const socketDelayMs = Math.max(0, Math.min(60000, Number(msg.socketDelayMs) || 0));
      const sequentialMode = msg.sequentialMode === true;

      if (!room || !targets.length) return;
      runKickQueue(room, targets, delayMs, loopCount, "kickAll", {
        socketDelayMs,
        sequentialMode,
        targetDelaysMs
      });
      return;
    }

  });

  dashboard.on("close", () => {
    clearInterval(dashboardHeartbeat);
    stopAutoKickCountdown("dashboard-closed");
    for (let i = 0; i < 10; i++) closeAccount(i, true);
  });
});

function shutdown(signal) {
  console.log(`${signal} received; shutting down`);
  wss.clients.forEach(client => {
    try { client.close(1001, "server shutdown"); } catch {}
  });
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 8000).unref();
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

if (require.main === module) {
  server.listen(PORT, "0.0.0.0", () => {
    console.log(`MigMaster backend listening on 0.0.0.0:${PORT}`);
    console.log(`Upstream MigReborn Developer API: ${MIG_WS_URL}`);
  });
}

module.exports = {
  createOutboundScheduler,
  buildKickSequence,
  detectVoteKickTimer,
  isRoomSender
};
