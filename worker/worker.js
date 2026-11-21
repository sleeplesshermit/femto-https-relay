// Femto worker: VLESS front over WebSocket OR XHTTP (stream-one).
// Byte pumps run over plain HTTPS to the slew relay app:
//   POST /s/new opens a target session, POST /s/up carries client->target,
//   long-poll GET /s/down carries target->client. Landing IP = slew's.
// XHTTP path follows the stitch template: /<token>&https=<relay-host>:<port>
// The XHTTP serving pattern mirrors the public zizifn/edgetunnel-family
// worker (the Danfeng template): IdentityTransformStream bridge, response
// returned immediately with grpc-style headers, BYOB upload reads.
// ---- Configuration: set these three values before deploying ----
// FEMTO_UUID: the VLESS UUID your clients will use.
// FEMTO_RELAY_URL: public HTTPS base URL of the femto relay app.
// FEMTO_RELAY_KEY: shared secret between this Worker and the relay app;
//                  it must match RELAY_KEY on the relay side.
const FEMTO_UUID = '11111111-2222-3333-4444-555555555555';
const FEMTO_RELAY_URL = 'https://your-relay-app.example.com';
const FEMTO_RELAY_KEY = 'change-me-relay-key';
const UUID_HEX = FEMTO_UUID.replace(/-/g, '').toLowerCase();
const RELAY_BASE = FEMTO_RELAY_URL.replace(/\/+$/, '');
const RELAY_KEY = FEMTO_RELAY_KEY;
const RELAY_HOST = new URL(RELAY_BASE).hostname;
const PATH_TOKEN = UUID_HEX.slice(0, 8);
const XHTTP_HEADERS = {
  'Content-Type': 'application/octet-stream',
  'grpc-status': '0',
  'X-Accel-Buffering': 'no',
  'Cache-Control': 'no-store',
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function bytesToHex(u8) {
  let s = '';
  for (const b of u8) s += b.toString(16).padStart(2, '0');
  return s;
}

// Returns null when more bytes are needed; throws on a bad header.
function tryParseVless(buf) {
  if (buf.length >= 1 && buf[0] !== 0) throw new Error('bad version');
  if (buf.length >= 17 && bytesToHex(buf.subarray(1, 17)) !== UUID_HEX) throw new Error('bad uuid');
  if (buf.length < 18) return null;
  let off = 17;
  const optLen = buf[off];
  off += 1 + optLen;
  if (buf.length < off + 4) return null;
  const cmd = buf[off];
  off += 1;
  if (cmd !== 1) throw new Error('only tcp supported');
  const port = (buf[off] << 8) | buf[off + 1];
  off += 2;
  const atype = buf[off];
  off += 1;
  let host;
  if (atype === 1) {
    if (buf.length < off + 4) return null;
    host = Array.from(buf.subarray(off, off + 4)).join('.');
    off += 4;
  } else if (atype === 2) {
    if (buf.length < off + 1) return null;
    const len = buf[off];
    off += 1;
    if (buf.length < off + len) return null;
    host = new TextDecoder().decode(buf.subarray(off, off + len));
    off += len;
  } else if (atype === 3) {
    if (buf.length < off + 16) return null;
    const parts = [];
    for (let i = 0; i < 8; i++) {
      parts.push(((buf[off] << 8) | buf[off + 1]).toString(16));
      off += 2;
    }
    host = parts.join(':');
  } else {
    throw new Error('bad address type');
  }
  return { host, port, payload: buf.subarray(off) };
}

// One proxied TCP stream. send(u8) delivers target->client bytes on the
// transport; closeTransport() tears the transport down.
function createSession(send, closeTransport) {
  let sid = null;
  let closed = false;
  let started = false;
  let upChain = Promise.resolve();
  let buf = new Uint8Array(0);

  const cleanup = () => {
    if (closed) return;
    closed = true;
    if (upTimer) { clearTimeout(upTimer); upTimer = null; }
    try { closeTransport(); } catch {}
    if (sid) {
      fetch(RELAY_BASE + '/s/close?k=' + RELAY_KEY + '&sid=' + sid, { method: 'POST' }).catch(() => {});
    }
  };

  const postUp = (u8) => {
    upChain = upChain.then(async () => {
      if (closed || !sid) return;
      const r = await fetch(RELAY_BASE + '/s/up?k=' + RELAY_KEY + '&sid=' + sid, { method: 'POST', body: u8 });
      if (r.status === 410 || r.status === 404) cleanup();
    }).catch(() => {});
  };

  // Upload coalescing: transports (XHTTP especially) deliver many small
  // chunks; POSTing each one separately costs a full Worker->relay round
  // trip per chunk. Buffer briefly and POST in ~64 KB batches instead.
  const FLUSH_SIZE = 65536;
  const FLUSH_MS = 8;
  let upParts = [];
  let upBytes = 0;
  let upTimer = null;
  const flushUp = () => {
    if (upTimer) { clearTimeout(upTimer); upTimer = null; }
    if (!upBytes) return;
    let merged;
    if (upParts.length === 1) {
      merged = upParts[0];
    } else {
      merged = new Uint8Array(upBytes);
      let o = 0;
      for (const p of upParts) { merged.set(p, o); o += p.length; }
    }
    upParts = [];
    upBytes = 0;
    postUp(merged);
  };
  const queueUp = (u8) => {
    if (closed) return;
    upParts.push(u8);
    upBytes += u8.length;
    if (upBytes >= FLUSH_SIZE) flushUp();
    else if (!upTimer) upTimer = setTimeout(flushUp, FLUSH_MS);
  };

  const downPump = async () => {
    let first = true;
    while (!closed && sid) {
      let res;
      try {
        res = await fetch(RELAY_BASE + '/s/down?k=' + RELAY_KEY + '&sid=' + sid + '&wait=45');
      } catch {
        await sleep(400);
        continue;
      }
      if (res.status === 404) break;
      const isClosed = res.headers.get('X-Closed') === '1';
      const data = new Uint8Array(await res.arrayBuffer());
      if (data.length) {
        try {
          if (first) {
            const merged = new Uint8Array(2 + data.length);
            merged.set([0, 0], 0);
            merged.set(data, 2);
            await send(merged);
            first = false;
          } else {
            await send(data);
          }
        } catch {
          break;
        }
      }
      if (isClosed && data.length === 0) break;
    }
    cleanup();
  };

  const feed = (u8) => {
    if (closed) return;
    if (started) {
      queueUp(u8);
      return;
    }
    const merged = new Uint8Array(buf.length + u8.length);
    merged.set(buf);
    merged.set(u8, buf.length);
    buf = merged;
    let parsed;
    try {
      parsed = tryParseVless(buf);
    } catch {
      cleanup();
      return;
    }
    if (!parsed) return;
    started = true;
    (async () => {
      try {
        let res = null;
        // The relay app scales to zero; a cold instance can refuse or drop
        // the first /s/new. Retry twice before giving up.
        for (let attempt = 0; attempt < 3; attempt++) {
          try {
            res = await fetch(RELAY_BASE + '/s/new?k=' + RELAY_KEY, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ host: parsed.host, port: parsed.port }),
            });
          } catch {
            res = null;
          }
          if (res && res.ok) break;
          res = null;
          await sleep(1500);
        }
        if (!res) { cleanup(); return; }
        const j = await res.json();
        sid = j.sid;
        downPump();
        if (parsed.payload.length) { queueUp(parsed.payload); flushUp(); }
      } catch {
        cleanup();
      }
    })();
  };

  return { feed, cleanup };
}

function handleWebSocket(request, ctx) {
  const pair = new WebSocketPair();
  const client = pair[0];
  const server = pair[1];
  server.accept({ allowHalfOpen: true });
  server.binaryType = 'arraybuffer';
  const session = createSession(
    async (u8) => { server.send(u8.buffer); },
    () => { try { server.close(); } catch {} }
  );
  server.addEventListener('message', (event) => {
    (async () => {
      if (typeof event.data === 'string') { session.cleanup(); return; }
      if (event.data instanceof Blob) session.feed(new Uint8Array(await event.data.arrayBuffer()));
      else session.feed(new Uint8Array(event.data));
    })();
  });
  server.addEventListener('close', session.cleanup);
  server.addEventListener('error', session.cleanup);
  return new Response(null, { status: 101, webSocket: client });
}

function handleXhttp(request, ctx) {
  const bridge = new IdentityTransformStream();
  const responseWriter = bridge.writable.getWriter();
  let writerGone = false;
  const session = createSession(
    async (u8) => { await responseWriter.write(u8); },
    () => {
      if (writerGone) return;
      writerGone = true;
      try { responseWriter.close(); } catch {}
    }
  );
  (async () => {
    try {
      const reader = request.body.getReader({ mode: 'byob' });
      for (;;) {
        const { done, value } = await reader.read(new Uint8Array(16384));
        if (done) break;
        if (value && value.byteLength) session.feed(value);
      }
    } catch {
      session.cleanup();
    }
  })();
  return new Response(bridge.readable, { headers: XHTTP_HEADERS });
}

export default {
  async fetch(request, env, ctx) {
    const up = request.headers.get('Upgrade');
    if (up && up.toLowerCase() === 'websocket') {
      return handleWebSocket(request, ctx);
    }
    if (request.method === 'POST') {
      const url = new URL(request.url);
      const m = /^\/([0-9a-f]{8})&https=([a-z0-9.\-]+):(\d+)/i.exec(url.pathname);
      if (m && m[1].toLowerCase() === PATH_TOKEN && m[2].toLowerCase() === RELAY_HOST) {
        return handleXhttp(request, ctx);
      }
      return new Response('not found', { status: 404 });
    }
    return new Response('femto worker ok', { status: 200 });
  },
};
