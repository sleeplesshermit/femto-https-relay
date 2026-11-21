// slew-relay: HTTPS request/response relay for TCP streams.
// A client (Cloudflare Worker) opens a session to a target host:port, then
// pumps bytes with POST /s/up (client->target) and long-poll GET /s/down
// (target->client). Built-ins only; slew runs it as `node server/index.mjs`.
import http from 'node:http';
import net from 'node:net';
import crypto from 'node:crypto';

// Shared secret with the femto Worker; override with env FEMTO_RELAY_KEY.
const KEY = process.env.FEMTO_RELAY_KEY || 'change-me-relay-key';
const PORT = process.env.PORT || 3000;
const MAX_SESSIONS = 128;
const IDLE_MS = 120000;
const BUF_PAUSE = 8 * 1024 * 1024;
const BUF_RESUME = 2 * 1024 * 1024;

const sessions = new Map();

function wake(sess) {
  for (const w of sess.waiters) { try { w(); } catch {} }
  sess.waiters.clear();
}

function destroySession(sess) {
  if (sess.dead) return;
  sess.dead = true;
  try { sess.sock.destroy(); } catch {}
  wake(sess);
  sessions.delete(sess.id);
}

function makeSession(sock, host, port) {
  const sess = {
    id: crypto.randomBytes(9).toString('hex'),
    sock, host, port,
    chunks: [], buffered: 0,
    rxTotal: 0, txTotal: 0,
    closed: false, dead: false,
    lastSeen: Date.now(),
    waiters: new Set(),
  };
  sock.on('data', (d) => {
    sess.chunks.push(d);
    sess.buffered += d.length;
    sess.rxTotal += d.length;
    sess.lastSeen = Date.now();
    if (sess.buffered > BUF_PAUSE) { try { sock.pause(); } catch {} }
    wake(sess);
  });
  const fin = () => { sess.closed = true; wake(sess); };
  sock.once('close', fin);
  sock.once('error', fin);
  sessions.set(sess.id, sess);
  return sess;
}

setInterval(() => {
  const now = Date.now();
  for (const sess of [...sessions.values()]) {
    if (now - sess.lastSeen > IDLE_MS) destroySession(sess);
  }
}, 30000).unref();

function readBody(req, cap) {
  return new Promise((resolve, reject) => {
    const parts = [];
    let total = 0;
    req.on('data', (d) => {
      total += d.length;
      if (total > cap) { reject(new Error('body too large')); req.destroy(); return; }
      parts.push(d);
    });
    req.on('end', () => resolve(Buffer.concat(parts)));
    req.on('error', reject);
  });
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const sendJson = (code, obj) => { res.writeHead(code, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(obj)); };

  if (url.pathname === '/') { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('slew-relay alive ' + new Date().toISOString()); return; }
  if (url.searchParams.get('k') !== KEY) { sendJson(403, { error: 'bad key' }); return; }

  try {
    if (url.pathname === '/s/new' && req.method === 'POST') {
      if (sessions.size >= MAX_SESSIONS) { sendJson(503, { error: 'too many sessions' }); return; }
      const body = await readBody(req, 4096);
      let target;
      try { target = JSON.parse(body.toString('utf8')); } catch { sendJson(400, { error: 'bad json' }); return; }
      const host = String(target.host || '');
      const port = Number(target.port || 0);
      if (!host || !port) { sendJson(400, { error: 'host and port required' }); return; }
      const t0 = Date.now();
      const sock = net.connect({ host, port });
      sock.setTimeout(10000);
      const onErr = (e) => { try { sock.destroy(); } catch {} if (!res.writableEnded) sendJson(502, { error: e.code || String(e) }); };
      sock.once('timeout', () => onErr(new Error('connect timeout')));
      sock.once('error', onErr);
      sock.once('connect', () => {
        sock.setTimeout(0);
        sock.removeListener('error', onErr);
        const sess = makeSession(sock, host, port);
        sendJson(200, { sid: sess.id, ms: Date.now() - t0 });
      });
      return;
    }

    const sid = url.searchParams.get('sid') || '';
    const sess = sessions.get(sid);

    if (url.pathname === '/s/stats') {
      sendJson(200, {
        sessions: sessions.size,
        list: [...sessions.values()].map((s) => ({
          sid: s.id, target: s.host + ':' + s.port, rx: s.rxTotal, tx: s.txTotal,
          buffered: s.buffered, closed: s.closed, idleSec: Math.round((Date.now() - s.lastSeen) / 1000),
        })),
      });
      return;
    }

    if (!sess) { sendJson(404, { error: 'no such session' }); return; }
    sess.lastSeen = Date.now();

    if (url.pathname === '/s/up' && req.method === 'POST') {
      const body = await readBody(req, 2 * 1024 * 1024);
      if (sess.closed || sess.dead) { sendJson(410, { error: 'session closed' }); return; }
      sess.txTotal += body.length;
      const ok = sess.sock.write(body);
      if (!ok) await new Promise((r) => sess.sock.once('drain', r));
      sendJson(200, { ok: true, rx: sess.rxTotal, closed: sess.closed });
      return;
    }

    if (url.pathname === '/s/down' && req.method === 'GET') {
      const wait = Math.min(Number(url.searchParams.get('wait') || 45), 50);
      const deadline = Date.now() + wait * 1000;
      while (sess.buffered === 0 && !sess.closed && !sess.dead && Date.now() < deadline) {
        await new Promise((resolve) => {
          const timer = setTimeout(() => { sess.waiters.delete(wakeFn); resolve(); }, Math.max(0, deadline - Date.now()));
          const wakeFn = () => { clearTimeout(timer); resolve(); };
          sess.waiters.add(wakeFn);
        });
      }
      sess.lastSeen = Date.now();
      const data = sess.chunks.length ? Buffer.concat(sess.chunks) : Buffer.alloc(0);
      sess.chunks = [];
      sess.buffered = 0;
      if (sess.sock.isPaused && sess.sock.isPaused() && !sess.closed) { try { sess.sock.resume(); } catch {} }
      res.writeHead(200, {
        'Content-Type': 'application/octet-stream',
        'Cache-Control': 'no-store',
        'X-Closed': sess.closed || sess.dead ? '1' : '0',
        'X-Rx-Total': String(sess.rxTotal),
      });
      res.end(data);
      if ((sess.closed || sess.dead) && data.length === 0) destroySession(sess);
      return;
    }

    if (url.pathname === '/s/close' && req.method === 'POST') {
      destroySession(sess);
      sendJson(200, { ok: true });
      return;
    }

    sendJson(404, { error: 'not found' });
  } catch (e) {
    if (!res.writableEnded) sendJson(500, { error: String(e && e.message || e) });
  }
});

server.listen(PORT, '0.0.0.0', () => console.log('slew-relay listening on ' + PORT));
