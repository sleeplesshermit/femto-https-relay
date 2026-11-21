# Femto — an HTTPS relay for hosts that can't take a tunnel

**Femto** is a VLESS relay for the awkward hosts: platforms that give your app
inbound HTTPS only — no raw ports, no WebSocket upgrades, no persistent
streams — but do allow outbound TCP. A normal tunnel (cloudflared, a direct
xray listener) can't live there. Femto can.

It is the next step down from a pico-style tunnel deployment: where pico needs
~75 MB of native binaries (xray + cloudflared) on the host, femto's host-side
piece is a single **6 KB Node.js file with zero dependencies**.

## How it works

```
client ──VLESS/WS or VLESS/XHTTP──▶ Cloudflare Worker ──plain HTTPS──▶ relay app ──raw TCP──▶ target
```

Two pieces:

- **`worker/worker.js`** — a Cloudflare Worker that speaks VLESS over
  WebSocket and over XHTTP (stream-one, following the serving pattern of the
  public edgetunnel-family workers). It cannot dial raw TCP, so it pumps the
  byte stream to the relay app over ordinary HTTPS requests:
  `POST /s/new` opens a session to the target, `POST /s/up` carries
  client→target bytes, long-poll `GET /s/down` carries target→client bytes,
  `POST /s/close` ends it.
- **`relay/server.mjs`** — the host-side relay (~6 KB, Node ≥ 18, no
  dependencies). It holds sessions in memory, dials the real targets over raw
  TCP, and answers the Worker's HTTPS calls. Any host that can run a small
  Node app behind an HTTPS URL works — it was built and verified on
  [slew.sh](https://slew.sh), whose edge strips WebSocket upgrades entirely.

The exit IP is the relay host's IP: the Worker is only the front door.

## Deploy

1. **Relay app.** Copy `relay/server.mjs` to your host (for slew.sh, use it as
   `server/index.mjs` with the included `relay/slew.json`). Set
   `FEMTO_RELAY_KEY` to a long random secret, or edit the default in the file.
2. **Worker.** Create a Cloudflare Worker from `worker/worker.js` and set the
   three configuration constants at the top: `FEMTO_UUID` (your VLESS UUID),
   `FEMTO_RELAY_URL` (the relay app's public HTTPS URL), `FEMTO_RELAY_KEY`
   (the same secret). Attach a custom domain — the `workers.dev` subdomain is
   unreachable from some networks.
3. **Client.** Build a VLESS string against the Worker's domain:

   WebSocket:
   ```
   vless://<UUID>@<worker-domain>:443?encryption=none&security=tls&sni=<worker-domain>&type=ws&host=<worker-domain>&path=%2F#femto-ws
   ```

   XHTTP (stream-one, stitch path):
   ```
   vless://<UUID>@<worker-domain>:443?encryption=none&security=tls&sni=<worker-domain>&fp=firefox&alpn=h3&type=xhttp&host=<worker-domain>&path=%2F<first-8-hex-of-UUID>%26https%3D<relay-host>%3A443&mode=stream-one#femto-xhttp
   ```

## Limits, honestly

- Sessions live in the relay's memory only. If the host restarts the app,
  live connections drop.
- On scale-to-zero hosts the first connection after an idle spell pays a
  cold start; the Worker retries session opens to absorb it.
- The pump is request/response HTTPS, so throughput and latency are worse
  than a real tunnel. Femto is for hosts where a real tunnel is impossible,
  not a replacement where one works.
- Bandwidth is bounded by your host's plan (slew.sh free: 50 GB/month).

## Security notes

- `FEMTO_RELAY_KEY` is the only thing stopping strangers from using your
  relay app as an open proxy. Make it long and random, and change the
  default.
- The VLESS UUID authenticates clients at the Worker, as usual.
- Run this only where the host's terms allow it. Some platforms prohibit
  proxying outright; that is your call to make, not this README's.

## License

MIT
