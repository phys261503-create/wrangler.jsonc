// Seed Chat on Cloudflare Workers + Durable Objects.
// One Durable Object per room, addressed by SHA-256 of the seed.
// No room list, no nicknames, no message storage (relay only).
import { DurableObject } from 'cloudflare:workers';

const COLOR_COUNT = 12;
const MAX_MSG_LEN = 500;
const MAX_ROOM_SIZE = 12;
const ROOM_GRACE_MS = 10 * 60 * 1000; // empty room survives 10 min
const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // 32 chars, no 0/O/1/I

const normalize = (s) => String(s || '').toUpperCase().replace(/[^A-Z0-9]/g, '');

async function sha256(s) {
  const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(s));
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

function makeSeed() {
  const bytes = crypto.getRandomValues(new Uint8Array(10));
  let out = '';
  for (const b of bytes) out += ALPHABET[b % 32];
  return out.slice(0, 5) + '-' + out.slice(5); // e.g. K7M2Q-9XH4T (50 bits)
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname !== '/ws') return env.ASSETS.fetch(request);
    if (request.headers.get('Upgrade') !== 'websocket') return new Response('expected websocket', { status: 426 });

    const headers = new Headers(request.headers);
    headers.delete('x-seed'); // only the worker may set this
    let hash;
    if (url.searchParams.has('create')) {
      const seed = makeSeed();
      hash = await sha256(normalize(seed));
      headers.set('x-seed', seed);
    } else {
      // the browser sends only the hash of the seed, never the seed itself
      hash = (url.searchParams.get('room') || '').toLowerCase();
      if (!/^[0-9a-f]{64}$/.test(hash)) return new Response('bad request', { status: 400 });
    }
    const stub = env.ROOMS.get(env.ROOMS.idFromName(hash));
    return stub.fetch(new Request(request, { headers }));
  },
};

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    // keep-alive answered without waking the object
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('ping', 'pong'));
  }

  async fetch(request) {
    const seed = request.headers.get('x-seed');
    let exists = await this.ctx.storage.get('exists');
    if (seed && !exists) { await this.ctx.storage.put('exists', true); exists = true; }

    const pair = new WebSocketPair();
    const client = pair[0], server = pair[1];
    const members = this.ctx.getWebSockets();

    // same answer for "no such room" and "full": reveals nothing
    if (!exists || members.length >= MAX_ROOM_SIZE) {
      server.accept();
      server.send(JSON.stringify({ t: 'denied' }));
      server.close(1000, 'denied');
      return new Response(null, { status: 101, webSocket: client });
    }

    const used = new Set(members.map((m) => (m.deserializeAttachment() || {}).color));
    let color = 0;
    while (used.has(color) && color < COLOR_COUNT - 1) color++;

    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ color, stamps: [] });
    await this.ctx.storage.deleteAlarm();

    const count = members.length + 1;
    if (seed) server.send(JSON.stringify({ t: 'created', seed, color, count }));
    else {
      server.send(JSON.stringify({ t: 'joined', color, count }));
      this.broadcast({ t: 'sys', text: 'someone joined', count });
    }
    return new Response(null, { status: 101, webSocket: client });
  }

  broadcast(obj, except) {
    const data = JSON.stringify(obj);
    for (const m of this.ctx.getWebSockets()) {
      if (m === except) continue;
      try { m.send(data); } catch {}
    }
  }

  async webSocketMessage(ws, raw) {
    if (typeof raw !== 'string' || raw.length > 4096) return;
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (!m || m.t !== 'msg') return;
    const att = ws.deserializeAttachment() || { color: 0, stamps: [] };
    const now = Date.now();
    att.stamps = (att.stamps || []).filter((s) => now - s < 3000);
    if (att.stamps.length >= 8) return; // flood guard
    att.stamps.push(now);
    ws.serializeAttachment(att);
    const text = String(m.text || '').replace(/[\u0000-\u001f\u007f]/g, '').slice(0, MAX_MSG_LEN);
    if (!text.trim()) return;
    this.broadcast({ t: 'msg', color: att.color, text });
  }

  async gone(ws) {
    try { ws.close(1000, 'bye'); } catch {}
    const left = this.ctx.getWebSockets().filter((m) => m !== ws).length;
    if (left === 0) await this.ctx.storage.setAlarm(Date.now() + ROOM_GRACE_MS);
    else this.broadcast({ t: 'sys', text: 'someone left', count: left }, ws);
  }
  async webSocketClose(ws) { await this.gone(ws); }
  async webSocketError(ws) { await this.gone(ws); }

  async alarm() {
    if (this.ctx.getWebSockets().length === 0) await this.ctx.storage.deleteAll(); // room is gone
  }
}
