// ============================================================================
// ISLAND SURVIVAL V40 — Multiplayer Server (Node.js, zero dependencies)
// Real-time WebSocket relay + authoritative world state for 3 isolated worlds.
// Run:  node server/server.js   (or: npm start)
// Env:  PORT (default 8080), MAX_PLAYERS (default 20 per world)
// ============================================================================
'use strict';
const http = require('http'), crypto = require('crypto'), fs = require('fs'), path = require('path');

const PORT = Number(process.env.PORT || 8080);
const MAX_PLAYERS = Number(process.env.MAX_PLAYERS || 20);
const CH = 32, H = 192, MAX_TILE_ID = 78, TILE_SIZE = 32, MAX_EDIT_REACH = 7;
const ROOT = path.join(__dirname, '..');
const PUBLIC_DIR = path.join(ROOT, 'public');
const DATA_DIR = path.join(ROOT, 'data');

// ---- World registry: three truly independent worlds -----------------------
const WORLDS = {
  island:    { name: 'Island Survival',  icon: '🏝️', seed: 13337,  type: 'classic' },
  adventure: { name: 'Adventure World',  icon: '⛏️', seed: 90424,  type: 'cavern'  },
  creative:  { name: 'Creative World',   icon: '🧱', seed: 777777, type: 'classic',
    kit: { plank: 256, stone: 256, glass: 128, brick: 128, torch: 64, door: 16, workbench: 1, furnace: 1, chest: 4 } }
};
// runtime state per world: {edits:{cx:{i:t}}, doors:{'x,y':{open}}, time, players:Map(name->id)}
const worlds = {};
const playersDb = {}; // world -> { playerName -> {items, equipment} }

function loadData() {
  try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch (e) {}
  let w = null, p = null;
  try { w = JSON.parse(fs.readFileSync(path.join(DATA_DIR, 'worlds.json'), 'utf8')); } catch (e) {}
  try { p = JSON.parse(fs.readFileSync(path.join(DATA_DIR, 'players.json'), 'utf8')); } catch (e) {}
  for (const id in WORLDS) {
    worlds[id] = (w && w[id]) ? w[id] : { edits: {}, doors: {}, time: 400 };
    worlds[id].players = new Map();
    playersDb[id] = (p && p[id]) ? p[id] : {};
  }
}
function saveData() {
  try {
    const w = {}, pd = {};
    for (const id in worlds) { w[id] = { edits: worlds[id].edits, doors: worlds[id].doors, time: worlds[id].time }; pd[id] = playersDb[id]; }
    fs.writeFileSync(path.join(DATA_DIR, 'worlds.json'), JSON.stringify(w));
    fs.writeFileSync(path.join(DATA_DIR, 'players.json'), JSON.stringify(pd));
  } catch (e) { console.error('[save]', e.message); }
}
loadData();
setInterval(saveData, 60000);
process.on('SIGINT', () => { saveData(); process.exit(0); });
process.on('SIGTERM', () => { saveData(); process.exit(0); });

// ---- Authoritative world clock --------------------------------------------
setInterval(() => {
  for (const id in worlds) {
    worlds[id].time += 1;
    if (Math.floor(worlds[id].time) % 30 === 0) broadcastWorld(id, { t: 'time', day: worlds[id].time });
  }
}, 1000);

// ---- HTTP: static client ---------------------------------------------------
const server = http.createServer((req, res) => {
  if (req.method !== 'GET') { res.writeHead(405); res.end('method not allowed'); return; }
  const pathname = new URL(req.url, 'http://localhost').pathname;
  if (pathname === '/' || pathname === '/index.html') {
    fs.readFile(path.join(PUBLIC_DIR, 'index.html'), (e, d) => {
      if (e) { res.writeHead(404); res.end('not found'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=UTF-8', 'Cache-Control': 'no-cache' });
      res.end(d);
    });
    return;
  }
  // Serve only the bundled, known art assets (no arbitrary filesystem paths).
  const artFiles = {
    '/assets/resource-and-ui-art.jpg': 'resource-and-ui-art.jpg',
    '/assets/block-and-controls-art.jpg': 'block-and-controls-art.jpg'
  };
  if (Object.prototype.hasOwnProperty.call(artFiles, pathname)) {
    fs.readFile(path.join(PUBLIC_DIR, 'assets', artFiles[pathname]), (e, d) => {
      if (e) { res.writeHead(404); res.end('not found'); return; }
      res.writeHead(200, { 'Content-Type': 'image/jpeg', 'Cache-Control': 'public, max-age=86400' });
      res.end(d);
    });
    return;
  }
  res.writeHead(404); res.end('not found');
});

// ---- Minimal RFC6455 WebSocket (no external deps) ----------------------------
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const clients = new Map(); // socket -> client record
let nextId = 1;

server.on('upgrade', (req, socket) => {
  if (!req.url.startsWith('/ws')) { socket.destroy(); return; }
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  const c = { id: 'p' + (nextId++), world: null, name: null, x: 0, y: 0, skin: 0, buf: Buffer.alloc(0), rate: {} };
  clients.set(socket, c);
  socket.on('data', d => {
    c.buf = Buffer.concat([c.buf, d]);
    let f;
    while ((f = readFrame(c)) !== null) {
      if (f === false) { socket.destroy(); return; }
      if (f.op === 1) {
        if (f.payload.length > 32768) { socket.destroy(); return; }
        let msg; try { msg = JSON.parse(f.payload.toString('utf8')); } catch (e) { continue; }
        handleMessage(c, socket, msg);
      } else if (f.op === 9) { try { sendFrame(socket, 10, f.payload); } catch (e) {} }
      else if (f.op === 8) { socket.end(); return; }
    }
  });
  const kill = () => { if (!clients.has(socket)) return; clients.delete(socket); onLeave(c); try { socket.destroy(); } catch (e) {} };
  socket.on('close', kill); socket.on('error', kill); socket.on('end', kill);
});

function readFrame(c) {
  const b = c.buf;
  if (b.length < 2) return null;
  const op = b[0] & 0x0f;
  let len = b[1] & 0x7f, off = 2;
  if (len === 126) { if (b.length < 4) return null; len = b.readUInt16BE(2); off = 4; }
  else if (len === 127) { if (b.length < 10) return null; len = Number(b.readBigUInt64BE(2)); off = 10; }
  if (len > 262144) return false;
  const masked = (b[1] & 0x80) !== 0;
  let mask = null;
  if (masked) { if (b.length < off + 4) return null; mask = b.slice(off, off + 4); off += 4; }
  if (b.length < off + len) return null;
  const payload = Buffer.from(b.slice(off, off + len));
  if (masked) for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
  c.buf = b.slice(off + len);
  return { op, payload };
}
function sendFrame(s, op, payload) {
  const len = payload.length; let head;
  if (len < 126) { head = Buffer.from([0x80 | op, len]); }
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  s.write(Buffer.concat([head, payload]));
}
function sendTo(s, obj) { if (!clients.has(s)) return; try { sendFrame(s, 1, Buffer.from(JSON.stringify(obj))); } catch (e) {} }
function broadcastWorld(worldId, obj, except) {
  for (const [s, c] of clients) if (c.world === worldId && s !== except) sendTo(s, obj);
}

// ---- Rate limiting (anti spam / basic anti-cheat) --------------------------
function overRate(c, s, key, max) {
  const now = Date.now();
  const r = c.rate[key];
  if (!r || now - r.t > 1000) { c.rate[key] = { t: now, n: 0 }; }
  if (++c.rate[key].n > max) { sendTo(s, { t: 'kick', reason: 'تم طردك: عدد طلبات مرتفع جداً (حماية الخادم)' }); return true; }
  return false;
}

// ---- Protocol ---------------------------------------------------------------
function handleMessage(c, s, msg) {
  if (!msg || typeof msg !== 'object') return;
  const t = msg.t;
  if (t === 'info') {
    const w = {};
    for (const id in WORLDS) w[id] = { online: true, players: worlds[id].players.size, max: MAX_PLAYERS };
    sendTo(s, { t: 'info', worlds: w }); return;
  }
  if (t === 'join') {
    const wid = String(msg.world || '');
    if (!WORLDS[wid]) { sendTo(s, { t: 'error', msg: 'هذا العالم غير موجود على الخادم' }); s.end(); return; }
    const w = worlds[wid];
    if (w.players.size >= MAX_PLAYERS) { sendTo(s, { t: 'error', msg: 'السيرفر ممتلئ — حاول لاحقاً' }); s.end(); return; }
    let name = String(msg.name || 'لاعب').slice(0, 16).replace(/[^\w\u0600-\u06FF\-# ]/g, '').trim() || 'لاعب';
    const base = name; let n = 2;
    while (w.players.has(name)) name = base + '#' + (n++);   // unique names per world
    c.world = wid; c.name = name; w.players.set(name, c.id);
    const others = [];
    for (const [nm, id] of w.players) {
      if (nm === name) continue;
      let cc = null; for (const v of clients.values()) if (v.id === id) { cc = v; break; }
      others.push({ id, name: nm, skin: cc ? cc.skin || 0 : 0, x: cc ? cc.x || 0 : 0, y: cc ? cc.y || 0 : 0, face: 1 });
    }
    sendTo(s, { t: 'welcome', id: c.id, world: wid, name, worldName: WORLDS[wid].name,
      seed: WORLDS[wid].seed, type: WORLDS[wid].type,
      edits: w.edits, doors: w.doors, time: w.time,
      kit: WORLDS[wid].kit || null, inv: playersDb[wid][name] || null, story: (playersDb[wid][name] || {}).story || null, players: others });
    broadcastWorld(wid, { t: 'player:join', id: c.id, name, skin: c.skin || 0 }, s);
    console.log('[join]', name, '->', wid, '(' + w.players.size + ' online)');
    return;
  }
  if (!c.world) return;
  const w = worlds[c.world];
  if (t === 'state') {
    const x = +msg.x, y = +msg.y;
    if (!isFinite(x) || !isFinite(y) || Math.abs(x) > 1e7 || Math.abs(y) > 1e8) return;
    c.skin = msg.skin | 0; c.x = x; c.y = y;
    if (overRate(c, s, 'state', 30)) return;
    broadcastWorld(c.world, { t: 'state', id: c.id, x, y, face: msg.face > 0 ? 1 : -1, skin: c.skin, anim: msg.anim ? 1 : 0 }, s);
    return;
  }
  if (t === 'block') {  // server-authoritative tile edits
    const x = msg.x | 0, y = msg.y | 0, tile = msg.tile | 0;
    if (Math.abs(x) > 2000000 || y < 0 || y >= H || tile < 0 || tile > MAX_TILE_ID) return;
    // Reject remote edits outside the player's reachable interaction radius.
    if (!Number.isFinite(c.x) || !Number.isFinite(c.y) || Math.hypot((x + .5) * TILE_SIZE - c.x, (y + .5) * TILE_SIZE - c.y) > MAX_EDIT_REACH * TILE_SIZE) return;
    if (overRate(c, s, 'block', 20)) return;
    const cx = Math.floor(x / CH), i = y * CH + (x & (CH - 1));
    (w.edits[cx] = w.edits[cx] || {})[i] = tile;
    broadcastWorld(c.world, { t: 'block', x, y, tile, by: c.id }, s);
    return;
  }
  if (t === 'door') {
    const x = msg.x | 0, y = msg.y | 0;
    if (Math.abs(x) > 2000000 || y < 0 || y >= H) return;
    if (!Number.isFinite(c.x) || !Number.isFinite(c.y) || Math.hypot((x + .5) * TILE_SIZE - c.x, (y + .5) * TILE_SIZE - c.y) > MAX_EDIT_REACH * TILE_SIZE) return;
    if (overRate(c, s, 'door', 6)) return;
    const k = x + ',' + y;
    (w.doors[k] = w.doors[k] || {}).open = !!msg.open;
    broadcastWorld(c.world, { t: 'door', x, y, open: !!msg.open }, s);
    return;
  }
  if (t === 'mobs') {
    if (overRate(c, s, 'mobs', 6)) return;
    const list = Array.isArray(msg.list) ? msg.list.slice(0, 40).filter(m => m && typeof m.id === 'string' && m.id.length <= 48 && Number.isFinite(+m.x) && Number.isFinite(+m.y) && Math.abs(+m.x) < 1e7 && Math.abs(+m.y) < 1e7).map(m => ({ id: m.id, type: String(m.type || '').slice(0, 20), x: Math.round(+m.x), y: Math.round(+m.y), face: m.face > 0 ? 1 : -1, hp: Math.max(0, Math.min(1000, +m.hp || 0)) })) : [];
    broadcastWorld(c.world, { t: 'mobs', list }, s);
    return;
  }
  if (t === 'mob:spawn') {
    if (overRate(c, s, 'mspawn', 10)) return;
    if (!msg.mob || typeof msg.mob.id !== 'string' || msg.mob.id.length > 48) return;
    const mx = Number(msg.mob.x), my = Number(msg.mob.y);
    if (!Number.isFinite(mx) || !Number.isFinite(my) || Math.abs(mx) > 1e7 || Math.abs(my) > 1e7) return;
    broadcastWorld(c.world, { t: 'mob:spawn', mob: { id: msg.mob.id, type: String(msg.mob.type).slice(0, 20), profession: msg.mob.profession, x: Math.round(mx), y: Math.round(my) } }, s);
    return;
  }
  if (t === 'mob:death') {
    broadcastWorld(c.world, { t: 'mob:death', id: String(msg.id).slice(0, 48) }, s);
    return;
  }
  if (t === 'inv') {  // server-saved player inventory per world
    const inv = {};
    if (msg.items && typeof msg.items === 'object') {
      for (const k in msg.items) {
        const v = +msg.items[k];
        if (isFinite(v) && v >= 0 && v <= 9999) inv[String(k).slice(0, 32)] = Math.floor(v);
      }
    }
    const eq = {};
    ['helmet', 'chest', 'legs', 'boots', 'shield'].forEach(sl => {
      const v = msg.equipment && msg.equipment[sl];
      eq[sl] = typeof v === 'string' ? v.slice(0, 40) : null;
    });
    playersDb[c.world][c.name] = Object.assign({}, playersDb[c.world][c.name] || {}, { items: inv, equipment: eq });
    return;
  }
  if (t === 'story:sync') { // Per-player progression snapshot; bounded and isolated from other players.
    if (overRate(c, s, 'story', 2)) return;
    let raw; try { raw = JSON.stringify(msg.story || {}); } catch (_) { return; }
    if (raw.length > 16000) return;
    const src = msg.story && typeof msg.story === 'object' ? msg.story : {};
    const stage = ['normal','nether','end'].includes(src.stage) ? src.stage : 'normal';
    const towerFloor = Math.max(0, Math.min(20, Math.floor(Number(src.towerFloor) || 0)));
    const cleared = Array.isArray(src.towerCleared) ? [...new Set(src.towerCleared.map(Number).filter(n => Number.isInteger(n) && n >= 1 && n <= 20))] : [];
    const old = playersDb[c.world][c.name] || {};
    old.story = { stage, towerFloor, towerCleared: cleared,
      portal: { grown: !!(src.portal && src.portal.grown), active: !!(src.portal && src.portal.active) },
      compressed: src.compressed && typeof src.compressed === 'object' ? {
        wood: !!src.compressed.wood, stone: !!src.compressed.stone, metal: !!src.compressed.metal, diamond: !!src.compressed.diamond } : {},
      dragon: src.dragon && typeof src.dragon === 'object' ? {
        hp: Math.max(0, Math.min(1200, Number(src.dragon.hp) || 0)), phase: Math.max(1, Math.min(3, Number(src.dragon.phase) || 1)),
        crystals: Math.max(0, Math.min(10, Math.floor(Number(src.dragon.crystals) || 0))), defeated: !!src.dragon.defeated } : {} };
    playersDb[c.world][c.name] = old;
    sendTo(s, { t: 'story:ack' });
    return;
  }
  if (t === 'chat') {
    if (overRate(c, s, 'chat', 3)) return;
    const txt = String(msg.text || '').replace(/[\u0000-\u001f\u007f]/g, '').trim().slice(0, 120);
    if (!txt) return;
    // Include the sender so their own message appears in the chat window.
    broadcastWorld(c.world, { t: 'chat', id: c.id, name: c.name, text: txt });
    return;
  }
  if (t === 'sleep') {
    if (overRate(c, s, 'sleep', 1)) return;
    const x = Number(msg.x), y = Number(msg.y);
    if (!Number.isFinite(x) || !Number.isFinite(y) || !Number.isFinite(c.x) || !Number.isFinite(c.y)) return;
    // Client interaction targets a bed; the server also requires the player to be close to it.
    if (Math.hypot(c.x / TILE_SIZE - x, c.y / TILE_SIZE - y) > 4) { sendTo(s, { t: 'sleep:result', ok: false, reason: 'اقترب من السرير أولاً' }); return; }
    const clock = worlds[c.world].time;
    const phase = ((clock % 1200) + 1200) % 1200 / 1200;
    if (!(phase > 0.65 || phase < 0.18)) { sendTo(s, { t: 'sleep:result', ok: false, reason: 'يمكنك النوم أثناء الليل فقط' }); return; }
    let dawn = Math.floor(clock / 1200) * 1200;
    if (phase > 0.65) dawn += 1200;
    dawn += 216; // 04:19 — morning phase begins at 18% of the day cycle.
    worlds[c.world].time = dawn;
    broadcastWorld(c.world, { t: 'time', day: dawn, sleptBy: c.name, message: 'نام ' + c.name + ' وأصبح الصباح!' });
    saveData();
    return;
  }
}
function onLeave(c) {
  if (!c.world) return;
  const w = worlds[c.world];
  if (w.players.has(c.name)) {
    w.players.delete(c.name);
    broadcastWorld(c.world, { t: 'player:leave', id: c.id, name: c.name });
    console.log('[leave]', c.name, '<-', c.world, '(' + w.players.size + ' online)');
  }
  c.world = null;
}

server.listen(PORT, () => {
  console.log('ISLAND SURVIVAL V40 server running');
  console.log('  HTTP + WS : ws://0.0.0.0:' + PORT + '/ws');
  console.log('  Worlds    : ' + Object.keys(WORLDS).join(', '));
  console.log('  Max/world : ' + MAX_PLAYERS);
});
