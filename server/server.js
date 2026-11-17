// ============================================================================
// ISLAND SURVIVAL V47 — Guest Multiplayer Server (Node.js, zero dependencies)
// Real-time WebSocket relay + authoritative world state for dynamic lobby architecture.
// Run:  node server/server.js   (or: npm start)
// Env:  PORT (default 8080), MAX_PLAYERS (default 20 per world)
// ============================================================================
'use strict';
const http = require('http'), crypto = require('crypto'), fs = require('fs'), path = require('path');

const PORT = Math.max(1, Math.min(65535, Number(process.env.PORT) || 8080));
const MAX_PLAYERS = Math.max(2, Math.min(100, Number(process.env.MAX_PLAYERS) || 20));
const CH = 32, H = 192, MAX_TILE_ID = 78, TILE_SIZE = 32, MAX_EDIT_REACH = 7;
const ROOT = path.join(__dirname, '..');
const PUBLIC_DIR = path.join(ROOT, 'public');
const DATA_DIR = process.env.DATA_DIR || path.join(ROOT, 'data');
const LOBBY_FILE = path.join(DATA_DIR, 'lobbies.json');
const lobbies = {};
const MAX_LOBBIES = Math.max(1, Math.min(50000, Number.isFinite(Number(process.env.MAX_LOBBIES)) ? Number(process.env.MAX_LOBBIES) : 10000));
const MAX_CONNECTIONS = Math.max(10, Math.min(5000, Number(process.env.MAX_CONNECTIONS) || 250));
let shuttingDown = false;
const DEFAULT_LOBBY_CAPACITY = Number(process.env.MAX_PLAYERS || 20);

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

function readJson(file, fallback){try{return JSON.parse(fs.readFileSync(file,'utf8'));}catch(_){return fallback;}}
function safeSecretEqual(a,b){const x=Buffer.from(String(a||'')),y=Buffer.from(String(b||''));return x.length===y.length&&x.length>0&&crypto.timingSafeEqual(x,y);}
function atomicWriteJson(file, value){
  fs.mkdirSync(path.dirname(file), {recursive:true});
  const tmp=file+'.'+process.pid+'.'+crypto.randomBytes(4).toString('hex')+'.tmp';
  fs.writeFileSync(tmp, JSON.stringify(value));
  fs.renameSync(tmp,file);
}
function saveLobbies(){try{const out={};for(const [id,l] of Object.entries(lobbies))out[id]={id:l.id,name:l.name,mode:l.mode,private:l.private,inviteCode:l.private?l.inviteCode:null,max:l.max,mods:l.mods||[],owner:l.owner,createdAt:l.createdAt,worldId:l.worldId||'island'};atomicWriteJson(LOBBY_FILE,out);}catch(e){console.error('[lobbies-save]',e.message);}}
function publicLobby(l){return {id:l.id,name:l.name,mode:l.mode,private:!!l.private,players:l.players?l.players.size:0,max:l.max,mods:l.mods||[],status:'online'};}
function makeLobby(id,name,mode='survival',isPrivate=false,max=DEFAULT_LOBBY_CAPACITY){return {id,name,mode,private:isPrivate,inviteCode:isPrivate?crypto.randomBytes(12).toString('base64url'):null,max:Math.max(2,Math.min(100,Number(max)||DEFAULT_LOBBY_CAPACITY)),mods:[],owner:null,createdAt:Date.now(),players:new Map(),worldId:'island',edits:{},doors:{},time:400};}
function nextLobbyId(){return String(Math.max(0,...Object.keys(lobbies).map(x=>Number(x)||0))+1).padStart(3,'0');}
function createPersistentLobby(id,name='ردهة '+id,mode='survival',isPrivate=false,max=DEFAULT_LOBBY_CAPACITY){
  const l=makeLobby(id,name,mode,isPrivate,max);l.worldId='island';lobbies[id]=l;
  const wid='lobby:'+id,base=WORLDS.island;
  WORLDS[wid]={name,icon:'🏝️',seed:(base.seed+Number(id)*7919)>>>0,type:base.type,kit:mode==='creative'?WORLDS.creative.kit:null};
  worlds[wid]={edits:{},doors:{},time:400,players:new Map()};playersDb[wid]={};
  saveLobbies();saveData();console.log('[lobby-created]',id);return l;
}
function ensureAvailableLobby(){
  const open=Object.values(lobbies).find(l=>!l.private&&l.players.size<l.max);if(open)return open;
  if(Object.keys(lobbies).length>=MAX_LOBBIES)throw new Error('وصل النظام إلى الحد التشغيلي للردهـات');
  return createPersistentLobby(nextLobbyId());
}
function apiJson(res,status,obj){const b=Buffer.from(JSON.stringify(obj));res.writeHead(status,{'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store','Access-Control-Allow-Origin':'*','Access-Control-Allow-Headers':'Content-Type, Authorization, x-admin-key','Access-Control-Allow-Methods':'GET, POST, OPTIONS','Content-Length':b.length});res.end(b);}
function readBody(req){return new Promise((resolve,reject)=>{let b='';req.on('data',c=>{b+=c;if(b.length>8192)reject(new Error('body too large'));});req.on('end',()=>{try{resolve(JSON.parse(b||'{}'));}catch(_){reject(new Error('invalid json'));}});});}
function handleApi(req,res,url){
 if(shuttingDown){apiJson(res,503,{error:'الخادم يستعد للإغلاق، حاول بعد لحظات'});return true;}
 if(req.method==='OPTIONS'){apiJson(res,204,{});return true;}
 if(url.pathname==='/api/health'&&req.method==='GET'){const mem=process.memoryUsage();apiJson(res,shuttingDown?503:200,{ok:!shuttingDown,status:shuttingDown?'shutting_down':'online',version:'47.0.0',websocket:'/ws',authentication:'disabled',lobbies:Object.keys(lobbies).length,onlinePlayers:clients.size,maxConnections:MAX_CONNECTIONS,connectionCapacity:Math.max(0,MAX_CONNECTIONS-clients.size),uptime:Math.floor(process.uptime()),memory:{rss:mem.rss,heapUsed:mem.heapUsed,heapTotal:mem.heapTotal},});return true;}
 if(url.pathname==='/api/lobbies'&&req.method==='GET'){try{ensureAvailableLobby();}catch(e){} const q=String(url.searchParams.get('q')||'').trim().toLowerCase();let list=Object.values(lobbies).filter(l=>!l.private).map(publicLobby);if(q)list=list.filter(l=>l.id.toLowerCase().includes(q)||l.name.toLowerCase().includes(q));apiJson(res,200,{lobbies:list});return true;}
 // Lobby provisioning is deliberately server-admin-only; there is no in-game create endpoint.
 if(url.pathname==='/api/admin/lobbies'&&req.method==='POST'){if(!process.env.ADMIN_API_KEY||!safeSecretEqual(req.headers['x-admin-key'],process.env.ADMIN_API_KEY)){apiJson(res,403,{error:'غير مصرح'});return true;}
   readBody(req).then(body=>{if(Object.keys(lobbies).length>=MAX_LOBBIES)return apiJson(res,503,{error:'وصل النظام إلى الحد التشغيلي'});
    const id=nextLobbyId();
    const name=String(body.name||('ردهة '+id)).trim().slice(0,32)||('ردهة '+id);
    const mode=body.mode==='creative'?'creative':'survival';const max=Math.max(2,Math.min(100,Number(body.max)||DEFAULT_LOBBY_CAPACITY));const l=createPersistentLobby(id,name,mode,!!body.private,max);saveLobbies();
    apiJson(res,201,{ok:true,lobby:publicLobby(l),inviteCode:l.private?l.inviteCode:undefined});
   }).catch(e=>apiJson(res,400,{error:e.message}));return true;}
 return false;
}
function loadData() {
  try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch (e) {}
  const savedLobbies=readJson(LOBBY_FILE,{});
  for(const [id,v] of Object.entries(savedLobbies)){const l=Object.assign(makeLobby(id,v.name,v.mode,v.private,v.max),v,{players:new Map()});if(l.private&&!l.inviteCode)l.inviteCode=crypto.randomBytes(12).toString('base64url');lobbies[id]=l;}
  if(!Object.keys(lobbies).length){const id='001';lobbies[id]=makeLobby(id,'ردهة 001','survival',false,DEFAULT_LOBBY_CAPACITY);}
  let w = null, p = null;
  try { w = JSON.parse(fs.readFileSync(path.join(DATA_DIR, 'worlds.json'), 'utf8')); } catch (e) {}
  try { p = JSON.parse(fs.readFileSync(path.join(DATA_DIR, 'players.json'), 'utf8')); } catch (e) {}
  for (const id in WORLDS) {
    worlds[id] = (w && w[id]) ? w[id] : { edits: {}, doors: {}, time: 400 };
    worlds[id].players = new Map();
    playersDb[id] = (p && p[id]) ? p[id] : {};
  }
  for(const [lid,l] of Object.entries(lobbies)){const id='lobby:'+lid;const base=WORLDS[l.worldId||'island']||WORLDS.island;WORLDS[id]={name:l.name,icon:'🏝️',seed:(base.seed+Number(lid)*7919)>>>0,type:base.type,kit:base.kit||null};worlds[id]=(w&&w[id])?w[id]:{edits:{},doors:{},time:400};worlds[id].players=new Map();playersDb[id]=(p&&p[id])?p[id]:{};}
}
function saveData() {
  try {
    const w = {}, pd = {};
    for (const id in worlds) { w[id] = { edits: worlds[id].edits, doors: worlds[id].doors, time: worlds[id].time }; pd[id] = playersDb[id]; }
    atomicWriteJson(path.join(DATA_DIR, 'worlds.json'), w);
    atomicWriteJson(path.join(DATA_DIR, 'players.json'), pd);
  } catch (e) { console.error('[save]', e.message); }
}
// Persist world edits shortly after changes, not only on the 60-second interval.
let saveSoonTimer = null;
function scheduleSave(){
  if(saveSoonTimer) clearTimeout(saveSoonTimer);
  saveSoonTimer=setTimeout(()=>{saveSoonTimer=null;saveData();},900);
  if(saveSoonTimer.unref) saveSoonTimer.unref();
}
loadData();
setInterval(saveData, 60000);
function gracefulShutdown(signal){
  if(shuttingDown)return;shuttingDown=true;console.log('[shutdown]',signal,'saving persistent state...');
  try{saveLobbies();saveData();}catch(e){console.error('[shutdown-save]',e.message);}
  for(const [socket] of clients){try{sendFrame(socket,8,Buffer.from([0x03,0xE9]));socket.end();}catch(_){try{socket.destroy();}catch(__){}}}
  const force=setTimeout(()=>process.exit(0),8000);force.unref();
  server.close(()=>{try{saveLobbies();saveData();}catch(_){}clearTimeout(force);process.exit(0);});
}
process.on('SIGINT',()=>gracefulShutdown('SIGINT'));
process.on('SIGTERM',()=>gracefulShutdown('SIGTERM'));

// ---- Authoritative world clock --------------------------------------------
setInterval(() => {
  for (const id in worlds) {
    worlds[id].time += 1;
    if (Math.floor(worlds[id].time) % 30 === 0) broadcastWorld(id, { t: 'time', day: worlds[id].time });
  }
}, 1000);

// ---- HTTP: static client ---------------------------------------------------
const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname.startsWith('/api/')) { if(handleApi(req,res,url)) return; }
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
  if (pathname === '/admin' || pathname === '/admin.html') {
    fs.readFile(path.join(PUBLIC_DIR, 'admin.html'), (e, d) => {
      if (e) { res.writeHead(404); res.end('not found'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=UTF-8', 'Cache-Control': 'no-store' });
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
setInterval(()=>{const now=Date.now();for(const [socket,c] of clients){if(c.lastPing&&now-c.lastPing>70000&&c.lastPong<c.lastPing){try{socket.destroy();}catch(_){}}else{c.lastPing=now;try{sendFrame(socket,9,Buffer.from('island'));}catch(_){try{socket.destroy();}catch(__){}}}}},25000).unref();

server.on('upgrade', (req, socket) => {
  if (shuttingDown || req.method!=='GET' || !['/ws','/ws/'].includes((req.url||'').split('?')[0]) ||
      String(req.headers.upgrade||'').toLowerCase()!=='websocket' || !/\bupgrade\b/i.test(String(req.headers.connection||'')) ||
      String(req.headers['sec-websocket-version']||'')!=='13' || clients.size>=MAX_CONNECTIONS) { socket.destroy(); return; }
  const key = req.headers['sec-websocket-key'];
  let decodedKey;try{decodedKey=Buffer.from(String(key||''),'base64');}catch(_){}
  if (!key || !decodedKey || decodedKey.length!==16) { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  const c = { id: 'p' + (nextId++), world: null, name: null, accountId:null, x: 0, y: 0, skin: 0, buf: Buffer.alloc(0), rate: {}, lastPing:0, lastPong:Date.now(), joined:false };
  socket.setTimeout(120000,()=>socket.destroy());
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
      else if (f.op === 10) { c.lastPong=Date.now(); }
      else if (f.op === 8) { socket.end(); return; }
    }
  });
  const kill = () => { if (!clients.has(socket)) return; clients.delete(socket); onLeave(c); try { socket.destroy(); } catch (e) {} };
  socket.on('close', kill); socket.on('error', kill); socket.on('end', kill);
});

function readFrame(c) {
  const b = c.buf;
  if (b.length < 2) return null;
  if((b[0]&0x70)!==0 || !(b[0]&0x80) || !(b[1]&0x80))return false;
  const op = b[0] & 0x0f;
  if(![1,8,9,10].includes(op))return false;
  let len = b[1] & 0x7f, off = 2;
  if (len === 126) { if (b.length < 4) return null; len = b.readUInt16BE(2); off = 4; }
  else if (len === 127) { if (b.length < 10) return null; len = Number(b.readBigUInt64BE(2)); off = 10; }
  if (len > 32768 || (op>=8 && (!(b[0]&0x80) || len>125))) return false;
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
  if (t === 'info') { const list={}; for(const [id,l] of Object.entries(lobbies)) list[id]={online:true,players:l.players.size,max:l.max,name:l.name,private:l.private,mode:l.mode}; sendTo(s,{t:'info',worlds:list,lobbies:Object.values(lobbies).filter(l=>!l.private).map(publicLobby)}); return; }
  if (t === 'join') {
    if(c.joined){sendTo(s,{t:'error',msg:'تم إرسال طلب دخول بالفعل'});return;}
    const guestId=String(msg.guestId||'');
    if(!/^[A-Za-z0-9_-]{12,80}$/.test(guestId)){sendTo(s,{t:'error',msg:'تعذر التحقق من هوية اللاعب المؤقتة. حدّث الصفحة.'});s.end();return;}
    // A reconnect with the same browser guest ID replaces its old session before capacity checks.
    for(const [oldSocket,oldClient] of clients){if(oldSocket!==s&&oldClient.accountId===guestId){clients.delete(oldSocket);try{oldSocket.destroy();}catch(_){}onLeave(oldClient);}}
    let lobbyId=String(msg.world||'');let lobby=lobbies[lobbyId];
    if(!lobby){sendTo(s,{t:'error',msg:'هذه الردهة غير موجودة'});s.end();return;}
    if(lobby.private){const supplied=String(msg.invite||''),expected=String(lobby.inviteCode||'');const a=Buffer.from(supplied),b=Buffer.from(expected);if(!expected||a.length!==b.length||!crypto.timingSafeEqual(a,b)){sendTo(s,{t:'error',msg:'هذه ردهة خاصة وتحتاج رمز الدعوة الصحيح'});s.end();return;}}
    // A room may fill between the browser refresh and the WebSocket join. Redirect
    // public-room joins to the next available room instead of rejecting the player.
    if(lobby.players.size>=lobby.max){
      if(lobby.private){sendTo(s,{t:'error',msg:'الردهة الخاصة ممتلئة'});s.end();return;}
      try{lobby=ensureAvailableLobby();lobbyId=lobby.id;}catch(e){sendTo(s,{t:'error',msg:'لا توجد ردهة متاحة حاليًا. حاول بعد قليل.'});s.end();return;}
    }
    const wid='lobby:'+lobbyId;const w=worlds[wid];
    if(!w){sendTo(s,{t:'error',msg:'تعذر تحميل عالم الردهة. أعد المحاولة.'});s.end();return;}

    let name = String(msg.name || 'مغامر').replace(/[^\p{L}\p{N}_ -]/gu,'').trim().slice(0,16) || 'مغامر';
    const base = name; let n = 2;
    while (w.players.has(name)) name = base + '#' + (n++);   // unique names per world
    c.world = wid; c.lobbyId=lobbyId; c.accountId=guestId; c.name = name; c.joined=true; lobby.players.set(name,c.id); w.players.set(name, c.id);
    const others = [];
    for (const [nm, id] of w.players) {
      if (nm === name) continue;
      let cc = null; for (const v of clients.values()) if (v.id === id) { cc = v; break; }
      others.push({ id, name: nm, skin: cc ? cc.skin || 0 : 0, x: cc ? cc.x || 0 : 0, y: cc ? cc.y || 0 : 0, face: 1 });
    }
    sendTo(s, { t: 'welcome', id: c.id, world: wid, lobbyId, accountId:guestId, name, worldName: lobby.name,
      seed: WORLDS[wid].seed, type: WORLDS[wid].type,
      edits: w.edits, doors: w.doors, time: w.time,
      kit: WORLDS[wid].kit || null, inv: playersDb[wid][guestId] || null, story: (playersDb[wid][guestId] || {}).story || null, players: others });
    broadcastWorld(wid, { t: 'player:join', id: c.id, name, skin: c.skin || 0 }, s);
    console.log('[join]', name, '-> lobby #' + lobbyId, '(' + lobby.players.size + '/' + lobby.max + ')');
    // Keep one persistent, non-full public lobby available; create the next ID only when all existing public lobbies are full.
    try{ensureAvailableLobby();}catch(e){console.error('[lobby-cap]',e.message);}
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
    scheduleSave();
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
    scheduleSave();
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
    playersDb[c.world][c.accountId] = Object.assign({}, playersDb[c.world][c.accountId] || {}, { items: inv, equipment: eq });
    saveData();return;
  }
  if (t === 'story:sync') { // Per-player progression snapshot; bounded and isolated from other players.
    if (overRate(c, s, 'story', 2)) return;
    let raw; try { raw = JSON.stringify(msg.story || {}); } catch (_) { return; }
    if (raw.length > 16000) return;
    const src = msg.story && typeof msg.story === 'object' ? msg.story : {};
    const stage = ['normal','nether','end'].includes(src.stage) ? src.stage : 'normal';
    const towerFloor = Math.max(0, Math.min(20, Math.floor(Number(src.towerFloor) || 0)));
    const cleared = Array.isArray(src.towerCleared) ? [...new Set(src.towerCleared.map(Number).filter(n => Number.isInteger(n) && n >= 1 && n <= 20))] : [];
    const old = playersDb[c.world][c.accountId] || {};
    old.story = { stage, towerFloor, towerCleared: cleared,
      portal: { grown: !!(src.portal && src.portal.grown), active: !!(src.portal && src.portal.active) },
      compressed: src.compressed && typeof src.compressed === 'object' ? {
        wood: !!src.compressed.wood, stone: !!src.compressed.stone, metal: !!src.compressed.metal, diamond: !!src.compressed.diamond } : {},
      dragon: src.dragon && typeof src.dragon === 'object' ? {
        hp: Math.max(0, Math.min(1200, Number(src.dragon.hp) || 0)), phase: Math.max(1, Math.min(3, Number(src.dragon.phase) || 1)),
        crystals: Math.max(0, Math.min(10, Math.floor(Number(src.dragon.crystals) || 0))), defeated: !!src.dragon.defeated } : {} };
    playersDb[c.world][c.accountId] = old;
    sendTo(s, { t: 'story:ack' });saveData();
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
  const lobby=c.lobbyId&&lobbies[c.lobbyId]; if(lobby&&lobby.players.has(c.name))lobby.players.delete(c.name);
  if (w.players.has(c.name)) {
    w.players.delete(c.name);
    broadcastWorld(c.world, { t: 'player:leave', id: c.id, name: c.name });
    console.log('[leave]', c.name, '<-', c.world, '(' + w.players.size + ' online)');
  }
  c.world = null; c.lobbyId=null;saveData();
}

server.listen(PORT, '0.0.0.0', () => {
  console.log('ISLAND SURVIVAL V47 server running');
  console.log('  HTTP + WS : ws://0.0.0.0:' + PORT + '/ws');
  console.log('  Worlds    : ' + Object.keys(WORLDS).join(', '));
  console.log('  Max/lobby : ' + DEFAULT_LOBBY_CAPACITY);
  console.log('  Max sockets: ' + MAX_CONNECTIONS);
  console.log('  Lobbies   : ' + Object.keys(lobbies).length + ' (dynamic registry)');
  console.log('  Auth      : disabled (guest multiplayer)');
  console.log('  Data dir  : ' + DATA_DIR);
});
