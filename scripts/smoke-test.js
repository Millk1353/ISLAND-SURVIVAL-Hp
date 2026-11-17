const { spawn } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');
const http = require('http');
const net = require('net');
const crypto = require('crypto');
const port = 19000 + Math.floor(Math.random() * 1000);
const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'island-v44-'));
let child, logs = '';
const wait = ms => new Promise(r => setTimeout(r, ms));
function startServer() {
  logs = '';
  child = spawn(process.execPath, [path.join(__dirname, '..', 'server', 'server.js')], {
    env: { ...process.env, PORT: String(port), DATA_DIR: dataDir, MAX_PLAYERS: '2', ADMIN_API_KEY: 'test-admin-secret' },
    stdio: ['ignore', 'pipe', 'pipe']
  });
  child.stdout.on('data', d => logs += d.toString());
  child.stderr.on('data', d => logs += d.toString());
}
async function stopServer() {
  if (!child || child.exitCode !== null) return;
  child.kill('SIGTERM');
  await Promise.race([new Promise(r => child.once('exit', r)), wait(1500)]);
}
function request(route, method='GET', body, extraHeaders={}) {
  return new Promise((resolve, reject) => {
    const req = http.request({ hostname: '127.0.0.1', port, path: route, method, headers: { ...(body ? {'content-type':'application/json'} : {}), ...extraHeaders } }, res => {
      let raw=''; res.on('data',d=>raw+=d); res.on('end',()=>{try{resolve({status:res.statusCode,data:JSON.parse(raw||'{}'),raw})}catch(e){resolve({status:res.statusCode,data:{},raw})}});
    }); req.on('error',reject); if(body)req.write(JSON.stringify(body)); req.end();
  });
}
async function waitReady() {
  for(let i=0;i<60;i++) { try { const r=await request('/api/health'); if(r.status===200)return r; } catch(_) {} await wait(100); }
  throw Error('server did not become ready: '+logs);
}
function maskedTextFrame(value) {
  const payload=Buffer.from(JSON.stringify(value));
  const mask=crypto.randomBytes(4);let header;
  if(payload.length<126)header=Buffer.from([0x81,0x80|payload.length]);
  else {header=Buffer.alloc(4);header[0]=0x81;header[1]=0x80|126;header.writeUInt16BE(payload.length,2);}
  const body=Buffer.from(payload);for(let i=0;i<body.length;i++)body[i]^=mask[i&3];
  return Buffer.concat([header,mask,body]);
}
async function wsJoin(guestId, lobby='001', invite='') {
  const socket=net.connect(port,'127.0.0.1');let buffer=Buffer.alloc(0),handshake=false;
  const messages=[];let resolveWelcome,rejectWelcome;
  const welcomePromise=new Promise((resolve,reject)=>{resolveWelcome=resolve;rejectWelcome=reject;});
  const parse=()=>{
    if(!handshake){const end=buffer.indexOf('\r\n\r\n');if(end<0)return;const head=buffer.slice(0,end).toString();if(!/101 Switching Protocols/i.test(head)){rejectWelcome(Error('WebSocket handshake failed: '+head));socket.destroy();return;}buffer=buffer.slice(end+4);handshake=true;socket.write(maskedTextFrame({t:'join',world:lobby,guestId,invite,name:'اختبار'+guestId.slice(-4)}));}
    while(buffer.length>=2){const op=buffer[0]&15;let len=buffer[1]&127,off=2;if(len===126){if(buffer.length<4)return;len=buffer.readUInt16BE(2);off=4;}else if(len===127){if(buffer.length<10)return;len=Number(buffer.readBigUInt64BE(2));off=10;}if(buffer.length<off+len)return;const payload=buffer.slice(off,off+len);buffer=buffer.slice(off+len);if(op===1){try{const msg=JSON.parse(payload.toString());messages.push(msg);if(msg.t==='welcome')resolveWelcome(msg);else if(msg.t==='error')rejectWelcome(Error(msg.msg));}catch(_){}}else if(op===9){socket.write(maskedControlFrame(10,payload));}}
  };
  function maskedControlFrame(op,payload){const mask=crypto.randomBytes(4),body=Buffer.from(payload);for(let i=0;i<body.length;i++)body[i]^=mask[i&3];return Buffer.concat([Buffer.from([0x80|op,0x80|body.length]),mask,body]);}
  socket.on('data',data=>{buffer=Buffer.concat([buffer,data]);parse();});
  socket.on('error',rejectWelcome);
  socket.on('connect',()=>{const key=crypto.randomBytes(16).toString('base64');socket.write(`GET /ws HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: ${key}\r\nSec-WebSocket-Version: 13\r\n\r\n`);});
  const timer=setTimeout(()=>rejectWelcome(Error('WebSocket welcome timeout; got '+JSON.stringify(messages))),4000);
  try{const welcome=await welcomePromise;return {socket,messages,welcome,send:o=>socket.write(maskedTextFrame(o))};}finally{clearTimeout(timer);}
}
(async()=>{
 try {
  startServer();
  let health=await waitReady();
  if(!health.data.ok||health.data.version!=='47.0.0')throw Error('health endpoint failed');
  const guestIds=[0,1,2].map(()=>('guest_'+crypto.randomBytes(12).toString('hex')));
  for (const route of ['/api/login','/api/register','/api/logout']) {
    const oldAuth=await request(route,'POST',{username:'nope',password:'nope'});
    if(oldAuth.status!==405 && oldAuth.status!==404)throw Error('auth endpoint should be removed: '+route+' '+oldAuth.status);
  }
  const privateCreateDenied=await request('/api/admin/lobbies','POST',{name:'private-test',private:true});
  if(privateCreateDenied.status!==403)throw Error('admin lobby endpoint must reject requests without admin key');
  let list=await request('/api/lobbies');
  if(list.status!==200||!list.data.lobbies?.length)throw Error('lobby list failed');
  const p1=await wsJoin(guestIds[0],'001');
  const p2=await wsJoin(guestIds[1],'001');
  if(p1.welcome.lobbyId!=='001'||p2.welcome.lobbyId!=='001')throw Error('first two players did not enter lobby 001');
  const p3=await wsJoin(guestIds[2],'001');
  if(p3.welcome.lobbyId!=='002')throw Error('full lobby did not redirect player to lobby 002: '+p3.welcome.lobbyId);
  const p1Reconnected=await wsJoin(guestIds[0],'001');
  if(p1Reconnected.welcome.lobbyId!=='001')throw Error('reconnecting guest did not reclaim its existing lobby slot');
  const afterReconnect=await request('/api/lobbies');
  if(afterReconnect.data.lobbies.find(l=>l.id==='001')?.players!==2)throw Error('duplicate guest reconnect left a stale player slot');
  const privateCreate=await request('/api/admin/lobbies','POST',{name:'private-test',private:true},{'x-admin-key':'test-admin-secret'});
  if(privateCreate.status!==201||!privateCreate.data.inviteCode)throw Error('private lobby creation did not return a private invite code');
  let denied=false;try{await wsJoin('guest_'+crypto.randomBytes(12).toString('hex'),privateCreate.data.lobby.id,'wrong-code')}catch(e){denied=/دعوة|رمز/.test(e.message)}
  if(!denied)throw Error('private lobby accepted an invalid invite code');
  const privateGuest=await wsJoin('guest_'+crypto.randomBytes(12).toString('hex'),privateCreate.data.lobby.id,privateCreate.data.inviteCode);
  if(privateGuest.welcome.lobbyId!==privateCreate.data.lobby.id)throw Error('private lobby rejected its valid invite code');
  // Verify a real WebSocket gameplay event is relayed between players.
  p1Reconnected.send({t:'state',x:0,y:0,face:1,skin:0});
  p1Reconnected.send({t:'block',x:0,y:0,tile:1});
  await wait(150);
  if(!p2.messages.some(m=>m.t==='block'&&m.x===0&&m.y===0&&m.tile===1))throw Error('block event did not sync to the second player');
  p2.socket.end();await wait(80);
  const lateJoin=await wsJoin('guest_'+crypto.randomBytes(12).toString('hex'),'001');
  if(!lateJoin.welcome.edits||!lateJoin.welcome.edits['0']||lateJoin.welcome.edits['0']['0']!==1)throw Error('new player did not receive the current world snapshot');
  await wait(1050);
  const worldFile=JSON.parse(fs.readFileSync(path.join(dataDir,'worlds.json'),'utf8'));
  if(!worldFile['lobby:001']||worldFile['lobby:001'].edits['0']['0']!==1)throw Error('debounced world autosave did not persist the block edit');
  p1.socket.end();p1Reconnected.socket.end();p3.socket.end();privateGuest.socket.end();lateJoin.socket.end();
  if(!fs.existsSync(path.join(dataDir,'lobbies.json')))throw Error('lobby persistence file missing');
  await stopServer();
  startServer();health=await waitReady();
  const guestAfterRestart=await wsJoin(guestIds[0],'001');
  if(guestAfterRestart.welcome.accountId!==guestIds[0])throw Error('guest identity did not survive server restart');
  if(!guestAfterRestart.welcome.edits||!guestAfterRestart.welcome.edits['0']||guestAfterRestart.welcome.edits['0']['0']!==1)throw Error('world block snapshot did not survive restart for a newly joining player');
  guestAfterRestart.socket.end();
  list=await request('/api/lobbies');
  if(!list.data.lobbies.some(l=>l.id==='002'))throw Error('dynamic lobby did not survive server restart');
  console.log('V47 integration tests passed: health, guest WebSocket join, lobby routing, live block sync, late-join world snapshot, debounced world autosave, private invite validation, duplicate-session replacement, and persistence after restart.');
 } catch(e) { console.error(e.message); console.error(logs); process.exitCode=1; }
 finally { await stopServer(); fs.rmSync(dataDir,{recursive:true,force:true}); }
})();
