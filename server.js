import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import mqtt from "mqtt";
import { WebSocketServer } from "ws";
import http from "http";
import { randomInt } from "node:crypto";

dotenv.config();
const PORT = Number(process.env.PORT || 3001);
const MQTT_URL = process.env.MQTT_URL;
const MQTT_USER = process.env.MQTT_USER;
const MQTT_PASS = process.env.MQTT_PASS;
const MQTT_PREFIX = process.env.MQTT_PREFIX || "geysersteam";
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || "*";
const ICEMAKER_PREFIX = "geysericemaker";
const iceStatuses = new Map();
const iceWsClients = new Map();
const ICE_COMMANDS = Object.freeze({stopaftercycle:null, power: null, mode:[0,12], leds:null, bright:[10,255], drytime:[5,300], icingtime:[10,900], deliverytime:[5,300], flush:null, displaybright:[10,255], overheatthresh:[30,120]});
function cleanDeviceId(id = "") { return String(id).replace(/[^a-fA-F0-9]/g, "").toUpperCase(); }
const ALLOWED_DEVICE_IDS = (process.env.ALLOWED_DEVICE_IDS || "").split(",").map(cleanDeviceId).filter(Boolean);
function deviceAllowed(id) { return id.length === 12 && (ALLOWED_DEVICE_IDS.length === 0 || ALLOWED_DEVICE_IDS.includes(id)); }
function booleanValue(value) { return value === true || value === 1 || ["on","true","1"].includes(String(value).toLowerCase()); }
function isBooleanCommandValue(value) { return ["on","off","true","false","1","0"].includes(String(value).toLowerCase()); }
function intRange(v, min, max) { return typeof v !== "boolean" && v !== null && v !== "" && Number.isInteger(Number(v)) && Number(v) >= min && Number(v) <= max; }
function validRange(total, start, end) { return intRange(total,1,5000) && intRange(start,0,4999) && intRange(end,0,4999) && Number(start)<=Number(end) && Number(end)<Number(total); }
if (!MQTT_URL || !MQTT_USER || !MQTT_PASS) { console.error("Missing MQTT_URL, MQTT_USER, or MQTT_PASS"); process.exit(1); }

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });
const allowedOrigins = ALLOWED_ORIGIN === "*" ? "*" : ALLOWED_ORIGIN.split(",").map(x => x.trim()).filter(Boolean);
app.use(cors({ origin: allowedOrigins, methods:["GET","POST","OPTIONS"], allowedHeaders:["Content-Type"] }));
app.use(express.json({ limit: "16kb" }));
const lastStatus = new Map();
const wsClients = new Map();
const pendingSettingsVerifications = new Map();
const mqttClient = mqtt.connect(MQTT_URL, { username: MQTT_USER, password: MQTT_PASS, clientId:`sauna-backend-${randomInt(0x7fffffff).toString(16)}`, reconnectPeriod:3000, clean:true });
mqttClient.on("connect", () => { console.log("MQTT connected"); mqttClient.subscribe([`${MQTT_PREFIX}/+/status`, `${ICEMAKER_PREFIX}/+/status`], err => { if (err) console.error("MQTT subscribe failed:",err.message); else console.log("Subscribed to Sauna and Ice Maker status topics"); }); });
mqttClient.on("error", err => console.error("MQTT error:",err.message));
mqttClient.on("message", (topic,payload) => {
  const parts = topic.split("/");
  if (parts.length!==3 || ![MQTT_PREFIX, ICEMAKER_PREFIX].includes(parts[0]) || parts[2]!=="status") return;
  const deviceId = cleanDeviceId(parts[1]);
  if (!deviceAllowed(deviceId)) return;
  let parsed;
  try { parsed=JSON.parse(payload.toString()); } catch { return; }
  if (!parsed || typeof parsed!=="object" || Array.isArray(parsed)) return;
  parsed._deviceId=deviceId;
  parsed._receivedAt=new Date().toISOString();
  const isIce = parts[0] === ICEMAKER_PREFIX;
  (isIce ? iceStatuses : lastStatus).set(deviceId,parsed);
  if (!isIce && parsed.settingsVerifyNonce!==undefined) {
    const key=`${deviceId}:${Number(parsed.settingsVerifyNonce)}`;
    const pending=pendingSettingsVerifications.get(key);
    if (pending) { clearTimeout(pending.timer); pendingSettingsVerifications.delete(key); pending.resolve(booleanValue(parsed.settingsVerifyValid)); }
  }
  const clients=(isIce ? iceWsClients : wsClients).get(deviceId);
  if (!clients) return;
  const msg=JSON.stringify({type:"status",deviceId,status:parsed});
  for (const ws of clients) if (ws.readyState===ws.OPEN) ws.send(msg);
});

wss.on("connection", ws => {
  let subscribedDeviceId=null;
  ws.on("message", raw => {
    let msg;
    try { msg=JSON.parse(raw.toString()); } catch { ws.send(JSON.stringify({type:"error",error:"Invalid JSON"})); return; }
    if (msg?.type!=="subscribe") return;
    const deviceId=cleanDeviceId(msg.deviceId);
    const isIce = msg.product === "icemaker";
    const clientMap = isIce ? iceWsClients : wsClients;
    if (!deviceAllowed(deviceId)) { ws.send(JSON.stringify({type:"error",error:"Device not allowed"})); return; }
    if (subscribedDeviceId) {
      const oldMap = ws._iceSubscription ? iceWsClients : wsClients;
      const previous=oldMap.get(subscribedDeviceId);
      if (previous) { previous.delete(ws); if (!previous.size) oldMap.delete(subscribedDeviceId); }
    }
    subscribedDeviceId=deviceId;
    ws._iceSubscription=isIce;
    if (!clientMap.has(deviceId)) clientMap.set(deviceId,new Set());
    clientMap.get(deviceId).add(ws);
    const status=(isIce ? iceStatuses : lastStatus).get(deviceId);
    if (status) ws.send(JSON.stringify({type:"status",deviceId,status}));
  });
  ws.on("close", () => { if (!subscribedDeviceId) return; const clientMap=ws._iceSubscription ? iceWsClients : wsClients; const clients=clientMap.get(subscribedDeviceId); if (clients) { clients.delete(ws); if (!clients.size) clientMap.delete(subscribedDeviceId); } });
});

app.get("/api/health", (_req,res) => res.json({ok:true,mqttConnected:mqttClient.connected,wsDevices:wsClients.size}));
app.get("/api/device/:deviceId/status", (req,res) => {
  const id=cleanDeviceId(req.params.deviceId);
  if (!deviceAllowed(id)) return res.status(403).json({error:"Device not allowed"});
  return res.json(lastStatus.get(id)||null);
});

// Separate Ice Maker API. Sauna endpoints and MQTT command semantics remain unchanged.
app.get("/api/icemaker/:deviceId/status", (req,res) => {
  const id=cleanDeviceId(req.params.deviceId);
  if (!deviceAllowed(id)) return res.status(403).json({error:"Device not allowed"});
  return res.json(iceStatuses.get(id)||null);
});
app.post("/api/icemaker/:deviceId/cmd/:command", (req,res) => {
  const id=cleanDeviceId(req.params.deviceId);
  if (!deviceAllowed(id)) return res.status(403).json({error:"Device not allowed"});
  const command=String(req.params.command||"");
  if (!Object.prototype.hasOwnProperty.call(ICE_COMMANDS,command)) return res.status(400).json({error:"Unsupported Ice Maker command"});
  if (!iceStatuses.has(id)) return res.status(409).json({error:"Ice Maker status not received yet"});
  let value=req.body?.value;
  if (value===undefined || value===null) return res.status(400).json({error:"Missing value"});
  if (command==="power" || command==="leds") {
    if (!["on","off"].includes(String(value))) return res.status(400).json({error:"Expected on or off"});
  } else if (command==="stopaftercycle") {
    if (!["request","cancel"].includes(String(value)))
      return res.status(400).json({error:"Expected request or cancel"});
    const st=iceStatuses.get(id);
    if (value==="request" && !booleanValue(st.on))
      return res.status(409).json({error:"Ice Maker is not running"});
  } else if (command==="flush") {
    if (!["arm","confirm","stop"].includes(String(value))) return res.status(400).json({error:"Expected arm, confirm, or stop"});
    const st=iceStatuses.get(id);
    if (value!=="stop" && (booleanValue(st.on) || booleanValue(st.ohAlarm))) return res.status(409).json({error:"Flush unavailable while machine is running or overheat alarm is active"});
    // Firmware enforces the arm/confirm window; status can lag the arm command.
  } else {
    const [min,max]=ICE_COMMANDS[command];
    const n=Number(value);
    if (!Number.isFinite(n) || n<min || n>max || (command!=="overheatthresh" && !Number.isInteger(n)))
      return res.status(400).json({error:`Invalid ${command}: expected ${min} to ${max}`});
    value=n;
  }
  return publishCommand(`${ICEMAKER_PREFIX}/${id}/cmd/${command}`,String(value),res);
});

function publishCommand(topic,value,res) {
  if (!mqttClient.connected) return res.status(503).json({error:"MQTT broker not connected"});
  mqttClient.publish(topic,typeof value==="string"?value:JSON.stringify(value),{qos:0,retain:false},err => {
    if (err) return res.status(500).json({error:err.message});
    return res.json({ok:true,topic,value});
  });
}

app.post("/api/device/:deviceId/cmd/:command", (req,res) => {
  const id=cleanDeviceId(req.params.deviceId), command=String(req.params.command||"");
  if (!deviceAllowed(id)) return res.status(403).json({error:"Device not allowed"});
  const allowed=new Set(["power","target","timer","irtime","mode","leds","bright","room","aroma-fragrance","aroma-enable","aroma-period","aroma-manual","steam"]);
  if (!allowed.has(command)) return res.status(400).json({error:"Invalid command"});
  let value=req.body?.value;
  if (value===undefined || value===null) return res.status(400).json({error:"Missing value"});
  if (["power","leds","aroma-enable","aroma-manual","steam"].includes(command) && !isBooleanCommandValue(value)) return res.status(400).json({error:`Invalid ${command} value`});
  const ranges={target:[120,220],timer:[0,120],irtime:[0,120],mode:[0,12],bright:[0,255],room:[1,3],"aroma-fragrance":[1,4],"aroma-period":[10,300]};
  if (ranges[command]) {
    const [min,max]=ranges[command];
    if (command==="target" ? (value==="" || !Number.isFinite(Number(value)) || Number(value)<min || Number(value)>max) : !intRange(value,min,max)) return res.status(400).json({error:`Invalid ${command} value (${min}-${max})`});
    value=Number(value);
  }
  const suffix={"aroma-fragrance":"aroma/fragrance","aroma-enable":"aroma/enable","aroma-period":"aroma/period","aroma-manual":"aroma/manual"}[command] || command;
  return publishCommand(`${MQTT_PREFIX}/${id}/cmd/${suffix}`,String(value),res);
});

app.post("/api/device/:deviceId/settings/verify",async(req,res) => {
  const id=cleanDeviceId(req.params.deviceId);
  if (!deviceAllowed(id)) return res.status(403).json({error:"Device not allowed"});
  const password=String(req.body?.password||"").trim();
  if (!password) return res.status(400).json({error:"Settings password required"});
  if (!mqttClient.connected) return res.status(503).json({error:"MQTT broker not connected"});
  let nonce,key;
  do { nonce=randomInt(1,0x7fffffff); key=`${id}:${nonce}`; } while (pendingSettingsVerifications.has(key));
  const verificationPromise=new Promise(resolve => {
    const timer=setTimeout(() => { pendingSettingsVerifications.delete(key); resolve(null); },5000);
    pendingSettingsVerifications.set(key,{resolve,timer});
  });
  const topic=`${MQTT_PREFIX}/${id}/cmd/settings/verify`;
  mqttClient.publish(topic,JSON.stringify({password,nonce}),{qos:0,retain:false},async err => {
    if (err) { const p=pendingSettingsVerifications.get(key); if (p) { clearTimeout(p.timer); pendingSettingsVerifications.delete(key); p.resolve(null); } return res.status(500).json({error:err.message}); }
    const valid=await verificationPromise;
    if (valid===null) return res.status(504).json({error:"Controller did not respond to password verification"});
    if (!valid) return res.status(401).json({error:"Incorrect password"});
    return res.json({ok:true});
  });
});

// Only firmware with the new hardware fields can safely receive multi-light settings.
// Older or unknown firmware always uses the legacy payload.
function modernFirmware(status) {
  if (!status || !["name1","name2","type1","type2","shareRGB"].every(k => status[k] !== undefined)) return false;
  const match=String(status.fw||"").match(/^(\d+)\.(\d+)\.(\d+)/);
  if (!match) return false;
  const [,major,minor,patch]=match.map(Number);
  return major>2 || (major===2 && (minor>4 || (minor===4 && patch>=3)));
}

app.post("/api/device/:deviceId/settings",(req,res) => {
  const id=cleanDeviceId(req.params.deviceId);
  if (!deviceAllowed(id)) return res.status(403).json({error:"Device not allowed"});
  const body=req.body||{};
  const password=String(body.password||"");
  if (!password) return res.status(400).json({error:"Settings password required"});
  const existing=lastStatus.get(id);
  if (!existing) return res.status(409).json({error:"No controller status available. Wait for the device to connect before saving settings."});
  const modern=modernFirmware(existing);
  if (body.controllerGeneration && body.controllerGeneration !== (modern ? "modern" : "legacy"))
    return res.status(409).json({error:"Controller firmware changed. Refresh the page before saving settings."});
  const ledTotal=Number(body.ledTotal ?? existing.ledTotal ?? 600);
  const r1Start=Number(body.r1Start ?? existing.r1Start ?? 3);
  const r1End=Number(body.r1End ?? existing.r1End ?? 272);
  const r2Start=Number(body.r2Start ?? existing.r2Start ?? 300);
  const r2End=Number(body.r2End ?? existing.r2End ?? 599);
  if (!validRange(ledTotal,r1Start,r1End) || !validRange(ledTotal,r2Start,r2End))
    return res.status(400).json({error:"Invalid LED ranges"});
  const settingsPayload={
    password,
    aromaUI:body.aromaUI===undefined ? booleanValue(existing.aromaUI) : booleanValue(body.aromaUI),
    infraredUI:body.infraredUI===undefined ? (existing.infraredUI===undefined ? true : booleanValue(existing.infraredUI)) : booleanValue(body.infraredUI),
    ledTotal,r1Start,r1End,r2Start,r2End
  };
  if (modern) {
    const name1=Number(body.name1 ?? existing.name1 ?? 1);
    const name2=Number(body.name2 ?? existing.name2 ?? 2);
    const type1=Number(body.type1 ?? existing.type1 ?? 1);
    const type2=Number(body.type2 ?? existing.type2 ?? 1);
    const shareRGB=Number(body.shareRGB ?? existing.shareRGB ?? 1);
    const led2Total=Number(body.led2Total ?? existing.led2Total ?? 300);
    const led2Start=Number(body.led2Start ?? existing.led2Start ?? 0);
    const led2End=Number(body.led2End ?? existing.led2End ?? 299);
    if (![1,2].includes(name1) || ![0,1,2].includes(name2)) return res.status(400).json({error:"Invalid room names"});
    if (![1,2].includes(type1) || ![0,1,2].includes(type2) || (name2===0)!==(type2===0)) return res.status(400).json({error:"Invalid room LED types"});
    if (![0,1].includes(shareRGB)) return res.status(400).json({error:"Invalid shared RGB setting"});
    if (type2===1 && !(type1===1 && shareRGB===1) && !validRange(led2Total,led2Start,led2End)) return res.status(400).json({error:"Invalid GPIO42 LED range"});
    Object.assign(settingsPayload,{name1,name2,type1,type2,shareRGB,led2Total,led2Start,led2End,
      reverseEncoder:body.reverseEncoder===undefined ? booleanValue(existing.reverseEncoder) : booleanValue(body.reverseEncoder)});
  } else {
    // Older firmware (including v2.4.02) REQUIRES reverseRooms in its settings JSON.
    settingsPayload.reverseRooms=body.reverseRooms===undefined ? booleanValue(existing.reverseRooms) : booleanValue(body.reverseRooms);
    if (existing.reverseEncoder!==undefined || body.reverseEncoder!==undefined)
      settingsPayload.reverseEncoder=body.reverseEncoder===undefined ? booleanValue(existing.reverseEncoder) : booleanValue(body.reverseEncoder);
    if (existing.whiteMode!==undefined) settingsPayload.whiteMode=body.whiteMode===undefined ? booleanValue(existing.whiteMode) : booleanValue(body.whiteMode);
    if (existing.white2!==undefined) settingsPayload.white2=body.white2===undefined ? booleanValue(existing.white2) : booleanValue(body.white2);
  }
  if (String(body.newPassword||"").trim()) settingsPayload.newPassword=String(body.newPassword).trim();
  if (!mqttClient.connected) return res.status(503).json({error:"MQTT broker not connected"});
  const topic=`${MQTT_PREFIX}/${id}/cmd/settings`;
  mqttClient.publish(topic,JSON.stringify(settingsPayload),{qos:0,retain:false},err => {
    if (err) return res.status(500).json({error:err.message});
    const {password:_secret,newPassword:_newSecret,...settings}=settingsPayload;
    return res.json({ok:true,topic,settings});
  });
});

server.listen(PORT,()=>console.log(`Backend running on port ${PORT}`));
