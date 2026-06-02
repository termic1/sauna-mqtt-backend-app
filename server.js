import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import mqtt from "mqtt";
import { WebSocketServer } from "ws";
import http from "http";

// Load secrets from server/.env. Never put this file in GitHub.
dotenv.config();

const PORT = Number(process.env.PORT || 3001);
const MQTT_URL = process.env.MQTT_URL;
const MQTT_USER = process.env.MQTT_USER;
const MQTT_PASS = process.env.MQTT_PASS;
const MQTT_PREFIX = process.env.MQTT_PREFIX || "geysersteam";
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || "http://localhost:5173";
const ALLOWED_DEVICE_IDS = (process.env.ALLOWED_DEVICE_IDS || "")
  .split(",")
  .map((x) => cleanDeviceId(x))
  .filter(Boolean);

if (!MQTT_URL || !MQTT_USER || !MQTT_PASS) {
  console.error("Missing MQTT_URL, MQTT_USER, or MQTT_PASS in server/.env");
  process.exit(1);
}

function cleanDeviceId(id = "") {
  return String(id).replace(/[^a-fA-F0-9]/g, "").toUpperCase();
}

function deviceAllowed(deviceId) {
  if (!deviceId || deviceId.length !== 12) return false;
  if (ALLOWED_DEVICE_IDS.length === 0) return true;
  return ALLOWED_DEVICE_IDS.includes(deviceId);
}

function statusTopic(deviceId) {
  return `${MQTT_PREFIX}/${deviceId}/status`;
}

function commandTopic(deviceId, command) {
  return `${MQTT_PREFIX}/${deviceId}/cmd/${command}`;
}

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });

app.use(cors({ origin: ALLOWED_ORIGIN }));
app.use(express.json());

const lastStatus = new Map();
const wsClients = new Map(); // deviceId -> Set<WebSocket>

const mqttClient = mqtt.connect(MQTT_URL, {
  username: MQTT_USER,
  password: MQTT_PASS,
  clientId: `sauna-backend-${Math.random().toString(16).slice(2)}`,
  reconnectPeriod: 3000,
  clean: true,
});

mqttClient.on("connect", () => {
  console.log("MQTT connected");
  mqttClient.subscribe(`${MQTT_PREFIX}/+/status`, (err) => {
    if (err) console.error("MQTT subscribe failed:", err.message);
    else console.log(`Subscribed to ${MQTT_PREFIX}/+/status`);
  });
});

mqttClient.on("error", (err) => {
  console.error("MQTT error:", err.message);
});

mqttClient.on("message", (topic, payload) => {
  const parts = topic.split("/");
  if (parts.length !== 3 || parts[0] !== MQTT_PREFIX || parts[2] !== "status") return;

  const deviceId = cleanDeviceId(parts[1]);
  if (!deviceAllowed(deviceId)) return;

  let parsed;
  try {
    parsed = JSON.parse(payload.toString());
  } catch {
    return;
  }

  parsed._deviceId = deviceId;
  parsed._receivedAt = new Date().toISOString();
  lastStatus.set(deviceId, parsed);

  const clients = wsClients.get(deviceId);
  if (!clients) return;

  const msg = JSON.stringify({ type: "status", deviceId, status: parsed });
  for (const ws of clients) {
    if (ws.readyState === ws.OPEN) ws.send(msg);
  }
});

app.get("/api/health", (req, res) => {
  res.json({ ok: true, mqttConnected: mqttClient.connected });
});

app.get("/api/device/:deviceId/status", (req, res) => {
  const deviceId = cleanDeviceId(req.params.deviceId);
  if (!deviceAllowed(deviceId)) return res.status(403).json({ error: "Device not allowed" });
  res.json(lastStatus.get(deviceId) || null);
});

app.post("/api/device/:deviceId/cmd/:command", (req, res) => {
  const deviceId = cleanDeviceId(req.params.deviceId);
  const command = String(req.params.command || "");
  const allowedCommands = new Set(["power", "target", "timer", "irtime", "mode", "leds", "bright"]);

  if (!deviceAllowed(deviceId)) return res.status(403).json({ error: "Device not allowed" });
  if (!allowedCommands.has(command)) return res.status(400).json({ error: "Invalid command" });

  let value = req.body?.value;
  if (value === undefined || value === null) return res.status(400).json({ error: "Missing value" });

  // Basic safety validation before publishing to the sauna.
  if (command === "power" && !["on", "off", "true", "false", "1", "0"].includes(String(value))) {
    return res.status(400).json({ error: "Invalid power value" });
  }
  if (command === "target") {
    const n = Number(value);
    if (!Number.isFinite(n) || n < 90 || n > 135) return res.status(400).json({ error: "Target must be 90-135" });
    value = n;
  }
  if (command === "timer" || command === "irtime") {
    const n = Number(value);
    if (!Number.isInteger(n) || n < 0 || n > 120) return res.status(400).json({ error: "Timer must be 0-120" });
    value = n;
  }
  if (command === "mode") {
    const n = Number(value);
    if (!Number.isInteger(n) || n < 0 || n > 12) return res.status(400).json({ error: "Mode must be 0-12" });
    value = n;
  }
  if (command === "bright") {
    const n = Number(value);
    if (!Number.isInteger(n) || n < 10 || n > 255) return res.status(400).json({ error: "Brightness must be 10-255" });
    value = n;
  }
  if (command === "leds" && !["on", "off", "true", "false", "1", "0"].includes(String(value))) {
    return res.status(400).json({ error: "Invalid LEDs value" });
  }

  mqttClient.publish(commandTopic(deviceId, command), String(value), { qos: 0, retain: false }, (err) => {
    if (err) return res.status(500).json({ error: err.message });
    res.json({ ok: true, topic: commandTopic(deviceId, command), value });
  });
});

wss.on("connection", (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const deviceId = cleanDeviceId(url.searchParams.get("deviceId") || "");

  if (!deviceAllowed(deviceId)) {
    ws.send(JSON.stringify({ type: "error", error: "Device not allowed" }));
    ws.close();
    return;
  }

  if (!wsClients.has(deviceId)) wsClients.set(deviceId, new Set());
  wsClients.get(deviceId).add(ws);

  ws.send(JSON.stringify({ type: "hello", deviceId, mqttConnected: mqttClient.connected }));
  if (lastStatus.has(deviceId)) {
    ws.send(JSON.stringify({ type: "status", deviceId, status: lastStatus.get(deviceId) }));
  }

  ws.on("close", () => {
    const set = wsClients.get(deviceId);
    if (!set) return;
    set.delete(ws);
    if (set.size === 0) wsClients.delete(deviceId);
  });
});

server.listen(PORT, () => {
  console.log(`Backend running on http://localhost:${PORT}`);
});
