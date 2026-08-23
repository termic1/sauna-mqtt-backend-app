import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import mqtt from "mqtt";
import { WebSocketServer } from "ws";
import http from "http";

dotenv.config();

const PORT = Number(process.env.PORT || 3001);
const MQTT_URL = process.env.MQTT_URL;
const MQTT_USER = process.env.MQTT_USER;
const MQTT_PASS = process.env.MQTT_PASS;
const MQTT_PREFIX = process.env.MQTT_PREFIX || "geysersteam";
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || "*";

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

function isBooleanCommandValue(value) {
  return ["on", "off", "true", "false", "1", "0"].includes(String(value));
}

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });

app.use(cors({
  origin: ALLOWED_ORIGIN === "*" ? "*" : ALLOWED_ORIGIN,
  methods: ["GET", "POST", "OPTIONS"],
  allowedHeaders: ["Content-Type"]
}));

app.use(express.json());

const lastStatus = new Map();
const wsClients = new Map();

const mqttClient = mqtt.connect(MQTT_URL, {
  username: MQTT_USER,
  password: MQTT_PASS,
  clientId: `sauna-backend-${Math.random().toString(16).slice(2)}`,
  reconnectPeriod: 3000,
  clean: true
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

  if (
    parts.length !== 3 ||
    parts[0] !== MQTT_PREFIX ||
    parts[2] !== "status"
  ) {
    return;
  }

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

  const msg = JSON.stringify({
    type: "status",
    deviceId,
    status: parsed
  });

  for (const ws of clients) {
    if (ws.readyState === ws.OPEN) {
      ws.send(msg);
    }
  }
});

wss.on("connection", (ws) => {
  let subscribedDeviceId = null;

  ws.on("message", (raw) => {
    let msg;

    try {
      msg = JSON.parse(raw.toString());
    } catch {
      ws.send(
        JSON.stringify({
          type: "error",
          error: "Invalid JSON"
        })
      );
      return;
    }

    if (msg.type !== "subscribe") return;

    const deviceId = cleanDeviceId(msg.deviceId);

    if (!deviceAllowed(deviceId)) {
      ws.send(
        JSON.stringify({
          type: "error",
          error: "Device not allowed"
        })
      );
      return;
    }

    subscribedDeviceId = deviceId;

    if (!wsClients.has(deviceId)) {
      wsClients.set(deviceId, new Set());
    }

    wsClients.get(deviceId).add(ws);

    const status = lastStatus.get(deviceId);

    if (status) {
      ws.send(
        JSON.stringify({
          type: "status",
          deviceId,
          status
        })
      );
    }
  });

  ws.on("close", () => {
    if (!subscribedDeviceId) return;

    const clients = wsClients.get(subscribedDeviceId);

    if (!clients) return;

    clients.delete(ws);

    if (clients.size === 0) {
      wsClients.delete(subscribedDeviceId);
    }
  });
});

app.get("/api/health", (req, res) => {
  res.json({
    ok: true,
    mqttConnected: mqttClient.connected,
    wsDevices: wsClients.size
  });
});

app.get("/api/device/:deviceId/status", (req, res) => {
  const deviceId = cleanDeviceId(req.params.deviceId);

  if (!deviceAllowed(deviceId)) {
    return res.status(403).json({
      error: "Device not allowed"
    });
  }

  res.json(lastStatus.get(deviceId) || null);
});

app.post("/api/device/:deviceId/cmd/:command", (req, res) => {
  const deviceId = cleanDeviceId(req.params.deviceId);
  const command = String(req.params.command || "");

  const allowedCommands = new Set([
    "power",
    "target",
    "timer",
    "irtime",
    "mode",
    "leds",
    "bright",
    "room",

    // Aromatherapy
    "aroma-fragrance",
    "aroma-enable",
    "aroma-period",
    "aroma-manual",
    "steam"
  ]);

  if (!deviceAllowed(deviceId)) {
    return res.status(403).json({
      error: "Device not allowed"
    });
  }

  if (!allowedCommands.has(command)) {
    return res.status(400).json({
      error: "Invalid command"
    });
  }

  let value = req.body?.value;

  if (value === undefined || value === null) {
    return res.status(400).json({
      error: "Missing value"
    });
  }

  if (
    command === "power" &&
    !isBooleanCommandValue(value)
  ) {
    return res.status(400).json({
      error: "Invalid power value"
    });
  }

  if (command === "target") {
    const n = Number(value);

    if (
      !Number.isFinite(n) ||
      n < 120 ||
      n > 220
    ) {
      return res.status(400).json({
        error: "Target must be 120-220"
      });
    }

    value = n;
  }

  if (
    command === "timer" ||
    command === "irtime"
  ) {
    const n = Number(value);

    if (
      !Number.isInteger(n) ||
      n < 0 ||
      n > 120
    ) {
      return res.status(400).json({
        error: "Timer must be 0-120"
      });
    }

    value = n;
  }

  if (command === "mode") {
    const n = Number(value);

    if (
      !Number.isInteger(n) ||
      n < 0 ||
      n > 12
    ) {
      return res.status(400).json({
        error: "Mode must be 0-12"
      });
    }

    value = n;
  }

  if (command === "bright") {
    const n = Number(value);

    if (
      !Number.isInteger(n) ||
      n < 10 ||
      n > 255
    ) {
      return res.status(400).json({
        error: "Brightness must be 10-255"
      });
    }

    value = n;
  }

  if (command === "room") {
    const n = Number(value);

    if (
      !Number.isInteger(n) ||
      n < 1 ||
      n > 3
    ) {
      return res.status(400).json({
        error: "Room must be 1, 2, or 3"
      });
    }

    value = n;
  }

  if (
    command === "leds" &&
    !isBooleanCommandValue(value)
  ) {
    return res.status(400).json({
      error: "Invalid LEDs value"
    });
  }

  // -----------------------------------
  // Aromatherapy validation
  // -----------------------------------

  if (command === "aroma-fragrance") {
    const n = Number(value);

    if (
      !Number.isInteger(n) ||
      n < 1 ||
      n > 4
    ) {
      return res.status(400).json({
        error: "Fragrance must be 1-4"
      });
    }

    value = n;
  }

  if (command === "aroma-period") {
    const n = Number(value);

    if (
      !Number.isInteger(n) ||
      n < 10 ||
      n > 300
    ) {
      return res.status(400).json({
        error: "Aroma period must be 10-300 seconds"
      });
    }

    value = n;
  }

  if (
    (
      command === "aroma-enable" ||
      command === "aroma-manual" ||
      command === "steam"
    ) &&
    !isBooleanCommandValue(value)
  ) {
    return res.status(400).json({
      error: `Invalid ${command} value`
    });
  }

  // -----------------------------------
  // Frontend command -> MQTT topic
  // -----------------------------------

  const topicSuffixMap = {
    "aroma-fragrance": "aroma/fragrance",
    "aroma-enable": "aroma/enable",
    "aroma-period": "aroma/period",
    "aroma-manual": "aroma/manual",
    "steam": "steam"
  };

  const topicSuffix =
    topicSuffixMap[command] || command;

  const topic =
    `${MQTT_PREFIX}/${deviceId}/cmd/${topicSuffix}`;

  mqttClient.publish(
    topic,
    String(value),
    {
      qos: 0,
      retain: false
    },
    (err) => {
      if (err) {
        return res.status(500).json({
          error: err.message
        });
      }

      res.json({
        ok: true,
        topic,
        value
      });
    }
  );
});


// ============================================================
// Remote Settings
// ============================================================

app.post("/api/device/:deviceId/settings", (req, res) => {
  const deviceId = cleanDeviceId(req.params.deviceId);

  if (!deviceAllowed(deviceId)) {
    return res.status(403).json({
      error: "Device not allowed"
    });
  }

  const password =
    String(req.body?.password || "");

  const newPassword =
    String(req.body?.newPassword || "");

  const aromaUI =
    Boolean(req.body?.aromaUI);

  const ledTotal =
    Number(req.body?.ledTotal);

  const r1Start =
    Number(req.body?.r1Start);

  const r1End =
    Number(req.body?.r1End);

  const r2Start =
    Number(req.body?.r2Start);

  const r2End =
    Number(req.body?.r2End);

  if (!password) {
    return res.status(400).json({
      error: "Settings password required"
    });
  }

  if (
    !Number.isInteger(ledTotal) ||
    !Number.isInteger(r1Start) ||
    !Number.isInteger(r1End) ||
    !Number.isInteger(r2Start) ||
    !Number.isInteger(r2End)
  ) {
    return res.status(400).json({
      error: "LED settings must be integers"
    });
  }

  if (
    ledTotal < 1 ||
    ledTotal > 5000
  ) {
    return res.status(400).json({
      error: "Total LEDs must be 1-5000"
    });
  }

  if (
    r1Start < 0 ||
    r1End < r1Start ||
    r1End >= ledTotal
  ) {
    return res.status(400).json({
      error: "Invalid Sauna LED range"
    });
  }

  if (
    r2Start < 0 ||
    r2End < r2Start ||
    r2End >= ledTotal
  ) {
    return res.status(400).json({
      error: "Invalid Steam LED range"
    });
  }

  /*
    IMPORTANT:

    Password validation is intentionally done
    by the ESP32.

    The backend does NOT need to know the
    currently saved device password.
  */

  const settingsPayload = {
    password,
    aromaUI,
    ledTotal,
    r1Start,
    r1End,
    r2Start,
    r2End
  };

  if (newPassword.trim()) {
    settingsPayload.newPassword =
      newPassword.trim();
  }

  const topic =
    `${MQTT_PREFIX}/${deviceId}/cmd/settings`;

  mqttClient.publish(
    topic,
    JSON.stringify(settingsPayload),
    {
      qos: 0,
      retain: false
    },
    (err) => {
      if (err) {
        return res.status(500).json({
          error: err.message
        });
      }

      res.json({
        ok: true,
        topic
      });
    }
  );
});

server.listen(PORT, () => {
  console.log(`Backend running on port ${PORT}`);
});
