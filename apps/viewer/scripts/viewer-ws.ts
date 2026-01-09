import "dotenv/config";
import http from "node:http";
import { WebSocketServer } from "ws";
import type { WebSocket } from "ws";

import type { Candle, Indicator, Timeframe } from "../src/lib/market/types";
import {
  ensureKafkaStarted,
  subscribeCandleStream,
  subscribeIndicatorStream,
  getSnapshot,
} from "../src/lib/data/kafkaStore";

const HOST = process.env.WS_HOST ?? "0.0.0.0";
const PORT = Number(process.env.PORT_WS ?? "3101");
const PING_INTERVAL_MS = Number(process.env.WS_PING_INTERVAL_MS ?? "25000");

const DEFAULT_MARKET = process.env.DEFAULT_MARKET ?? "KRW-BTC";
const DEFAULT_TF = (process.env.DEFAULT_TF ?? "1m") as Timeframe;

type ClientMsg =
  | { type: "subscribe"; market: string; tf: Timeframe; include?: ("candle" | "indicator")[]; replay?: number }
  | { type: "unsubscribe" }
  | { type: "ping" }
  | { type: string; [k: string]: any };

type ServerMsg =
  | { type: "hello"; tfs: Timeframe[]; defaultMarket: string; defaultTf: Timeframe }
  | { type: "subscribed"; market: string; tf: Timeframe; include: ("candle" | "indicator")[] }
  | { type: "unsubscribed" }
  | { type: "candle"; market: string; tf: Timeframe; data: Candle }
  | { type: "indicator"; market: string; tf: Timeframe; data: Indicator }
  | { type: "pong" }
  | { type: "error"; message: string };

type Sub = {
  market: string;
  tf: Timeframe;
  include: Set<"candle" | "indicator">;
  lastCandleTime?: number;
  lastIndTime?: number;
};

const ALLOWED_TFS: Timeframe[] = ["1m", "5m", "15m", "1h", "4h"];

function safeSend(ws: WebSocket, msg: ServerMsg) {
  if (ws.readyState !== ws.OPEN) return;
  try { ws.send(JSON.stringify(msg)); } catch {}
}

function parseMsg(raw: string): ClientMsg | null {
  try { return JSON.parse(raw); } catch { return null; }
}

function isTimeframe(tf: any): tf is Timeframe {
  return typeof tf === "string" && (ALLOWED_TFS as string[]).includes(tf);
}

function normalizeInclude(v: any): Set<"candle" | "indicator"> {
  const s = new Set<"candle" | "indicator">();
  const arr = Array.isArray(v) ? v : ["candle", "indicator"];
  for (const x of arr) if (x === "candle" || x === "indicator") s.add(x);
  if (!s.size) { s.add("candle"); s.add("indicator"); }
  return s;
}

function getTime(obj: any): number | undefined {
  const t = obj?.time;
  return typeof t === "number" && Number.isFinite(t) ? t : undefined;
}

const clients = new Map<WebSocket, Sub>();

async function main() {
  await ensureKafkaStarted();

  // ✅ HTTP 서버 (snapshot 제공)
  const server = http.createServer((req, res) => {
    try {
      const url = new URL(req.url ?? "/", `http://${req.headers.host}`);

      if (req.method === "GET" && url.pathname === "/snapshot") {
        const market = (url.searchParams.get("market") ?? DEFAULT_MARKET) as string;
        const tf = (url.searchParams.get("tf") ?? DEFAULT_TF) as Timeframe;
        const limit = Math.min(5000, Math.max(1, Number(url.searchParams.get("limit") ?? "1000")));

        if (!isTimeframe(tf)) {
          res.writeHead(400, { "content-type": "application/json" });
          res.end(JSON.stringify({ error: "invalid tf" }));
          return;
        }

        const snap = getSnapshot(market, tf, limit);
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify(snap));
        return;
      }

      res.writeHead(404, { "content-type": "text/plain" });
      res.end("not found");
    } catch {
      res.writeHead(500, { "content-type": "text/plain" });
      res.end("internal error");
    }
  });

  // ✅ WS 서버는 같은 HTTP 서버에 붙여서 /ws 경로로 제공
  const wss = new WebSocketServer({ server, path: "/ws" });

  console.log(`[viewer-ws] http+ws listening on http://${HOST}:${PORT} (ws path: /ws)`);

  const pingTimer = setInterval(() => {
    for (const ws of clients.keys()) {
      if (ws.readyState !== ws.OPEN) continue;
      try { ws.ping(); } catch {}
    }
  }, PING_INTERVAL_MS);

  wss.on("connection", (ws) => {
    const initial: Sub = {
      market: DEFAULT_MARKET,
      tf: DEFAULT_TF,
      include: new Set(["candle", "indicator"]),
    };
    clients.set(ws, initial);

    safeSend(ws, { type: "hello", tfs: ALLOWED_TFS, defaultMarket: DEFAULT_MARKET, defaultTf: DEFAULT_TF });
    safeSend(ws, { type: "subscribed", market: initial.market, tf: initial.tf, include: Array.from(initial.include) });

    ws.on("message", (buf) => {
      const msg = parseMsg(buf.toString("utf8"));
      if (!msg || typeof msg.type !== "string") {
        safeSend(ws, { type: "error", message: "invalid message" });
        return;
      }

      if (msg.type === "ping") { safeSend(ws, { type: "pong" }); return; }

      if (msg.type === "unsubscribe") {
        clients.delete(ws);
        safeSend(ws, { type: "unsubscribed" });
        return;
      }

      if (msg.type === "subscribe") {
        const market = typeof (msg as any).market === "string" ? (msg as any).market : DEFAULT_MARKET;
        const tf = (msg as any).tf;

        if (!isTimeframe(tf)) {
          safeSend(ws, { type: "error", message: `invalid tf: ${String(tf)}` });
          return;
        }

        const include = normalizeInclude((msg as any).include);
        const replay = Number.isFinite((msg as any).replay) ? Number((msg as any).replay) : 0;

        const sub: Sub = { market, tf, include };
        clients.set(ws, sub);

        safeSend(ws, { type: "subscribed", market, tf, include: Array.from(include) });

        if (replay > 0) {
          const { candles, indicators } = getSnapshot(market, tf, replay);

          if (include.has("candle")) {
            for (const c of candles) safeSend(ws, { type: "candle", market, tf, data: c });
            const t = candles.length ? getTime(candles[candles.length - 1]) : undefined;
            if (t !== undefined) sub.lastCandleTime = t;
          }
          if (include.has("indicator")) {
            for (const ind of indicators) safeSend(ws, { type: "indicator", market, tf, data: ind });
            const t = indicators.length ? getTime(indicators[indicators.length - 1]) : undefined;
            if (t !== undefined) sub.lastIndTime = t;
          }
        }
        return;
      }
    });

    ws.on("close", () => clients.delete(ws));
    ws.on("error", () => clients.delete(ws));
  });

  // Kafka -> broadcast
  subscribeCandleStream((tf, candle) => {
    const market = (candle as any).market as string | undefined;
    if (!market) return;
    const t = getTime(candle);

    for (const [ws, sub] of clients) {
      if (ws.readyState !== ws.OPEN) continue;
      if (!sub.include.has("candle")) continue;
      if (sub.market !== market || sub.tf !== tf) continue;

      if (t !== undefined && sub.lastCandleTime !== undefined && t < sub.lastCandleTime) continue;
      if (t !== undefined) sub.lastCandleTime = t;

      safeSend(ws, { type: "candle", market, tf, data: candle });
    }
  });

  subscribeIndicatorStream((tf, ind) => {
    const market = (ind as any).market as string | undefined;
    if (!market) return;
    const t = getTime(ind);

    for (const [ws, sub] of clients) {
      if (ws.readyState !== ws.OPEN) continue;
      if (!sub.include.has("indicator")) continue;
      if (sub.market !== market || sub.tf !== tf) continue;

      if (t !== undefined && sub.lastIndTime !== undefined && t < sub.lastIndTime) continue;
      if (t !== undefined) sub.lastIndTime = t;

      safeSend(ws, { type: "indicator", market, tf, data: ind });
    }
  });

  server.listen(PORT, HOST);

  const shutdown = () => {
    console.log("[viewer-ws] shutting down...");
    clearInterval(pingTimer);
    try { wss.close(); } catch {}
    try { server.close(); } catch {}
    for (const ws of clients.keys()) {
      try { ws.close(); } catch {}
    }
    clients.clear();
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((e) => {
  console.error("[viewer-ws] fatal:", e);
  process.exit(1);
});
