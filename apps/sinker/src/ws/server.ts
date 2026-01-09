// sinker/src/ws/server.ts
import http from "node:http";
import { WebSocketServer } from "ws";
import type { BuiltPayload } from "../types";

type SubKey = string; // market::tf

function subKey(market: string, tf: string) {
  return `${market}::${tf}`;
}

export function startWsServer(port: number) {
  const server = http.createServer();
  const wss = new WebSocketServer({ server });

  const subs = new Map<any, Set<SubKey>>();

  wss.on("connection", (ws) => {
    subs.set(ws, new Set());

    ws.on("message", (buf) => {
      try {
        const msg = JSON.parse(buf.toString("utf8")) as { op: string; market?: string; tf?: string };
        const set = subs.get(ws);
        if (!set) return;

        if (msg.op === "sub" && msg.market && msg.tf) {
          set.add(subKey(msg.market, msg.tf));
          ws.send(JSON.stringify({ op: "sub_ok", market: msg.market, tf: msg.tf }));
        } else if (msg.op === "unsub" && msg.market && msg.tf) {
          set.delete(subKey(msg.market, msg.tf));
          ws.send(JSON.stringify({ op: "unsub_ok", market: msg.market, tf: msg.tf }));
        } else if (msg.op === "ping") {
          ws.send(JSON.stringify({ op: "pong" }));
        }
      } catch {
        // ignore
      }
    });

    ws.on("close", () => {
      subs.delete(ws);
    });
  });

  server.listen(port);

  function broadcast(payload: BuiltPayload) {
    const k = subKey(payload.market, payload.tf);
    const msg = JSON.stringify({ op: "candle", ...payload });

    for (const client of wss.clients) {
      const set = subs.get(client);
      if (!set || !set.has(k)) continue;
      if (client.readyState === 1) client.send(msg);
    }
  }

  return { server, wss, broadcast };
}
