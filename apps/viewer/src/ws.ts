import http from "http";
import { WebSocketServer } from "ws";
import type { RingStore } from "./store/ring.js";
import type { StreamKind } from "./store/types.js";
import { log } from "./lib/logger.js";

type SubKey = string; // kind|market|tf

function subKey(kind: StreamKind, market: string, tf: string) {
  return `${kind}|${market}|${tf}`;
}

type ClientState = {
  subs: Set<SubKey>;
};

export type WsHub = {
  broadcast(kind: StreamKind, payload: any): void;
};

export function startWsServer(params: {
  port: number;
  store: RingStore;
}): WsHub {
  const server = http.createServer();
  const wss = new WebSocketServer({ server, path: "/ws" });

  wss.on("connection", (ws) => {
    const st: ClientState = { subs: new Set() };
    log.info(`[ws] connected`);

    ws.on("message", (buf) => {
      const s = buf.toString("utf8");
      try {
        const msg = JSON.parse(s);

        // {type:"subscribe", kind:"candle"|"indicator", market:"KRW-BTC", tf:"1m", snapshot:500}
        if (msg?.type === "subscribe") {
          const kind = msg.kind as StreamKind;
          const market = String(msg.market ?? "");
          const tf = String(msg.tf ?? "");
          const snapshot = Number(msg.snapshot ?? 0);

          if ((kind !== "candle" && kind !== "indicator") || !market || !tf) return;

          st.subs.add(subKey(kind, market, tf));

          if (snapshot > 0) {
            if (kind === "candle") {
              const candles = params.store.getCandles(market, tf as any, snapshot);
              ws.send(JSON.stringify({ type: "snapshot", kind, market, tf, items: candles }));
            } else {
              const items = params.store.getIndicators(market, tf as any, snapshot);
              ws.send(JSON.stringify({ type: "snapshot", kind, market, tf, items }));
            }
          }
          return;
        }

        // {type:"unsubscribe", kind, market, tf}
        if (msg?.type === "unsubscribe") {
          const kind = msg.kind as StreamKind;
          const market = String(msg.market ?? "");
          const tf = String(msg.tf ?? "");
          if (!market || !tf) return;
          st.subs.delete(subKey(kind, market, tf));
          return;
        }
      } catch {
        // ignore
      }
    });

    ws.on("close", () => {
      log.info(`[ws] closed`);
    });
  });

  server.listen(params.port, () => {
    log.info(`[ws] listening on :${params.port} path=/ws`);
  });

  const hub: WsHub = {
    broadcast(kind, payload) {
      const market = String(payload.market ?? "");
      const tf = String(payload.tf ?? "");
      if (!market || !tf) return;

      const k = subKey(kind, market, tf);
      const out = JSON.stringify({ type: "update", kind, market, tf, item: payload });

      wss.clients.forEach((c: any) => {
        if (c.readyState !== 1) return;
        const st = (c as any)._st as ClientState | undefined;
        // 상태를 ws 객체에 안 붙였으니, 여기서 그냥 전체 브로드캐스트(단순)로 가도 되지만
        // 구독 필터를 하려면 아래처럼 attach가 필요.
      });

      // 구독 필터를 위해 connection 때 attach
      wss.clients.forEach((client: any) => {
        if (client.readyState !== 1) return;
        const st: ClientState | undefined = client.__subs;
        if (!st) return;
        if (!st.subs.has(k)) return;
        client.send(out);
      });
    }
  };

  // attach hook
  wss.on("connection", (ws: any) => {
    ws.__subs = { subs: new Set<SubKey>() } as ClientState;
    ws.on("message", (buf: any) => {
      const s = buf.toString("utf8");
      try {
        const msg = JSON.parse(s);
        if (msg?.type === "subscribe") {
          const kind = msg.kind as StreamKind;
          const market = String(msg.market ?? "");
          const tf = String(msg.tf ?? "");
          if ((kind !== "candle" && kind !== "indicator") || !market || !tf) return;
          ws.__subs.subs.add(subKey(kind, market, tf));
        }
        if (msg?.type === "unsubscribe") {
          const kind = msg.kind as StreamKind;
          const market = String(msg.market ?? "");
          const tf = String(msg.tf ?? "");
          if (!market || !tf) return;
          ws.__subs.subs.delete(subKey(kind, market, tf));
        }
      } catch {}
    });
  });

  return hub;
}
