import WebSocket from "ws";
import { config } from "./config";
import type { KafkaOut } from "./kafka";

function sleep(ms: number) {
  return new Promise((res) => setTimeout(res, ms));
}

function decodeWsData(data: WebSocket.RawData): string {
  if (typeof data === "string") return data;
  if (data instanceof Buffer) return data.toString("utf8");
  if (Array.isArray(data)) return Buffer.concat(data).toString("utf8");
  return Buffer.from(data as ArrayBuffer).toString("utf8");
}

type CollectorState = {
  ws?: WebSocket;
  stopped: boolean;
};

export class BybitCollector {
  private state: CollectorState = { stopped: false };
  private pingTimer?: NodeJS.Timeout;

  constructor(private kafka: KafkaOut) { }

  async start() {
    this.state.stopped = false;
    console.log(`[collector] bybit symbols=${config.bybit.symbols.join(",")} interval=${config.bybit.interval}`);
    void this.connectLoop();
  }

  async stop() {
    this.state.stopped = true;

    if (this.pingTimer) clearInterval(this.pingTimer);

    const ws = this.state.ws;
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.close(1000, "shutdown");
    }
  }

  private async connectLoop() {
    let attempt = 0;

    while (!this.state.stopped) {
      try {
        attempt += 1;
        await this.openWsOnce();
        attempt = 0;
      } catch (err) {
        const backoff = Math.min(30_000, 1_000 * Math.pow(2, Math.min(attempt, 5)));
        console.error(`[collector] ws error, retry in ${backoff}ms`, err);
        await sleep(backoff);
      }
    }
  }

  private async openWsOnce(): Promise<void> {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(config.bybit.wsUrl, { handshakeTimeout: 10_000 });
      this.state.ws = ws;

      let opened = false;

      ws.on("open", () => {
        opened = true;
        console.log("[collector] ws connected");

        // subscribe: kline.{interval}.{symbol}
        const args = config.bybit.symbols.map((s) => `kline.${config.bybit.interval}.${s}`);
        const subMsg = { op: "subscribe", args };
        ws.send(JSON.stringify(subMsg));
        console.log(`[collector] subscribed ${args.length} topics`);

        // ping loop (Bybit v5: op:"ping")
        this.pingTimer = setInterval(() => {
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ op: "ping" }));
          }
        }, Math.max(5, config.bybit.pingSec) * 1000);
      });

      ws.on("message", async (data) => {
        const recvTs = Date.now();
        try {
          const text = decodeWsData(data);
          const payload = JSON.parse(text) as any;

          const topic: string | undefined = payload?.topic;
          const arr = payload?.data;
          if (!topic || !Array.isArray(arr) || arr.length === 0) return;

          const k = arr[0];
          // ✅ 1분봉 마감(확정)만 통과
          if (k?.confirm !== true) return;

          const parts = topic.split(".");
          const symbol = parts[2] || k?.symbol;
          if (!symbol || typeof symbol !== "string") return;

          await this.kafka.sendRaw1m({
            exchange: "bybit",
            type: "kline.1m",
            symbol,
            recv_ts: recvTs,
            payload,
          });
        } catch (e) {
          console.error("[collector] message handling error", e);
        }
      });

      ws.on("close", (code, reason) => {
        console.warn(`[collector] ws closed code=${code} reason=${reason.toString()}`);
        if (this.pingTimer) clearInterval(this.pingTimer);
        if (!opened) reject(new Error("ws closed before open"));
        else resolve();
      });

      ws.on("error", (err) => {
        console.error("[collector] ws error event", err);
        if (!opened) reject(err);
      });
    });
  }
}
