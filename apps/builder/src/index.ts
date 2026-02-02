import { KafkaIO } from "./kafka.js";
import type { Candle, Timeframe } from "./candle.js";
import { bucketStart, makeEmptyAgg, updateAgg } from "./candle.js";
import { buildIndicatorFromWindow } from "./indicator.js";

/* ---------- logging ---------- */
type LogLevel = "debug" | "info" | "warn" | "error";
const LOG_LEVEL = (process.env.LOG_LEVEL ?? "info") as LogLevel;
const rank: Record<LogLevel, number> = { debug: 10, info: 20, warn: 30, error: 40 };

function log(lv: LogLevel, msg: string, extra?: any) {
  if (rank[lv] < rank[LOG_LEVEL]) return;
  const ts = new Date().toISOString();
  extra ? console.log(`${ts} [builder][${lv}] ${msg}`, extra)
        : console.log(`${ts} [builder][${lv}] ${msg}`);
}
/* ----------------------------- */

const MAX_WINDOW: Record<Timeframe, number> = {
  "1m": 900, "5m": 900, "15m": 900, "1h": 900, "4h": 900,
};

type MarketState = {
  agg: Partial<Record<Exclude<Timeframe, "1m">, Candle>>;
  closedWindow: Record<Timeframe, Candle[]>;
  lastClosed1mOpenTime?: number;
};

function emptyClosedWindow(): Record<Timeframe, Candle[]> {
  return { "1m": [], "5m": [], "15m": [], "1h": [], "4h": [] };
}

function closeCandle<T extends Candle>(c: T): T {
  return { ...c, is_closed: true };
}

function rawToClosed1mCandle(raw: any): Candle | null {
  if (!raw || raw.exchange !== "bybit" || raw.type !== "kline.1m") return null;

  const k = raw.payload?.data?.[0];
  if (!k || k.confirm !== true) return null;

  const openTime = bucketStart(Number(k.start), "1m");

  return {
    exchange: "bybit",
    market: raw.symbol,
    tf: "1m",
    open_time: openTime,
    close_time: openTime + 60_000,
    open: Number(k.open),
    high: Number(k.high),
    low: Number(k.low),
    close: Number(k.close),
    volume: Number(k.volume),
    is_closed: true,
    source: "collector_kline",
  };
}

export class BuilderApp {
  private io = new KafkaIO();
  private state = new Map<string, MarketState>();

  private lastStatTs = 0;
  private recv = 0;
  private c1m = 0;
  private ind1m = 0;

  async start() {
    await this.io.connect();
    log("info", "builder started");

    await this.io.run(async (raw) => {
      this.recv++;

      const c1m = rawToClosed1mCandle(raw);
      if (!c1m) return;

      const market = c1m.market;
      const st =
        this.state.get(market) ??
        ({ agg: {}, closedWindow: emptyClosedWindow() } satisfies MarketState);

      if (!this.state.has(market)) {
        this.state.set(market, st);
        log("info", `new market ${market}`);
      }

      if (st.lastClosed1mOpenTime === c1m.open_time) return;
      st.lastClosed1mOpenTime = c1m.open_time;

      this.c1m++;

      log("debug", `1m close ${market} ${c1m.open_time}`);

      await this.io.emitCandle(c1m);
      await this.onClosedCandle(st, c1m);
      this.ind1m++;

      await this.updateHigherAgg(st, c1m);
      this.maybeLogStats(market, c1m.open_time);
    });
  }

  private maybeLogStats(market?: string, openTime?: number) {
    const now = Date.now();
    if (now - this.lastStatTs < 10_000) return;
    this.lastStatTs = now;

    log(
      "info",
      `stat recv=${this.recv} c1m=${this.c1m} ind1m=${this.ind1m} markets=${this.state.size}` +
        (market ? ` last=${market}@${openTime}` : "")
    );

    this.recv = this.c1m = this.ind1m = 0;
  }

  private async onClosedCandle(st: MarketState, c: Candle) {
    const w = st.closedWindow[c.tf];
    w.push(c);
    if (w.length > MAX_WINDOW[c.tf]) w.shift();

    const ind = buildIndicatorFromWindow(c.tf, w);
    await this.io.emitIndicator(ind);
  }

  private async updateHigherAgg(st: MarketState, c1m: Candle) {
    const tfs: Exclude<Timeframe, "1m">[] = ["5m", "15m", "1h", "4h"];

    for (const tf of tfs) {
      const b = bucketStart(c1m.open_time, tf);
      const cur = st.agg[tf];

      if (!cur || cur.open_time !== b) {
        if (cur) {
          const closed = closeCandle(cur);
          log("debug", `close ${tf} ${closed.market} ${closed.open_time}`);
          await this.io.emitCandle(closed);
          await this.onClosedCandle(st, closed);
        }

        st.agg[tf] = makeEmptyAgg(
          c1m.exchange,
          c1m.market,
          tf,
          b,
          {
            open: c1m.open,
            high: c1m.high,
            low: c1m.low,
            close: c1m.close,
            volume: c1m.volume,
          }
        );
      } else {
        st.agg[tf] = updateAgg(cur, c1m);
      }
    }
  }
}

/* ---------- bootstrap ---------- */
async function main() {
  const app = new BuilderApp();
  await app.start();

  const shutdown = async (sig: string) => {
    log("warn", `shutdown ${sig}`);
    process.exit(0);
  };

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

main().catch((e) => {
  log("error", "fatal", e);
  process.exit(1);
});
