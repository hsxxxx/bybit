// apps/builder/src/index.ts
import { KafkaIO } from "./kafka.js";
import type { Candle, Timeframe } from "./candle.js";
import { bucketStart, makeEmptyAgg, updateAgg } from "./candle.js";
import { buildIndicatorFromWindow } from "./indicator.js";

const MAX_WINDOW: Record<Timeframe, number> = {
  "1m": 900,
  "5m": 900,
  "15m": 900,
  "1h": 900,
  "4h": 900,
};

type MarketState = {
  agg: Partial<Record<Exclude<Timeframe, "1m">, Candle>>;
  closedWindow: Record<Timeframe, Candle[]>;
  lastClosed1mOpenTime?: number; // 중복 방지
};

function emptyClosedWindow(): Record<Timeframe, Candle[]> {
  return { "1m": [], "5m": [], "15m": [], "1h": [], "4h": [] };
}

function closeCandle<T extends Candle>(c: T): T {
  return { ...c, is_closed: true };
}

/**
 * collector가 publish 하는 bybit.kline1m.raw 메시지(원본 래핑)에서
 * confirm=true 1m 확정봉을 Candle로 변환한다.
 *
 * collector가 confirm=true만 보내도록 이미 필터링 중이라면
 * payload.data[0]만 봐도 충분하지만, 방어적으로 확인한다.
 */
function rawToClosed1mCandle(raw: any): Candle | null {
  if (!raw || raw.exchange !== "bybit") return null;
  if (raw.type !== "kline.1m") return null;

  const symbol = raw.symbol;
  const payload = raw.payload;
  const arr = payload?.data;

  if (!symbol || typeof symbol !== "string") return null;
  if (!Array.isArray(arr) || arr.length === 0) return null;

  const k = arr[0];
  if (k?.confirm !== true) return null;

  const start = Number(k.start);
  if (!Number.isFinite(start)) return null;

  const openTime = bucketStart(start, "1m");

  return {
    exchange: "bybit",
    market: symbol,
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

  // 로그 스로틀용
  private lastStatLogTs = 0;
  private recvCount = 0;
  private c1mCount = 0;
  private ind1mCount = 0;
  private aggEmitCount: Record<Exclude<Timeframe, "1m">, number> = {
    "5m": 0,
    "15m": 0,
    "1h": 0,
    "4h": 0,
  };

  async start() {
    await this.io.connect();
    console.log("[builder] connected");

    await this.io.run(async (raw) => {
      this.recvCount++;

      // 1) raw -> closed 1m candle 변환
      const c1m = rawToClosed1mCandle(raw);
      if (!c1m) {
        // 원하면 디버깅 시 아래 주석 해제 (너무 많이 찍힐 수 있음)
        // console.log("[builder] skip raw", raw?.exchange, raw?.type, raw?.symbol);
        this.maybeLogStats();
        return;
      }

      // 2) state get/create
      const market = c1m.market;
      const st =
        this.state.get(market) ??
        ({
          agg: {},
          closedWindow: emptyClosedWindow(),
        } satisfies MarketState);
      if (!this.state.has(market)) this.state.set(market, st);

      // 3) 중복 방지 (같은 open_time 확정봉 재수신 시 스킵)
      if (st.lastClosed1mOpenTime === c1m.open_time) {
        // console.log("[builder] dup 1m closed skip", market, c1m.open_time);
        this.maybeLogStats();
        return;
      }
      st.lastClosed1mOpenTime = c1m.open_time;

      this.c1mCount++;

      // 4) 1m candle emit
      await this.io.emitCandle(c1m);

      // 5) 1m indicator emit
      await this.onClosedCandle(st, c1m);
      this.ind1mCount++;

      // 6) higher TF aggregation update (+ close 시 emit)
      await this.updateHigherAgg(st, c1m);

      // 7) 로그(주기)
      this.maybeLogStats(market, c1m.open_time);
    });
  }

  async stop() {
    await this.io.disconnect();
  }

  private maybeLogStats(lastMarket?: string, lastOpenTime?: number) {
    const now = Date.now();
    // 10초마다 한 번만 요약 로그
    if (now - this.lastStatLogTs < 10_000) return;
    this.lastStatLogTs = now;

    console.log(
      `[builder] stats recv=${this.recvCount} c1m=${this.c1mCount} ind1m=${this.ind1mCount} ` +
        `aggEmit(5m=${this.aggEmitCount["5m"]},15m=${this.aggEmitCount["15m"]},1h=${this.aggEmitCount["1h"]},4h=${this.aggEmitCount["4h"]}) ` +
        `markets=${this.state.size}` +
        (lastMarket ? ` last=${lastMarket}@${lastOpenTime}` : "")
    );
  }

  private async onClosedCandle(st: MarketState, c: Candle) {
    const w = st.closedWindow[c.tf];
    w.push(c);
    if (w.length > MAX_WINDOW[c.tf]) w.shift();

    const ind = buildIndicatorFromWindow(c.tf, w);
    await this.io.emitIndicator(ind);
  }

  private async updateHigherAgg(st: MarketState, c1mClosed: Candle) {
    const market = c1mClosed.market;
    const tfs: Exclude<Timeframe, "1m">[] = ["5m", "15m", "1h", "4h"];

    for (const tf of tfs) {
      const b = bucketStart(c1mClosed.open_time, tf);
      const current = st.agg[tf];

      if (!current || current.open_time !== b) {
        // 이전 agg가 있으면 close emit (+ indicator)
        if (current) {
          const closedTf = closeCandle(current);
          await this.io.emitCandle(closedTf);
          await this.onClosedCandle(st, closedTf);
          this.aggEmitCount[tf]++;

          // 디버깅 로그(원하면)
          // console.log("[builder] emit", tf, closedTf.market, closedTf.open_time);
        }

        // 새 agg 시작
        st.agg[tf] = makeEmptyAgg(c1mClosed.exchange, market, tf, b, {
          open: c1mClosed.open,
          high: c1mClosed.high,
          low: c1mClosed.low,
          close: c1mClosed.close,
          volume: c1mClosed.volume,
        });
      } else {
        st.agg[tf] = updateAgg(current, c1mClosed);
      }
    }
  }
}

// bootstrap
async function main() {
  const app = new BuilderApp();
  await app.start();

  const shutdown = async (sig: string) => {
    console.log(`[builder] shutting down (${sig})...`);
    await app.stop();
    process.exit(0);
  };

  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
}

main().catch((e) => {
  console.error("[builder] fatal", e);
  process.exit(1);
});
