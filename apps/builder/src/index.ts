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
};

function closeCandle<T extends Candle>(c: T): T {
  return { ...c, is_closed: true };
}

function emptyClosedWindow(): Record<Timeframe, Candle[]> {
  return { "1m": [], "5m": [], "15m": [], "1h": [], "4h": [] };
}

export class BuilderApp {
  private io = new KafkaIO();
  private state = new Map<string, MarketState>();

  async start() {
    await this.io.connect();
    console.log("[builder] connected");

    // Bybit: collector가 confirm=true(확정봉)만 Kafka로 내보낸다는 전제.
    // => builder는 들어온 메시지를 "닫힌 1m"으로 보고 바로 처리.
    await this.io.run(async (c1mClosed) => {
      if (c1mClosed.tf !== "1m") return;
      if (!c1mClosed.is_closed) return;

      // 혹시 open_time이 흔들리면 강제 정규화
      const openTime = bucketStart(c1mClosed.open_time, "1m");
      const closed1m: Candle = {
        ...c1mClosed,
        open_time: openTime,
        close_time: openTime + 60_000,
        is_closed: true,
      };

      const market = closed1m.market;
      const st =
        this.state.get(market) ??
        ({
          agg: {},
          closedWindow: emptyClosedWindow(),
        } satisfies MarketState);

      this.state.set(market, st);

      // 1) 1m 캔들 emit
      await this.io.emitCandle(closed1m);

      // 2) 1m indicator emit
      await this.onClosedCandle(st, closed1m);

      // 3) 상위 TF 집계 업데이트 (닫힐 때마다 해당 TF candle+indicator emit)
      await this.updateHigherAgg(st, closed1m);
    });
  }

  async stop() {
    await this.io.disconnect();
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
        // 이전 agg 닫힘 => emit + indicator
        if (current) {
          const closedTf = closeCandle(current);
          await this.io.emitCandle(closedTf);
          await this.onClosedCandle(st, closedTf);
        }

        // 새 버킷 시작 (exchange는 입력 캔들을 그대로 따름)
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
