// apps/builder/src/index.ts
import { KafkaIO, type RawCandleMessage } from './kafka.js';
import type { Candle, Timeframe } from './candle.js';
import { bucketStart, makeEmptyAgg, updateAgg } from './candle.js';
import { buildFeature1m } from './feature.js';
import { fetchKrwMarkets, fetchRecent1mClosedCandles, mapLimit } from './upbit_rest.js';

const UPBIT_REST_URL = process.env.UPBIT_REST_URL ?? 'https://api.upbit.com/v1';
const WARMUP_1M_COUNT = Number(process.env.WARMUP_1M_COUNT ?? '600');
const WARMUP_CONCURRENCY = Number(process.env.WARMUP_CONCURRENCY ?? '3');

type MarketState = {
  last1mBucket?: number;
  last1mCandle?: Candle;
  agg: Partial<Record<Exclude<Timeframe, '1m'>, Candle>>;
  closed1mWindow: Candle[];
};

function to1mCandle(raw: RawCandleMessage): Candle | null {
  const p = raw.payload;
  const market = raw.market;

  const kst = p?.candle_date_time_kst;
  const open = p?.opening_price;
  const high = p?.high_price;
  const low = p?.low_price;
  const close = p?.trade_price;
  const vol = p?.candle_acc_trade_volume;

  if (!market || typeof kst !== 'string') return null;
  if (![open, high, low, close, vol].every((x: any) => typeof x === 'number')) return null;

  const ts = Date.parse(kst + '+09:00');
  const openTime = bucketStart(ts, '1m');
  const closeTime = openTime + 60_000;

  return {
    exchange: 'upbit',
    market,
    tf: '1m',
    open_time: openTime,
    close_time: closeTime,
    open,
    high,
    low,
    close,
    volume: vol,
    is_closed: false,
    source: 'ws_candle1m'
  };
}

function closeCandle<T extends Candle>(c: T): T {
  return { ...c, is_closed: true };
}

export class BuilderApp {
  private io = new KafkaIO();
  private state = new Map<string, MarketState>();

  async start() {
    await this.io.connect();
    console.log('[builder] connected');

    // 1) WARMUP before consuming Kafka
    await this.warmup();

    // 2) Consume raw 1m candle stream
    await this.io.run(async (raw) => {
      const c1m = to1mCandle(raw);
      if (!c1m) return;

      const market = c1m.market;
      const st = this.state.get(market) ?? { agg: {}, closed1mWindow: [] };
      this.state.set(market, st);

      const bucket = c1m.open_time;
      const prevBucket = st.last1mBucket;

      if (prevBucket === undefined) {
        st.last1mBucket = bucket;
        st.last1mCandle = c1m;
        return;
      }

      if (bucket === prevBucket) {
        st.last1mCandle = c1m;
        return;
      }

      // bucket rollover => close previous 1m
      const prev1m = st.last1mCandle;
      if (prev1m) {
        const closed1m = closeCandle(prev1m);

        // emit closed 1m
        await this.io.emitCandle(closed1m);

        // update rolling window (keep last WARMUP_1M_COUNT candles)
        st.closed1mWindow.push(closed1m);
        if (st.closed1mWindow.length > WARMUP_1M_COUNT) st.closed1mWindow.shift();

        // build + emit feature (now it should have enough history immediately after warmup)
        const feat = buildFeature1m(market, closed1m, st.closed1mWindow);
        await this.io.emitFeature1m(feat);

        // update higher TF aggs
        await this.updateHigherAgg(st, closed1m);
      }

      st.last1mBucket = bucket;
      st.last1mCandle = c1m;
    });
  }

  async stop() {
    await this.io.disconnect();
  }

  private async warmup() {
    console.log(`[builder] warmup start: count=${WARMUP_1M_COUNT} concurrency=${WARMUP_CONCURRENCY}`);

    const markets = await fetchKrwMarkets(UPBIT_REST_URL);
    console.log(`[builder] warmup markets: ${markets.length}`);

    await mapLimit(markets, WARMUP_CONCURRENCY, async (market) => {
      try {
        const candles = await fetchRecent1mClosedCandles({
          restUrl: UPBIT_REST_URL,
          market,
          count: WARMUP_1M_COUNT
        });

        const st: MarketState = { agg: {}, closed1mWindow: candles };
        // Prime aggregation states from history WITHOUT emitting
        this.primeAggFromHistory(st, candles);

        // Set last1mBucket to last closed candle bucket; next WS rollover will close correctly
        const last = candles[candles.length - 1];
        if (last) {
          st.last1mBucket = last.open_time;
          st.last1mCandle = last; // closed candle; will be replaced by live in-progress updates
        }

        this.state.set(market, st);

        if (candles.length < 60) {
          console.warn(`[builder] warmup 부족 market=${market} candles=${candles.length}`);
        }
      } catch (e) {
        console.error(`[builder] warmup failed market=${market}`, e);
        // still create empty state so live stream can fill
        this.state.set(market, { agg: {}, closed1mWindow: [] });
      }
    });

    console.log('[builder] warmup done');
  }

  private primeAggFromHistory(st: MarketState, candles: Candle[]) {
    // Replay closed 1m candles to build the latest in-progress agg buckets (no emit)
    const tfs: Exclude<Timeframe, '1m'>[] = ['5m', '15m', '1h', '4h'];

    for (const c of candles) {
      for (const tf of tfs) {
        const b = bucketStart(c.open_time, tf);
        const current = st.agg[tf];

        if (!current || current.open_time !== b) {
          // start new bucket (do NOT emit old one during warmup)
          st.agg[tf] = makeEmptyAgg(c.market, tf, b, {
            open: c.open,
            high: c.high,
            low: c.low,
            close: c.close,
            volume: c.volume
          });
        } else {
          st.agg[tf] = updateAgg(current, c);
        }
      }
    }
  }

  private async updateHigherAgg(st: MarketState, c1mClosed: Candle) {
    const market = c1mClosed.market;
    const tfs: Exclude<Timeframe, '1m'>[] = ['5m', '15m', '1h', '4h'];

    for (const tf of tfs) {
      const b = bucketStart(c1mClosed.open_time, tf);
      const current = st.agg[tf];

      if (!current || current.open_time !== b) {
        // close + emit previous aggregation if it exists
        if (current) {
          await this.io.emitCandle(closeCandle(current));
        }

        // start new aggregation bucket
        st.agg[tf] = makeEmptyAgg(market, tf, b, {
          open: c1mClosed.open,
          high: c1mClosed.high,
          low: c1mClosed.low,
          close: c1mClosed.close,
          volume: c1mClosed.volume
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

  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
}

main().catch((e) => {
  console.error('[builder] fatal', e);
  process.exit(1);
});
