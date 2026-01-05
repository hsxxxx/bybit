import { KafkaIO, type RawCandleMessage } from './kafka.js';
import type { Candle, Timeframe } from './candle.js';
import { bucketStart, makeEmptyAgg, updateAgg } from './candle.js';
import { buildFeature1m } from './feature.js';

type MarketState = {
  last1mBucket?: number; // last seen 1m bucket start (ms)
  last1mCandle?: Candle; // latest 1m candle update for current bucket (not closed yet)
  agg: Partial<Record<Exclude<Timeframe, '1m'>, Candle>>; // in-progress aggregations
  closed1mWindow: Candle[]; // rolling window of CLOSED 1m candles (for indicators/features)
};

function to1mCandle(raw: RawCandleMessage): Candle | null {
  const p = raw.payload;
  const market = raw.market;

  // Upbit candle WS payload fields:
  // code, candle_date_time_kst, opening_price, high_price, low_price, trade_price, candle_acc_trade_volume ...
  const kst = p?.candle_date_time_kst;
  const open = p?.opening_price;
  const high = p?.high_price;
  const low = p?.low_price;
  const close = p?.trade_price;
  const vol = p?.candle_acc_trade_volume;

  if (!market || typeof kst !== 'string') return null;
  if (![open, high, low, close, vol].every((x: any) => typeof x === 'number')) return null;

  // Example: "2026-01-06T01:23:00" (KST)
  // Add timezone suffix to avoid local machine tz ambiguity.
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
    is_closed: false, // will be closed when bucket rolls over
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

    await this.io.run(async (raw) => {
      const c1m = to1mCandle(raw);
      if (!c1m) return;

      const market = c1m.market;
      const st = this.state.get(market) ?? { agg: {}, closed1mWindow: [] };
      this.state.set(market, st);

      // 1) Detect 1m bucket rollover
      const bucket = c1m.open_time;
      const prevBucket = st.last1mBucket;

      if (prevBucket === undefined) {
        // first candle for this market
        st.last1mBucket = bucket;
        st.last1mCandle = c1m;
        return;
      }

      if (bucket === prevBucket) {
        // same 1m bucket: just update latest in-progress candle
        st.last1mCandle = c1m;
        return;
      }

      // 2) Bucket changed => finalize previous 1m candle
      const prev1m = st.last1mCandle;
      if (prev1m) {
        const closed1m = closeCandle(prev1m);

        // (a) emit closed 1m candle
        await this.io.emitCandle(closed1m);

        // (a-2) update rolling window (keep last 600 closed candles ~ 10 hours)
        st.closed1mWindow.push(closed1m);
        if (st.closed1mWindow.length > 600) st.closed1mWindow.shift();

        // (a-3) build + emit 1m features
        const feat = buildFeature1m(market, closed1m, st.closed1mWindow);
        await this.io.emitFeature1m(feat);

        // (b) update and emit higher timeframe candles as buckets roll
        await this.updateHigherAgg(st, closed1m);
      }

      // 3) Start new bucket tracking
      st.last1mBucket = bucket;
      st.last1mCandle = c1m;
    });
  }

  async stop() {
    await this.io.disconnect();
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

        // start new aggregation bucket with first 1m candle
        st.agg[tf] = makeEmptyAgg(market, tf, b, {
          open: c1mClosed.open,
          high: c1mClosed.high,
          low: c1mClosed.low,
          close: c1mClosed.close,
          volume: c1mClosed.volume
        });
      } else {
        // update existing aggregation bucket
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
