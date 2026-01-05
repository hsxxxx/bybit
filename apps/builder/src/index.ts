import { KafkaIO, type RawCandleMessage } from './kafka.js';
import type { Candle, Timeframe } from './candle.js';
import { bucketStart, makeEmptyAgg, updateAgg } from './candle.js';

type MarketState = {
  last1mBucket?: number;               // 마지막으로 확정/진행 중인 1m bucket start
  last1mCandle?: Candle;               // 현재 진행중인 1m candle(업비트 ws가 업데이트 보내는 값)
  agg: Partial<Record<Exclude<Timeframe,'1m'>, Candle>>; // 5m/15m/1h/4h 진행중 집계
};

function to1mCandle(raw: RawCandleMessage): Candle | null {
  const p = raw.payload;
  const market = raw.market;

  // Upbit candle ws payload fields:
  // code, candle_date_time_utc, candle_date_time_kst,
  // opening_price, high_price, low_price, trade_price, candle_acc_trade_volume ...
  const kst = p?.candle_date_time_kst;
  const open = p?.opening_price;
  const high = p?.high_price;
  const low = p?.low_price;
  const close = p?.trade_price;
  const vol = p?.candle_acc_trade_volume;

  if (!market || typeof kst !== 'string') return null;
  if (![open, high, low, close, vol].every((x: any) => typeof x === 'number')) return null;

  // kst string example: "2026-01-06T01:23:00"
  const ts = Date.parse(kst + '+09:00'); // ensure KST parse
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
    is_closed: false,      // builder가 bucket rollover 시점에 true로 확정
    source: 'ws_candle1m'
  };
}

function closeCandle(c: Candle): Candle {
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

      const key = c1m.market;
      const st = this.state.get(key) ?? { agg: {} };
      this.state.set(key, st);

      // 1) 1m bucket rollover 감지
      const bucket = c1m.open_time;
      const prevBucket = st.last1mBucket;

      if (prevBucket === undefined) {
        st.last1mBucket = bucket;
        st.last1mCandle = c1m;
        // 1m 진행중 값도 emit 하고 싶으면 여기서 (하지만 우리는 "닫힌 봉" 위주로 downstream)
        return;
      }

      if (bucket === prevBucket) {
        // 같은 1m bucket 업데이트(업비트 ws가 1초 단위로 업데이트)
        st.last1mCandle = c1m;
        return;
      }

      // bucket이 바뀜 => 이전 1m candle 확정(close)
      const prev1m = st.last1mCandle;
      if (prev1m) {
        const closed1m = closeCandle(prev1m);

        // (a) 1m 확정 캔들 emit
        await this.io.emitCandle(closed1m);

        // (b) 상위 TF 집계에 반영
        await this.updateHigherAgg(st, closed1m);
      }

      // 새로운 bucket으로 갱신
      st.last1mBucket = bucket;
      st.last1mCandle = c1m;
    });
  }

  async stop() {
    await this.io.disconnect();
  }

  private async updateHigherAgg(st: MarketState, c1mClosed: Candle) {
    const market = c1mClosed.market;

    const tfs: Exclude<Timeframe,'1m'>[] = ['5m','15m','1h','4h'];

    for (const tf of tfs) {
      const b = bucketStart(c1mClosed.open_time, tf);
      const current = st.agg[tf];

      if (!current || current.open_time !== b) {
        // 기존 agg가 있었으면 close 시켜서 emit
        if (current) {
          await this.io.emitCandle(closeCandle(current));
        }
        // 새 agg 시작
        st.agg[tf] = makeEmptyAgg(market, tf, b, {
          open: c1mClosed.open,
          high: c1mClosed.high,
          low: c1mClosed.low,
          close: c1mClosed.close,
          volume: c1mClosed.volume
        });
      } else {
        // 동일 bucket 업데이트
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
