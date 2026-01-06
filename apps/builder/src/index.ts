// apps/builder/src/index.ts
import { KafkaIO, type RawCandleMessage } from './kafka.js';
import type { Candle, Timeframe } from './candle.js';
import { bucketStart, makeEmptyAgg, updateAgg } from './candle.js';
import { buildIndicatorFromWindow } from './indicator.js';
import { fetchKrwMarkets, fetchRecent1mClosedCandles, mapLimit } from './upbit_rest.js';

const UPBIT_REST_URL = process.env.UPBIT_REST_URL ?? 'https://api.upbit.com/v1';
const WARMUP_1M_COUNT = Number(process.env.WARMUP_1M_COUNT ?? '600');
const WARMUP_CONCURRENCY = Number(process.env.WARMUP_CONCURRENCY ?? '1');

// TF별 윈도우 길이 (MA800까지 고려하면 800 이상 필요)
// 운영 부담 크면 1m/5m/15m만 길게, 1h/4h는 짧게 가져가도 됨.
const MAX_WINDOW: Record<Timeframe, number> = {
  '1m': 900,
  '5m': 900,
  '15m': 900,
  '1h': 900,
  '4h': 900
};

type MarketState = {
  last1mBucket?: number; // 마지막으로 관찰한 1m bucket start
  last1mCandle?: Candle; // 현재 1m bucket의 최신 업데이트(미닫힘)

  // 5m/15m/1h/4h 진행중 집계(현재 버킷)
  agg: Partial<Record<Exclude<Timeframe, '1m'>, Candle>>;

  // 닫힌 캔들 윈도우(팩트 지표 계산용)
  closedWindow: Record<Timeframe, Candle[]>;
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

  return {
    exchange: 'upbit',
    market,
    tf: '1m',
    open_time: openTime,
    close_time: openTime + 60_000,
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

function emptyClosedWindow(): Record<Timeframe, Candle[]> {
  return { '1m': [], '5m': [], '15m': [], '1h': [], '4h': [] };
}

export class BuilderApp {
  private io = new KafkaIO();
  private state = new Map<string, MarketState>();

  async start() {
    await this.io.connect();
    console.log('[builder] connected');

    await this.warmup();

    await this.io.run(async (raw) => {
      const c1m = to1mCandle(raw);
      if (!c1m) return;

      const market = c1m.market;

      const st = this.state.get(market) ?? {
        agg: {},
        closedWindow: emptyClosedWindow()
      };
      this.state.set(market, st);

      const bucket = c1m.open_time;
      const prevBucket = st.last1mBucket;

      if (prevBucket === undefined) {
        st.last1mBucket = bucket;
        st.last1mCandle = c1m;
        return;
      }

      if (bucket === prevBucket) {
        // 같은 분의 업데이트(미닫힘 candle 갱신)
        st.last1mCandle = c1m;
        return;
      }

      // 분이 바뀜 => 이전 1m candle을 닫아서 확정
      const prev1m = st.last1mCandle;
      if (prev1m) {
        const closed1m = closeCandle(prev1m);

        // 1m 캔들 emit
        await this.io.emitCandle(closed1m);

        // 1m indicator emit (팩트)
        await this.onClosedCandle(st, closed1m);

        // 상위 TF 집계 업데이트 (집계가 닫히면 그 TF 캔들도 emit + indicator emit)
        await this.updateHigherAgg(st, closed1m);
      }

      // 새 bucket tracking
      st.last1mBucket = bucket;
      st.last1mCandle = c1m;
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

  private async warmup() {
    console.log(`[builder] warmup start: 1m_count=${WARMUP_1M_COUNT} concurrency=${WARMUP_CONCURRENCY}`);

    const markets = await fetchKrwMarkets(UPBIT_REST_URL);
    console.log(`[builder] warmup markets: ${markets.length}`);

    await mapLimit(markets, WARMUP_CONCURRENCY, async (market) => {
      try {
        const c1m = await fetchRecent1mClosedCandles({
          restUrl: UPBIT_REST_URL,
          market,
          count: WARMUP_1M_COUNT
        });

        const st: MarketState = {
          agg: {},
          closedWindow: emptyClosedWindow(),
          // live 시작 시점의 버킷 기준값만 세팅(워밍업 데이터 emit은 하지 않음)
          last1mBucket: c1m.length ? c1m[c1m.length - 1].open_time : undefined,
          last1mCandle: undefined
        };

        // 1m 윈도우 채우기 (emit 없음)
        st.closedWindow['1m'] = c1m.slice(-MAX_WINDOW['1m']);

        // 워밍업 1m를 리플레이해서 5m/15m/1h/4h 윈도우 + 현재 agg 프라임(emit 없음)
        this.primeHigherFromHistory(st, c1m);

        this.state.set(market, st);

        if (c1m.length < 60) {
          console.warn(`[builder] warmup 부족 market=${market} candles=${c1m.length}`);
        }
      } catch (e) {
        console.error(`[builder] warmup failed market=${market}`, e);
        // live로라도 채우게 빈 상태 생성
        this.state.set(market, {
          agg: {},
          closedWindow: emptyClosedWindow()
        });
      }
    });

    console.log('[builder] warmup done');
  }

  private primeHigherFromHistory(st: MarketState, candles1m: Candle[]) {
    const tfs: Exclude<Timeframe, '1m'>[] = ['5m', '15m', '1h', '4h'];

    // 각 TF에 대해 “이전 버킷이 닫힐 때” closedWindow에 넣어줌(emit 없음)
    const lastClosed: Partial<Record<Exclude<Timeframe, '1m'>, Candle>> = {};

    for (const c of candles1m) {
      for (const tf of tfs) {
        const b = bucketStart(c.open_time, tf);
        const current = st.agg[tf];

        if (!current || current.open_time !== b) {
          // 버킷이 바뀜 => 이전 current는 닫힌 봉으로 간주하고 closedWindow에 push
          if (current) {
            const closedTf = closeCandle(current);
            const w = st.closedWindow[tf];
            w.push(closedTf);
            if (w.length > MAX_WINDOW[tf]) w.shift();
            lastClosed[tf] = closedTf;
          }

          // 새 버킷 시작
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

    // 워밍업 끝났을 때 st.agg[tf]는 “진행중 최신 버킷” 상태
    // closedWindow에는 “완전히 닫힌 TF 봉들”이 누적됨
  }

  private async updateHigherAgg(st: MarketState, c1mClosed: Candle) {
    const market = c1mClosed.market;
    const tfs: Exclude<Timeframe, '1m'>[] = ['5m', '15m', '1h', '4h'];

    for (const tf of tfs) {
      const b = bucketStart(c1mClosed.open_time, tf);
      const current = st.agg[tf];

      if (!current || current.open_time !== b) {
        // 이전 agg 닫힘 => emit + indicator
        if (current) {
          const closedTf = closeCandle(current);

          // TF 캔들 emit
          await this.io.emitCandle(closedTf);

          // TF indicator emit
          await this.onClosedCandle(st, closedTf);
        }

        // 새 버킷 시작
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
