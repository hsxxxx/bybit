// apps/builder/src/index.ts
import { KafkaIO, type RawCandleMessage } from './kafka.js';
import type { Candle, Timeframe } from './candle.js';
import { bucketStart, makeEmptyAgg, updateAgg } from './candle.js';
import { buildIndicatorFromWindow } from './indicator.js';
import { fetchKrwMarkets, fetchRecent1mClosedCandles, mapLimit } from './upbit_rest.js';

type LogLv = 'debug' | 'info' | 'warn' | 'error';
const LOG_LEVEL = (process.env.LOG_LEVEL ?? 'info').toLowerCase() as LogLv;

function lvRank(lv: LogLv) {
  return lv === 'debug' ? 10 : lv === 'info' ? 20 : lv === 'warn' ? 30 : 40;
}
function canLog(lv: LogLv) {
  return lvRank(lv) >= lvRank(LOG_LEVEL);
}
function nowIso() {
  return new Date().toISOString();
}
function log(lv: LogLv, msg: string, extra?: unknown) {
  if (!canLog(lv)) return;
  if (extra !== undefined) {
    // eslint-disable-next-line no-console
    console.log(`${nowIso()} [builder][${lv}] ${msg}`, extra);
  } else {
    // eslint-disable-next-line no-console
    console.log(`${nowIso()} [builder][${lv}] ${msg}`);
  }
}

const UPBIT_REST_URL = process.env.UPBIT_REST_URL ?? 'https://api.upbit.com/v1';
const WARMUP_1M_COUNT = Number(process.env.WARMUP_1M_COUNT ?? '600');
const WARMUP_CONCURRENCY = Number(process.env.WARMUP_CONCURRENCY ?? '1');

// TF별 윈도우 길이 (MA800까지 고려하면 800 이상 필요)
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

  // stats
  private recvRaw = 0;
  private closed1m = 0;
  private emittedCandle: Record<Timeframe, number> = { '1m': 0, '5m': 0, '15m': 0, '1h': 0, '4h': 0 };
  private emittedInd: Record<Timeframe, number> = { '1m': 0, '5m': 0, '15m': 0, '1h': 0, '4h': 0 };
  private statTimer?: NodeJS.Timeout;

  async start() {
    log('info', `start (rest=${UPBIT_REST_URL}) warmup_count=${WARMUP_1M_COUNT} concurrency=${WARMUP_CONCURRENCY}`);
    await this.io.connect();
    log('info', 'kafka connected');

    await this.warmup();

    const statEveryMs = Number(process.env.BUILDER_STAT_EVERY_MS ?? '10000');
    this.statTimer = setInterval(() => {
      const summary = {
        recvRaw: this.recvRaw,
        closed1m: this.closed1m,
        emitCandle: this.emittedCandle,
        emitInd: this.emittedInd,
        markets: this.state.size
      };
      log('info', `stat`, summary);

      // reset interval counters
      this.recvRaw = 0;
      this.closed1m = 0;
      this.emittedCandle = { '1m': 0, '5m': 0, '15m': 0, '1h': 0, '4h': 0 };
      this.emittedInd = { '1m': 0, '5m': 0, '15m': 0, '1h': 0, '4h': 0 };
    }, statEveryMs);

    await this.io.run(async (raw) => {
      this.recvRaw++;

      const c1m = to1mCandle(raw);
      if (!c1m) {
        if (LOG_LEVEL === 'debug') log('debug', 'drop raw (to1mCandle=null)', raw);
        return;
      }

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

        if (LOG_LEVEL === 'debug') {
          log('debug', `init bucket market=${market} bucket=${bucket}`);
        }
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
        this.closed1m++;

        if (LOG_LEVEL === 'debug') {
          log(
            'debug',
            `close 1m market=${market} open=${closed1m.open_time} close=${closed1m.close_time} o=${closed1m.open} h=${closed1m.high} l=${closed1m.low} c=${closed1m.close} v=${closed1m.volume}`
          );
        }

        // 1m 캔들 emit
        await this.io.emitCandle(closed1m);
        this.emittedCandle['1m']++;

        // 1m indicator emit (팩트)
        await this.onClosedCandle(st, closed1m);

        // 상위 TF 집계 업데이트 (집계가 닫히면 그 TF 캔들도 emit + indicator emit)
        await this.updateHigherAgg(st, closed1m);
      } else {
        // 정상이라면 거의 없음. (버킷 변했는데 last1mCandle이 없는 경우)
        log('warn', `bucket changed but last1mCandle missing market=${market} prevBucket=${prevBucket} newBucket=${bucket}`);
      }

      // 새 bucket tracking
      st.last1mBucket = bucket;
      st.last1mCandle = c1m;
    });
  }

  async stop() {
    if (this.statTimer) clearInterval(this.statTimer);
    await this.io.disconnect();
  }

  private async onClosedCandle(st: MarketState, c: Candle) {
    const w = st.closedWindow[c.tf];
    w.push(c);
    if (w.length > MAX_WINDOW[c.tf]) w.shift();

    const ind = buildIndicatorFromWindow(c.tf, w);
    await this.io.emitIndicator(ind);
    this.emittedInd[c.tf]++;

    if (LOG_LEVEL === 'debug') {
      log('debug', `emit ind tf=${c.tf} market=${c.market} time=${ind.time}`);
    }
  }

  private async warmup() {
    log('info', `warmup start: 1m_count=${WARMUP_1M_COUNT} concurrency=${WARMUP_CONCURRENCY}`);

    const markets = await fetchKrwMarkets(UPBIT_REST_URL);
    log('info', `warmup markets: ${markets.length}`);

    let ok = 0;
    let fail = 0;

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
          log('warn', `warmup 부족 market=${market} candles=${c1m.length}`);
        } else if (LOG_LEVEL === 'debug') {
          log('debug', `warmup ok market=${market} candles=${c1m.length} lastBucket=${st.last1mBucket}`);
        }

        ok++;
      } catch (e) {
        fail++;
        log('error', `warmup failed market=${market}`, String(e));
        // live로라도 채우게 빈 상태 생성
        this.state.set(market, {
          agg: {},
          closedWindow: emptyClosedWindow()
        });
      }
    });

    log('info', `warmup done ok=${ok} fail=${fail}`);
  }

  private primeHigherFromHistory(st: MarketState, candles1m: Candle[]) {
    const tfs: Exclude<Timeframe, '1m'>[] = ['5m', '15m', '1h', '4h'];

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

          if (LOG_LEVEL === 'debug') {
            log(
              'debug',
              `close ${tf} market=${market} open=${closedTf.open_time} close=${closedTf.close_time} o=${closedTf.open} h=${closedTf.high} l=${closedTf.low} c=${closedTf.close} v=${closedTf.volume}`
            );
          }

          // TF 캔들 emit
          await this.io.emitCandle(closedTf);
          this.emittedCandle[tf]++;

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
    log('info', `shutting down (${sig})...`);
    await app.stop();
    process.exit(0);
  };

  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
}

main().catch((e) => {
  log('error', 'fatal', e);
  process.exit(1);
});
