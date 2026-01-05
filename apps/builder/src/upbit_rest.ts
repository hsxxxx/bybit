// apps/builder/src/upbit_rest.ts
import { request } from 'undici';
import type { Candle } from './candle.js';
import { bucketStart } from './candle.js';

type UpbitMarket = { market: string };
type UpbitMinuteCandle = {
  market: string;
  candle_date_time_kst: string; // "YYYY-MM-DDTHH:mm:ss"
  opening_price: number;
  high_price: number;
  low_price: number;
  trade_price: number;
  candle_acc_trade_volume: number;
};

function sleep(ms: number) {
  return new Promise(res => setTimeout(res, ms));
}

function nowMinusClosedMinuteMs(): number {
  const now = Date.now();
  const m = Math.floor((now - 60_000) / 60_000) * 60_000; // last fully closed minute bucket start
  return m;
}

export async function fetchKrwMarkets(restUrl: string): Promise<string[]> {
  const url = `${restUrl}/market/all?isDetails=false`;
  const res = await request(url);
  if (res.statusCode !== 200) throw new Error(`market/all failed: ${res.statusCode} ${await res.body.text()}`);
  const json = (await res.body.json()) as UpbitMarket[];
  return json.map(x => x.market).filter(m => m.startsWith('KRW-')).sort();
}

async function requestJsonWithRetry(url: string, maxRetries = 8) {
  let attempt = 0;
  while (true) {
    attempt += 1;
    const res = await request(url);

    if (res.statusCode === 200) {
      return res.body.json();
    }

    const body = await res.body.text();

    // Rate limit
    if (res.statusCode === 429 && attempt <= maxRetries) {
      // exponential backoff + jitter (ms)
      const base = 500; // 0.5s
      const wait = Math.min(30_000, base * Math.pow(2, attempt - 1)) + Math.floor(Math.random() * 250);
      console.warn(`[builder] 429 rate-limited. retry in ${wait}ms (attempt ${attempt}/${maxRetries}) url=${url}`);
      await sleep(wait);
      continue;
    }

    throw new Error(`${res.statusCode} ${body}`);
  }
}

function toIsoKst(ms: number): string {
  // Upbit accepts ISO8601; safest is UTC ISO string.
  // We'll pass UTC ISO string for "to".
  return new Date(ms).toISOString();
}

function mapToCandle1m(x: UpbitMinuteCandle): Candle {
  const ts = Date.parse(x.candle_date_time_kst + '+09:00');
  const openTime = bucketStart(ts, '1m');
  return {
    exchange: 'upbit',
    market: x.market,
    tf: '1m',
    open_time: openTime,
    close_time: openTime + 60_000,
    open: x.opening_price,
    high: x.high_price,
    low: x.low_price,
    close: x.trade_price,
    volume: x.candle_acc_trade_volume,
    is_closed: true,
    source: 'ws_candle1m'
  };
}

/**
 * Upbit minute candles REST: max 200 per call.
 * We page backwards using `to` to collect `count` candles.
 */
export async function fetchRecent1mClosedCandles(params: {
  restUrl: string;
  market: string;
  count: number; // e.g. 600
}): Promise<Candle[]> {
  const { restUrl, market, count } = params;

  const out: Candle[] = [];
  let remaining = count;

  // Start from last fully closed minute
  let toMs = nowMinusClosedMinuteMs() + 60_000; // "to" is exclusive-ish; safe to set to next minute
  // We will request with `to` and take returned candles (usually newest-first)

  while (remaining > 0) {
    const chunk = Math.min(200, remaining);
    const url =
      `${restUrl}/candles/minutes/1?market=${encodeURIComponent(market)}` +
      `&count=${chunk}&to=${encodeURIComponent(toIsoKst(toMs))}`;

    const json = await requestJsonWithRetry(url);
    const arr = json as UpbitMinuteCandle[];
    if (!Array.isArray(arr) || arr.length === 0) break;

    // Upbit returns newest-first; convert to candles then reverse to chronological when we finalize
    const mapped = arr.map(mapToCandle1m);
    out.push(...mapped);

    remaining -= arr.length;

    // Move `to` to the oldest candle time in this batch (go further back)
    const oldest = mapped[mapped.length - 1]; // because mapped is still newest-first mapping? wait:
    // mapped corresponds 1:1 with arr order (newest-first), so last element is oldest in that batch.
    toMs = oldest.open_time;

    // be gentle to REST
    await sleep(120);
  }

  // out is in newest-first chunks appended, still overall "newest-first-ish"
  // Sort ascending by open_time and trim to exact count.
  out.sort((a, b) => a.open_time - b.open_time);

  // Remove duplicates (just in case)
  const dedup: Candle[] = [];
  let last = -1;
  for (const c of out) {
    if (c.open_time !== last) dedup.push(c);
    last = c.open_time;
  }

  // Keep last `count`
  return dedup.length > count ? dedup.slice(dedup.length - count) : dedup;
}

// simple concurrency limiter
export async function mapLimit<T, R>(
  items: T[],
  limit: number,
  fn: (item: T) => Promise<R>
): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let idx = 0;

  async function worker() {
    while (true) {
      const cur = idx++;
      if (cur >= items.length) return;
      results[cur] = await fn(items[cur]);
    }
  }

  const workers = Array.from({ length: Math.max(1, limit) }, () => worker());
  await Promise.all(workers);
  return results;
}
