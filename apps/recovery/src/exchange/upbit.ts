// apps/recovery/src/exchange/upbit.ts
import type { Candle, Tf } from "../types.js";
import { TF_SEC, toUpbitMinutes, floorToTfSec } from "../timeframes.js";

type UpbitCandleRow = {
  market: string;
  candle_date_time_utc: string; // "YYYY-MM-DDTHH:mm:ss"
  opening_price: number;
  high_price: number;
  low_price: number;
  trade_price: number;
  candle_acc_trade_volume: number;
};

export async function fetchUpbitCandlesRange(params: {
  baseUrl: string;
  market: string;
  tf: Tf;           // upbit minutes supported only
  startSec: number; // inclusive
  endSec: number;   // exclusive
  sleepMs: number;  // base throttle between pages
  signal?: AbortSignal;

  // retry config (optional)
  maxRetries?: number;      // default 8
  retryBaseDelayMs?: number; // default 400
}): Promise<Candle[]> {
  const unit = toUpbitMinutes(params.tf);
  const step = TF_SEC[params.tf];

  const maxRetries = params.maxRetries ?? 8;
  const retryBaseDelayMs = params.retryBaseDelayMs ?? 400;

  const out: Candle[] = [];
  let cursorSec = params.endSec;

  while (cursorSec > params.startSec) {
    const toIso = new Date(cursorSec * 1000).toISOString();
    const url = new URL(`${params.baseUrl}/v1/candles/minutes/${unit}`);
    url.searchParams.set("market", params.market);
    url.searchParams.set("to", toIso);
    url.searchParams.set("count", "200");

    const rows = await fetchJsonWithRetry<UpbitCandleRow[]>(
      url.toString(),
      params.signal,
      maxRetries,
      retryBaseDelayMs
    );

    if (!Array.isArray(rows) || rows.length === 0) break;

    for (const r of rows) {
      const openSec = floorToTfSec(parseUpbitUtcSec(r.candle_date_time_utc), params.tf);
      if (openSec < params.startSec || openSec >= params.endSec) continue;

      out.push({
        market: r.market,
        tf: params.tf,
        time: openSec,
        open: r.opening_price,
        high: r.high_price,
        low: r.low_price,
        close: r.trade_price,
        volume: r.candle_acc_trade_volume
      });
    }

    const oldest = rows[rows.length - 1];
    const oldestOpenSec = floorToTfSec(parseUpbitUtcSec(oldest.candle_date_time_utc), params.tf);

    if (oldestOpenSec >= cursorSec) cursorSec -= step * 200;
    else cursorSec = oldestOpenSec;

    if (params.sleepMs > 0) await sleep(params.sleepMs);
  }

  out.sort((a, b) => a.time - b.time);
  const uniq: Candle[] = [];
  let prev = -1;
  for (const c of out) {
    if (c.time === prev) continue;
    prev = c.time;
    uniq.push(c);
  }
  return uniq;
}

async function fetchJsonWithRetry<T>(
  url: string,
  signal: AbortSignal | undefined,
  maxRetries: number,
  baseDelayMs: number
): Promise<T> {
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    const res = await fetch(url, { method: "GET", signal });

    if (res.ok) return (await res.json()) as T;

    const txt = await res.text().catch(() => "");
    const retryAfter = parseRetryAfterMs(res);

    // 429/5xx 는 재시도
    if (res.status === 429 || (res.status >= 500 && res.status <= 599)) {
      if (attempt === maxRetries) {
        throw new Error(`Upbit fetch failed ${res.status} ${res.statusText} ${txt}`.trim());
      }

      const backoff = Math.min(
        30_000,
        retryAfter ?? jitter(baseDelayMs * Math.pow(2, attempt))
      );

      await sleep(backoff);
      continue;
    }

    // 그 외는 즉시 실패
    throw new Error(`Upbit fetch failed ${res.status} ${res.statusText} ${txt}`.trim());
  }

  // unreachable
  throw new Error("fetchJsonWithRetry: unreachable");
}

function parseRetryAfterMs(res: Response): number | undefined {
  const ra = res.headers.get("retry-after");
  if (!ra) return undefined;

  // seconds format
  if (/^\d+$/.test(ra)) return Number(ra) * 1000;

  // http-date format
  const t = Date.parse(ra);
  if (Number.isFinite(t)) {
    const ms = t - Date.now();
    return ms > 0 ? ms : undefined;
  }
  return undefined;
}

function jitter(ms: number): number {
  const r = 0.7 + Math.random() * 0.6; // 0.7~1.3
  return Math.floor(ms * r);
}

function parseUpbitUtcSec(s: string): number {
  const iso = s.endsWith("Z") ? s : `${s}Z`;
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) throw new Error(`Bad upbit utc time: ${s}`);
  return Math.floor(t / 1000);
}

function sleep(ms: number) {
  return new Promise<void>(r => setTimeout(r, ms));
}
