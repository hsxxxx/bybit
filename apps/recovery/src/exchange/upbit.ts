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
  tf: Tf;
  startSec: number;
  endSec: number;
  sleepMs: number;
  signal?: AbortSignal;

  // retry config
  maxRetries?: number;          // default 20
  retryBaseDelayMs?: number;    // default 800
  retryMinDelayMs?: number;     // default 1500 (429 최소 대기)
  retryMaxDelayMs?: number;     // default 60000
}): Promise<Candle[]> {
  const unit = toUpbitMinutes(params.tf);
  const step = TF_SEC[params.tf];

  const maxRetries = params.maxRetries ?? 20;
  const retryBaseDelayMs = params.retryBaseDelayMs ?? 800;
  const retryMinDelayMs = params.retryMinDelayMs ?? 1500;
  const retryMaxDelayMs = params.retryMaxDelayMs ?? 60_000;

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
      retryBaseDelayMs,
      retryMinDelayMs,
      retryMaxDelayMs
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
  baseDelayMs: number,
  minDelayMs: number,
  maxDelayMs: number
): Promise<T> {
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    const res = await fetch(url, { method: "GET", signal });

    if (res.ok) return (await res.json()) as T;

    const txt = await res.text().catch(() => "");
    const retryAfter = parseRetryAfterMs(res);

    const isRetryable = res.status === 429 || (res.status >= 500 && res.status <= 599);
    if (!isRetryable) {
      throw new Error(`Upbit fetch failed ${res.status} ${res.statusText} ${txt}`.trim());
    }

    if (attempt === maxRetries) {
      throw new Error(`Upbit fetch failed ${res.status} ${res.statusText} ${txt}`.trim());
    }

    // 429일 때는 최소 대기(minDelayMs) 보장 + Retry-After 있으면 우선
    const exp = baseDelayMs * Math.pow(2, attempt);
    const candidate = retryAfter ?? jitter(exp);

    const backoff = clamp(Math.max(minDelayMs, candidate), minDelayMs, maxDelayMs);
    await sleep(backoff);
  }

  throw new Error("fetchJsonWithRetry: unreachable");
}

function parseRetryAfterMs(res: Response): number | undefined {
  const ra = res.headers.get("retry-after");
  if (!ra) return undefined;

  if (/^\d+$/.test(ra)) return Number(ra) * 1000;

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

function clamp(v: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, v));
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
