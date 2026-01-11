// src/exchange/upbit.ts
import { tfKeyFromSec } from "../timeframes.js";

export type UpbitCandle = {
  market: string;
  candle_date_time_utc: string; // "YYYY-MM-DDTHH:mm:ss"
  candle_date_time_kst: string; // "YYYY-MM-DDTHH:mm:ss"
  opening_price: number;
  high_price: number;
  low_price: number;
  trade_price: number;
  timestamp: number;
  candle_acc_trade_price: number;
  candle_acc_trade_volume: number;
  unit?: number;
};

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function fetchJson<T>(url: string, init?: RequestInit, retry = 7): Promise<T> {
  let lastErr: unknown = null;

  for (let i = 0; i < retry; i++) {
    try {
      const res = await fetch(url, {
        ...init,
        headers: { accept: "application/json", ...(init?.headers ?? {}) },
      });

      if (res.status === 429) {
        await sleep(300 + i * 700);
        continue;
      }
      if (!res.ok) {
        const text = await res.text().catch(() => "");
        throw new Error(`HTTP ${res.status} ${res.statusText} :: ${text}`);
      }
      return (await res.json()) as T;
    } catch (e) {
      lastErr = e;
      await sleep(300 + i * 700);
    }
  }
  throw lastErr instanceof Error ? lastErr : new Error(String(lastErr));
}

function upbitMinutesUnitFromTfSec(tfSec: number): number {
  const tfKey = tfKeyFromSec(tfSec);
  switch (tfKey) {
    case "1m": return 1;
    case "3m": return 3;
    case "5m": return 5;
    case "10m": return 10;
    case "15m": return 15;
    case "30m": return 30;
    case "1h": return 60;
    case "4h": return 240;
    default: throw new Error(`Unsupported tfKey=${tfKey}`);
  }
}

// candle 시간(ms) - KST 기준 start
function candleKstMs(c: UpbitCandle): number {
  return new Date(`${c.candle_date_time_kst}+09:00`).getTime();
}

// candle 시간(ms) - UTC 기준 start
function candleUtcMs(c: UpbitCandle): number {
  return new Date(`${c.candle_date_time_utc}Z`).getTime();
}

// epochMs -> UTC ISO (no ms) with Z
function toUtcIsoNoMs(epochMs: number): string {
  const d = new Date(epochMs);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const mm = String(d.getUTCMinutes()).padStart(2, "0");
  const ss = String(d.getUTCSeconds()).padStart(2, "0");
  return `${y}-${m}-${day}T${hh}:${mm}:${ss}Z`;
}

/**
 * Upbit minute candles range fetcher
 * - `to`는 반드시 UTC ISO(Z)로 보냄 (무한 반복 방지)
 * - 페이징 커서도 oldest candle의 UTC 시간으로 내림
 */
export async function fetchUpbitCandlesRange(params: {
  market: string;
  tfSec: number;
  fromMs: number; // KST epoch ms 기준 range filter
  toMs: number;   // KST epoch ms 기준 range filter (exclusive)
  limitPerReq?: number;
  throttleMs?: number;
  onPage?: (info: { market: string; got: number; cursorUtc: string; oldestUtc: string; oldestKst: string }) => void;
}): Promise<UpbitCandle[]> {
  const { market, tfSec, fromMs, toMs } = params;
  const limit = Math.min(Math.max(params.limitPerReq ?? 200, 1), 200);
  const throttleMs = Math.max(params.throttleMs ?? 120, 0);
  const unit = upbitMinutesUnitFromTfSec(tfSec);

  // 커서: UTC 기준
  // toMs는 KST 기준으로 들어오지만, 실제 "요청 커서"는 UTC로 만들어 보내면 됨
  const startCursorUtc = toUtcIsoNoMs(toMs - 1);
  let cursorUtc = startCursorUtc;

  const out: UpbitCandle[] = [];

  let guard = 0;
  const GUARD_MAX = 5000;

  // 반복 감지(같은 oldestUtc가 반복되면 break)
  let lastOldestUtc = "";

  while (guard++ < GUARD_MAX) {
    const url =
      `https://api.upbit.com/v1/candles/minutes/${unit}` +
      `?market=${encodeURIComponent(market)}` +
      `&to=${encodeURIComponent(cursorUtc)}` +
      `&count=${limit}`;

    const rows = await fetchJson<UpbitCandle[]>(url);
    if (!rows || rows.length === 0) break;

    // 필터링은 KST candle time 기준으로 수행 (네 시스템은 KST 기준)
    for (const r of rows) {
      const tKst = candleKstMs(r);
      if (tKst >= fromMs && tKst < toMs) out.push(r);
    }

    const oldest = rows[rows.length - 1];
    const oldestUtcMs = candleUtcMs(oldest);
    const oldestUtc = `${oldest.candle_date_time_utc}Z`;

    params.onPage?.({
      market,
      got: rows.length,
      cursorUtc,
      oldestUtc,
      oldestKst: oldest.candle_date_time_kst,
    });

    // 반복 방지
    if (oldestUtc === lastOldestUtc) break;
    lastOldestUtc = oldestUtc;

    // fromMs는 KST 기준이므로 비교는 KST candle time으로
    const oldestKstMs = candleKstMs(oldest);
    if (oldestKstMs <= fromMs) break;

    // 다음 커서는 oldestUtcMs - 1초
    cursorUtc = toUtcIsoNoMs(oldestUtcMs - 1000);

    if (throttleMs > 0) await sleep(throttleMs);
  }

  if (guard >= GUARD_MAX) {
    throw new Error(`[fetchUpbitCandlesRange] guard overflow market=${market} startCursor=${startCursorUtc}`);
  }

  // candle start(KST) 기준 uniq + asc
  const map = new Map<number, UpbitCandle>();
  for (const c of out) map.set(candleKstMs(c), c);

  return Array.from(map.values()).sort((a, b) => candleKstMs(a) - candleKstMs(b));
}

// market list
export type UpbitMarket = { market: string; korean_name: string; english_name: string };

export async function fetchUpbitMarkets(params?: { quote?: "KRW" | "BTC" | "USDT" | "ALL" }) {
  const quote = params?.quote ?? "KRW";
  const url = "https://api.upbit.com/v1/market/all?isDetails=false";
  const rows = await fetchJson<UpbitMarket[]>(url);
  if (quote === "ALL") return rows;
  return rows.filter((m) => m.market.startsWith(`${quote}-`));
}
