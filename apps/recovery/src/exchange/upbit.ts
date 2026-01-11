// src/exchange/upbit.ts
import { tfKeyFromSec, toKstIsoNoMs } from "../timeframes.js";

export type UpbitCandle = {
  market: string;
  candle_date_time_utc: string;
  candle_date_time_kst: string; // "YYYY-MM-DDTHH:mm:ss"
  opening_price: number;
  high_price: number;
  low_price: number;
  trade_price: number;
  timestamp: number; // last tick time (ms) - 쓰지 말 것(커서/시간용)
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
        headers: {
          accept: "application/json",
          ...(init?.headers ?? {}),
        },
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
    case "1m":
      return 1;
    case "3m":
      return 3;
    case "5m":
      return 5;
    case "10m":
      return 10;
    case "15m":
      return 15;
    case "30m":
      return 30;
    case "1h":
      return 60;
    case "4h":
      return 240;
    default:
      throw new Error(`Unsupported tfKey=${tfKey}`);
  }
}

/**
 * candle_date_time_kst (KST) -> epochMs (KST 기준 시각)
 * Upbit 응답의 candle_date_time_kst는 timezone이 없으므로 +09:00 붙여 파싱
 */
function candleKstMs(c: UpbitCandle): number {
  return new Date(`${c.candle_date_time_kst}+09:00`).getTime();
}

/**
 * Upbit minute candles range fetcher (KST candle time 기준으로 페이징/필터링)
 *
 * @param fromMs inclusive
 * @param toMs exclusive
 */
export async function fetchUpbitCandlesRange(params: {
  market: string;
  tfSec: number;
  fromMs: number;
  toMs: number;
  limitPerReq?: number;
  throttleMs?: number;
  onPage?: (info: { market: string; got: number; cursorKst: string }) => void;
}): Promise<UpbitCandle[]> {
  const { market, tfSec, fromMs, toMs } = params;
  const limit = Math.min(Math.max(params.limitPerReq ?? 200, 1), 200);
  const throttleMs = Math.max(params.throttleMs ?? 120, 0);
  const unit = upbitMinutesUnitFromTfSec(tfSec);

  // 커서는 "to" (KST candle time)로 과거로 내려가며 페이징
  let cursorToKst = toKstIsoNoMs(toMs - 1);
  const out: UpbitCandle[] = [];

  // 루프 안전장치
  let guard = 0;
  const GUARD_MAX = 5000;

  while (guard++ < GUARD_MAX) {
    const url =
      `https://api.upbit.com/v1/candles/minutes/${unit}` +
      `?market=${encodeURIComponent(market)}` +
      `&to=${encodeURIComponent(cursorToKst)}` +
      `&count=${limit}`;

    const rows = await fetchJson<UpbitCandle[]>(url);
    if (!rows || rows.length === 0) break;

    // rows: 최신 -> 과거(내림차순). candle time 기준으로 필터
    for (const r of rows) {
      const t = candleKstMs(r); // ⭐ candle time (정렬/경계 안정)
      if (t >= fromMs && t < toMs) out.push(r);
    }

    // 다음 커서: 가장 오래된 캔들의 candle time으로 이동 (더 과거로)
    const oldest = rows[rows.length - 1];
    const oldestKstMs = candleKstMs(oldest);

    params.onPage?.({
      market,
      got: rows.length,
      cursorKst: oldest.candle_date_time_kst,
    });

    if (oldestKstMs <= fromMs) break;

    // cursorToKst는 oldest 시각으로 설정 (동일 시각 반복 방지 위해 1초 빼기)
    cursorToKst = toKstIsoNoMs(oldestKstMs - 1000);

    if (throttleMs > 0) await sleep(throttleMs);
  }

  if (guard >= GUARD_MAX) {
    throw new Error(`[fetchUpbitCandlesRange] guard overflow market=${market}`);
  }

  // candle time 기준으로 중복 제거 + 오름차순 정렬
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
