// src/exchange/upbit.ts
import { tfKeyFromSec, toKstIsoNoMs } from "../timeframes.js";

export type UpbitCandle = {
  market: string;
  candle_date_time_utc: string;
  candle_date_time_kst: string;
  opening_price: number;
  high_price: number;
  low_price: number;
  trade_price: number;
  timestamp: number; // ms
  candle_acc_trade_price: number;
  candle_acc_trade_volume: number;
  unit?: number;
};

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function fetchJson<T>(url: string, init?: RequestInit, retry = 5): Promise<T> {
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

      // Upbit: 429 가끔 뜸
      if (res.status === 429) {
        const wait = 250 + i * 500;
        await sleep(wait);
        continue;
      }

      if (!res.ok) {
        const text = await res.text().catch(() => "");
        throw new Error(`HTTP ${res.status} ${res.statusText} :: ${text}`);
      }

      return (await res.json()) as T;
    } catch (e) {
      lastErr = e;
      const wait = 250 + i * 500;
      await sleep(wait);
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
 * Upbit minute candles range fetcher (KST 기준으로 to 파라미터를 내림차순 페이징에 사용)
 *
 * @param market ex) "KRW-BTC"
 * @param tfSec ex) 60, 300, 900 ...
 * @param fromMs inclusive (epoch ms, KST 기준으로 해석)
 * @param toMs exclusive (epoch ms)
 * @param limitPerReq max 200
 */
export async function fetchUpbitCandlesRange(params: {
  market: string;
  tfSec: number;
  fromMs: number;
  toMs: number;
  limitPerReq?: number;
  throttleMs?: number;
}): Promise<UpbitCandle[]> {
  const { market, tfSec, fromMs, toMs } = params;
  const limit = Math.min(Math.max(params.limitPerReq ?? 200, 1), 200);
  const throttleMs = Math.max(params.throttleMs ?? 120, 0);

  const unit = upbitMinutesUnitFromTfSec(tfSec);

  // Upbit API는 `to` 기준으로 과거로 내려가며(내림차순) 최대 count개 반환
  // 우리가 원하는 건 [fromMs, toMs) 구간
  let cursorToMs = toMs;
  const out: UpbitCandle[] = [];

  while (true) {
    const toIsoKst = toKstIsoNoMs(cursorToMs - 1); // exclusive -> inclusive 느낌으로 한 틱 빼서 안전
    const url =
      `https://api.upbit.com/v1/candles/minutes/${unit}` +
      `?market=${encodeURIComponent(market)}` +
      `&to=${encodeURIComponent(toIsoKst)}` +
      `&count=${limit}`;

    const rows = await fetchJson<UpbitCandle[]>(url);

    if (!rows || rows.length === 0) break;

    // rows는 최신 -> 과거(내림차순)
    for (const r of rows) {
      const t = r.timestamp; // ms, KST close time 기반으로 들어옴(Upbit candle timestamp)
      if (t >= fromMs && t < toMs) out.push(r);
    }

    // 페이징: 가장 오래된 캔들의 timestamp로 커서를 이동
    const oldest = rows[rows.length - 1];
    const nextTo = oldest.timestamp;
    if (!Number.isFinite(nextTo)) break;

    // 더 내려가도 fromMs보다 과거면 종료
    if (nextTo <= fromMs) break;

    // 다음 요청은 oldest 보다 더 과거로
    cursorToMs = nextTo;

    if (throttleMs > 0) await sleep(throttleMs);

    // 안전장치: 같은 timestamp로 무한루프 방지
    if (rows.length === 1 && cursorToMs === toMs) break;
  }

  // 중복 제거 + 오름차순 정렬 (timestamp 기준)
  const map = new Map<number, UpbitCandle>();
  for (const c of out) {
    map.set(c.timestamp, c);
  }
  return Array.from(map.values()).sort((a, b) => a.timestamp - b.timestamp);
}
