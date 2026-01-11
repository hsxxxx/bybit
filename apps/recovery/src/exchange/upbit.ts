// apps/recovery/src/exchange/upbit.ts  (신규/전체)
import type { Candle, Tf } from "../types.js";
import { TF_SEC, floorToTfSec, toUpbitMinutes } from "../timeframes.js";

type Params = {
  baseUrl: string;
  market: string;
  tf: Tf;
  startSec: number; // inclusive (open time sec)
  endSec: number;   // exclusive (open time sec)
  sleepMs: number;
};

function sleep(ms: number) {
  return new Promise<void>(r => setTimeout(r, ms));
}

function fmtUpbitToUTC(sec: number): string {
  // Upbit 'to' 파라미터용: YYYY-MM-DDTHH:mm:ss (UTC)
  const d = new Date(sec * 1000);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const mm = String(d.getUTCMinutes()).padStart(2, "0");
  const ss = String(d.getUTCSeconds()).padStart(2, "0");
  return `${y}-${m}-${dd}T${hh}:${mm}:${ss}`;
}

function parseOpenSecFromUpbit(item: any, tf: Tf): number | null {
  // Upbit candles: candle_date_time_utc = "2020-01-01T00:00:00"
  const s = item?.candle_date_time_utc;
  if (typeof s !== "string") return null;

  const t = Date.parse(s.endsWith("Z") ? s : `${s}Z`);
  if (!Number.isFinite(t)) return null;

  const sec = Math.floor(t / 1000);
  return floorToTfSec(sec, tf); // ✅ 시가 초 정규화
}

async function fetchJsonWithRetry(url: string, maxRetry: number, sleepMs: number) {
  let lastErr: any = null;

  for (let i = 0; i <= maxRetry; i++) {
    try {
      const res = await fetch(url, {
        method: "GET",
        headers: { accept: "application/json" }
      });

      // ✅ 레이트리밋/서버 오류 재시도
      if (res.status === 429 || (res.status >= 500 && res.status <= 599)) {
        const body = await res.text().catch(() => "");
        console.warn(`[upbit] retryable status=${res.status} try=${i + 1}/${maxRetry + 1} url=${url} body=${body.slice(0, 200)}`);
        await sleep(Math.max(200, sleepMs));
        continue;
      }

      if (!res.ok) {
        const body = await res.text().catch(() => "");
        throw new Error(`[upbit] http ${res.status} ${res.statusText} body=${body.slice(0, 300)}`);
      }

      const json = await res.json();
      return json;
    } catch (e) {
      lastErr = e;
      console.warn(`[upbit] fetch error try=${i + 1}/${maxRetry + 1} url=${url} err=${String(e).slice(0, 300)}`);
      await sleep(Math.max(200, sleepMs));
    }
  }

  throw lastErr ?? new Error("[upbit] failed");
}

export async function fetchUpbitCandlesRange(p: Params): Promise<Candle[]> {
  if (!(p.startSec < p.endSec)) return [];

  const step = TF_SEC[p.tf];

  // Upbit는 한번에 최대 200개. endSec 기준으로 "역방향"으로 페이징해야 함.
  const limit = 200;
  const maxRetry = 6;

  const out: Candle[] = [];
  let cursorToSec = p.endSec; // exclusive

  const endpoint =
    p.tf === "1d"
      ? `${p.baseUrl}/v1/candles/days`
      : `${p.baseUrl}/v1/candles/minutes/${toUpbitMinutes(p.tf)}`;

  while (true) {
    // 다음 페이지: cursorToSec(초) 직전까지
    const toStr = fmtUpbitToUTC(Math.max(p.startSec, cursorToSec) - 1);

    const url =
      `${endpoint}?market=${encodeURIComponent(p.market)}` +
      `&count=${limit}` +
      `&to=${encodeURIComponent(toStr)}`;

    const arr = await fetchJsonWithRetry(url, maxRetry, p.sleepMs);

    if (!Array.isArray(arr) || arr.length === 0) {
      // 더 이상 없음
      break;
    }

    // Upbit는 최신→과거 순서로 내려줌
    let oldestOpenSec: number | null = null;

    for (const item of arr) {
      const openSec = parseOpenSecFromUpbit(item, p.tf);
      if (openSec == null) continue;

      oldestOpenSec = oldestOpenSec == null ? openSec : Math.min(oldestOpenSec, openSec);

      if (openSec < p.startSec) continue;
      if (openSec >= p.endSec) continue;

      const c: Candle = {
        market: p.market,
        tf: p.tf,
        time: openSec,
        open: Number(item.opening_price),
        high: Number(item.high_price),
        low: Number(item.low_price),
        close: Number(item.trade_price),
        volume: Number(item.candle_acc_trade_volume)
      };

      // 숫자 검증
      if (
        !Number.isFinite(c.open) ||
        !Number.isFinite(c.high) ||
        !Number.isFinite(c.low) ||
        !Number.isFinite(c.close) ||
        !Number.isFinite(c.volume)
      ) {
        continue;
      }

      out.push(c);
    }

    // cursor 이동: 이번 페이지의 가장 오래된 candle 이전으로
    if (oldestOpenSec == null) break;

    const nextCursor = oldestOpenSec;
    if (nextCursor <= p.startSec) break;

    cursorToSec = nextCursor; // 다음 요청에서 nextCursor-1로 toStr 생성됨

    if (p.sleepMs > 0) await sleep(p.sleepMs);

    // 안전장치: 너무 많은 루프 방지(이상 케이스)
    if (out.length > 2_000_000) break;
  }

  // asc + uniq
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
