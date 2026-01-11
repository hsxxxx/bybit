// apps/recovery/src/exchange/upbit.ts
import type { Candle, Tf } from "../types.js";
import { TF_SEC, floorToTfSec, toUpbitMinutes } from "../timeframes.js";

type Params = {
  baseUrl: string;
  market: string;
  tf: Tf;
  startSec: number; // inclusive
  endSec: number;   // exclusive
  sleepMs: number;  // base pacing
};

function sleep(ms: number) {
  return new Promise<void>(r => setTimeout(r, ms));
}

function jitter(ms: number) {
  return Math.floor(Math.random() * ms);
}

function fmtUpbitToUTC(sec: number): string {
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
  const s = item?.candle_date_time_utc;
  if (typeof s !== "string") return null;
  const t = Date.parse(s.endsWith("Z") ? s : `${s}Z`);
  if (!Number.isFinite(t)) return null;
  return floorToTfSec(Math.floor(t / 1000), tf);
}

/**
 * ✅ 전역 레이트리미터: 프로세스 내 모든 요청을 직렬/간격 유지
 * - recovery의 worker 여러 개가 있어도 여기서 최종적으로 간격을 강제함
 */
class RateLimiter {
  private nextAllowed = 0;
  constructor(private minIntervalMs: number) {}

  async waitTurn() {
    const now = Date.now();
    const wait = Math.max(0, this.nextAllowed - now);
    if (wait > 0) await sleep(wait);
    this.nextAllowed = Date.now() + this.minIntervalMs;
  }
}

// 기본값: 최소 250ms 간격(=초당 4회 수준). 필요시 ENV로 조정
const GLOBAL_MIN_INTERVAL_MS = Number(process.env.UPBIT_MIN_INTERVAL_MS ?? "250");
const limiter = new RateLimiter(GLOBAL_MIN_INTERVAL_MS);

function parseRetryAfterMs(res: Response): number | null {
  // 표준 Retry-After (seconds)
  const ra = res.headers.get("retry-after");
  if (ra) {
    const sec = Number(ra);
    if (Number.isFinite(sec) && sec >= 0) return Math.ceil(sec * 1000);
  }
  // 일부 프록시/게이트웨이에서 ms 혹은 reset 계열이 있을 수 있어 fallback만 둠
  const reset = res.headers.get("ratelimit-reset"); // 보장X
  if (reset) {
    const v = Number(reset);
    if (Number.isFinite(v) && v > 0) return Math.ceil(v * 1000);
  }
  return null;
}

async function fetchJsonWithRetry(url: string, maxRetry: number, baseSleepMs: number) {
  let lastErr: any = null;

  for (let i = 0; i <= maxRetry; i++) {
    await limiter.waitTurn(); // ✅ 글로벌 pacing

    try {
      const res = await fetch(url, {
        method: "GET",
        headers: { accept: "application/json" }
      });

      if (res.status === 429) {
        const body = await res.text().catch(() => "");
        const raMs = parseRetryAfterMs(res);

        // ✅ Retry-After 있으면 우선, 없으면 지수백오프
        const backoff = Math.min(60_000, Math.max(500, baseSleepMs) * Math.pow(2, i));
        const waitMs = (raMs ?? backoff) + jitter(250);

        console.warn(
          `[upbit] retryable status=429 try=${i + 1}/${maxRetry + 1} waitMs=${waitMs} url=${url} body=${body.slice(0, 200)}`
        );
        await sleep(waitMs);
        continue;
      }

      if (res.status >= 500 && res.status <= 599) {
        const body = await res.text().catch(() => "");
        const backoff = Math.min(60_000, Math.max(500, baseSleepMs) * Math.pow(2, i)) + jitter(250);
        console.warn(
          `[upbit] retryable status=${res.status} try=${i + 1}/${maxRetry + 1} waitMs=${backoff} url=${url} body=${body.slice(0, 200)}`
        );
        await sleep(backoff);
        continue;
      }

      if (!res.ok) {
        const body = await res.text().catch(() => "");
        throw new Error(`[upbit] http ${res.status} ${res.statusText} body=${body.slice(0, 300)}`);
      }

      return await res.json();
    } catch (e) {
      lastErr = e;
      const backoff = Math.min(60_000, Math.max(500, baseSleepMs) * Math.pow(2, i)) + jitter(250);
      console.warn(`[upbit] fetch error try=${i + 1}/${maxRetry + 1} waitMs=${backoff} url=${url} err=${String(e).slice(0, 300)}`);
      await sleep(backoff);
    }
  }

  throw lastErr ?? new Error("[upbit] failed");
}

export async function fetchUpbitCandlesRange(p: Params): Promise<Candle[]> {
  if (!(p.startSec < p.endSec)) return [];

  const limit = 200;
  const maxRetry = 8;

  const endpoint =
    p.tf === "1d"
      ? `${p.baseUrl}/v1/candles/days`
      : `${p.baseUrl}/v1/candles/minutes/${toUpbitMinutes(p.tf)}`;

  const out: Candle[] = [];
  let cursorToSec = p.endSec; // exclusive

  while (true) {
    const toStr = fmtUpbitToUTC(Math.max(p.startSec, cursorToSec) - 1);

    const url =
      `${endpoint}?market=${encodeURIComponent(p.market)}` +
      `&count=${limit}` +
      `&to=${encodeURIComponent(toStr)}`;

    const arr = await fetchJsonWithRetry(url, maxRetry, p.sleepMs);

    if (!Array.isArray(arr) || arr.length === 0) break;

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

      if (
        !Number.isFinite(c.open) ||
        !Number.isFinite(c.high) ||
        !Number.isFinite(c.low) ||
        !Number.isFinite(c.close) ||
        !Number.isFinite(c.volume)
      ) continue;

      out.push(c);
    }

    if (oldestOpenSec == null) break;
    if (oldestOpenSec <= p.startSec) break;

    cursorToSec = oldestOpenSec;

    // ✅ 정상요청도 약간 쉬어줘서 429 자체를 줄임
    if (p.sleepMs > 0) await sleep(p.sleepMs + jitter(100));
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
