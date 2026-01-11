// apps/recovery/src/exchange/upbit.ts
import type { Candle, Tf } from "../types.js";
import { floorToTfSec, toUpbitMinutes, TF_SEC } from "../timeframes.js";

export type FetchResult = {
  candles: Candle[];
  empty: boolean;     // 첫 페이지부터 []이면 true
  statusOk: boolean;  // http ok 여부(여기선 ok면 true, 실패면 throw)
};

type Params = {
  baseUrl: string;
  market: string;
  tf: Tf;
  startSec: number;
  endSec: number;

  sleepMs: number;
  minIntervalMs: number;

  debug?: boolean;

  // ✅ DB time이 "close time"으로 저장된 경우 임시 호환 옵션
  // - false(기본): Upbit candle_date_time_utc를 "open time"으로 사용
  // - true: open time 대신 close time(=open+step)으로 time을 저장/필터
  useCloseTime?: boolean;
};

function sleep(ms: number) {
  return new Promise<void>(r => setTimeout(r, ms));
}
function jitter(ms: number) {
  return Math.floor(Math.random() * ms);
}

function fmtUpbitToUTC(sec: number): string {
  // Upbit "to"는 UTC 기준
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
  // Upbit: candle_date_time_utc = "YYYY-MM-DDTHH:mm:ss" (Z 없음)
  const s = item?.candle_date_time_utc;
  if (typeof s !== "string") return null;

  // UTC 강제
  const t = Date.parse(s.endsWith("Z") ? s : `${s}Z`);
  if (!Number.isFinite(t)) return null;

  // open time을 tf 그리드에 맞춰 floor
  return floorToTfSec(Math.floor(t / 1000), tf);
}

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

let limiter: RateLimiter | null = null;
function getLimiter(minIntervalMs: number) {
  if (!limiter) limiter = new RateLimiter(minIntervalMs);
  return limiter;
}

function parseRetryAfterMs(res: Response): number | null {
  const ra = res.headers.get("retry-after");
  if (!ra) return null;
  const sec = Number(ra);
  if (!Number.isFinite(sec) || sec < 0) return null;
  return Math.ceil(sec * 1000);
}

async function fetchJsonWithRetry(
  url: string,
  maxRetry: number,
  baseSleepMs: number,
  minIntervalMs: number,
  debug?: boolean
): Promise<any> {
  let lastErr: any = null;
  const lim = getLimiter(minIntervalMs);

  for (let i = 0; i <= maxRetry; i++) {
    await lim.waitTurn();

    try {
      const res = await fetch(url, { method: "GET", headers: { accept: "application/json" } });

      if (res.status === 429) {
        const body = await res.text().catch(() => "");
        const raMs = parseRetryAfterMs(res);
        const backoff = Math.min(60_000, Math.max(800, baseSleepMs) * Math.pow(2, i));
        const waitMs = (raMs ?? backoff) + jitter(250);
        console.warn(`[upbit] 429 try=${i + 1}/${maxRetry + 1} waitMs=${waitMs} url=${url} body=${body.slice(0, 120)}`);
        await sleep(waitMs);
        continue;
      }

      if (res.status >= 500 && res.status <= 599) {
        const body = await res.text().catch(() => "");
        const backoff = Math.min(60_000, Math.max(800, baseSleepMs) * Math.pow(2, i)) + jitter(250);
        console.warn(`[upbit] ${res.status} try=${i + 1}/${maxRetry + 1} waitMs=${backoff} url=${url} body=${body.slice(0, 120)}`);
        await sleep(backoff);
        continue;
      }

      if (!res.ok) {
        const body = await res.text().catch(() => "");
        throw new Error(`[upbit] http ${res.status} ${res.statusText} body=${body.slice(0, 300)}`);
      }

      const json = await res.json();

      if (debug) {
        const len = Array.isArray(json) ? json.length : -1;
        console.log(`[dbg] upbit ok url=${url} arrLen=${len}`);
      }

      return json;
    } catch (e) {
      lastErr = e;
      const backoff = Math.min(60_000, Math.max(800, baseSleepMs) * Math.pow(2, i)) + jitter(250);
      console.warn(`[upbit] fetch err try=${i + 1}/${maxRetry + 1} waitMs=${backoff} url=${url} err=${String(e).slice(0, 200)}`);
      await sleep(backoff);
    }
  }

  throw lastErr ?? new Error("[upbit] failed");
}

export async function fetchUpbitCandlesRange(p: Params): Promise<FetchResult> {
  if (!(p.startSec < p.endSec)) return { candles: [], empty: true, statusOk: true };

  const step = TF_SEC[p.tf];
  const limit = 200;
  const maxRetry = 8;

  const endpoint =
    p.tf === "1d"
      ? `${p.baseUrl}/v1/candles/days`
      : `${p.baseUrl}/v1/candles/minutes/${toUpbitMinutes(p.tf)}`;

  const out: Candle[] = [];

  // cursorToSec는 "to"의 기준점. Upbit는 to 이전의 데이터를 준다.
  let cursorToSec = p.endSec;
  let firstPage = true;
  let firstPageEmpty = false;

  while (true) {
    // inclusive/exclusive 경계 꼬임 방지: to = (cursorToSec - 1초)
    const toStr = fmtUpbitToUTC(Math.max(p.startSec, cursorToSec) - 1);

    const url =
      `${endpoint}?market=${encodeURIComponent(p.market)}` +
      `&count=${limit}` +
      `&to=${encodeURIComponent(toStr)}`;

    const arr = await fetchJsonWithRetry(url, maxRetry, p.sleepMs, p.minIntervalMs, p.debug);

    if (!Array.isArray(arr) || arr.length === 0) {
      if (firstPage) firstPageEmpty = true;
      break;
    }

    // ✅ 원인 확정용 샘플 로그(첫/끝 캔들 시간 파싱 결과)
    if (p.debug && firstPage) {
      const s0 = arr[0]?.candle_date_time_utc;
      const o0 = parseOpenSecFromUpbit(arr[0], p.tf);
      const sL = arr[arr.length - 1]?.candle_date_time_utc;
      const oL = parseOpenSecFromUpbit(arr[arr.length - 1], p.tf);
      console.log(
        `[dbg] upbit sample market=${p.market} tf=${p.tf} utc0=${s0} open0=${o0} utclast=${sL} openLast=${oL} want=[${p.startSec},${p.endSec}) useCloseTime=${!!p.useCloseTime}`
      );
    }

    firstPage = false;

    let oldestOpenSec: number | null = null;

    for (const item of arr) {
      const openSec0 = parseOpenSecFromUpbit(item, p.tf);
      if (openSec0 == null) continue;

      // ✅ DB가 close time을 쓰면 open+step으로 time을 맞춘다(임시 호환)
      const sec = p.useCloseTime ? (openSec0 + step) : openSec0;

      oldestOpenSec = oldestOpenSec == null ? openSec0 : Math.min(oldestOpenSec, openSec0);

      if (sec < p.startSec) continue;
      if (sec >= p.endSec) continue;

      const c: Candle = {
        market: p.market,
        tf: p.tf,
        time: sec,
        open: Number(item.opening_price),
        high: Number(item.high_price),
        low: Number(item.low_price),
        close: Number(item.trade_price),
        volume: Number(item.candle_acc_trade_volume)
      };

      if (!Number.isFinite(c.open) || !Number.isFinite(c.high) || !Number.isFinite(c.low) || !Number.isFinite(c.close) || !Number.isFinite(c.volume)) {
        continue;
      }

      out.push(c);
    }

    if (oldestOpenSec == null) break;
    if (oldestOpenSec <= p.startSec) break;

    // 다음 페이지: 더 과거로
    cursorToSec = oldestOpenSec;

    if (p.sleepMs > 0) await sleep(p.sleepMs + jitter(120));
    if (out.length > 2_000_000) break;
  }

  out.sort((a, b) => a.time - b.time);
  const uniq: Candle[] = [];
  let prev = -1;
  for (const c of out) {
    if (c.time === prev) continue;
    prev = c.time;
    uniq.push(c);
  }

  return { candles: uniq, empty: firstPageEmpty, statusOk: true };
}
