import { request, setGlobalDispatcher, Agent } from "undici";
import type { Timeframe, UpbitCandle } from "./types";
import { tfToMinutes } from "./utils/time";
import { log } from "./logger";

const BASE = "https://api.upbit.com";

/**
 * undici는 AbortController만으로도 대부분 끊기지만,
 * 환경에 따라 socket이 "영원히" 걸리는 케이스가 있어
 * Agent timeout을 같이 강제한다.
 */
setGlobalDispatcher(
  new Agent({
    connectTimeout: 7_000,      // TCP connect timeout
    headersTimeout: 12_000,     // 응답 헤더 timeout
    bodyTimeout: 12_000,        // 응답 바디 timeout
    keepAliveTimeout: 10_000,
    keepAliveMaxTimeout: 20_000
  })
);

async function sleep(ms: number) {
  await new Promise((r) => setTimeout(r, ms));
}

function jitter(ms: number) {
  const j = Math.floor(Math.random() * Math.min(400, ms * 0.2));
  return ms + j;
}

class RateLimiter {
  private nextAllowedAt = 0;
  constructor(private minIntervalMs: number) {}
  async wait() {
    const now = Date.now();
    const waitMs = this.nextAllowedAt - now;
    if (waitMs > 0) await sleep(waitMs);
    this.nextAllowedAt = Math.max(this.nextAllowedAt, Date.now()) + this.minIntervalMs;
  }
  bump(extraMs: number) {
    this.nextAllowedAt = Math.max(this.nextAllowedAt, Date.now()) + extraMs;
  }
}

// 전체 마켓 * 전체 TF를 안전하게 돌리려면 사실상 1~2 RPS로 가야 함
const limiter = new RateLimiter(1200);

type JsonResult<T> = { status: number; body: T | string };

async function requestJson<T>(url: string, timeoutMs: number, tag: string): Promise<JsonResult<T>> {
  await limiter.wait();

  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), timeoutMs);

  const started = Date.now();
  try {
    const res = await request(url, {
      method: "GET",
      signal: ac.signal,
      headers: { accept: "application/json" }
    });

    const status = res.statusCode;

    if (status !== 200) {
      const text = await res.body.text();
      return { status, body: text };
    }

    const data = (await res.body.json()) as T;
    return { status, body: data };
  } catch (e: any) {
    // 여기로 떨어지면 무조건 timeout/네트워크 문제
    const ms = Date.now() - started;
    log.warn(`[upbit] request failed`, { tag, ms, err: String(e?.message || e) });
    throw e;
  } finally {
    clearTimeout(t);
  }
}

export async function fetchMarkets(marketFilter?: string): Promise<string[]> {
  const url = `${BASE}/v1/market/all?isDetails=false`;
  const r = await requestJson<Array<{ market: string }>>(url, 15_000, "markets");

  if (r.status !== 200) {
    throw new Error(`Upbit markets error: ${r.status} ${String(r.body).slice(0, 200)}`);
  }

  const markets = (r.body as Array<{ market: string }>).map((d) => d.market);
  if (!marketFilter) return markets;
  return markets.filter((m) => m.startsWith(marketFilter));
}

export async function fetchCandlesChunk(params: {
  market: string;
  tf: Timeframe;
  toKstIso?: string;
  count: number;
  timeoutMs?: number;
  maxRetry?: number;
}): Promise<UpbitCandle[]> {
  const unit = tfToMinutes(params.tf);
  const qs: string[] = [
    `market=${encodeURIComponent(params.market)}`,
    `count=${Math.min(200, Math.max(1, params.count))}`
  ];
  if (params.toKstIso) qs.push(`to=${encodeURIComponent(params.toKstIso)}`);

  const url = `${BASE}/v1/candles/minutes/${unit}?${qs.join("&")}`;

  const timeoutMs = params.timeoutMs ?? 12_000;
  const maxRetry = params.maxRetry ?? 12;

  const tag = `${params.market}:${params.tf}:${params.toKstIso ?? "none"}`;

  for (let attempt = 0; attempt <= maxRetry; attempt++) {
    let r: JsonResult<UpbitCandle[]>;

    try {
      r = (await requestJson<UpbitCandle[]>(url, timeoutMs, tag)) as any;
    } catch (e: any) {
      // timeout/네트워크: 전역적으로 잠깐 쉬고 재시도
      const backoff = jitter(Math.min(30_000, 1_000 * Math.pow(2, attempt)));
      log.warn(`[upbit] net/timeout retry`, { tag, attempt, backoff });
      limiter.bump(backoff);
      await sleep(backoff);
      continue;
    }

    if (r.status === 200) return r.body as UpbitCandle[];

    if (r.status === 429) {
      const backoff = jitter(Math.min(90_000, 3_000 * Math.pow(2, attempt)));
      log.warn(`[upbit] 429 rate limited`, { tag, attempt, backoff });
      limiter.bump(backoff);
      await sleep(backoff);
      continue;
    }

    if (r.status >= 500 && r.status <= 599) {
      const backoff = jitter(Math.min(30_000, 1_000 * Math.pow(2, attempt)));
      log.warn(`[upbit] 5xx retry`, { tag, status: r.status, attempt, backoff });
      limiter.bump(backoff);
      await sleep(backoff);
      continue;
    }

    throw new Error(`Upbit candles error: ${r.status} ${String(r.body).slice(0, 300)}`);
  }

  throw new Error(`Upbit candles failed after retries: ${tag}`);
}
