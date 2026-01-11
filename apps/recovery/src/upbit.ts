import { request } from "undici";
import type { Timeframe, UpbitCandle } from "./types";
import { tfToMinutes } from "./utils/time";
import { log } from "./logger";

const BASE = "https://api.upbit.com";

async function sleep(ms: number) {
  await new Promise((r) => setTimeout(r, ms));
}

function jitter(ms: number) {
  const j = Math.floor(Math.random() * Math.min(400, ms * 0.2));
  return ms + j;
}

/**
 * Upbit는 (특히 candles) 429가 매우 빈번함.
 * 전역 RateLimiter로 모든 요청을 직렬/완만하게 흘려서 429 자체를 줄인다.
 */
class RateLimiter {
  private nextAllowedAt = 0;

  constructor(private minIntervalMs: number) {}

  async wait() {
    const now = Date.now();
    const waitMs = this.nextAllowedAt - now;
    if (waitMs > 0) await sleep(waitMs);
    // 다음 요청 가능 시각 예약
    this.nextAllowedAt = Math.max(this.nextAllowedAt, Date.now()) + this.minIntervalMs;
  }

  bump(extraMs: number) {
    this.nextAllowedAt = Math.max(this.nextAllowedAt, Date.now()) + extraMs;
  }
}

// 기본: 1초에 1회 정도 (CONCURRENCY를 1~2로 두고도 429 방지)
const limiter = new RateLimiter(1100);

async function requestJson<T>(url: string, timeoutMs: number): Promise<{ status: number; body: T | string }> {
  await limiter.wait();

  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), timeoutMs);

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
  } finally {
    clearTimeout(t);
  }
}

export async function fetchMarkets(marketFilter?: string): Promise<string[]> {
  const url = `${BASE}/v1/market/all?isDetails=false`;
  const r = await requestJson<Array<{ market: string }>>(url, 15_000);

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

  for (let attempt = 0; attempt <= maxRetry; attempt++) {
    let r: { status: number; body: UpbitCandle[] | string };

    try {
      r = (await requestJson<UpbitCandle[]>(url, timeoutMs)) as any;
    } catch (e: any) {
      const backoff = jitter(Math.min(20_000, 800 * Math.pow(2, attempt)));
      log.warn(`[upbit] network/timeout retry`, { market: params.market, tf: params.tf, attempt, backoff });
      limiter.bump(backoff);
      await sleep(backoff);
      continue;
    }

    if (r.status === 200) return r.body as UpbitCandle[];

    if (r.status === 429) {
      // 429가 나오면 전역적으로 더 느리게
      const backoff = jitter(Math.min(60_000, 2_000 * Math.pow(2, attempt)));
      log.warn(`[upbit] 429 rate limited`, { market: params.market, tf: params.tf, attempt, backoff });
      limiter.bump(backoff);
      await sleep(backoff);
      continue;
    }

    if (r.status >= 500 && r.status <= 599) {
      const backoff = jitter(Math.min(20_000, 800 * Math.pow(2, attempt)));
      log.warn(`[upbit] 5xx retry`, { status: r.status, market: params.market, tf: params.tf, attempt, backoff });
      limiter.bump(backoff);
      await sleep(backoff);
      continue;
    }

    throw new Error(`Upbit candles error: ${r.status} ${String(r.body).slice(0, 300)}`);
  }

  throw new Error(`Upbit candles failed after retries: ${params.market} ${params.tf}`);
}
