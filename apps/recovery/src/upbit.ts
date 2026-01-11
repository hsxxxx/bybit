import { request } from "undici";
import type { Timeframe, UpbitCandle } from "./types";
import { tfToMinutes } from "./utils/time";

const BASE = "https://api.upbit.com";

async function sleep(ms: number) {
  await new Promise((r) => setTimeout(r, ms));
}

function jitter(ms: number) {
  const j = Math.floor(Math.random() * Math.min(250, ms * 0.1));
  return ms + j;
}

async function requestJson<T>(url: string, timeoutMs: number): Promise<{ status: number; body: T | string }> {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), timeoutMs);

  try {
    const res = await request(url, {
      method: "GET",
      signal: ac.signal,
      headers: {
        "accept": "application/json"
      }
    });

    const status = res.statusCode;

    // 에러 바디도 문자열로 확보
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
  const r = await requestJson<Array<{ market: string }>>(url, 10_000);

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
  toKstIso?: string; // "YYYY-MM-DDTHH:mm:ss"
  count: number; // <= 200
  timeoutMs?: number; // default 10s
  maxRetry?: number; // default 8
}): Promise<UpbitCandle[]> {
  const unit = tfToMinutes(params.tf);
  const qs: string[] = [
    `market=${encodeURIComponent(params.market)}`,
    `count=${Math.min(200, Math.max(1, params.count))}`
  ];
  if (params.toKstIso) qs.push(`to=${encodeURIComponent(params.toKstIso)}`);

  const url = `${BASE}/v1/candles/minutes/${unit}?${qs.join("&")}`;

  const timeoutMs = params.timeoutMs ?? 10_000;
  const maxRetry = params.maxRetry ?? 8;

  for (let attempt = 0; attempt <= maxRetry; attempt++) {
    let r;
    try {
      r = await requestJson<UpbitCandle[]>(url, timeoutMs);
    } catch (e: any) {
      // 네트워크/Abort 등
      const backoff = jitter(Math.min(15_000, 600 * Math.pow(2, attempt)));
      await sleep(backoff);
      continue;
    }

    if (r.status === 200) return r.body as UpbitCandle[];

    // 429: exponential backoff
    if (r.status === 429) {
      const backoff = jitter(Math.min(20_000, 800 * Math.pow(2, attempt)));
      await sleep(backoff);
      continue;
    }

    // 기타 5xx도 재시도
    if (r.status >= 500 && r.status <= 599) {
      const backoff = jitter(Math.min(15_000, 600 * Math.pow(2, attempt)));
      await sleep(backoff);
      continue;
    }

    // 4xx는 즉시 실패
    throw new Error(`Upbit candles error: ${r.status} ${String(r.body).slice(0, 300)}`);
  }

  throw new Error(`Upbit candles failed after retries: ${params.market} ${params.tf}`);
}
