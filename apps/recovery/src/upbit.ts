import { request } from "undici";
import type { Timeframe, UpbitCandle } from "./types";
import { tfToMinutes } from "./utils/time";

const BASE = "https://api.upbit.com";

export async function fetchMarkets(marketFilter?: string): Promise<string[]> {
  const url = `${BASE}/v1/market/all?isDetails=false`;
  const res = await request(url, { method: "GET" });
  if (res.statusCode !== 200) {
    throw new Error(`Upbit markets error: ${res.statusCode}`);
  }
  const data = (await res.body.json()) as Array<{ market: string }>;
  const markets = data.map((d) => d.market);
  if (!marketFilter) return markets;
  return markets.filter((m) => m.startsWith(marketFilter));
}

export async function fetchCandlesChunk(params: {
  market: string;
  tf: Timeframe;
  toKstIso?: string; // "YYYY-MM-DDTHH:mm:ss"
  count: number; // <= 200
}): Promise<UpbitCandle[]> {
  const unit = tfToMinutes(params.tf);
  const qs: string[] = [`market=${encodeURIComponent(params.market)}`, `count=${params.count}`];
  if (params.toKstIso) qs.push(`to=${encodeURIComponent(params.toKstIso)}`);

  const url = `${BASE}/v1/candles/minutes/${unit}?${qs.join("&")}`;
  const res = await request(url, { method: "GET" });

  // Upbit는 429가 자주 나서 status 체크
  if (res.statusCode === 429) {
    throw new Error("Upbit rate limited (429)");
  }
  if (res.statusCode !== 200) {
    const text = await res.body.text();
    throw new Error(`Upbit candles error: ${res.statusCode} ${text}`);
  }

  const data = (await res.body.json()) as UpbitCandle[];
  return data;
}
