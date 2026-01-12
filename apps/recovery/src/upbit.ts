import axios, { AxiosError } from "axios";
import { config } from "./config.js";
import { RateLimiter, withRetry } from "./retry.js";
import { kstIsoToUnixSec } from "./time.js";

export type UpbitCandle = {
  market: string;
  candle_date_time_utc: string;
  candle_date_time_kst: string;
  opening_price: number;
  high_price: number;
  low_price: number;
  trade_price: number;
  timestamp: number;
  candle_acc_trade_price: number;
  candle_acc_trade_volume: number;
  unit: number;
};

export type CandleRow = {
  market: string;
  tf: string;
  time: number; // unix seconds (kst open time)
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

const http = axios.create({
  baseURL: "https://api.upbit.com",
  timeout: 15000,
  headers: { Accept: "application/json" }
});

const limiter = new RateLimiter(config.upbit.rps);

function isAxiosError(e: unknown): e is AxiosError {
  return !!(e && typeof e === "object" && (e as any).isAxiosError);
}

function shouldRetryUpbit(e: unknown): { retry: boolean; waitMs?: number; reason?: string } {
  if (!isAxiosError(e)) return { retry: false };
  const status = e.response?.status;

  // 429: 레이트리밋
  if (status === 429) {
    const ra = e.response?.headers?.["retry-after"];
    const waitMs = ra ? Number(ra) * 1000 : undefined;
    return { retry: true, waitMs, reason: "429 rate limited" };
  }

  // 5xx, timeout, network
  if (!status) return { retry: true, reason: "network/timeout" };
  if (status >= 500) return { retry: true, reason: `server ${status}` };

  // 400: Upbit는 때때로 잘못된 to/count 조합이나 비정상 파라미터에서 발생
  // -> 동일 요청 재시도는 의미 없을 수도 있으니 기본은 재시도 안함
  return { retry: false, reason: `status ${status}` };
}

function tfToUpbitUnit(tf: string): number {
  const t = tf.trim().toLowerCase();
  if (t === "1h") return 60;
  if (t === "4h") return 240;
  if (t.endsWith("m")) return Number(t.slice(0, -1));
  if (t.endsWith("h")) return Number(t.slice(0, -1)) * 60;
  throw new Error(`Unsupported tf: ${tf}`);
}

export async function fetchMinuteCandles(params: {
  market: string;
  tf: string; // "1m" | "5m" | ...
  toKstIso?: string; // inclusive-ish upper bound by Upbit semantics
  count: number; // <= 200
}): Promise<CandleRow[]> {
  const unit = tfToUpbitUnit(params.tf);

  await limiter.wait();

  const label = `candles ${params.market} ${params.tf} to=${params.toKstIso ?? "latest"} count=${params.count}`;

  return withRetry(
    async () => {
      const res = await http.get<UpbitCandle[]>(`/v1/candles/minutes/${unit}`, {
        params: {
          market: params.market,
          to: params.toKstIso,
          count: params.count
        }
      });

      const rows = res.data ?? [];
      // Upbit 응답은 최신 -> 과거 순. 우리는 asc로 변환
      const mapped: CandleRow[] = rows
        .map((c) => ({
          market: c.market,
          tf: params.tf,
          time: kstIsoToUnixSec(c.candle_date_time_kst),
          open: Number(c.opening_price),
          high: Number(c.high_price),
          low: Number(c.low_price),
          close: Number(c.trade_price),
          volume: Number(c.candle_acc_trade_volume)
        }))
        .sort((a, b) => a.time - b.time);

      return mapped;
    },
    shouldRetryUpbit,
    { label }
  );
}
