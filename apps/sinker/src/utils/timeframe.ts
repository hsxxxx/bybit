// sinker/src/utils/timeframe.ts
import type { Timeframe } from "../types";

const TF_TO_SEC: Record<Timeframe, number> = {
  "1m": 60,
  "3m": 180,
  "5m": 300,
  "15m": 900,
  "30m": 1800,
  "1h": 3600,
  "2h": 7200,
  "4h": 14400,
  "1d": 86400
};

export function tfToSec(tf: Timeframe): number {
  return TF_TO_SEC[tf];
}

export function parseTimeframe(tf: string): Timeframe {
  const norm = tf.trim();
  if (norm in TF_TO_SEC) return norm as Timeframe;
  throw new Error(`Unsupported timeframe: ${tf}`);
}

export function yyyymmddFromUnixSec(ts: number): string {
  const d = new Date(ts * 1000);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}${m}${day}`;
}

export function unixSecNow(): number {
  return Math.floor(Date.now() / 1000);
}
