// bits/apps/viewer/src/lib/market/timeframes.ts
import type { Timeframe } from "./types";

export const ALLOWED_TFS: Timeframe[] = ["1m", "5m", "15m", "1h", "4h"];

export function isTimeframe(v: any): v is Timeframe {
  return typeof v === "string" && (ALLOWED_TFS as readonly string[]).includes(v);
}
