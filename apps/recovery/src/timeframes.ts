// apps/recovery/src/timeframes.ts  (기존 유지 + 1d 지원만 추가/전체)
import type { Tf } from "./types.js";

export const TF_SEC: Record<Tf, number> = {
  "1m": 60,
  "3m": 180,
  "5m": 300,
  "10m": 600,
  "15m": 900,
  "30m": 1800,
  "1h": 3600,
  "4h": 14400,
  "1d": 86400
};

export function floorToTfSec(t: number, tf: Tf): number {
  const step = TF_SEC[tf];
  return Math.floor(t / step) * step;
}

export function rangeSlotsSec(startSec: number, endSec: number, tf: Tf): number[] {
  const step = TF_SEC[tf];
  const s = floorToTfSec(startSec, tf);
  const e = floorToTfSec(endSec - 1, tf) + step;
  const out: number[] = [];
  for (let t = s; t < e; t += step) out.push(t);
  return out;
}

export function toUpbitMinutes(tf: Tf): number {
  switch (tf) {
    case "1m": return 1;
    case "3m": return 3;
    case "5m": return 5;
    case "10m": return 10;
    case "15m": return 15;
    case "30m": return 30;
    case "1h": return 60;
    case "4h": return 240;
    default:
      throw new Error(`Upbit minutes not supported for tf=${tf}`);
  }
}
