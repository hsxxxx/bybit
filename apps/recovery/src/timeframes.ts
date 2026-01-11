// src/timeframes.ts
export type TfKey = "1m" | "3m" | "5m" | "10m" | "15m" | "30m" | "1h" | "4h";

export const TF_SEC: Record<TfKey, number> = {
  "1m": 60,
  "3m": 180,
  "5m": 300,
  "10m": 600,
  "15m": 900,
  "30m": 1800,
  "1h": 3600,
  "4h": 14400,
};

export function tfKeyFromSec(tfSec: number): TfKey {
  const entry = Object.entries(TF_SEC).find(([, v]) => v === tfSec);
  if (!entry) throw new Error(`Unsupported tfSec=${tfSec}`);
  return entry[0] as TfKey;
}

export function floorToTfSec(epochMs: number, tfSec: number): number {
  const s = Math.floor(epochMs / 1000);
  return Math.floor(s / tfSec) * tfSec;
}

export function ceilToTfSec(epochMs: number, tfSec: number): number {
  const s = Math.floor(epochMs / 1000);
  return Math.ceil(s / tfSec) * tfSec;
}

export function toKstIsoNoMs(epochMs: number): string {
  // epochMs -> KST ISO (no ms): YYYY-MM-DDTHH:mm:ss
  const kst = new Date(epochMs + 9 * 3600 * 1000);
  const y = kst.getUTCFullYear();
  const m = String(kst.getUTCMonth() + 1).padStart(2, "0");
  const d = String(kst.getUTCDate()).padStart(2, "0");
  const hh = String(kst.getUTCHours()).padStart(2, "0");
  const mm = String(kst.getUTCMinutes()).padStart(2, "0");
  const ss = String(kst.getUTCSeconds()).padStart(2, "0");
  return `${y}-${m}-${d}T${hh}:${mm}:${ss}`;
}
