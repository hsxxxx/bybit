import type { Timeframe } from "../types";

export function parseKstToEpochSeconds(kst: string): number {
  const iso = kst.includes("+") ? kst : `${kst}+09:00`;
  const ms = Date.parse(iso);
  if (!Number.isFinite(ms)) throw new Error(`Invalid KST datetime: ${kst}`);
  return Math.floor(ms / 1000);
}

export function parseKstTextToEpochSeconds(kstText: string): number {
  const norm = kstText.trim().replace(" ", "T");
  return parseKstToEpochSeconds(norm);
}

export function tfToMinutes(tf: Timeframe): number {
  switch (tf) {
    case "1m":
      return 1;
    case "5m":
      return 5;
    case "15m":
      return 15;
    case "1h":
      return 60;
    case "4h":
      return 240;
  }
}

export function tfStepSeconds(tf: Timeframe): number {
  return tfToMinutes(tf) * 60;
}

/**
 * Upbit candles "to"는 safest가 UTC ISO(Z)임
 * epoch seconds -> "YYYY-MM-DDTHH:mm:ssZ"
 */
export function utcIsoFromEpochSeconds(epochSec: number): string {
  return new Date(epochSec * 1000).toISOString().replace(".000Z", "Z");
}

/**
 * 디버깅용: epoch -> KST ISO(+09:00)
 */
export function kstIsoWithOffsetFromEpochSeconds(epochSec: number): string {
  const d = new Date(epochSec * 1000);
  const kst = new Date(d.getTime() + 9 * 3600 * 1000);
  const yyyy = kst.getUTCFullYear();
  const mm = String(kst.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(kst.getUTCDate()).padStart(2, "0");
  const hh = String(kst.getUTCHours()).padStart(2, "0");
  const mi = String(kst.getUTCMinutes()).padStart(2, "0");
  const ss = String(kst.getUTCSeconds()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}T${hh}:${mi}:${ss}+09:00`;
}
