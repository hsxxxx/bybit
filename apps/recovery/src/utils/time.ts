import type { Timeframe } from "../types";

export function parseKstToEpochSeconds(kst: string): number {
  // Upbit kst: "YYYY-MM-DDTHH:mm:ss"
  // 반드시 +09:00 붙여서 파싱
  const iso = kst.includes("+") ? kst : `${kst}+09:00`;
  const ms = Date.parse(iso);
  if (!Number.isFinite(ms)) throw new Error(`Invalid KST datetime: ${kst}`);
  return Math.floor(ms / 1000);
}

export function parseKstTextToEpochSeconds(kstText: string): number {
  // "YYYY-MM-DD HH:mm:ss" -> "YYYY-MM-DDTHH:mm:ss"
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
    default: {
      const _exhaustive: never = tf;
      return _exhaustive;
    }
  }
}

export function minutesToSeconds(m: number): number {
  return m * 60;
}

export function tfStepSeconds(tf: Timeframe): number {
  return minutesToSeconds(tfToMinutes(tf));
}

export function kstIsoFromEpochSeconds(epochSec: number): string {
  // epoch -> KST ISO (to param 용)
  const d = new Date((epochSec + 9 * 3600) * 1000); // shift to KST for formatting
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const mi = String(d.getUTCMinutes()).padStart(2, "0");
  const ss = String(d.getUTCSeconds()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}T${hh}:${mi}:${ss}`; // KST local time string (no offset)
}
