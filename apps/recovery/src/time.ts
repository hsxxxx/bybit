import dayjs from "dayjs";

/**
 * "2026-01-01T00:08:00" (KST) -> unix seconds
 */
export function kstIsoToUnixSec(kstIso: string): number {
  const iso = kstIso.endsWith("Z") || kstIso.includes("+")
    ? kstIso
    : `${kstIso}+09:00`;
  return Math.floor(dayjs(iso).valueOf() / 1000);
}

/**
 * unix seconds -> "YYYY-MM-DDTHH:mm:ss" (KST)
 * dayjs plugin 없이 안전하게 처리
 */
export function unixSecToKstIso(unixSec: number): string {
  const kstMs = unixSec * 1000 + 9 * 60 * 60 * 1000;
  return new Date(kstMs).toISOString().slice(0, 19);
}

export function tfToMinutes(tf: string): number {
  const t = tf.trim().toLowerCase();
  if (t.endsWith("m")) return Number(t.slice(0, -1));
  if (t === "1h") return 60;
  if (t === "4h") return 240;
  if (t.endsWith("h")) return Number(t.slice(0, -1)) * 60;
  throw new Error(`Unsupported tf: ${tf}`);
}

export function floorToTf(unixSec: number, tfMinutes: number): number {
  const step = tfMinutes * 60;
  return Math.floor(unixSec / step) * step;
}

export function buildTimeGrid(startSec: number, endSec: number, tfMinutes: number): number[] {
  const step = tfMinutes * 60;
  const s = floorToTf(startSec, tfMinutes);
  const e = floorToTf(endSec, tfMinutes);
  const out: number[] = [];
  for (let t = s; t <= e; t += step) out.push(t);
  return out;
}
