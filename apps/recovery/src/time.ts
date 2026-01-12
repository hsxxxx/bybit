/**
 * "YYYY-MM-DDTHH:mm:ss" (KST) -> unix seconds
 */
export function kstIsoToUnixSec(kstIso: string): number {
  const s = kstIso.includes("Z") || kstIso.includes("+") ? kstIso : `${kstIso}+09:00`;
  const ms = Date.parse(s);
  if (!Number.isFinite(ms)) throw new Error(`Invalid KST ISO: ${kstIso}`);
  return Math.floor(ms / 1000);
}

/**
 * unix seconds -> "YYYY-MM-DDTHH:mm:ss" (KST, suffix 없음)
 */
export function unixSecToKstIso(unixSec: number): string {
  const kstMs = unixSec * 1000 + 9 * 60 * 60 * 1000;
  return new Date(kstMs).toISOString().slice(0, 19);
}

/**
 * unix seconds -> "YYYY-MM-DDTHH:mm:ss+09:00" (Upbit to 파라미터용, 가장 안전)
 */
export function unixSecToUpbitToParam(unixSec: number): string {
  return `${unixSecToKstIso(unixSec)}+09:00`;
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
