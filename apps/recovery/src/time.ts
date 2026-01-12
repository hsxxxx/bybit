import dayjs from "dayjs";

/**
 * Upbit candle_date_time_kst: "2026-01-01T00:08:00"
 * -> unix seconds (KST 기준 candle open time)
 */
export function kstIsoToUnixSec(kstIso: string): number {
  const iso = kstIso.endsWith("Z") || kstIso.includes("+") ? kstIso : `${kstIso}+09:00`;
  const ms = dayjs(iso).valueOf();
  return Math.floor(ms / 1000);
}

/**
 * unix seconds -> "YYYY-MM-DDTHH:mm:ss" (KST, timezone suffix 없이)
 * Upbit `to` 파라미터에 그대로 넣기 위함.
 */
export function unixSecToKstIso(unixSec: number): string {
  // dayjs는 utcOffset(분) 지원 (플러그인 불필요)
  return dayjs.unix(unixSec).utcOffset(9 * 60).format("YYYY-MM-DDTHH:mm:ss");
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
