import dayjs from "dayjs";

/**
 * Upbit candle_date_time_kst: "2026-01-01T00:08:00"
 * -> unix seconds (KST 기준의 candle open time)
 *
 * 주의: JS Date는 UTC로 파싱할 수 있으므로, KST 문자열을 +09:00로 명시한다.
 */
export function kstIsoToUnixSec(kstIso: string): number {
  // "2026-01-01T00:08:00" -> "2026-01-01T00:08:00+09:00"
  const iso = kstIso.endsWith("Z") || kstIso.includes("+") ? kstIso : `${kstIso}+09:00`;
  const ms = dayjs(iso).valueOf();
  return Math.floor(ms / 1000);
}

export function unixSecToKstIso(unixSec: number): string {
  // KST로 고정 표기
  const d = new Date(unixSec * 1000);
  // UTC ms +09:00 표현을 위해 toISOString() 기반으로 shift 하는 방식 대신,
  // 한국환경(Asia/Seoul) 의존을 피하려면 수동으로 +9h.
  const kstMs = d.getTime() + 9 * 60 * 60 * 1000;
  const kst = new Date(kstMs).toISOString().replace(".000Z", "");
  return kst; // "YYYY-MM-DDTHH:mm:ss"
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
