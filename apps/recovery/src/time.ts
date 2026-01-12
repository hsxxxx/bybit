/**
 * "YYYY-MM-DDTHH:mm:ss" 를 KST(+09:00)로 강제 파싱해서 unix seconds 반환
 * dayjs 의존 제거 (환경/플러그인/타입 이슈 방지)
 */
export function kstIsoToUnixSec(kstIso: string): number {
  const s = kstIso.includes("Z") || kstIso.includes("+") ? kstIso : `${kstIso}+09:00`;
  const ms = Date.parse(s);
  if (!Number.isFinite(ms)) {
    throw new Error(`Invalid KST ISO: ${kstIso}`);
  }
  return Math.floor(ms / 1000);
}

/**
 * unix seconds -> "YYYY-MM-DDTHH:mm:ss" (KST)
 * Upbit to 파라미터에 사용 (timezone suffix 없이)
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
