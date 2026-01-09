export function isFiniteNumber(v: any): v is number {
  return typeof v === "number" && Number.isFinite(v);
}

/**
 * 서버/WS가 ms를 줄 때가 있어서, heuristic으로 seconds로 통일.
 * - 1e12 이상이면 ms로 보고 /1000
 */
export function ensureSeconds(t: number): number {
  return t >= 1_000_000_000_000 ? Math.floor(t / 1000) : t;
}
