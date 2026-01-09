import type { Candle } from "./types";
import { ensureSeconds, isFiniteNumber } from "@/utils/time";

/**
 * ✅ time ASC 정렬 + time 중복 제거
 * - 동일 time이 여러 개면 "마지막 값"을 남김
 */
export function normalizeCandlesAscUnique(raw: Candle[]): Candle[] {
  if (!Array.isArray(raw) || raw.length === 0) return [];

  const filtered = raw
    .filter(
      (c) =>
        c &&
        isFiniteNumber(c.time) &&
        isFiniteNumber(c.open) &&
        isFiniteNumber(c.high) &&
        isFiniteNumber(c.low) &&
        isFiniteNumber(c.close) &&
        (c.volume === undefined || isFiniteNumber(c.volume))
    )
    .map((c) => ({
      ...c,
      time: ensureSeconds(c.time),
    }));

  filtered.sort((a, b) => a.time - b.time);

  const map = new Map<number, Candle>();
  for (const c of filtered) map.set(c.time, c);

  return Array.from(map.values()).sort((a, b) => a.time - b.time);
}

/**
 * ✅ 이미 ASC+unique인 base에 incoming 1개 업서트
 * - same time -> overwrite(last)
 * - newer -> push
 * - older/out-of-order -> merge 후 normalize(안전)
 */
export function upsertCandleAscUnique(base: Candle[], incoming: Candle): Candle[] {
  const c: Candle = { ...incoming, time: ensureSeconds(incoming.time) };
  if (!base.length) return [c];

  const last = base[base.length - 1];

  if (c.time === last.time) {
    const next = base.slice();
    next[next.length - 1] = c;
    return next;
  }

  if (c.time > last.time) {
    return [...base, c];
  }

  // out-of-order 방어 (드물지만 StrictMode/WS burst 때 발생 가능)
  return normalizeCandlesAscUnique([...base, c]);
}
