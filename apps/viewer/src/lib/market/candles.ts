import type { Candle } from "./types";
import { ensureSeconds, isFiniteNumber } from "@/utils/time";

/**
 * ✅ time ASC 정렬 + time 중복 제거
 * - 동일 time이 여러 개면 "마지막 값"을 남김
 * - 숫자 필드 유효성 검사 + time ms/sec 정규화(ensureSeconds)
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
      volume: c.volume ?? 0,
    }));

  // asc
  filtered.sort((a, b) => a.time - b.time);

  // unique (keep last)
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
  const c: Candle = {
    ...incoming,
    time: ensureSeconds(incoming.time),
    volume: incoming.volume ?? 0,
  };

  if (!base || base.length === 0) return [c];

  const last = base[base.length - 1];

  if (c.time === last.time) {
    const next = base.slice();
    next[next.length - 1] = c;
    return next;
  }

  if (c.time > last.time) {
    return [...base, c];
  }

  // out-of-order 방어 (StrictMode/WS burst 등)
  return normalizeCandlesAscUnique([...base, c]);
}

/**
 * ✅ (호환용) 내가 이전에 예시로 준 이름을 쓰는 코드가 있으면 깨질 수 있으니 alias 제공
 * - 기존 코드가 upsertCandleAscUnique 쓰면 이 함수는 필요없음
 */
export function mergeCandleIntoAsc(base: Candle[], incoming: Candle): Candle[] {
  return upsertCandleAscUnique(base, incoming);
}
