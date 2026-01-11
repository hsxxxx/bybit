import type { Tf } from "../store/types.js";

export function tfFromTopic(topic: string): Tf | null {
  // upbit.candle.1m / upbit.indicator.4h
  const parts = topic.split(".");
  const last = parts[parts.length - 1];
  // normalize
  if (last === "1m" || last === "3m" || last === "5m" || last === "10m" || last === "15m" || last === "30m" || last === "1h" || last === "4h" || last === "1d") {
    return last as Tf;
  }
  return null;
}
