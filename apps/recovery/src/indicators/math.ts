// apps/recovery/src/indicators/math.ts
export function sma(values: number[], period: number): number | null {
  if (values.length < period) return null;
  let sum = 0;
  for (let i = values.length - period; i < values.length; i++) sum += values[i];
  return sum / period;
}

export function stddev(values: number[], period: number): number | null {
  const m = sma(values, period);
  if (m == null) return null;
  let s = 0;
  for (let i = values.length - period; i < values.length; i++) {
    const d = values[i] - m;
    s += d * d;
  }
  return Math.sqrt(s / period);
}

export function clamp(v: number, lo: number, hi: number) {
  return Math.max(lo, Math.min(hi, v));
}
