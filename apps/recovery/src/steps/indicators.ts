export type IndicatorOutput = {
  ma10?: number;
  ma60?: number;
  rsi14?: number;
  bb_mid_20?: number;
  bb_upper_20_2?: number;
  bb_lower_20_2?: number;
};

function sma(values: number[], period: number, idx: number): number | undefined {
  if (idx + 1 < period) return undefined;
  let sum = 0;
  for (let i = idx; i > idx - period; i--) sum += values[i];
  return sum / period;
}

function stddev(values: number[], period: number, idx: number): number | undefined {
  const m = sma(values, period, idx);
  if (m === undefined) return undefined;
  let v = 0;
  for (let i = idx; i > idx - period; i--) {
    const d = values[i] - m;
    v += d * d;
  }
  return Math.sqrt(v / period);
}

function rsi(values: number[], period: number, idx: number): number | undefined {
  if (idx < period) return undefined;
  let gain = 0;
  let loss = 0;
  for (let i = idx - period + 1; i <= idx; i++) {
    const diff = values[i] - values[i - 1];
    if (diff >= 0) gain += diff;
    else loss -= diff;
  }
  const avgGain = gain / period;
  const avgLoss = loss / period;
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return 100 - 100 / (1 + rs);
}

export function calcIndicatorsForSeries(closes: number[], idx: number): IndicatorOutput {
  const ma10 = sma(closes, 10, idx);
  const ma60 = sma(closes, 60, idx);

  const rsi14 = rsi(closes, 14, idx);

  const bbMid = sma(closes, 20, idx);
  const sd = stddev(closes, 20, idx);
  const bbUpper = bbMid !== undefined && sd !== undefined ? bbMid + 2 * sd : undefined;
  const bbLower = bbMid !== undefined && sd !== undefined ? bbMid - 2 * sd : undefined;

  return {
    ma10,
    ma60,
    rsi14,
    bb_mid_20: bbMid,
    bb_upper_20_2: bbUpper,
    bb_lower_20_2: bbLower
  };
}
