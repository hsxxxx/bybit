// src/indicators.ts
export type Candle = {
  time: number; // unix seconds (open time)
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

function sma(arr: number[], period: number, idx: number): number | null {
  if (idx + 1 < period) return null;
  let s = 0;
  for (let i = idx - period + 1; i <= idx; i++) s += arr[i];
  return s / period;
}

function std(arr: number[], period: number, idx: number): number | null {
  const m = sma(arr, period, idx);
  if (m == null) return null;
  let v = 0;
  for (let i = idx - period + 1; i <= idx; i++) {
    const d = arr[i] - m;
    v += d * d;
  }
  return Math.sqrt(v / period);
}

function emaSeries(arr: number[], period: number): (number | null)[] {
  const out: (number | null)[] = new Array(arr.length).fill(null);
  const k = 2 / (period + 1);

  let prev: number | null = null;
  for (let i = 0; i < arr.length; i++) {
    const v = arr[i];
    if (i + 1 < period) continue;

    if (prev === null) {
      let s = 0;
      for (let j = i - period + 1; j <= i; j++) s += arr[j];
      prev = s / period;
      out[i] = prev;
      continue;
    }

    prev = v * k + prev * (1 - k);
    out[i] = prev;
  }
  return out;
}

function rsiSeries(closes: number[], period: number): (number | null)[] {
  const out: (number | null)[] = new Array(closes.length).fill(null);
  if (closes.length < period + 1) return out;

  let gain = 0;
  let loss = 0;

  for (let i = 1; i <= period; i++) {
    const diff = closes[i] - closes[i - 1];
    if (diff >= 0) gain += diff;
    else loss -= diff;
  }

  let avgGain = gain / period;
  let avgLoss = loss / period;

  const rs0 = avgLoss === 0 ? Infinity : avgGain / avgLoss;
  out[period] = 100 - 100 / (1 + rs0);

  for (let i = period + 1; i < closes.length; i++) {
    const diff = closes[i] - closes[i - 1];
    const g = diff > 0 ? diff : 0;
    const l = diff < 0 ? -diff : 0;

    avgGain = (avgGain * (period - 1) + g) / period;
    avgLoss = (avgLoss * (period - 1) + l) / period;

    const rs = avgLoss === 0 ? Infinity : avgGain / avgLoss;
    out[i] = 100 - 100 / (1 + rs);
  }

  return out;
}

function roc(closes: number[], period: number, idx: number): number | null {
  if (idx < period) return null;
  const prev = closes[idx - period];
  if (prev === 0) return null;
  return ((closes[idx] - prev) / prev) * 100;
}

export function computeIndicators(candlesAsc: Candle[]) {
  const closes = candlesAsc.map((c) => c.close);
  const vols = candlesAsc.map((c) => c.volume);

  const ema12 = emaSeries(closes, 12);
  const ema26 = emaSeries(closes, 26);

  const macdLine: (number | null)[] = closes.map((_, i) => {
    if (ema12[i] == null || ema26[i] == null) return null;
    return (ema12[i] as number) - (ema26[i] as number);
  });

  const macdFill = macdLine.map((v) => (v == null ? 0 : v));
  const macdSignal = emaSeries(macdFill, 9);

  const rsi14 = rsiSeries(closes, 14);

  let obv = 0;
  let pvt = 0;

  const out = candlesAsc.map((c, i) => {
    if (i > 0) {
      if (closes[i] > closes[i - 1]) obv += vols[i];
      else if (closes[i] < closes[i - 1]) obv -= vols[i];

      const prev = closes[i - 1];
      if (prev !== 0) pvt += ((closes[i] - prev) / prev) * vols[i];
    }

    const mid20 = sma(closes, 20, i);
    const sd20 = std(closes, 20, i);
    const upper = mid20 != null && sd20 != null ? mid20 + 2 * sd20 : null;
    const lower = mid20 != null && sd20 != null ? mid20 - 2 * sd20 : null;

    const macd = macdLine[i];
    const sig = macdSignal[i];
    const hist = macd != null && sig != null ? macd - sig : null;

    return {
      sma_10: sma(closes, 10, i),
      sma_60: sma(closes, 60, i),
      sma_120: sma(closes, 120, i),

      ema_12: ema12[i],
      ema_26: ema26[i],

      rsi_14: rsi14[i],

      macd,
      macd_signal: sig,
      macd_hist: hist,

      bb_mid_20: mid20,
      bb_upper_20_2: upper,
      bb_lower_20_2: lower,

      obv,
      pvt,
      roc_20: roc(closes, 20, i),
    };
  });

  return out;
}
