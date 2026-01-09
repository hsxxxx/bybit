import type { Candle } from "./types";

export function calcRSI(closes: number[], length = 14): (number | null)[] {
  const out: (number | null)[] = Array(closes.length).fill(null);
  if (closes.length < length + 1) return out;

  let gain = 0;
  let loss = 0;

  for (let i = 1; i <= length; i++) {
    const diff = closes[i] - closes[i - 1];
    if (diff >= 0) gain += diff;
    else loss -= diff;
  }

  gain /= length;
  loss /= length;

  let rs = loss === 0 ? Infinity : gain / loss;
  out[length] = 100 - 100 / (1 + rs);

  for (let i = length + 1; i < closes.length; i++) {
    const diff = closes[i] - closes[i - 1];
    const g = diff > 0 ? diff : 0;
    const l = diff < 0 ? -diff : 0;

    gain = (gain * (length - 1) + g) / length;
    loss = (loss * (length - 1) + l) / length;

    rs = loss === 0 ? Infinity : gain / loss;
    out[i] = 100 - 100 / (1 + rs);
  }

  return out;
}

export function calcStochRSI(
  rsi: (number | null)[],
  stochLen = 14,
  kLen = 3,
  dLen = 3
): { k: (number | null)[]; d: (number | null)[] } {
  const n = rsi.length;
  const stoch: (number | null)[] = Array(n).fill(null);

  for (let i = 0; i < n; i++) {
    if (i < stochLen - 1) continue;

    const window = rsi
      .slice(i - stochLen + 1, i + 1)
      .filter((v): v is number => v !== null);

    if (window.length !== stochLen) continue;

    const min = Math.min(...window);
    const max = Math.max(...window);
    const cur = rsi[i];
    if (cur === null) continue;

    const denom = max - min;
    stoch[i] = denom === 0 ? 0 : ((cur - min) / denom) * 100;
  }

  const sma = (arr: (number | null)[], len: number) => {
    const out: (number | null)[] = Array(arr.length).fill(null);
    for (let i = 0; i < arr.length; i++) {
      if (i < len - 1) continue;
      const win = arr
        .slice(i - len + 1, i + 1)
        .filter((v): v is number => v !== null);
      if (win.length !== len) continue;
      out[i] = win.reduce((a, b) => a + b, 0) / len;
    }
    return out;
  };

  const k = sma(stoch, kLen);
  const d = sma(k, dLen);
  return { k, d };
}

export function calcOBV(candles: Candle[]): (number | null)[] {
  const out: (number | null)[] = Array(candles.length).fill(null);
  if (!candles.length) return out;

  let obv = 0;
  out[0] = obv;

  for (let i = 1; i < candles.length; i++) {
    const prevClose = candles[i - 1].close;
    const curClose = candles[i].close;
    const vol = candles[i].volume ?? 0;

    if (curClose > prevClose) obv += vol;
    else if (curClose < prevClose) obv -= vol;

    out[i] = obv;
  }

  return out;
}

export function calcPVT(candles: Candle[]): (number | null)[] {
  const out: (number | null)[] = Array(candles.length).fill(null);
  if (!candles.length) return out;

  let pvt = 0;
  out[0] = pvt;

  for (let i = 1; i < candles.length; i++) {
    const prevClose = candles[i - 1].close;
    const curClose = candles[i].close;
    const vol = candles[i].volume ?? 0;

    if (prevClose !== 0) pvt += ((curClose - prevClose) / prevClose) * vol;
    out[i] = pvt;
  }

  return out;
}
