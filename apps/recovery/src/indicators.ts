import type { CandleRow } from "./db.js";

/**
 * 간단 구현 (외부 TA-Lib 없이)
 * - SMA
 * - RSI(14)
 * - StochRSI(14)
 * - BB(20, 2)
 * - OBV
 * - PVT
 */

function sma(values: number[], period: number, idx: number): number | null {
  if (idx + 1 < period) return null;
  let sum = 0;
  for (let i = idx - period + 1; i <= idx; i++) sum += values[i];
  return sum / period;
}

function std(values: number[], period: number, idx: number): number | null {
  const m = sma(values, period, idx);
  if (m == null) return null;
  let v = 0;
  for (let i = idx - period + 1; i <= idx; i++) {
    const d = values[i] - m;
    v += d * d;
  }
  return Math.sqrt(v / period);
}

function rsi(values: number[], period: number, idx: number): number | null {
  if (idx < period) return null;
  let gain = 0;
  let loss = 0;
  for (let i = idx - period + 1; i <= idx; i++) {
    const ch = values[i] - values[i - 1];
    if (ch >= 0) gain += ch;
    else loss += -ch;
  }
  if (gain === 0 && loss === 0) return 50;
  if (loss === 0) return 100;
  const rs = gain / loss;
  return 100 - 100 / (1 + rs);
}

function stoch(values: number[], period: number, idx: number): number | null {
  if (idx + 1 < period) return null;
  let lo = Infinity;
  let hi = -Infinity;
  for (let i = idx - period + 1; i <= idx; i++) {
    lo = Math.min(lo, values[i]);
    hi = Math.max(hi, values[i]);
  }
  const denom = hi - lo;
  if (denom === 0) return 0;
  return ((values[idx] - lo) / denom) * 100;
}

export type IndicatorPack = Record<string, number | null>;

export function computeIndicators(candlesAsc: CandleRow[]): IndicatorPack[] {
  const closes = candlesAsc.map((c) => c.close);
  const volumes = candlesAsc.map((c) => c.volume);

  const out: IndicatorPack[] = [];

  let obv = 0;
  let pvt = 0;

  // StochRSI 계산용 RSI 배열
  const rsiArr: (number | null)[] = new Array(closes.length).fill(null);

  for (let i = 0; i < candlesAsc.length; i++) {
    // OBV
    if (i === 0) {
      obv = 0;
      pvt = 0;
    } else {
      const prevClose = closes[i - 1];
      const curClose = closes[i];

      if (curClose > prevClose) obv += volumes[i];
      else if (curClose < prevClose) obv -= volumes[i];

      if (prevClose !== 0) {
        pvt += ((curClose - prevClose) / prevClose) * volumes[i];
      }
    }

    const rsi14 = rsi(closes, 14, i);
    rsiArr[i] = rsi14;

    // StochRSI(14): RSI 값에 대해 stoch(14)
    const stochRsi14 =
      rsi14 == null ? null : stoch(rsiArr.map((x) => x ?? 0), 14, i);

    const sma20 = sma(closes, 20, i);
    const std20 = std(closes, 20, i);
    const bbUpper = sma20 == null || std20 == null ? null : sma20 + 2 * std20;
    const bbLower = sma20 == null || std20 == null ? null : sma20 - 2 * std20;

    const sma10 = sma(closes, 10, i);
    const sma60 = sma(closes, 60, i);
    const sma120 = sma(closes, 120, i);

    out.push({
      sma_10: sma10,
      sma_20: sma20,
      sma_60: sma60,
      sma_120: sma120,
      rsi_14: rsi14,
      stoch_rsi_14: stochRsi14,
      bb_upper_20_2: bbUpper,
      bb_lower_20_2: bbLower,
      obv,
      pvt
    });
  }

  return out;
}
