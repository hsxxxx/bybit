// apps/recovery/src/indicators/core.ts
import type { Candle, IndicatorRow } from "../types.js";

// 외부 라이브러리 없이 최소한만 구현 (필요한 것만)
// - MA, RSI, StochRSI, OBV, PVT, BB(20,2), ret
export const DEFAULT_PARAMS = {
  bbLen: 20,
  bbMult: 2,
  rsiLen: 14,
  stochLen: 14,
  maLens: [7, 10, 20, 50, 60, 200, 400, 800],
  version: "v1"
};

function sma(values: number[], i: number, len: number): number | null {
  if (i + 1 < len) return null;
  let s = 0;
  for (let k = i - len + 1; k <= i; k++) s += values[k];
  return s / len;
}

function std(values: number[], i: number, len: number): number | null {
  const m = sma(values, i, len);
  if (m == null) return null;
  let v = 0;
  for (let k = i - len + 1; k <= i; k++) {
    const d = values[k] - m;
    v += d * d;
  }
  return Math.sqrt(v / len);
}

function rsi(closes: number[], len: number): Array<number | null> {
  const out: Array<number | null> = Array(closes.length).fill(null);
  let gain = 0;
  let loss = 0;

  for (let i = 1; i < closes.length; i++) {
    const ch = closes[i] - closes[i - 1];
    const g = ch > 0 ? ch : 0;
    const l = ch < 0 ? -ch : 0;

    if (i <= len) {
      gain += g;
      loss += l;
      if (i === len) {
        const avgG = gain / len;
        const avgL = loss / len;
        const rs = avgL === 0 ? Infinity : avgG / avgL;
        out[i] = 100 - 100 / (1 + rs);
      }
      continue;
    }

    // Wilder smoothing
    gain = (gain * (len - 1) + g) / len;
    loss = (loss * (len - 1) + l) / len;

    const rs = loss === 0 ? Infinity : gain / loss;
    out[i] = 100 - 100 / (1 + rs);
  }
  return out;
}

function stoch(values: Array<number | null>, len: number): Array<number | null> {
  const out: Array<number | null> = Array(values.length).fill(null);
  for (let i = 0; i < values.length; i++) {
    if (i + 1 < len) continue;
    // 구간 내 null 있으면 skip
    let minV = Infinity;
    let maxV = -Infinity;
    let ok = true;
    for (let k = i - len + 1; k <= i; k++) {
      const v = values[k];
      if (v == null) { ok = false; break; }
      minV = Math.min(minV, v);
      maxV = Math.max(maxV, v);
    }
    if (!ok) continue;
    const cur = values[i]!;
    const denom = (maxV - minV);
    out[i] = denom === 0 ? 0 : ((cur - minV) / denom) * 100;
  }
  return out;
}

function smaSeries(values: Array<number | null>, len: number): Array<number | null> {
  const out: Array<number | null> = Array(values.length).fill(null);
  for (let i = 0; i < values.length; i++) {
    if (i + 1 < len) continue;
    let s = 0;
    for (let k = i - len + 1; k <= i; k++) {
      const v = values[k];
      if (v == null) { s = NaN; break; }
      s += v;
    }
    if (Number.isNaN(s)) continue;
    out[i] = s / len;
  }
  return out;
}

export function computeIndicatorsForCandles(candles: Candle[], params = DEFAULT_PARAMS): IndicatorRow[] {
  if (candles.length === 0) return [];

  const closes = candles.map(c => c.close);
  const volumes = candles.map(c => c.volume);

  // returns
  const ret1: Array<number | null> = Array(closes.length).fill(null);
  const ret5: Array<number | null> = Array(closes.length).fill(null);
  for (let i = 1; i < closes.length; i++) {
    ret1[i] = closes[i - 1] === 0 ? null : (closes[i] / closes[i - 1] - 1);
  }
  for (let i = 5; i < closes.length; i++) {
    ret5[i] = closes[i - 5] === 0 ? null : (closes[i] / closes[i - 5] - 1);
  }

  // MA
  const maMap: Record<number, Array<number | null>> = {};
  for (const L of params.maLens) {
    maMap[L] = Array(closes.length).fill(null);
    for (let i = 0; i < closes.length; i++) maMap[L][i] = sma(closes, i, L);
  }

  // BB(20,2)
  const bbMid: Array<number | null> = Array(closes.length).fill(null);
  const bbUp: Array<number | null> = Array(closes.length).fill(null);
  const bbDn: Array<number | null> = Array(closes.length).fill(null);
  for (let i = 0; i < closes.length; i++) {
    const m = sma(closes, i, params.bbLen);
    const sd = std(closes, i, params.bbLen);
    if (m == null || sd == null) continue;
    bbMid[i] = m;
    bbUp[i] = m + params.bbMult * sd;
    bbDn[i] = m - params.bbMult * sd;
  }

  // RSI / StochRSI
  const rsi14 = rsi(closes, params.rsiLen);
  const stochRsi = stoch(rsi14, params.stochLen);
  const stochK = stochRsi;                 // K는 그대로
  const stochD = smaSeries(stochK, 3);     // D는 3 SMA

  // OBV / PVT
  const obv: Array<number | null> = Array(closes.length).fill(null);
  const pvt: Array<number | null> = Array(closes.length).fill(null);
  obv[0] = 0;
  pvt[0] = 0;
  for (let i = 1; i < closes.length; i++) {
    const dir = closes[i] > closes[i - 1] ? 1 : closes[i] < closes[i - 1] ? -1 : 0;
    obv[i] = (obv[i - 1] ?? 0) + dir * volumes[i];
    const r = closes[i - 1] === 0 ? 0 : (closes[i] - closes[i - 1]) / closes[i - 1];
    pvt[i] = (pvt[i - 1] ?? 0) + r * volumes[i];
  }

  // output
  const out: IndicatorRow[] = [];
  for (let i = 0; i < candles.length; i++) {
    const c = candles[i];
    out.push({
      market: c.market,
      tf: c.tf,
      time: c.time,

      bb_mid_20: bbMid[i],
      bb_upper_20_2: bbUp[i],
      bb_lower_20_2: bbDn[i],

      rsi_14: rsi14[i],
      stoch_rsi_k_14: stochK[i],
      stoch_rsi_d_14: stochD[i],

      obv: obv[i],
      pvt: pvt[i],

      ma_7: maMap[7]?.[i],
      ma_10: maMap[10]?.[i],
      ma_20: maMap[20]?.[i],
      ma_50: maMap[50]?.[i],
      ma_60: maMap[60]?.[i],
      ma_200: maMap[200]?.[i],
      ma_400: maMap[400]?.[i],
      ma_800: maMap[800]?.[i],

      ret_1: ret1[i],
      ret_5: ret5[i],

      indicator_version: params.version
    });
  }
  return out;
}
