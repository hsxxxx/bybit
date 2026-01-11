// apps/recovery/src/indicators/core.ts
import type { Candle, IndicatorRow } from "../types.js";
import { sma, stddev, clamp } from "./math.js";

export type IndicatorParams = {
  bbPeriod: number;
  bbStdMult: number;
  rsiPeriod: number;
  stochPeriod: number;
  stochSmoothK: number;
  stochSmoothD: number;
  ma10: number;
  ma20: number;
  ma60: number;
};

export const DEFAULT_PARAMS: IndicatorParams = {
  bbPeriod: 20,
  bbStdMult: 2,
  rsiPeriod: 14,
  stochPeriod: 14,
  stochSmoothK: 3,
  stochSmoothD: 3,
  ma10: 10,
  ma20: 20,
  ma60: 60
};

type State = {
  closes: number[];
  prevClose: number | null;
  avgGain: number | null;
  avgLoss: number | null;

  rsiSeries: number[];
  stochKSeries: number[];

  obv: number;
  pvt: number;
};

export function createIndicatorState(): State {
  return {
    closes: [],
    prevClose: null,
    avgGain: null,
    avgLoss: null,
    rsiSeries: [],
    stochKSeries: [],
    obv: 0,
    pvt: 0
  };
}

function updateRsi(st: State, close: number, period: number): number | null {
  if (st.prevClose == null) {
    st.prevClose = close;
    return null;
  }

  const change = close - st.prevClose;
  const gain = Math.max(0, change);
  const loss = Math.max(0, -change);

  if (st.avgGain == null || st.avgLoss == null) {
    const n = st.closes.length;
    if (n < period + 1) {
      st.prevClose = close;
      return null;
    }

    let sumGain = 0;
    let sumLoss = 0;
    for (let i = n - (period + 1) + 1; i < n; i++) {
      const c0 = st.closes[i - 1];
      const c1 = st.closes[i];
      const d = c1 - c0;
      sumGain += Math.max(0, d);
      sumLoss += Math.max(0, -d);
    }
    st.avgGain = sumGain / period;
    st.avgLoss = sumLoss / period;

    const rs = st.avgLoss === 0 ? Infinity : st.avgGain / st.avgLoss;
    const rsi = 100 - 100 / (1 + rs);

    st.prevClose = close;
    return rsi;
  }

  st.avgGain = (st.avgGain * (period - 1) + gain) / period;
  st.avgLoss = (st.avgLoss * (period - 1) + loss) / period;

  const rs = st.avgLoss === 0 ? Infinity : st.avgGain / st.avgLoss;
  const rsi = 100 - 100 / (1 + rs);

  st.prevClose = close;
  return rsi;
}

function stochRsiK(st: State, rsi: number, period: number): number | null {
  st.rsiSeries.push(rsi);
  if (st.rsiSeries.length < period) return null;

  const window = st.rsiSeries.slice(st.rsiSeries.length - period);
  let lo = Infinity, hi = -Infinity;
  for (const v of window) { lo = Math.min(lo, v); hi = Math.max(hi, v); }
  const denom = hi - lo;
  const k = denom === 0 ? 0 : ((rsi - lo) / denom) * 100;
  return clamp(k, 0, 100);
}

export function computeIndicatorsForCandles(candlesAsc: Candle[], params = DEFAULT_PARAMS): IndicatorRow[] {
  const st = createIndicatorState();
  const out: IndicatorRow[] = [];

  for (const c of candlesAsc) {
    st.closes.push(c.close);

    const ma10 = sma(st.closes, params.ma10);
    const ma20 = sma(st.closes, params.ma20);
    const ma60 = sma(st.closes, params.ma60);

    const bbMid = sma(st.closes, params.bbPeriod);
    const bbStd = stddev(st.closes, params.bbPeriod);
    const bbUpper = bbMid != null && bbStd != null ? bbMid + params.bbStdMult * bbStd : null;
    const bbLower = bbMid != null && bbStd != null ? bbMid - params.bbStdMult * bbStd : null;

    const rsi = updateRsi(st, c.close, params.rsiPeriod);

    let stochK: number | null = null;
    let stochD: number | null = null;
    if (rsi != null) {
      const k0 = stochRsiK(st, rsi, params.stochPeriod);
      if (k0 != null) {
        st.stochKSeries.push(k0);
        stochK = sma(st.stochKSeries, params.stochSmoothK);
        stochD = sma(st.stochKSeries, params.stochSmoothD);
      }
    }

    // OBV/PVT
    if (st.prevClose != null) {
      if (c.close > st.prevClose) st.obv += c.volume;
      else if (c.close < st.prevClose) st.obv -= c.volume;

      if (st.prevClose !== 0) {
        st.pvt += ((c.close - st.prevClose) / st.prevClose) * c.volume;
      }
    }

    out.push({
      market: c.market,
      tf: c.tf,
      time: c.time,

      bb_mid_20: bbMid,
      bb_upper_20_2: bbUpper,
      bb_lower_20_2: bbLower,

      rsi_14: rsi,
      stoch_rsi_k_14: stochK,
      stoch_rsi_d_14: stochD,

      obv: st.obv,
      pvt: st.pvt,

      ma_10: ma10,
      ma_20: ma20,
      ma_60: ma60
    });
  }

  return out;
}
