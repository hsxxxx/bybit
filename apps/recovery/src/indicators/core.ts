// apps/recovery/src/indicators/core.ts
import type { Candle, IndicatorRow } from "../types.js";
import { sma, stddev, clamp } from "./math.js";

export type IndicatorParams = {
  bbPeriod: number;     // 20
  bbStdMult: number;    // 2
  rsiPeriod: number;    // 14
  stochPeriod: number;  // 14
  stochSmoothK: number; // 3
  stochSmoothD: number; // 3
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
  volumes: number[];

  // RSI
  prevClose: number | null;
  avgGain: number | null;
  avgLoss: number | null;

  // StochRSI
  rsiSeries: number[];
  stochKSeries: number[];

  // OBV/PVT
  obv: number;
  pvt: number;
};

export function createIndicatorState(): State {
  return {
    closes: [],
    volumes: [],
    prevClose: null,
    avgGain: null,
    avgLoss: null,
    rsiSeries: [],
    stochKSeries: [],
    obv: 0,
    pvt: 0
  };
}

function updateRsi(state: State, close: number, period: number): number | null {
  if (state.prevClose == null) {
    state.prevClose = close;
    return null;
  }

  const change = close - state.prevClose;
  const gain = Math.max(0, change);
  const loss = Math.max(0, -change);

  if (state.avgGain == null || state.avgLoss == null) {
    // warmup: until we have "period" changes, approximate via SMA of gains/losses
    // we store temporary in rsiSeries as "change" for warmup? keep simple:
    // Use closes[] length to determine warmup window
    const n = state.closes.length; // after push close, includes current
    // Need at least period+1 closes to compute first RSI with simple average
    if (n < period + 1) {
      state.prevClose = close;
      return null;
    }

    let sumGain = 0;
    let sumLoss = 0;
    for (let i = n - (period + 1) + 1; i < n; i++) {
      const c0 = state.closes[i - 1];
      const c1 = state.closes[i];
      const d = c1 - c0;
      sumGain += Math.max(0, d);
      sumLoss += Math.max(0, -d);
    }
    state.avgGain = sumGain / period;
    state.avgLoss = sumLoss / period;

    const rs = state.avgLoss === 0 ? Infinity : state.avgGain / state.avgLoss;
    const rsi = 100 - 100 / (1 + rs);

    state.prevClose = close;
    return rsi;
  }

  // Wilder smoothing
  state.avgGain = (state.avgGain * (period - 1) + gain) / period;
  state.avgLoss = (state.avgLoss * (period - 1) + loss) / period;

  const rs = state.avgLoss === 0 ? Infinity : state.avgGain / state.avgLoss;
  const rsi = 100 - 100 / (1 + rs);

  state.prevClose = close;
  return rsi;
}

function stochRsiK(state: State, rsi: number, period: number): number | null {
  state.rsiSeries.push(rsi);
  if (state.rsiSeries.length < period) return null;

  const window = state.rsiSeries.slice(state.rsiSeries.length - period);
  let lo = Infinity, hi = -Infinity;
  for (const v of window) { lo = Math.min(lo, v); hi = Math.max(hi, v); }
  const denom = hi - lo;
  const k = denom === 0 ? 0 : ((rsi - lo) / denom) * 100;
  return clamp(k, 0, 100);
}

function smoothSma(series: number[], period: number): number | null {
  return sma(series, period);
}

export function computeIndicatorsForCandles(
  candlesAsc: Candle[],
  params: IndicatorParams = DEFAULT_PARAMS
): IndicatorRow[] {
  const st = createIndicatorState();
  const out: IndicatorRow[] = [];

  for (const c of candlesAsc) {
    st.closes.push(c.close);
    st.volumes.push(c.volume);

    // MA
    const ma10 = sma(st.closes, params.ma10);
    const ma20 = sma(st.closes, params.ma20);
    const ma60 = sma(st.closes, params.ma60);

    // BB
    const bbMid = sma(st.closes, params.bbPeriod);
    const bbStd = stddev(st.closes, params.bbPeriod);
    const bbUpper = bbMid != null && bbStd != null ? bbMid + params.bbStdMult * bbStd : null;
    const bbLower = bbMid != null && bbStd != null ? bbMid - params.bbStdMult * bbStd : null;

    // RSI
    const rsi = updateRsi(st, c.close, params.rsiPeriod);
    let stochK: number | null = null;
    let stochD: number | null = null;

    if (rsi != null) {
      const k0 = stochRsiK(st, rsi, params.stochPeriod);
      if (k0 != null) {
        st.stochKSeries.push(k0);
        stochK = smoothSma(st.stochKSeries, params.stochSmoothK);
        if (stochK != null) {
          // D = SMA(K, smoothD)
          const kSeries = st.stochKSeries;
          const d = sma(kSeries, params.stochSmoothD);
          stochD = d;
        }
      }
    }

    // OBV
    if (st.prevClose != null) {
      const prev = st.prevClose;
      if (c.close > prev) st.obv += c.volume;
      else if (c.close < prev) st.obv -= c.volume;
    }

    // PVT
    if (st.prevClose != null && st.prevClose !== 0) {
      const prev = st.prevClose;
      st.pvt += ((c.close - prev) / prev) * c.volume;
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
