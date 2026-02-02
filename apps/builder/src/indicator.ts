// apps/builder/src/indicator.ts
import type { Candle, Timeframe, Exchange } from "./candle.js";

export type Indicator = {
  exchange: Exchange;
  market: string;
  tf: Timeframe;

  open_time: number;
  close_time: number;
  close: number;
  volume: number;

  // Bollinger Bands (20, 2)
  bb_mid_20: number | null;
  bb_upper_20_2: number | null;
  bb_lower_20_2: number | null;
  bb_width_20: number | null; // (upper-lower)/mid
  bb_pos_20: number | null; // (close-lower)/(upper-lower)

  // MA (50, 200, 400, 800) + special 7 (esp for 4h)
  ma_7: number | null;
  ma_50: number | null;
  ma_200: number | null;
  ma_400: number | null;
  ma_800: number | null;

  dist_ma7: number | null;
  dist_ma50: number | null;
  dist_ma200: number | null;
  dist_ma400: number | null;
  dist_ma800: number | null;

  // RSI(14)
  rsi_14: number | null;

  // Stoch RSI (rsiLen=14, stochLen=14, smoothK=3, smoothD=3)
  stoch_rsi_k: number | null;
  stoch_rsi_d: number | null;

  // OBV / PVT
  obv: number | null;
  obv_slope_5: number | null;
  obv_slope_20: number | null;

  pvt: number | null;
  pvt_slope_5: number | null;
  pvt_slope_20: number | null;

  // returns
  ret_1: number | null;
  ret_5: number | null;

  indicator_version: "v1";
};

function mean(xs: number[]): number {
  return xs.reduce((s, x) => s + x, 0) / xs.length;
}
function stdev(xs: number[]): number {
  const m = mean(xs);
  const v = xs.reduce((s, x) => s + (x - m) * (x - m), 0) / xs.length;
  return Math.sqrt(v);
}
function sma(values: number[], period: number): number | null {
  if (values.length < period) return null;
  return mean(values.slice(values.length - period));
}
function pctChange(a: number, b: number): number {
  if (a === 0) return 0;
  return b / a - 1;
}

function rsi14(closes: number[]): number | null {
  const period = 14;
  if (closes.length < period + 1) return null;

  let gains = 0;
  let losses = 0;
  for (let i = closes.length - period; i < closes.length; i++) {
    const diff = closes[i] - closes[i - 1];
    if (diff >= 0) gains += diff;
    else losses += -diff;
  }
  const avgGain = gains / period;
  const avgLoss = losses / period;
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return 100 - 100 / (1 + rs);
}

function stochRsi(closes: number[]): { k: number | null; d: number | null } {
  const rsiLen = 14,
    stochLen = 14,
    smoothK = 3,
    smoothD = 3;
  const need = rsiLen + stochLen + smoothK + smoothD + 5;
  if (closes.length < need) return { k: null, d: null };

  const rsiSeries: number[] = [];
  const start = Math.max(0, closes.length - (stochLen + smoothK + smoothD + 20));
  for (let i = start + rsiLen; i < closes.length; i++) {
    const slice = closes.slice(0, i + 1);
    const r = rsi14(slice);
    if (r != null) rsiSeries.push(r);
  }
  if (rsiSeries.length < stochLen + smoothK + smoothD) return { k: null, d: null };

  const raw: number[] = [];
  for (let i = stochLen - 1; i < rsiSeries.length; i++) {
    const win = rsiSeries.slice(i - (stochLen - 1), i + 1);
    const lo = Math.min(...win);
    const hi = Math.max(...win);
    const cur = rsiSeries[i];
    const v = hi === lo ? 0 : (cur - lo) / (hi - lo);
    raw.push(v * 100);
  }

  const kArr: number[] = [];
  for (let i = smoothK - 1; i < raw.length; i++) {
    kArr.push(mean(raw.slice(i - (smoothK - 1), i + 1)));
  }
  if (kArr.length < smoothD) return { k: null, d: null };

  const k = kArr[kArr.length - 1];
  const d = mean(kArr.slice(kArr.length - smoothD));
  return { k, d };
}

function bb20(closes: number[]) {
  const period = 20;
  if (closes.length < period) return { mid: null, upper: null, lower: null, width: null, pos: null };
  const win = closes.slice(closes.length - period);
  const m = mean(win);
  const sd = stdev(win);
  const upper = m + 2 * sd;
  const lower = m - 2 * sd;
  const width = m === 0 ? null : (upper - lower) / m;
  const denom = upper - lower;
  const pos = denom === 0 ? null : (closes[closes.length - 1] - lower) / denom;
  return { mid: m, upper, lower, width, pos };
}

function slope(values: number[], lookback: number): number | null {
  if (values.length < lookback + 1) return null;
  const a = values[values.length - lookback - 1];
  const b = values[values.length - 1];
  return b - a;
}

export function buildIndicatorFromWindow(tf: Timeframe, window: Candle[]): Indicator {
  const c = window[window.length - 1];
  const closes = window.map((x) => x.close);

  const bb = bb20(closes);

  const ma7 = sma(closes, 7);
  const ma50 = sma(closes, 50);
  const ma200 = sma(closes, 200);
  const ma400 = sma(closes, 400);
  const ma800 = sma(closes, 800);

  const dist = (ma: number | null) => (ma ? c.close / ma - 1 : null);

  const rsi = rsi14(closes);
  const st = stochRsi(closes);

  const obvSeries: number[] = [];
  const pvtSeries: number[] = [];
  let obv = 0;
  let pvt = 0;
  for (let i = 0; i < window.length; i++) {
    const cur = window[i];
    if (i === 0) {
      obvSeries.push(obv);
      pvtSeries.push(pvt);
      continue;
    }
    const prev = window[i - 1];
    if (cur.close > prev.close) obv += cur.volume;
    else if (cur.close < prev.close) obv -= cur.volume;

    const chg = pctChange(prev.close, cur.close);
    pvt += cur.volume * chg;
    obvSeries.push(obv);
    pvtSeries.push(pvt);
  }

  const ret1 = window.length >= 2 ? pctChange(window[window.length - 2].close, c.close) : null;
  const ret5 = window.length >= 6 ? pctChange(window[window.length - 6].close, c.close) : null;

  return {
    exchange: c.exchange,
    market: c.market,
    tf,
    open_time: c.open_time,
    close_time: c.close_time,
    close: c.close,
    volume: c.volume,

    bb_mid_20: bb.mid,
    bb_upper_20_2: bb.upper,
    bb_lower_20_2: bb.lower,
    bb_width_20: bb.width,
    bb_pos_20: bb.pos,

    ma_7: ma7,
    ma_50: ma50,
    ma_200: ma200,
    ma_400: ma400,
    ma_800: ma800,

    dist_ma7: dist(ma7),
    dist_ma50: dist(ma50),
    dist_ma200: dist(ma200),
    dist_ma400: dist(ma400),
    dist_ma800: dist(ma800),

    rsi_14: rsi,
    stoch_rsi_k: st.k,
    stoch_rsi_d: st.d,

    obv: obvSeries[obvSeries.length - 1] ?? null,
    obv_slope_5: slope(obvSeries, 5),
    obv_slope_20: slope(obvSeries, 20),

    pvt: pvtSeries[pvtSeries.length - 1] ?? null,
    pvt_slope_5: slope(pvtSeries, 5),
    pvt_slope_20: slope(pvtSeries, 20),

    ret_1: ret1,
    ret_5: ret5,

    indicator_version: "v1",
  };
}
