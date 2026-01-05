import type { Candle } from './candle.js';

export type Feature1m = {
  exchange: 'upbit';
  market: string;
  tf: '1m';

  open_time: number;  // candle open_time 기준으로 feature 키를 맞춤
  close_time: number;

  close: number;
  volume: number;

  ret_1m: number | null;
  ret_5m: number | null;

  vol_sum_5m: number | null;
  vol_z_20: number | null;

  ma_20: number | null;
  ma_60: number | null;
  dist_ma20: number | null; // (close/ma - 1)
  dist_ma60: number | null;

  score: number; // 단순 스코어(필터링용)
  feature_version: 'v1';
};

export function pctChange(a: number, b: number): number {
  // (b/a - 1)
  if (a === 0) return 0;
  return b / a - 1;
}

export function mean(xs: number[]): number {
  return xs.reduce((s, x) => s + x, 0) / xs.length;
}

export function stdev(xs: number[]): number {
  const m = mean(xs);
  const v = xs.reduce((s, x) => s + (x - m) * (x - m), 0) / xs.length;
  return Math.sqrt(v);
}

export function sma(values: number[], period: number): number | null {
  if (values.length < period) return null;
  const slice = values.slice(values.length - period);
  return mean(slice);
}

export function zscore(values: number[], period: number): number | null {
  if (values.length < period) return null;
  const slice = values.slice(values.length - period);
  const m = mean(slice);
  const sd = stdev(slice);
  if (sd === 0) return 0;
  return (values[values.length - 1] - m) / sd;
}

/**
 * 아주 단순한 스코어(초기용):
 * - 거래량 zscore가 높고
 * - 5분 수익률이 양수이고
 * - MA20/MA60 위에 있으면 가산
 */
export function computeScore(params: {
  ret5: number | null;
  volZ: number | null;
  dist20: number | null;
  dist60: number | null;
}): number {
  const { ret5, volZ, dist20, dist60 } = params;

  let s = 0;

  if (volZ != null) s += Math.max(-3, Math.min(3, volZ));     // clamp
  if (ret5 != null) s += Math.max(-2, Math.min(2, ret5 * 50)); // 2%면 +1 정도
  if (dist20 != null) s += Math.max(-1, Math.min(1, dist20 * 20));
  if (dist60 != null) s += Math.max(-1, Math.min(1, dist60 * 20));

  return s;
}

export function buildFeature1m(market: string, c: Candle, window1m: Candle[]): Feature1m {
  // window1m includes c as last element (closed candles only)
  const closes = window1m.map(x => x.close);
  const vols = window1m.map(x => x.volume);

  const prev = window1m.length >= 2 ? window1m[window1m.length - 2] : null;
  const ret1 = prev ? pctChange(prev.close, c.close) : null;

  const c5 = window1m.length >= 6 ? window1m[window1m.length - 6] : null; // 5분 전 close
  const ret5 = c5 ? pctChange(c5.close, c.close) : null;

  const volSum5 = window1m.length >= 5
    ? window1m.slice(window1m.length - 5).reduce((s, x) => s + x.volume, 0)
    : null;

  const ma20 = sma(closes, 20);
  const ma60 = sma(closes, 60);

  const dist20 = ma20 ? (c.close / ma20 - 1) : null;
  const dist60 = ma60 ? (c.close / ma60 - 1) : null;

  const volZ20 = zscore(vols, 20);

  const score = computeScore({
    ret5,
    volZ: volZ20,
    dist20,
    dist60
  });

  return {
    exchange: 'upbit',
    market,
    tf: '1m',
    open_time: c.open_time,
    close_time: c.close_time,
    close: c.close,
    volume: c.volume,
    ret_1m: ret1,
    ret_5m: ret5,
    vol_sum_5m: volSum5,
    vol_z_20: volZ20,
    ma_20: ma20,
    ma_60: ma60,
    dist_ma20: dist20,
    dist_ma60: dist60,
    score,
    feature_version: 'v1'
  };
}
