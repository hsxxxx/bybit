export type Timeframe = '1m' | '5m' | '15m' | '1h' | '4h';

export type Candle = {
  exchange: 'upbit';
  market: string;
  tf: Timeframe;
  open_time: number;
  close_time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  is_closed: boolean;
  source: string;
};

export type Indicator = {
  exchange: 'upbit';
  market: string;
  tf: Timeframe;
  open_time: number;
  close_time: number;
  close: number;
  volume: number;

  bb_mid_20: number | null;
  bb_upper_20_2: number | null;
  bb_lower_20_2: number | null;
  bb_width_20: number | null;
  bb_pos_20: number | null;

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

  rsi_14: number | null;
  stoch_rsi_k: number | null;
  stoch_rsi_d: number | null;

  obv: number | null;
  obv_slope_5: number | null;
  obv_slope_20: number | null;

  pvt: number | null;
  pvt_slope_5: number | null;
  pvt_slope_20: number | null;

  ret_1: number | null;
  ret_5: number | null;

  indicator_version: 'v1';
};

export type SnapshotResponse = {
  market: string;
  tf: Timeframe;
  candles: Candle[];
  indicators: Indicator[];
};
