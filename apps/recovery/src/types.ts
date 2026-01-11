export type Timeframe = "1m" | "5m" | "15m" | "1h" | "4h";

export type UpbitCandle = {
  market: string;
  candle_date_time_utc: string;
  candle_date_time_kst: string;
  opening_price: number;
  high_price: number;
  low_price: number;
  trade_price: number;
  candle_acc_trade_volume: number;
  timestamp: number; // ms
  unit?: number;
};

export type CandleRow = {
  market: string;
  tf: Timeframe;
  time: number; // unix seconds (open time)
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

export type IndicatorRow = {
  market: string;
  tf: Timeframe;
  time: number; // unix seconds (same candle.time)
  indicators: Record<string, unknown>;
};
