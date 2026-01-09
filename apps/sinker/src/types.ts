// sinker/src/types.ts
export type Timeframe = "1m" | "3m" | "5m" | "15m" | "30m" | "1h" | "2h" | "4h" | "1d";

export type Candle = {
  time: number; // unix seconds
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

export type BuiltPayload = {
  market: string; // e.g. BTCUSDT or KRW-BTC
  tf: Timeframe;
  candle: Candle;

  // optional: builder가 같이 내보내는 지표들
  indicators?: Record<string, number | null>;
};

export type SnapshotResponse = {
  market: string;
  tf: Timeframe;
  candles: Array<Candle & { indicators?: Record<string, number | null> }>;
  from?: number;
  to?: number;
  count: number;
};
