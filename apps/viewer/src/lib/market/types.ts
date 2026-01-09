// bits/apps/viewer/src/lib/market/types.ts

export type Timeframe = "1m" | "3m" | "5m" | "15m" | "30m" | "1h" | "4h" | "1d";

export type Candle = {
  market: string;
  tf: Timeframe;

  time: number; // unix seconds
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;

  [key: string]: unknown;
};

export type Indicator = {
  market: string;
  tf: Timeframe;

  time: number; // unix seconds

  kind:
    | "rsi"
    | "stoch_rsi"
    | "obv"
    | "pvt"
    | "macd"
    | "macd_signal"
    | "macd_hist"
    | "bb"
    | "ma";

  value?: number;
  values?: Record<string, number>;

  [key: string]: unknown;
};

export type SnapshotResponse = {
  market: string;
  tf: Timeframe;
  candles: Candle[];
  indicators: Indicator[];
};

export type WsMessage =
  | { type: "hello"; tfs: Timeframe[]; defaultMarket: string; defaultTf: Timeframe }
  | { type: "subscribed"; market: string; tf: Timeframe; include: ("candle" | "indicator")[] }
  | { type: "unsubscribed" }
  | { type: "candle"; market: string; tf: Timeframe; data: Candle }
  | { type: "indicator"; market: string; tf: Timeframe; data: Indicator }
  | { type: "pong" }
  | { type: "error"; message: string };
