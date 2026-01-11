export type Tf = "1m" | "3m" | "5m" | "10m" | "15m" | "30m" | "1h" | "4h" | "1d";

export type Candle = {
  market: string;
  tf: Tf;
  time: number; // unix seconds (open time)
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

export type Indicator = {
  market: string;
  tf: Tf;
  time: number;
  indicators: Record<string, unknown>;
};

export type StreamKind = "candle" | "indicator";
