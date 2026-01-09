export type Timeframe = "5m" | "15m" | "1h" | "4h";

export type Candle = {
  time: number; // seconds (Unix timestamp in seconds)
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;
};

export type SnapshotResponse = {
  candles: Candle[];
};

export type WsMessage =
  | { type: "candle"; tf: Timeframe; candle: Candle }
  | { type: "snapshot_ack"; tf: Timeframe }
  | { type: "ping" }
  | { type: string; [k: string]: any };
