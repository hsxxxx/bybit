export type Timeframe = "1m" | "5m" | "15m" | "1h" | "4h";

export type Exchange = "bybit";

export type CandleSource =
  | "collector_kline" // collector에서 WS kline(확정봉) 수신
  | "builder_agg"; // builder가 1m을 집계해 만든 상위 TF

export type Candle = {
  exchange: Exchange;
  market: string; // Bybit: symbol (ex: ETHUSDT)
  tf: Timeframe;
  open_time: number; // ms, bucket start
  close_time: number; // ms, bucket end (exclusive)
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number; // base asset volume
  is_closed: boolean; // true when finalized (collector confirm=true)
  source: CandleSource;
};

export function tfToMs(tf: Timeframe): number {
  switch (tf) {
    case "1m":
      return 60_000;
    case "5m":
      return 5 * 60_000;
    case "15m":
      return 15 * 60_000;
    case "1h":
      return 60 * 60_000;
    case "4h":
      return 4 * 60 * 60_000;
  }
}

export function bucketStart(tsMs: number, tf: Timeframe): number {
  const ms = tfToMs(tf);
  return Math.floor(tsMs / ms) * ms;
}

export function makeEmptyAgg(
  exchange: Exchange,
  market: string,
  tf: Timeframe,
  openTime: number,
  first: Pick<Candle, "open" | "high" | "low" | "close" | "volume">
): Candle {
  const ms = tfToMs(tf);
  return {
    exchange,
    market,
    tf,
    open_time: openTime,
    close_time: openTime + ms,
    open: first.open,
    high: first.high,
    low: first.low,
    close: first.close,
    volume: first.volume,
    is_closed: false,
    source: "builder_agg",
  };
}

export function updateAgg(agg: Candle, c1m: Candle): Candle {
  agg.high = Math.max(agg.high, c1m.high);
  agg.low = Math.min(agg.low, c1m.low);
  agg.close = c1m.close;
  agg.volume += c1m.volume;
  return agg;
}
