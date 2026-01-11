// apps/recovery/src/types.ts
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

export type IndicatorRow = {
  market: string;
  tf: Tf;
  time: number; // unix seconds (open time)
  // --- core indicators (extend freely) ---
  bb_mid_20?: number | null;
  bb_upper_20_2?: number | null;
  bb_lower_20_2?: number | null;

  rsi_14?: number | null;
  stoch_rsi_k_14?: number | null;
  stoch_rsi_d_14?: number | null;

  obv?: number | null;
  pvt?: number | null;

  // optional MAs (if you want)
  ma_10?: number | null;
  ma_20?: number | null;
  ma_60?: number | null;
};

export type RecoveryTask = "candle" | "indicator" | "both";

export type RecoveryConfig = {
  task: RecoveryTask;

  markets: string[];
  tfs: Tf[];

  startSec: number; // inclusive
  endSec: number;   // exclusive

  // candle missing guard
  maxMissingPerMarket: number;

  // REST pacing (Upbit 429 대응)
  restConcurrency: number;
  restSleepMs: number;
  upbitMinIntervalMs: number;

  // indicator warmup/lookback
  indicatorLookbackBars: number; // max window bars (ex: 600)

  db: {
    host: string;
    port: number;
    user: string;
    pass: string;
    name: string;
  };

  upbit: {
    baseUrl: string;
  };
};
