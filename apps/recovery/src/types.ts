// apps/recovery/src/types.ts
export type Tf = "1m" | "3m" | "5m" | "10m" | "15m" | "30m" | "1h" | "4h" | "1d";

export type Candle = {
  market: string;
  tf: Tf;
  time: number; // unix seconds (candle open time)  ✅ schema aligns
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

export type RecoveryMode = "republish" | "direct_db";

export type RecoveryConfig = {
  mode: RecoveryMode;

  markets: string[];
  tfs: Tf[];

  startSec: number; // inclusive
  endSec: number;   // exclusive

  maxMissingPerMarket: number;
  restConcurrency: number;
  restSleepMs: number;

  kafka: {
    enabled: boolean;
    brokers: string[];
    clientId: string;
    topicCandle: string;
    batchBytes: number;
  };

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
