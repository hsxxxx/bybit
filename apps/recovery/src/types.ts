// apps/recovery/src/types.ts
export type Tf = "1m" | "3m" | "5m" | "10m" | "15m" | "30m" | "1h" | "4h" | "1d";

export type Candle = {
  market: string;
  tf: Tf;
  time: number; // unix seconds (KST 기준 candle open time)
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

export type IndicatorRow = {
  market: string;
  tf: Tf;
  time: number;

  // 필요한 것만 채워서 JSON으로 저장함
  bb_mid_20?: number | null;
  bb_upper_20_2?: number | null;
  bb_lower_20_2?: number | null;
  rsi_14?: number | null;
  stoch_rsi_k_14?: number | null;
  stoch_rsi_d_14?: number | null;
  obv?: number | null;
  pvt?: number | null;

  ma_7?: number | null;
  ma_10?: number | null;
  ma_20?: number | null;
  ma_50?: number | null;
  ma_60?: number | null;
  ma_200?: number | null;
  ma_400?: number | null;
  ma_800?: number | null;

  ret_1?: number | null;
  ret_5?: number | null;

  indicator_version?: string | null;
};

export type RecoveryTask = "candle" | "indicator" | "both";
export type NoTradeMode = "skip" | "mark" | "mark_db";

export type RecoveryConfig = {
  task: RecoveryTask;

  markets: string[];
  tfs: Tf[];

  startSec: number; // inclusive
  endSec: number;   // exclusive

  maxMissingPerMarket: number;
  restConcurrency: number;
  restSleepMs: number;

  // Upbit 호출 간 최소 간격 (429 완화)
  upbitMinIntervalMs: number;

  // indicator 계산 warmup bar 수
  indicatorLookbackBars: number;

  // 거래 없는 구간 처리
  noTradeMode: NoTradeMode;              // skip|mark|mark_db
  noTradeMarkWholeRange: boolean;        // fetch range에서 [start,end)내 캔들이 0개면 전체 슬롯을 no-trade로 마킹

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
