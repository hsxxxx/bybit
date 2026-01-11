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
  time: number;
  bb_mid_20?: number | null;
  bb_upper_20_2?: number | null;
  bb_lower_20_2?: number | null;
  rsi_14?: number | null;
  stoch_rsi_k_14?: number | null;
  stoch_rsi_d_14?: number | null;
  obv?: number | null;
  pvt?: number | null;
  ma_10?: number | null;
  ma_20?: number | null;
  ma_60?: number | null;
};

export type RecoveryTask = "candle" | "indicator" | "both";

// ✅ no-trade 처리 모드
// - "skip": 빈 캔들([])인 range는 이번 실행에서만 스킵 (다음번엔 다시 시도 가능)
// - "mark": 빈 캔들([])인 range의 슬롯들을 'no-trade'로 메모리에 기록해 missing에서 제외
// - "mark_db": 'no-trade'를 DB 테이블에 영구 기록 (추천)
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
  upbitMinIntervalMs: number;

  indicatorLookbackBars: number;

  // ✅ 빈 캔들([]) 처리
  noTradeMode: NoTradeMode;
  // range가 []이면 그 range 전체 슬롯을 no-trade로 처리할지(기본 true)
  noTradeMarkWholeRange: boolean;

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
