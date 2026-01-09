// bits/apps/viewer/src/lib/market/types.ts

export type Timeframe = "1m" | "3m" | "5m" | "15m" | "30m" | "1h" | "4h" | "1d";

export type Candle = {
  market?: string; // viewer-ws에서 (candle as any).market 쓰고 있으니 optional로 둠
  time: number; // unix seconds
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;

  [key: string]: unknown;
};

/**
 * viewer-ws.ts 에서 "indicator" 메시지의 data로 쓰는 타입
 * - getTime(ind) 가 동작하려면 time:number 가 있어야 함
 * - market 은 indicator에도 붙어오는 구조라 optional로 둠
 */
export type Indicator = {
  market?: string;
  tf?: Timeframe;

  time: number; // unix seconds

  // 어떤 지표인지 식별 (필요한 것만 확장)
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

  // 지표 값(단일/복수 모두 커버)
  value?: number;
  values?: Record<string, number>;

  [key: string]: unknown;
};
