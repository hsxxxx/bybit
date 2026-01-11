// src/recovery.ts
import { TF_SEC, floorToTfSec } from "./timeframes.js";
import { fetchUpbitCandlesRange, type UpbitCandle } from "./exchange/upbit.js";

export type CandleRow = {
  exchange: "upbit";
  market: string;
  tf: string;
  open_time: number; // ms
  close_time: number; // ms
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

function assert(cond: unknown, msg: string): asserts cond {
  if (!cond) throw new Error(msg);
}

function tfSecFromTf(tf: keyof typeof TF_SEC): number {
  return TF_SEC[tf];
}

function toCandleRow(market: string, tf: keyof typeof TF_SEC, c: UpbitCandle): CandleRow {
  const tfSec = tfSecFromTf(tf);
  const close_time = c.timestamp; // ms (Upbit)
  const open_time = (Math.floor(close_time / 1000) - tfSec) * 1000;

  return {
    exchange: "upbit",
    market,
    tf,
    open_time,
    close_time,
    open: c.opening_price,
    high: c.high_price,
    low: c.low_price,
    close: c.trade_price,
    volume: c.candle_acc_trade_volume,
  };
}

export async function recoverUpbitCandles(params: {
  market: string;
  tf: keyof typeof TF_SEC;
  fromMs: number;
  toMs: number;
  limitPerReq?: number;
  throttleMs?: number;
}): Promise<CandleRow[]> {
  const { market, tf } = params;
  const tfSec = tfSecFromTf(tf);

  assert(params.fromMs < params.toMs, "fromMs must be < toMs");

  const fromAlignedSec = floorToTfSec(params.fromMs, tfSec);
  const toAlignedSec = floorToTfSec(params.toMs, tfSec);

  const fromAlignedMs = fromAlignedSec * 1000;
  const toAlignedMs = (toAlignedSec + tfSec) * 1000; // 안전 +1

  const candles = await fetchUpbitCandlesRange({
    market,
    tfSec,
    fromMs: fromAlignedMs,
    toMs: Math.min(toAlignedMs, params.toMs),
    limitPerReq: params.limitPerReq,
    throttleMs: params.throttleMs,
  });

  const rows = candles.map((c) => toCandleRow(market, tf, c));

  const m = new Map<number, CandleRow>();
  for (const r of rows) m.set(r.close_time, r);

  return Array.from(m.values()).sort((a, b) => a.close_time - b.close_time);
}

/**
 * index.ts에서 호출하는 엔트리 함수
 */
export async function runRecovery(args: {
  market: string;
  tf: keyof typeof TF_SEC;
  fromIsoKst: string; // "YYYY-MM-DDTHH:mm:ss" (tz 없으면 KST로 간주)
  toIsoKst: string;
  limitPerReq?: number;
  throttleMs?: number;
}): Promise<CandleRow[]> {
  const parseKstIso = (s: string): number => {
    const hasTz = /Z$|[+-]\d{2}:\d{2}$/.test(s);
    const iso = hasTz ? s : `${s}+09:00`;
    return new Date(iso).getTime();
  };

  const fromMs = parseKstIso(args.fromIsoKst);
  const toMs = parseKstIso(args.toIsoKst);

  return recoverUpbitCandles({
    market: args.market,
    tf: args.tf,
    fromMs,
    toMs,
    limitPerReq: args.limitPerReq,
    throttleMs: args.throttleMs,
  });
}
