// src/recovery.ts
import { TF_SEC, floorToTfSec } from "./timeframes.js";
import { fetchUpbitCandlesRange, type UpbitCandle } from "./exchange/upbit.js";

export type CandleRow = {
  exchange: "upbit";
  market: string;
  tf: string;
  open_time: number;  // ms (candle start)
  close_time: number; // ms (candle end)
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

function assert(cond: unknown, msg: string): asserts cond {
  if (!cond) throw new Error(msg);
}

function candleKstMs(c: UpbitCandle): number {
  return new Date(`${c.candle_date_time_kst}+09:00`).getTime();
}

function toCandleRow(market: string, tf: keyof typeof TF_SEC, tfSec: number, c: UpbitCandle): CandleRow {
  const start = candleKstMs(c);
  const end = start + tfSec * 1000;

  return {
    exchange: "upbit",
    market,
    tf,
    open_time: start,
    close_time: end,
    open: c.opening_price,
    high: c.high_price,
    low: c.low_price,
    close: c.trade_price,
    volume: c.candle_acc_trade_volume,
  };
}

export async function runRecovery(args: {
  market: string;
  tf: keyof typeof TF_SEC;
  fromIsoKst: string;
  toIsoKst: string;
  limitPerReq?: number;
  throttleMs?: number;
}): Promise<CandleRow[]> {
  const parseKstIso = (s: string): number => {
    const hasTz = /Z$|[+-]\d{2}:\d{2}$/.test(s);
    const iso = hasTz ? s : `${s}+09:00`;
    return new Date(iso).getTime();
  };

  const tfSec = TF_SEC[args.tf];
  assert(tfSec, `Unsupported tf=${args.tf}`);

  const fromMs = parseKstIso(args.fromIsoKst);
  const toMs = parseKstIso(args.toIsoKst);
  assert(fromMs < toMs, "from < to");

  // candle start 경계로 정렬
  const fromAligned = floorToTfSec(fromMs, tfSec) * 1000;
  const toAligned = floorToTfSec(toMs, tfSec) * 1000;

  const candles = await fetchUpbitCandlesRange({
    market: args.market,
    tfSec,
    fromMs: fromAligned,
    toMs: toAligned,
    limitPerReq: args.limitPerReq,
    throttleMs: args.throttleMs,
    // 진행 로그(멈춘 것처럼 보이는 문제 해결)
    onPage: ({ market, got, cursorKst }) => {
      if (process.env.RECOVERY_DEBUG === "1") {
        console.log(`[page] ${args.tf} ${market} got=${got} cursor=${cursorKst}`);
      }
    },
  });

  const rows = candles.map((c) => toCandleRow(args.market, args.tf, tfSec, c));

  // open_time 기준 uniq
  const m = new Map<number, CandleRow>();
  for (const r of rows) m.set(r.open_time, r);

  return Array.from(m.values()).sort((a, b) => a.open_time - b.open_time);
}
