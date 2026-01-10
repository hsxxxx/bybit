// apps/recovery/src/exchange/upbit.ts
import type { Candle, Tf } from "../types.js";
import { TF_SEC, toUpbitMinutes, floorToTfSec } from "../timeframes.js";

type UpbitCandleRow = {
  market: string;
  candle_date_time_utc: string; // "YYYY-MM-DDTHH:mm:ss"
  opening_price: number;
  high_price: number;
  low_price: number;
  trade_price: number;
  candle_acc_trade_volume: number;
};

export async function fetchUpbitCandlesRange(params: {
  baseUrl: string;
  market: string;
  tf: Tf;          // upbit minutes supported only
  startSec: number; // inclusive
  endSec: number;   // exclusive
  sleepMs: number;
  signal?: AbortSignal;
}): Promise<Candle[]> {
  const unit = toUpbitMinutes(params.tf);
  const step = TF_SEC[params.tf];

  const out: Candle[] = [];
  let cursorSec = params.endSec;

  while (cursorSec > params.startSec) {
    const toIso = new Date(cursorSec * 1000).toISOString();
    const url = new URL(`${params.baseUrl}/v1/candles/minutes/${unit}`);
    url.searchParams.set("market", params.market);
    url.searchParams.set("to", toIso);
    url.searchParams.set("count", "200");

    const res = await fetch(url, { method: "GET", signal: params.signal });
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      throw new Error(`Upbit fetch failed ${res.status} ${res.statusText} ${txt}`.trim());
    }

    const rows = (await res.json()) as UpbitCandleRow[];
    if (!Array.isArray(rows) || rows.length === 0) break;

    for (const r of rows) {
      const openSec = floorToTfSec(parseUpbitUtcSec(r.candle_date_time_utc), params.tf);
      if (openSec < params.startSec || openSec >= params.endSec) continue;

      out.push({
        market: r.market,
        tf: params.tf,
        time: openSec,
        open: r.opening_price,
        high: r.high_price,
        low: r.low_price,
        close: r.trade_price,
        volume: r.candle_acc_trade_volume
      });
    }

    const oldest = rows[rows.length - 1];
    const oldestOpenSec = floorToTfSec(parseUpbitUtcSec(oldest.candle_date_time_utc), params.tf);

    if (oldestOpenSec >= cursorSec) cursorSec -= step * 200;
    else cursorSec = oldestOpenSec;

    if (params.sleepMs > 0) await sleep(params.sleepMs);
  }

  // asc + uniq by time
  out.sort((a, b) => a.time - b.time);
  const uniq: Candle[] = [];
  let prev = -1;
  for (const c of out) {
    if (c.time === prev) continue;
    prev = c.time;
    uniq.push(c);
  }
  return uniq;
}

function parseUpbitUtcSec(s: string): number {
  const iso = s.endsWith("Z") ? s : `${s}Z`;
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) throw new Error(`Bad upbit utc time: ${s}`);
  return Math.floor(t / 1000);
}

function sleep(ms: number) {
  return new Promise<void>(r => setTimeout(r, ms));
}
