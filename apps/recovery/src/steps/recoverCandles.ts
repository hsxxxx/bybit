import type { Pool } from "mariadb";
import type { CandleRow, Timeframe } from "../types";
import { fetchCandlesChunk } from "../upbit";
import { log } from "../logger";
import { parseKstToEpochSeconds, tfStepSeconds, kstIsoFromEpochSeconds } from "../utils/time";
import { upsertCandles } from "../db";

function toCandleRow(market: string, tf: Timeframe, c: any): CandleRow {
  const openSec = parseKstToEpochSeconds(c.candle_date_time_kst);
  return {
    market,
    tf,
    time: openSec,
    open: Number(c.opening_price),
    high: Number(c.high_price),
    low: Number(c.low_price),
    close: Number(c.trade_price),
    volume: Number(c.candle_acc_trade_volume)
  };
}

export async function recoverCandlesForMarketTf(params: {
  pool: Pool;
  market: string;
  tf: Timeframe;
  startSec: number;
  endSec: number;
}): Promise<{ inserted: number }> {
  const { pool, market, tf, startSec, endSec } = params;

  let cursorToSec = endSec;
  let total = 0;
  const step = tfStepSeconds(tf);

  let loop = 0;

  while (true) {
    loop += 1;

    const toIso = kstIsoFromEpochSeconds(cursorToSec);

    // 너무 자주 찍지 않음(10회마다)
    if (loop === 1 || loop % 10 === 0) {
      log.info(`[candles] ${market} ${tf} fetching...`, { toIso, cursorToSec });
    }

    const candles = await fetchCandlesChunk({
      market,
      tf,
      toKstIso: toIso,
      count: 200,
      timeoutMs: 12_000,
      maxRetry: 12
    });

    if (!candles || candles.length === 0) break;

    const rows = candles
      .map((c) => toCandleRow(market, tf, c))
      .filter((r) => r.time >= startSec && r.time <= endSec);

    await upsertCandles(pool, rows);
    total += rows.length;

    const oldest = candles[candles.length - 1];
    const oldestSec = parseKstToEpochSeconds(oldest.candle_date_time_kst);

    if (oldestSec <= startSec) break;

    cursorToSec = oldestSec - step;
  }

  log.info(`[candles] ${market} ${tf} done`, { total });
  return { inserted: total };
}
