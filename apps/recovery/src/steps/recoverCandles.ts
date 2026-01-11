import type { Pool } from "mariadb";
import type { CandleRow, Timeframe } from "../types";
import { fetchCandlesChunk } from "../upbit";
import { log } from "../logger";
import { parseKstToEpochSeconds, tfStepSeconds, utcIsoFromEpochSeconds } from "../utils/time";
import { upsertCandles } from "../db";

function toCandleRow(market: string, tf: Timeframe, c: any): CandleRow {
  const openSec = parseKstToEpochSeconds(c.candle_date_time_kst); // KST 기준 open time
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
  const step = tfStepSeconds(tf);

  let total = 0;
  let loop = 0;

  let lastCursor = -1;
  let sameCursorCount = 0;

  while (true) {
    loop += 1;

    if (cursorToSec === lastCursor) sameCursorCount += 1;
    else {
      sameCursorCount = 0;
      lastCursor = cursorToSec;
    }
    if (sameCursorCount >= 3) {
      cursorToSec = cursorToSec - step * 50;
      log.warn(`[candles] cursor stuck -> force move`, { market, tf, cursorToSec });
      sameCursorCount = 0;
    }

    // ✅ 핵심: to 파라미터는 UTC ISO(Z)로 보냄
    const toIsoUtc = utcIsoFromEpochSeconds(cursorToSec);

    if (loop === 1 || loop % 10 === 0) {
      log.info(`[candles] ${market} ${tf} fetching...`, { toIsoUtc, cursorToSec });
    }

    const candles = await fetchCandlesChunk({
      market,
      tf,
      toKstIso: toIsoUtc, // (변수명은 그대로지만 실제로 UTC ISO를 넣음)
      count: 200,
      timeoutMs: 12_000,
      maxRetry: 12
    });

    if (!candles || candles.length === 0) break;

    const uniq = new Map<number, CandleRow>();
    let oldestSec = Infinity;

    for (const c of candles) {
      const row = toCandleRow(market, tf, c);
      if (row.time >= startSec && row.time <= endSec) uniq.set(row.time, row);

      const t = row.time;
      if (t < oldestSec) oldestSec = t;
    }

    const rows = Array.from(uniq.values()).sort((a, b) => a.time - b.time);
    await upsertCandles(pool, rows);
    total += rows.length;

    if (!Number.isFinite(oldestSec)) break;
    if (oldestSec <= startSec) break;

    const nextCursor = oldestSec - step;
    if (nextCursor >= cursorToSec) {
      // 이제는 거의 안 나와야 함. 나오면 강제 이동
      const forced = cursorToSec - step * 200;
      log.warn(`[candles] non-decreasing cursor -> force move`, { market, tf, cursorToSec, oldestSec, forced });
      cursorToSec = forced;
      continue;
    }

    cursorToSec = nextCursor;

    if (loop > 20000) {
      log.error(`[candles] loop limit exceeded`, { market, tf, startSec, endSec });
      break;
    }
  }

  log.info(`[candles] ${market} ${tf} done`, { total });
  return { inserted: total };
}
