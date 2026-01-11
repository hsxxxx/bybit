import type { Pool } from "mariadb";
import type { CandleRow, Timeframe } from "../types";
import { fetchCandlesChunk } from "../upbit";
import { log } from "../logger";
import { parseKstToEpochSeconds, tfStepSeconds, kstIsoFromEpochSeconds } from "../utils/time";
import { upsertCandles } from "../db";

function toCandleRow(market: string, tf: Timeframe, c: any): CandleRow {
  // KST 기준 open time -> epoch seconds
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

  // Upbit API는 최신→과거 순으로 내려줌. to=해당시각 "이전" 캔들 포함.
  while (true) {
    const toIso = kstIsoFromEpochSeconds(cursorToSec);
    let candles;
    try {
      candles = await fetchCandlesChunk({
        market,
        tf,
        toKstIso: toIso,
        count: 200
      });
    } catch (e: any) {
      // 429는 간단 backoff
      if (String(e?.message || "").includes("429")) {
        await new Promise((r) => setTimeout(r, 1200));
        continue;
      }
      throw e;
    }

    if (!candles || candles.length === 0) break;

    const rows = candles
      .map((c) => toCandleRow(market, tf, c))
      .filter((r) => r.time >= startSec && r.time <= endSec);

    await upsertCandles(pool, rows);
    total += rows.length;

    // 가장 오래된 캔들로 cursor 이동
    const oldest = candles[candles.length - 1];
    const oldestSec = parseKstToEpochSeconds(oldest.candle_date_time_kst);

    if (oldestSec <= startSec) break;

    // 다음 요청은 oldest - step 로 이동 (중복 최소화)
    cursorToSec = oldestSec - step;
  }

  log.info(`[candles] ${market} ${tf} done`, { total });
  return { inserted: total };
}
