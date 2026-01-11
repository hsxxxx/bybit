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
  const step = tfStepSeconds(tf);

  let total = 0;
  let loop = 0;

  // 진행 가드
  let lastCursor = -1;
  let sameCursorCount = 0;

  // 동일 응답(가장 오래된 time 동일) 반복 감지
  let lastOldest = -1;
  let sameOldestCount = 0;

  while (true) {
    loop += 1;

    // cursor가 안 변하면 강제로 당김
    if (cursorToSec === lastCursor) {
      sameCursorCount += 1;
    } else {
      sameCursorCount = 0;
      lastCursor = cursorToSec;
    }
    if (sameCursorCount >= 3) {
      const forced = cursorToSec - step * 10;
      log.warn(`[candles] cursor stuck -> force move`, { market, tf, cursorToSec, forced });
      cursorToSec = forced;
      sameCursorCount = 0;
    }

    const toIso = kstIsoFromEpochSeconds(cursorToSec);
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

    // time 중복 제거 + 범위 필터
    const uniq = new Map<number, CandleRow>();
    for (const c of candles) {
      const r = toCandleRow(market, tf, c);
      if (r.time < startSec || r.time > endSec) continue;
      uniq.set(r.time, r);
    }
    const rows = Array.from(uniq.values()).sort((a, b) => a.time - b.time);

    await upsertCandles(pool, rows);
    total += rows.length;

    // Upbit는 최신->과거. 가장 오래된 것은 마지막 요소가 맞지만,
    // 이상케이스 대비해서 min time으로 계산.
    let oldestSec = Infinity;
    for (const c of candles) {
      const t = parseKstToEpochSeconds(c.candle_date_time_kst);
      if (t < oldestSec) oldestSec = t;
    }

    if (!Number.isFinite(oldestSec)) break;

    // oldest가 안 줄면 루프가 stuck
    if (oldestSec === lastOldest) {
      sameOldestCount += 1;
    } else {
      sameOldestCount = 0;
      lastOldest = oldestSec;
    }

    if (sameOldestCount >= 2) {
      const forced = cursorToSec - step * 50;
      log.warn(`[candles] oldest stuck -> force move`, { market, tf, cursorToSec, oldestSec, forced });
      cursorToSec = forced;
      sameOldestCount = 0;
      continue;
    }

    if (oldestSec <= startSec) break;

    // 정상 이동 (중복 방지)
    const nextCursor = oldestSec - step;
    if (nextCursor >= cursorToSec) {
      // 여기로 오면 진행이 안 되는 상황
      const forced = cursorToSec - step * 50;
      log.warn(`[candles] non-decreasing cursor -> force move`, { market, tf, cursorToSec, oldestSec, forced });
      cursorToSec = forced;
      continue;
    }

    cursorToSec = nextCursor;

    // 안전장치(무한루프 방지)
    if (loop > 20000) {
      log.error(`[candles] loop limit exceeded`, { market, tf, startSec, endSec });
      break;
    }
  }

  log.info(`[candles] ${market} ${tf} done`, { total });
  return { inserted: total };
}
