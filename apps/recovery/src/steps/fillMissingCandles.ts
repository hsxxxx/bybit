import type { Pool } from "mariadb";
import type { CandleRow, Timeframe } from "../types";
import { tfStepSeconds } from "../utils/time";
import { log } from "../logger";
import { upsertCandles, selectCandlesAsc, selectLastCandleBefore } from "../db";

type FillResult = {
  market: string;
  tf: Timeframe;
  filled: number;
  kept: number;
};

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

/**
 * 공식 문서 기준:
 * - 체결 없는 분은 캔들이 "생성되지 않으며 응답에도 포함되지 않음"
 * -> DB에서 1m grid가 필요하면 missing minute는 synthetic candle로 채움
 *
 * fill 정책:
 * - OHLC = prevClose
 * - volume = 0
 * - prevClose는 (1) 범위 내 직전 실제 캔들 close, (2) 범위 시작 이전의 마지막 캔들 close
 * - prevClose가 없으면(완전 시작 구간) 첫 실제 캔들이 나오기 전까지는 채우지 않음
 */
export async function fillMissingCandles(params: {
  pool: Pool;
  market: string;
  tf: Timeframe; // 보통 '1m'
  startSec: number;
  endSec: number;
  writeChunkSize?: number; // default 5000
}): Promise<FillResult> {
  const { pool, market, tf, startSec, endSec } = params;
  const step = tfStepSeconds(tf);
  const writeChunkSize = params.writeChunkSize ?? 5000;

  // 범위 내 캔들 (오름차순)
  const rows = await selectCandlesAsc(pool, market, tf, startSec, endSec);

  // 범위 시작 이전 마지막 캔들(시드)
  const seed = await selectLastCandleBefore(pool, market, tf, startSec);
  let prevClose: number | null = seed ? seed.close : null;

  // existing time -> row
  const m = new Map<number, CandleRow>();
  for (const r of rows) m.set(r.time, r);

  const toWrite: CandleRow[] = [];
  let kept = 0;
  let filled = 0;

  for (let t = startSec; t <= endSec; t += step) {
    const exist = m.get(t);
    if (exist) {
      kept += 1;
      prevClose = exist.close;
      continue;
    }

    if (prevClose == null) {
      // 아직 seed/실데이터가 없으면, 시작부 synthetic 생성 불가 -> 스킵
      continue;
    }

    // synthetic candle
    toWrite.push({
      market,
      tf,
      time: t,
      open: prevClose,
      high: prevClose,
      low: prevClose,
      close: prevClose,
      volume: 0
    });
    filled += 1;
  }

  if (toWrite.length > 0) {
    log.info(`[fill] ${market} ${tf} writing`, { filled: toWrite.length });
    for (const c of chunk(toWrite, writeChunkSize)) {
      await upsertCandles(pool, c);
    }
  }

  log.info(`[fill] ${market} ${tf} done`, { kept, filled });
  return { market, tf, kept, filled };
}
