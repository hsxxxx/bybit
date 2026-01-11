import type { Pool } from "mariadb";
import type { IndicatorRow, Timeframe } from "../types";
import { log } from "../logger";
import { selectCandlesForRange, upsertIndicators } from "../db";
import { calcIndicatorsForSeries } from "./indicators";
import { chunkArray } from "../utils/chunk";

export async function rebuildIndicatorsForMarketTf(params: {
  pool: Pool;
  market: string;
  tf: Timeframe;
  startSec: number;
  endSec: number;
  lookback: number;
}): Promise<{ upserted: number }> {
  const { pool, market, tf, startSec, endSec, lookback } = params;

  // 지표 계산 안정화를 위해 start 이전 lookback 구간까지 포함
  const fromSec = Math.max(0, startSec - lookback * 60 * 5); // 대충(넉넉하게)
  const rows = await selectCandlesForRange(pool, {
    market,
    tf,
    fromSec,
    toSec: endSec
  });

  if (rows.length === 0) {
    log.warn(`[indicators] no candles: ${market} ${tf}`);
    return { upserted: 0 };
  }

  const closes = rows.map((r) => Number(r.close));

  const out: IndicatorRow[] = [];
  for (let i = 0; i < rows.length; i++) {
    const t = Number(rows[i].time);
    if (t < startSec || t > endSec) continue;

    const ind = calcIndicatorsForSeries(closes, i);

    out.push({
      market,
      tf,
      time: t,
      indicators: ind
    });
  }

  // batch upsert
  for (const part of chunkArray(out, 2000)) {
    await upsertIndicators(pool, part);
  }

  log.info(`[indicators] ${market} ${tf} done`, { total: out.length });
  return { upserted: out.length };
}
