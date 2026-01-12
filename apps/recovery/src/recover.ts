import type { Mode, Tf } from "./config.js";
import { log } from "./logger.js";
import { fetchMinuteCandles } from "./upbit.js";
import {
  selectExistingCandleTimes,
  selectCandlesAsc,
  selectExistingIndicatorTimes,
  upsertCandles,
  upsertIndicators,
  type CandleRow,
  type IndicatorRow
} from "./db.js";
import { buildTimeGrid, tfToMinutes, floorToTf, unixSecToKstIso } from "./time.js";
import { computeIndicators } from "./indicators.js";

function chunk<T>(arr: T[], n: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

/**
 * Upbit 캔들 fetch는 "to" 기준으로 과거로 내려가며 count<=200.
 * 우리는 start~end를 채우는 것이 목적.
 */
async function fetchCandlesRange(params: {
  market: string;
  tf: Tf;
  startSec: number;
  endSec: number;
}): Promise<CandleRow[]> {
  const tfMin = tfToMinutes(params.tf);
  const stepSec = tfMin * 60;

  const start = floorToTf(params.startSec, tfMin);
  const end = floorToTf(params.endSec, tfMin);

  const all: CandleRow[] = [];

  // pagination: end부터 과거로
  let cursorEnd = end;

  while (cursorEnd >= start) {
    // Upbit "to"는 KST ISO로 주고, count는 최대 200
    const toIso = unixSecToKstIso(cursorEnd);

    const batch = await fetchMinuteCandles({
      market: params.market,
      tf: params.tf,
      toKstIso: toIso,
      count: 200
    });

    if (batch.length === 0) {
      // 더 이상 못가면 break (하지만 이 경우도 gap fill로 처리)
      cursorEnd -= 200 * stepSec;
      continue;
    }

    all.push(...batch);

    const oldest = batch[0]!.time;
    // 다음 cursor는 oldest - step
    cursorEnd = oldest - stepSec;

    // start를 충분히 넘겼으면 종료
    if (oldest <= start) break;
  }

  // 범위 필터 + uniq
  const map = new Map<number, CandleRow>();
  for (const r of all) {
    if (r.time < start || r.time > end) continue;
    map.set(r.time, r);
  }

  return Array.from(map.values()).sort((a, b) => a.time - b.time);
}

/**
 * 누락 캔들(거래 없음) 생성:
 * - time grid 기준으로 candle이 없으면
 *   open/high/low/close = 이전 close (없으면 0)
 *   volume = 0
 */
function gapFillCandles(params: {
  market: string;
  tf: Tf;
  startSec: number;
  endSec: number;
  fetchedAsc: CandleRow[];
}): CandleRow[] {
  const tfMin = tfToMinutes(params.tf);
  const grid = buildTimeGrid(params.startSec, params.endSec, tfMin);
  const m = new Map<number, CandleRow>();
  for (const r of params.fetchedAsc) m.set(r.time, r);

  const out: CandleRow[] = [];
  let prevClose = params.fetchedAsc.length > 0 ? params.fetchedAsc[0]!.close : 0;

  for (const t of grid) {
    const r = m.get(t);
    if (r) {
      prevClose = r.close;
      out.push(r);
    } else {
      out.push({
        market: params.market,
        tf: params.tf,
        time: t,
        open: prevClose,
        high: prevClose,
        low: prevClose,
        close: prevClose,
        volume: 0
      });
    }
  }
  return out;
}

export async function recoverCandles(params: {
  markets: string[];
  tfs: Tf[];
  startSec: number;
  endSec: number;
  mode: Mode;
}): Promise<void> {
  for (const market of params.markets) {
    for (const tf of params.tfs) {
      log.info(`[candle] start market=${market} tf=${tf} mode=${params.mode}`);

      const fetched = await fetchCandlesRange({
        market,
        tf,
        startSec: params.startSec,
        endSec: params.endSec
      });

      const filled = gapFillCandles({
        market,
        tf,
        startSec: params.startSec,
        endSec: params.endSec,
        fetchedAsc: fetched
      });

      if (params.mode === "missing") {
        // existing 조회 후 missing만 insert
        // 너무 큰 범위면 부담 → 1일 단위로 쪼개기 권장, 하지만 여기선 범위 기반으로 처리
        const existing = await selectExistingCandleTimes({
          market,
          tf,
          start: filled[0]!.time,
          end: filled[filled.length - 1]!.time
        });
        const onlyMissing = filled.filter((r) => !existing.has(r.time));
        const total = onlyMissing.length;
        let inserted = 0;
        for (const part of chunk(onlyMissing, 2000)) {
          inserted += await upsertCandles(part, "missing");
        }
        log.info(`[candle] done market=${market} tf=${tf} missing=${total} inserted=${inserted}`);
      } else {
        let affected = 0;
        for (const part of chunk(filled, 2000)) {
          affected += await upsertCandles(part, "all");
        }
        log.info(`[candle] done market=${market} tf=${tf} upserted(affected)=${affected}`);
      }
    }
  }
}

export async function rebuildIndicators(params: {
  markets: string[];
  tfs: Tf[];
  startSec: number;
  endSec: number;
  mode: Mode;
}): Promise<void> {
  for (const market of params.markets) {
    for (const tf of params.tfs) {
      log.info(`[indicator] start market=${market} tf=${tf} mode=${params.mode}`);

      // warmup: 최대 120 SMA 고려 (여유로 200개)
      const tfMin = tfToMinutes(tf);
      const warmupSec = tfMin * 60 * 250;

      const qStart = params.startSec - warmupSec;
      const candles = await selectCandlesAsc({
        market,
        tf,
        start: qStart,
        end: params.endSec
      });

      if (candles.length === 0) {
        log.warn(`[indicator] skip (no candles) market=${market} tf=${tf}`);
        continue;
      }

      const packs = computeIndicators(candles);

      // start~end 구간만 저장
      const rows: IndicatorRow[] = [];
      for (let i = 0; i < candles.length; i++) {
        const c = candles[i]!;
        if (c.time < params.startSec || c.time > params.endSec) continue;
        rows.push({
          market,
          tf,
          time: c.time,
          indicators: packs[i]!
        });
      }

      if (rows.length === 0) {
        log.warn(`[indicator] skip (range empty) market=${market} tf=${tf}`);
        continue;
      }

      if (params.mode === "missing") {
        const existing = await selectExistingIndicatorTimes({
          market,
          tf,
          start: rows[0]!.time,
          end: rows[rows.length - 1]!.time
        });
        const onlyMissing = rows.filter((r) => !existing.has(r.time));
        let inserted = 0;
        for (const part of chunk(onlyMissing, 2000)) {
          inserted += await upsertIndicators(part, "missing");
        }
        log.info(`[indicator] done market=${market} tf=${tf} missing=${onlyMissing.length} inserted=${inserted}`);
      } else {
        let affected = 0;
        for (const part of chunk(rows, 2000)) {
          affected += await upsertIndicators(part, "all");
        }
        log.info(`[indicator] done market=${market} tf=${tf} upserted(affected)=${affected}`);
      }
    }
  }
}
