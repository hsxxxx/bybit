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
 * (핵심 변경) 범위를 6시간 단위로 쪼개서 Upbit 호출량/대기/로그를 안정화
 * - 1m: 6h=360개 -> 2페이지면 충분
 * - 5m: 6h=72개  -> 1페이지
 */
function splitRange(startSec: number, endSec: number, chunkSec: number): Array<{ s: number; e: number }> {
  const out: Array<{ s: number; e: number }> = [];
  let s = startSec;
  while (s <= endSec) {
    const e = Math.min(endSec, s + chunkSec - 1);
    out.push({ s, e });
    s = e + 1;
  }
  return out;
}

async function fetchCandlesRangeChunk(params: {
  market: string;
  tf: Tf;
  startSec: number;
  endSec: number;
}): Promise<CandleRow[]> {
  const tfMin = tfToMinutes(params.tf);
  const stepSec = tfMin * 60;

  const start = floorToTf(params.startSec, tfMin);
  const end = floorToTf(params.endSec, tfMin);

  const map = new Map<number, CandleRow>();

  let cursorEnd = end;
  let page = 0;
  let lastOldest: number | null = null;

  while (cursorEnd >= start) {
    page += 1;
    const toIso = unixSecToKstIso(cursorEnd);

    log.info(`[fetch] ${params.market} ${params.tf} page=${page} to=${toIso}`);

    const batch = await fetchMinuteCandles({
      market: params.market,
      tf: params.tf,
      toKstIso: toIso,
      count: 200
    });

    if (batch.length === 0) {
      cursorEnd -= 200 * stepSec;
      continue;
    }

    const oldest = batch[0]!.time;
    const newest = batch[batch.length - 1]!.time;

    // ✅ 무한루프 방지: 같은 oldest가 반복되면 to가 무시/고정된 상태
    if (lastOldest !== null && oldest === lastOldest) {
      log.warn(`[fetch] no progress detected. force step back. oldest=${oldest} cursorEnd=${cursorEnd}`);
      cursorEnd -= 200 * stepSec;
      continue;
    }
    lastOldest = oldest;

    for (const r of batch) {
      if (r.time < start || r.time > end) continue;
      map.set(r.time, r);
    }

    log.info(
      `[fetch] ${params.market} ${params.tf} got=${batch.length} range=[${oldest}..${newest}] uniq=${map.size}`
    );

    cursorEnd = oldest - stepSec;
    if (oldest <= start) break;
  }

  return Array.from(map.values()).sort((a, b) => a.time - b.time);
}


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
  // 6시간 단위(21600초)
  const CHUNK_SEC = 6 * 60 * 60;

  const ranges = splitRange(params.startSec, params.endSec, CHUNK_SEC);

  for (const market of params.markets) {
    for (const tf of params.tfs) {
      log.info(`[candle] start market=${market} tf=${tf} mode=${params.mode} chunks=${ranges.length}`);

      for (let i = 0; i < ranges.length; i++) {
        const { s, e } = ranges[i]!;
        log.info(`[candle] chunk ${i + 1}/${ranges.length} market=${market} tf=${tf} start=${unixSecToKstIso(s)} end=${unixSecToKstIso(e)}`);

        const fetched = await fetchCandlesRangeChunk({ market, tf, startSec: s, endSec: e });
        const filled = gapFillCandles({ market, tf, startSec: s, endSec: e, fetchedAsc: fetched });

        if (params.mode === "missing") {
          const existing = await selectExistingCandleTimes({
            market,
            tf,
            start: filled[0]!.time,
            end: filled[filled.length - 1]!.time
          });
          const onlyMissing = filled.filter((r) => !existing.has(r.time));

          let inserted = 0;
          for (const part of chunk(onlyMissing, 2000)) {
            inserted += await upsertCandles(part, "missing");
          }
          log.info(`[candle] chunk done market=${market} tf=${tf} missing=${onlyMissing.length} inserted=${inserted}`);
        } else {
          let affected = 0;
          for (const part of chunk(filled, 2000)) {
            affected += await upsertCandles(part, "all");
          }
          log.info(`[candle] chunk done market=${market} tf=${tf} affected=${affected}`);
        }
      }

      log.info(`[candle] done market=${market} tf=${tf}`);
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

      const rows: IndicatorRow[] = [];
      for (let i = 0; i < candles.length; i++) {
        const c = candles[i]!;
        if (c.time < params.startSec || c.time > params.endSec) continue;
        rows.push({ market, tf, time: c.time, indicators: packs[i]! });
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
        log.info(`[indicator] done market=${market} tf=${tf} affected=${affected}`);
      }
    }
  }
}
