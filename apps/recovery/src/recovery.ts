// apps/recovery/src/recovery.ts
import type { RecoveryConfig, Tf, Candle, IndicatorRow } from "./types.js";
import { TF_SEC, floorToTfSec, rangeSlotsSec } from "./timeframes.js";
import { createMaria } from "./db/mariadb.js";
import { fetchUpbitCandlesRange } from "./exchange/upbit.js";
import { computeIndicatorsForCandles, DEFAULT_PARAMS } from "./indicators/core.js";

type Range = { startSec: number; endSec: number };

function envBool(name: string, def = false): boolean {
  const v = process.env[name];
  if (v == null) return def;
  return ["1", "true", "yes", "y", "on"].includes(String(v).toLowerCase());
}
const DEBUG_RECOVERY = envBool("DEBUG_RECOVERY", false);

export async function runRecovery(cfg: RecoveryConfig) {
  const maria = await createMaria(cfg.db);

  try {
    for (const market of cfg.markets) {
      for (const tf of cfg.tfs) {
        const step = TF_SEC[tf];

        const start = floorToTfSec(cfg.startSec, tf);
        const end = floorToTfSec(cfg.endSec - 1, tf) + step; // exclusive
        if (!(start < end)) continue;

        // -------------------------
        // 1) CANDLE BACKFILL (optional)
        // -------------------------
        if (cfg.task === "candle" || cfg.task === "both") {
          const expected = rangeSlotsSec(start, end, tf);
          const existing = await maria.getExistingCandleTimes({ market, tf, startSec: start, endSec: end });

          const missing = expected.filter(t => !existing.has(t));
          if (missing.length > 0) {
            if (missing.length > cfg.maxMissingPerMarket) {
              throw new Error(`Too many candle missing market=${market} tf=${tf} missing=${missing.length}`);
            }

            const ranges = toContiguousRanges(missing, step);
            console.log(`[recovery] candle ${market} ${tf} missing=${missing.length} ranges=${ranges.length}`);

            // worker queue
            const queue = ranges.slice();
            const fetchedAll: Candle[] = [];

            const workers = Array.from({ length: Math.max(1, cfg.restConcurrency) }, async () => {
              while (queue.length) {
                const r = queue.shift();
                if (!r) break;

                const candles = await fetchUpbitCandlesRange({
                  baseUrl: cfg.upbit.baseUrl,
                  market,
                  tf,
                  startSec: r.startSec,
                  endSec: r.endSec,
                  sleepMs: cfg.restSleepMs,
                  minIntervalMs: cfg.upbitMinIntervalMs
                });

                if (DEBUG_RECOVERY) {
                  let minT = Number.POSITIVE_INFINITY;
                  let maxT = 0;
                  for (const c of candles) { minT = Math.min(minT, c.time); maxT = Math.max(maxT, c.time); }
                  console.log(`[dbg] candleFetch ${market} ${tf} range=[${r.startSec},${r.endSec}) got=${candles.length} min=${Number.isFinite(minT)?minT:-1} max=${maxT||-1}`);
                }

                fetchedAll.push(...candles);
              }
            });

            await Promise.all(workers);

            // uniq by time
            fetchedAll.sort((a, b) => a.time - b.time);
            const uniq: Candle[] = [];
            let prev = -1;
            for (const c of fetchedAll) {
              if (c.time === prev) continue;
              prev = c.time;
              uniq.push(c);
            }

            if (uniq.length > 0) {
              await maria.upsertCandles(uniq);
              console.log(`[recovery] candle upserted=${uniq.length} market=${market} tf=${tf}`);
            }
          }
        }

        // -------------------------
        // 2) INDICATOR BACKFILL (optional)
        // -------------------------
        if (cfg.task === "indicator" || cfg.task === "both") {
          // indicator 생성은 lookback warmup 필요
          const warmupSec = cfg.indicatorLookbackBars * step;

          const indStart = start;
          const indEnd = end;

          // indicator 존재 여부로 missing 판단
          const expectedInd = rangeSlotsSec(indStart, indEnd, tf);
          const existingInd = await maria.getExistingIndicatorTimes({ market, tf, startSec: indStart, endSec: indEnd });

          const missingInd = expectedInd.filter(t => !existingInd.has(t));
          if (missingInd.length === 0) continue;

          // indicator 계산을 위해 더 과거 candle 필요
          const computeStart = Math.max(0, indStart - warmupSec);
          const computeEnd = indEnd;

          // candle 로드
          const candles = await maria.getCandles({ market, tf, startSec: computeStart, endSec: computeEnd });
          if (candles.length === 0) {
            console.log(`[recovery] indicator skip(no candles) market=${market} tf=${tf}`);
            continue;
          }

          // indicator 계산
          const rows = computeIndicatorsForCandles(candles, DEFAULT_PARAMS);

          // target 범위 + missing만 필터
          const missingSet = new Set(missingInd);
          const toUpsert: IndicatorRow[] = [];
          for (const r of rows) {
            if (r.time < indStart || r.time >= indEnd) continue;
            if (!missingSet.has(r.time)) continue;

            // "계산 가능" 최소조건: BB/RSI 같은 핵심이 null이면 skip (원하면 완화 가능)
            // 여기선 BB mid + RSI 둘 중 하나라도 있으면 넣도록(유연)
            const ok =
              (r.bb_mid_20 != null) ||
              (r.rsi_14 != null) ||
              (r.obv != null) ||
              (r.pvt != null);

            if (ok) toUpsert.push(r);
          }

          if (DEBUG_RECOVERY) {
            console.log(`[dbg] indicator ${market} ${tf} missing=${missingInd.length} candlesLoaded=${candles.length} computedRows=${rows.length} upsert=${toUpsert.length} computeRange=[${computeStart},${computeEnd})`);
          } else {
            console.log(`[recovery] indicator ${market} ${tf} missing=${missingInd.length} upsert=${toUpsert.length}`);
          }

          if (toUpsert.length > 0) {
            // 너무 큰 배치는 쪼개기
            const chunkSize = 2000;
            for (let i = 0; i < toUpsert.length; i += chunkSize) {
              await maria.upsertIndicators(toUpsert.slice(i, i + chunkSize));
            }
            console.log(`[recovery] indicator upserted=${toUpsert.length} market=${market} tf=${tf}`);
          }
        }
      }
    }
  } finally {
    await maria.close().catch(() => {});
  }
}

function toContiguousRanges(times: number[], step: number): Range[] {
  if (times.length === 0) return [];
  const sorted = [...times].sort((a, b) => a - b);

  const ranges: Range[] = [];
  let s = sorted[0];
  let prev = sorted[0];

  for (let i = 1; i < sorted.length; i++) {
    const t = sorted[i];
    if (t === prev + step) {
      prev = t;
      continue;
    }
    ranges.push({ startSec: s, endSec: prev + step });
    s = t;
    prev = t;
  }
  ranges.push({ startSec: s, endSec: prev + step });
  return ranges;
}
