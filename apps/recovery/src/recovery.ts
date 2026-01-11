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

  // ✅ no-trade 메모리 캐시 (market|tf -> Set<time>)
  const noTradeCache = new Map<string, Set<number>>();

  function key(m: string, tf: Tf) {
    return `${m}|${tf}`;
  }

  async function loadNoTradeSet(market: string, tf: Tf, startSec: number, endSec: number): Promise<Set<number>> {
    const k = key(market, tf);
    if (!noTradeCache.has(k)) noTradeCache.set(k, new Set<number>());
    const set = noTradeCache.get(k)!;

    if (cfg.noTradeMode === "mark_db") {
      const dbSet = await maria.getNoTradeTimes({ market, tf, startSec, endSec });
      for (const t of dbSet) set.add(t);
    }
    return set;
  }

  async function markNoTrade(market: string, tf: Tf, times: number[]) {
    if (cfg.noTradeMode === "skip") return;

    const k = key(market, tf);
    if (!noTradeCache.has(k)) noTradeCache.set(k, new Set<number>());
    const set = noTradeCache.get(k)!;
    for (const t of times) set.add(t);

    if (cfg.noTradeMode === "mark_db") {
      await maria.upsertNoTradeSlots({ market, tf, times });
    }
  }

  try {
    for (const market of cfg.markets) {
      for (const tf of cfg.tfs) {
        const step = TF_SEC[tf];

        const start = floorToTfSec(cfg.startSec, tf);
        const end = floorToTfSec(cfg.endSec - 1, tf) + step;
        if (!(start < end)) continue;

        // ✅ no-trade existing 로드(옵션)
        const noTradeSet = await loadNoTradeSet(market, tf, start, end);

        // -------------------------
        // 1) CANDLE BACKFILL
        // -------------------------
        if (cfg.task === "candle" || cfg.task === "both") {
          const expected = rangeSlotsSec(start, end, tf);
          const existing = await maria.getExistingCandleTimes({ market, tf, startSec: start, endSec: end });

          // ✅ no-trade로 마킹된 슬롯은 missing에서 제외
          const missing = expected.filter(t => !existing.has(t) && !noTradeSet.has(t));

          if (missing.length > 0) {
            if (missing.length > cfg.maxMissingPerMarket) {
              throw new Error(`Too many candle missing market=${market} tf=${tf} missing=${missing.length}`);
            }

            const ranges = toContiguousRanges(missing, step);
            console.log(`[recovery] candle ${market} ${tf} missing=${missing.length} ranges=${ranges.length}`);

            const queue = ranges.slice();
            const fetchedAll: Candle[] = [];

            const workers = Array.from({ length: Math.max(1, cfg.restConcurrency) }, async () => {
              while (queue.length) {
                const r = queue.shift();
                if (!r) break;

                const res = await fetchUpbitCandlesRange({
                  baseUrl: cfg.upbit.baseUrl,
                  market,
                  tf,
                  startSec: r.startSec,
                  endSec: r.endSec,
                  sleepMs: cfg.restSleepMs,
                  minIntervalMs: cfg.upbitMinIntervalMs,
                  debug: DEBUG_RECOVERY
                });

                // ✅ 빈 캔들([]) = 거래 없음으로 취급
                if (res.empty && cfg.noTradeMarkWholeRange) {
                  const slots = rangeSlotsSec(r.startSec, r.endSec, tf);
                  await markNoTrade(market, tf, slots);
                  if (DEBUG_RECOVERY) {
                    console.log(`[dbg] no-trade mark market=${market} tf=${tf} range=[${r.startSec},${r.endSec}) slots=${slots.length} mode=${cfg.noTradeMode}`);
                  }
                  continue;
                }

                if (DEBUG_RECOVERY) {
                  let minT = Number.POSITIVE_INFINITY;
                  let maxT = 0;
                  for (const c of res.candles) { minT = Math.min(minT, c.time); maxT = Math.max(maxT, c.time); }
                  console.log(`[dbg] candleFetch ${market} ${tf} range=[${r.startSec},${r.endSec}) got=${res.candles.length} min=${Number.isFinite(minT)?minT:-1} max=${maxT||-1}`);
                }

                fetchedAll.push(...res.candles);
              }
            });

            await Promise.all(workers);

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
        // 2) INDICATOR BACKFILL
        // -------------------------
        if (cfg.task === "indicator" || cfg.task === "both") {
          const warmupSec = cfg.indicatorLookbackBars * step;

          const indStart = start;
          const indEnd = end;

          const expectedInd = rangeSlotsSec(indStart, indEnd, tf);
          const existingInd = await maria.getExistingIndicatorTimes({ market, tf, startSec: indStart, endSec: indEnd });

          // ✅ no-trade 슬롯은 indicator도 생성하지 않음
          const missingInd = expectedInd.filter(t => !existingInd.has(t) && !noTradeSet.has(t));
          if (missingInd.length === 0) continue;

          const computeStart = Math.max(0, indStart - warmupSec);
          const computeEnd = indEnd;

          const candles = await maria.getCandles({ market, tf, startSec: computeStart, endSec: computeEnd });
          if (candles.length === 0) {
            console.log(`[recovery] indicator skip(no candles) market=${market} tf=${tf}`);
            continue;
          }

          const rows = computeIndicatorsForCandles(candles, DEFAULT_PARAMS);

          const missingSet = new Set(missingInd);
          const toUpsert: IndicatorRow[] = [];

          for (const r of rows) {
            if (r.time < indStart || r.time >= indEnd) continue;
            if (!missingSet.has(r.time)) continue;

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
