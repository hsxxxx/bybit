// apps/recovery/src/recovery.ts
import type { RecoveryConfig, Tf, Candle, IndicatorRow } from "./types.js";
import { TF_SEC, floorToTfSec } from "./timeframes.js";
import { createMaria } from "./db/mariadb.js";
import { fetchUpbitCandlesRange } from "./exchange/upbit.js";
import { computeIndicatorsForCandles, DEFAULT_PARAMS } from "./indicators/core.js";

type Range = { startSec: number; endSec: number };

const DEBUG_RECOVERY = ["1", "true", "yes", "y", "on"].includes(
  String(process.env.DEBUG_RECOVERY ?? "0").toLowerCase()
);

function rangeSlotsAligned(startAligned: number, endAligned: number, step: number): number[] {
  // start/end는 이미 tf grid에 align된 값이어야 함. end는 exclusive.
  const out: number[] = [];
  for (let t = startAligned; t < endAligned; t += step) out.push(t);
  return out;
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

export async function runRecovery(cfg: RecoveryConfig) {
  const maria = await createMaria(cfg.db);

  // no-trade cache
  const noTradeCache = new Map<string, Set<number>>();
  const key = (m: string, tf: Tf) => `${m}|${tf}`;

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

        // ✅ raw range (입력)
        const rawStart = cfg.startSec;
        const rawEnd = cfg.endSec;

        // ✅ 항상 tf grid로 정렬해서 사용
        const start = floorToTfSec(rawStart, tf);
        const end = floorToTfSec(rawEnd, tf); // end exclusive

        if (!(start < end)) continue;

        if (DEBUG_RECOVERY) {
          console.log(
            `[dbg] range_raw market=${market} tf=${tf} startSec=${rawStart} endSec=${rawEnd} startMod=${rawStart % step} endMod=${rawEnd % step}`
          );
          console.log(
            `[dbg] range_aligned market=${market} tf=${tf} start=${start} end=${end} startMod=${start % step} endMod=${end % step}`
          );
        }

        const noTradeSet = await loadNoTradeSet(market, tf, start, end);

        // -------------------------
        // 1) CANDLE BACKFILL
        // -------------------------
        if (cfg.task === "candle" || cfg.task === "both") {
          // expected 슬롯은 정렬된 start/end로만 생성
          const expected = rangeSlotsAligned(start, end, step);

          const existing = await maria.getExistingCandleTimes({
            market,
            tf,
            startSec: start,
            endSec: end
          });

          const missing = expected.filter(t => !existing.has(t) && !noTradeSet.has(t));

          if (DEBUG_RECOVERY && missing.length > 0) {
            console.log(`[dbg] missing_head market=${market} tf=${tf} head=${missing.slice(0, 8).join(",")}`);
          }

          if (missing.length > 0) {
            if (missing.length > cfg.maxMissingPerMarket) {
              throw new Error(
                `Too many candle missing market=${market} tf=${tf} missing=${missing.length} > MAX_MISSING_PER_MARKET=${cfg.maxMissingPerMarket}`
              );
            }

            const ranges = toContiguousRanges(missing, step);
            console.log(`[recovery] candle ${market} ${tf} missing=${missing.length} ranges=${ranges.length}`);

            const fetchedAll: Candle[] = [];
            const missingSet = new Set(missing);
            const queue = ranges.slice();

            const workers = Array.from({ length: Math.max(1, cfg.restConcurrency) }, async () => {
              while (queue.length) {
                const r = queue.shift();
                if (!r) break;

                // Upbit fetch는 range 그대로 주고, 응답 중 missingSet만 필터
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

                // 거래 없음(빈 배열) 처리: 전체 슬롯을 no-trade로 마킹(옵션)
                if (res.empty && cfg.noTradeMarkWholeRange) {
                  const slots = rangeSlotsAligned(r.startSec, r.endSec, step);
                  await markNoTrade(market, tf, slots);
                  if (DEBUG_RECOVERY) {
                    console.log(
                      `[dbg] no-trade mark market=${market} tf=${tf} range=[${r.startSec},${r.endSec}) slots=${slots.length} mode=${cfg.noTradeMode}`
                    );
                  }
                  continue;
                }

                let gotAdded = 0;
                let minT = Number.POSITIVE_INFINITY;
                let maxT = -1;

                for (const c of res.candles) {
                  minT = Math.min(minT, c.time);
                  maxT = Math.max(maxT, c.time);
                  if (missingSet.has(c.time)) {
                    fetchedAll.push(c);
                    gotAdded++;
                  }
                }

                if (DEBUG_RECOVERY) {
                  console.log(
                    `[dbg] candleFetch ${market} ${tf} range=[${r.startSec},${r.endSec}) got_added=${gotAdded} fetchedMin=${Number.isFinite(minT) ? minT : -1} fetchedMax=${maxT}`
                  );
                }
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
            } else {
              console.log(`[recovery] fetched=0 market=${market} tf=${tf}`);
            }
          }
        }

        // -------------------------
        // 2) INDICATOR BACKFILL
        // -------------------------
        if (cfg.task === "indicator" || cfg.task === "both") {
          // indicator도 정렬된 start/end 기준
          const expectedInd = rangeSlotsAligned(start, end, step);

          const existingInd = await maria.getExistingIndicatorTimes({
            market,
            tf,
            startSec: start,
            endSec: end
          });

          const missingInd = expectedInd.filter(t => !existingInd.has(t) && !noTradeSet.has(t));
          if (missingInd.length === 0) continue;

          // lookback 확보
          const warmupSec = cfg.indicatorLookbackBars * step;
          const computeStart = Math.max(0, start - warmupSec);

          const candles = await maria.getCandles({
            market,
            tf,
            startSec: computeStart,
            endSec: end
          });

          if (candles.length === 0) {
            console.log(`[recovery] indicator skip(no candles) market=${market} tf=${tf}`);
            continue;
          }

          const computed = computeIndicatorsForCandles(candles, DEFAULT_PARAMS);

          const missingSet = new Set(missingInd);
          const toUpsert: IndicatorRow[] = [];
          for (const r of computed) {
            if (r.time < start || r.time >= end) continue;
            if (!missingSet.has(r.time)) continue;
            toUpsert.push(r);
          }

          if (DEBUG_RECOVERY) {
            console.log(
              `[dbg] indicator ${market} ${tf} missing=${missingInd.length} candlesLoaded=${candles.length} computedRows=${computed.length} upsert=${toUpsert.length} computeRange=[${computeStart},${end})`
            );
          }

          if (toUpsert.length > 0) {
            // 대량 upsert chunk
            const chunk = 2000;
            for (let i = 0; i < toUpsert.length; i += chunk) {
              await maria.upsertIndicators(toUpsert.slice(i, i + chunk));
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
