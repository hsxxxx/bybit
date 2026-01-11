// apps/recovery/src/recovery.ts  (offset 추정 + expected/필터에 적용)
import type { RecoveryConfig, Tf, Candle, IndicatorRow } from "./types.js";
import { TF_SEC, floorToTfSec, rangeSlotsSec } from "./timeframes.js";
import { createMaria } from "./db/mariadb.js";
import { fetchUpbitCandlesRange } from "./exchange/upbit.js";
import { computeIndicatorsForCandles, DEFAULT_PARAMS } from "./indicators/core.js";

type Range = { startSec: number; endSec: number };

const DEBUG_RECOVERY = ["1","true","yes","y","on"].includes(String(process.env.DEBUG_RECOVERY ?? "0").toLowerCase());

function rangeSlotsWithOffset(startSec: number, endSec: number, tf: Tf, offsetSec: number): number[] {
  const step = TF_SEC[tf];
  // startSec/endSec는 tf 그리드 기준으로 들어오지만, 실제 슬롯은 offset을 더한 그리드로 만든다
  const s0 = floorToTfSec(startSec, tf) + offsetSec;
  // end는 exclusive. 마지막 포함 슬롯을 잡기 위해 end-1을 사용.
  const e0 = floorToTfSec(endSec - 1, tf) + offsetSec + step;

  const out: number[] = [];
  for (let t = s0; t < e0; t += step) {
    if (t >= startSec && t < endSec) out.push(t);
  }
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
    if (t === prev + step) { prev = t; continue; }
    ranges.push({ startSec: s, endSec: prev + step });
    s = t; prev = t;
  }
  ranges.push({ startSec: s, endSec: prev + step });
  return ranges;
}

function guessOffsetFromTimes(times: number[], step: number): number {
  if (times.length === 0) return 0;
  const freq = new Map<number, number>();
  for (const t of times) {
    const r = ((t % step) + step) % step;
    freq.set(r, (freq.get(r) ?? 0) + 1);
  }
  let bestR = 0;
  let bestN = -1;
  for (const [r, n] of freq.entries()) {
    if (n > bestN) { bestN = n; bestR = r; }
  }
  return bestR;
}

export async function runRecovery(cfg: RecoveryConfig) {
  const maria = await createMaria(cfg.db);

  // offset cache per market|tf
  const offsetCache = new Map<string, number>();
  const key = (m: string, tf: Tf) => `${m}|${tf}`;

  async function getOffset(market: string, tf: Tf, startSec: number, endSec: number): Promise<number> {
    const k = key(market, tf);
    if (offsetCache.has(k)) return offsetCache.get(k)!;

    const step = TF_SEC[tf];
    const samples = await maria.sampleCandleTimes({
      market, tf,
      startSec: Math.max(0, startSec - step * 500),
      endSec,
      limit: 500
    });

    const off = guessOffsetFromTimes(samples, step);
    offsetCache.set(k, off);

    if (DEBUG_RECOVERY) {
      console.log(`[dbg] offset market=${market} tf=${tf} step=${step} offset=${off} (sampleN=${samples.length})`);
    }
    return off;
  }

  try {
    for (const market of cfg.markets) {
      for (const tf of cfg.tfs) {
        const step = TF_SEC[tf];

        const start = floorToTfSec(cfg.startSec, tf);
        const end = floorToTfSec(cfg.endSec - 1, tf) + step;
        if (!(start < end)) continue;

        const offset = await getOffset(market, tf, start, end);

        // -------------------------
        // 1) CANDLE BACKFILL
        // -------------------------
        if (cfg.task === "candle" || cfg.task === "both") {
          const expected = rangeSlotsWithOffset(start, end, tf, offset);
          const existing = await maria.getExistingCandleTimes({ market, tf, startSec: start, endSec: end });

          const missing = expected.filter(t => !existing.has(t));
          if (missing.length > 0) {
            if (missing.length > cfg.maxMissingPerMarket) {
              throw new Error(`Too many candle missing market=${market} tf=${tf} missing=${missing.length}`);
            }

            const ranges = toContiguousRanges(missing, step);
            console.log(`[recovery] candle ${market} ${tf} missing=${missing.length} ranges=${ranges.length} offset=${offset}`);

            const queue = ranges.slice();
            const fetchedAll: Candle[] = [];
            const missingSet = new Set(missing);

            const workers = Array.from({ length: Math.max(1, cfg.restConcurrency) }, async () => {
              while (queue.length) {
                const r = queue.shift();
                if (!r) break;

                const res = await fetchUpbitCandlesRange({
                  baseUrl: cfg.upbit.baseUrl,
                  market,
                  tf,
                  // ✅ Upbit는 정각(open time)로 주므로, 요청 범위는 offset을 제거한 open-time 기준으로 내려준다
                  startSec: r.startSec - offset,
                  endSec: r.endSec - offset,
                  sleepMs: cfg.restSleepMs,
                  minIntervalMs: cfg.upbitMinIntervalMs,
                  debug: DEBUG_RECOVERY,
                  useCloseTime: false
                });

                // ✅ 응답 open time을 DB time 그리드로 맞춤(=open + offset)
                for (const c of res.candles) {
                  const shifted: Candle = { ...c, time: c.time + offset };
                  if (missingSet.has(shifted.time)) fetchedAll.push(shifted);
                }

                if (DEBUG_RECOVERY) {
                  let minT = Number.POSITIVE_INFINITY;
                  let maxT = 0;
                  for (const c of fetchedAll) { minT = Math.min(minT, c.time); maxT = Math.max(maxT, c.time); }
                  console.log(`[dbg] candleFetch ${market} ${tf} range=[${r.startSec},${r.endSec}) got_added=${res.candles.length} fetchedMin=${Number.isFinite(minT)?minT:-1} fetchedMax=${maxT||-1}`);
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

          const expectedInd = rangeSlotsWithOffset(indStart, indEnd, tf, offset);
          const existingInd = await maria.getExistingIndicatorTimes({ market, tf, startSec: indStart, endSec: indEnd });
          const missingInd = expectedInd.filter(t => !existingInd.has(t));
          if (missingInd.length === 0) continue;

          const computeStart = Math.max(0, indStart - warmupSec);
          const computeEnd = indEnd;

          const candles = await maria.getCandles({ market, tf, startSec: computeStart, endSec: computeEnd });
          if (candles.length === 0) continue;

          // ✅ indicator 계산은 candle 순서/값이 중요하므로, time이 offset 그리드여도 그냥 진행 가능
          const rows = computeIndicatorsForCandles(candles, DEFAULT_PARAMS);

          const missingSet = new Set(missingInd);
          const toUpsert: IndicatorRow[] = [];
          for (const r of rows) {
            if (r.time < indStart || r.time >= indEnd) continue;
            if (!missingSet.has(r.time)) continue;
            toUpsert.push(r);
          }

          if (DEBUG_RECOVERY) {
            console.log(`[dbg] indicator ${market} ${tf} missing=${missingInd.length} candlesLoaded=${candles.length} computedRows=${rows.length} upsert=${toUpsert.length} offset=${offset}`);
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
