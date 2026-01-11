// apps/recovery/src/recovery.ts
import type { RecoveryConfig, Candle, Tf } from "./types.js";
import { rangeSlotsSec, TF_SEC, floorToTfSec } from "./timeframes.js";
import { createMaria } from "./db/mariadb.js";
import { createCandleProducer } from "./kafka/producer.js";
import { fetchUpbitCandlesRange } from "./exchange/upbit.js";

type SlotPlan = {
  market: string;
  tf: Tf;
  missingTimes: number[]; // seconds (candle open time)
};

type Range = { startSec: number; endSec: number };

function envBool(name: string, def = false): boolean {
  const v = process.env[name];
  if (v == null) return def;
  return ["1", "true", "yes", "y", "on"].includes(String(v).toLowerCase());
}

const DEBUG_RECOVERY = envBool("DEBUG_RECOVERY", false);

export async function runRecovery(cfg: RecoveryConfig) {
  const maria = await createMaria(cfg.db);

  const producer = cfg.kafka.enabled
    ? createCandleProducer({ brokers: cfg.kafka.brokers, clientId: cfg.kafka.clientId })
    : null;

  if (producer) await producer.connect();

  try {
    const plans: SlotPlan[] = [];

    for (const market of cfg.markets) {
      for (const tf of cfg.tfs) {
        const step = TF_SEC[tf];

        // ✅ expected / DB 조회 범위가 항상 동일한 규칙이 되게 통일
        const start = floorToTfSec(cfg.startSec, tf);
        const end = floorToTfSec(cfg.endSec - 1, tf) + step; // exclusive

        if (!(start < end)) continue;

        const expected = rangeSlotsSec(start, end, tf);
        const existing = await maria.getExistingTimes({
          market,
          tf,
          startSec: start,
          endSec: end
        });

        const missing = expected.filter(t => !existing.has(t));
        if (missing.length === 0) continue;

        if (missing.length > cfg.maxMissingPerMarket) {
          throw new Error(
            `Too many missing slots market=${market} tf=${tf} missing=${missing.length} > MAX_MISSING_PER_MARKET=${cfg.maxMissingPerMarket}`
          );
        }

        plans.push({ market, tf, missingTimes: missing });
      }
    }

    if (plans.length === 0) {
      console.log("[recovery] no missing slots");
      return;
    }

    console.log(`[recovery] plans=${plans.length}`);

    for (const p of plans) {
      const step = TF_SEC[p.tf];
      const ranges = toContiguousRanges(p.missingTimes, step);

      console.log(
        `[recovery] ${p.market} ${p.tf} missing=${p.missingTimes.length} ranges=${ranges.length}`
      );

      const missingSet = new Set(p.missingTimes);

      const fetchedAll: Candle[] = [];

      // ✅ 큐 기반 워커 (권장: REST_CONCURRENCY=1로 429 줄이기)
      const queue = ranges.slice();
      const workers = Array.from({ length: Math.max(1, cfg.restConcurrency) }, async () => {
        while (queue.length) {
          const r = queue.shift();
          if (!r) break;

          // Upbit REST fetch (역방향 페이징 + 레이트리밋은 upbit.ts에서 처리)
          const candles = await fetchUpbitCandlesRange({
            baseUrl: cfg.upbit.baseUrl,
            market: p.market,
            tf: p.tf,
            startSec: r.startSec,
            endSec: r.endSec,
            sleepMs: cfg.restSleepMs
          });

          if (DEBUG_RECOVERY) {
            // ✅ 어디서 0이 되는지 한 줄로 확정
            let inRange = 0;
            let matched = 0;
            let minT = Number.POSITIVE_INFINITY;
            let maxT = 0;

            for (const c of candles) {
              if (c.time >= r.startSec && c.time < r.endSec) inRange++;
              minT = Math.min(minT, c.time);
              maxT = Math.max(maxT, c.time);
              if (missingSet.has(c.time)) matched++;
            }

            console.log(
              `[dbg] market=${p.market} tf=${p.tf} range=[${r.startSec},${r.endSec}) got=${candles.length} inRange=${inRange} matchedMissing=${matched} gotMin=${Number.isFinite(minT) ? minT : -1} gotMax=${maxT || -1}`
            );
          }

          // ✅ missing에 해당하는 time만 수집
          for (const c of candles) {
            if (missingSet.has(c.time)) fetchedAll.push(c);
          }
        }
      });

      await Promise.all(workers);

      // ✅ 정렬 + time uniq
      fetchedAll.sort((a, b) => a.time - b.time);
      const uniq: Candle[] = [];
      let prev = -1;
      for (const c of fetchedAll) {
        if (c.time === prev) continue;
        prev = c.time;
        uniq.push(c);
      }

      if (uniq.length === 0) {
        console.log(`[recovery] fetched=0 market=${p.market} tf=${p.tf}`);
        continue;
      }

      if (cfg.mode === "direct_db") {
        await maria.upsertCandles(uniq);
        console.log(`[recovery] direct_db upserted=${uniq.length} market=${p.market} tf=${p.tf}`);
      } else {
        if (!producer) throw new Error("RECOVERY_MODE=republish requires kafka enabled");
        await producer.sendCandles(cfg.kafka.topicCandle, uniq, cfg.kafka.batchBytes);
        console.log(
          `[recovery] republished=${uniq.length} topic=${cfg.kafka.topicCandle} market=${p.market} tf=${p.tf}`
        );
      }
    }
  } finally {
    if (producer) await producer.disconnect().catch(() => {});
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
