// apps/recovery/src/recovery.ts
import type { RecoveryConfig, Candle, Tf } from "./types.js";
import { rangeSlotsSec, TF_SEC, floorToTfSec } from "./timeframes.js";
import { createMaria } from "./db/mariadb.js";
import { createCandleProducer } from "./kafka/producer.js";
import { fetchUpbitCandlesRange } from "./exchange/upbit.js";

type SlotPlan = {
  market: string;
  tf: Tf;
  missingTimes: number[]; // seconds
};

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
        const start = floorToTfSec(cfg.startSec, tf);
        const end = floorToTfSec(cfg.endSec, tf);

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
      console.log(`[recovery] ${p.market} ${p.tf} missing=${p.missingTimes.length} ranges=${ranges.length}`);

      const fetchedAll: Candle[] = [];
      const missingSet = new Set(p.missingTimes);

      const queue = ranges.slice();
      const workers = Array.from({ length: Math.max(1, cfg.restConcurrency) }, async () => {
        while (queue.length) {
          const r = queue.shift();
          if (!r) break;

          const candles = await fetchUpbitCandlesRange({
            baseUrl: cfg.upbit.baseUrl,
            market: p.market,
            tf: p.tf,
            startSec: r.startSec,
            endSec: r.endSec,
            sleepMs: cfg.restSleepMs
          });

          for (const c of candles) {
            if (missingSet.has(c.time)) fetchedAll.push(c);
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
        console.log(`[recovery] republished=${uniq.length} topic=${cfg.kafka.topicCandle} market=${p.market} tf=${p.tf}`);
      }
    }
  } finally {
    if (producer) await producer.disconnect().catch(() => {});
    await maria.close().catch(() => {});
  }
}

function toContiguousRanges(times: number[], step: number): Array<{ startSec: number; endSec: number }> {
  if (times.length === 0) return [];
  const sorted = [...times].sort((a, b) => a - b);

  const ranges: Array<{ startSec: number; endSec: number }> = [];
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
