import pLimit from "p-limit";
import { loadConfig } from "./config";
import { log } from "./logger";
import { createPool } from "./db";
import { ensureTables } from "./schema";
import { fetchMarkets } from "./upbit";
import type { Timeframe } from "./types";
import { recoverCandlesForMarketTf } from "./steps/recoverCandles";
import { rebuildIndicatorsForMarketTf } from "./steps/rebuildIndicators";
import { fillMissingCandles } from "./steps/fillMissingCandles";
import { verifyVolume5m } from "./steps/verifyVolume5m";

async function main() {
  const cfg = loadConfig();

  log.info("recovery start", {
    tfList: cfg.tfList,
    marketFilter: cfg.marketFilter,
    startSec: cfg.startSec,
    endSec: cfg.endSec,
    mode: cfg.mode,
    concurrency: cfg.concurrency,
    fill1m: cfg.fill1m,
    verify5m: cfg.verify5m
  });

  const pool = createPool(cfg.db);
  await ensureTables(pool);

  const markets = await fetchMarkets(cfg.marketFilter);
  if (markets.length === 0) throw new Error("No markets found");
  log.info(`markets loaded: ${markets.length}`);

  let doneCount = 0;
  const totalJobs = markets.length * cfg.tfList.length;
  const hb = setInterval(() => {
    log.info(`[progress] ${doneCount}/${totalJobs} jobs done`);
  }, 15_000);

  const limit = pLimit(cfg.concurrency);

  const tasks: Array<Promise<void>> = [];

  for (const market of markets) {
    for (const tf of cfg.tfList) {
      tasks.push(
        limit(async () => {
          const t0 = Date.now();
          log.info(`[start] ${market} ${tf}`);

          if (cfg.mode === "candles" || cfg.mode === "all") {
            await recoverCandlesForMarketTf({
              pool,
              market,
              tf: tf as Timeframe,
              startSec: cfg.startSec,
              endSec: cfg.endSec
            });
          }

          // ✅ 1m fill (candles 이후)
          if (cfg.fill1m && tf === "1m") {
            await fillMissingCandles({
              pool,
              market,
              tf: "1m",
              startSec: cfg.startSec,
              endSec: cfg.endSec
            });
          }

          if (cfg.mode === "indicators" || cfg.mode === "all") {
            await rebuildIndicatorsForMarketTf({
              pool,
              market,
              tf: tf as Timeframe,
              startSec: cfg.startSec,
              endSec: cfg.endSec,
              lookback: cfg.indicatorLookback
            });
          }

          // ✅ 5m 검증 (5m 작업 끝난 뒤)
          if (cfg.verify5m && tf === "5m") {
            await verifyVolume5m({
              pool,
              market,
              startSec: cfg.startSec,
              endSec: cfg.endSec,
              epsilon: 1e-6,
              limit: 200
            });
          }

          const ms = Date.now() - t0;
          doneCount += 1;
          log.info(`[done] ${market} ${tf} ${ms}ms`);
        })
      );
    }
  }

  await Promise.all(tasks);

  clearInterval(hb);
  await pool.end();
  log.info("recovery finished");
}

main().catch((e) => {
  log.error("recovery failed", e);
  process.exit(1);
});
