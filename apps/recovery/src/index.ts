// apps/recovery/src/index.ts
import "dotenv/config";

import { runRecovery } from "./recovery.js";
import { createMaria } from "./db/mariadb.js";
import type { RecoveryConfig, Tf, RecoveryTask, NoTradeMode } from "./types.js";

function must(v: string | undefined, name: string): string {
  if (!v) throw new Error(`Missing env ${name}`);
  return v;
}

function parseCsv(v: string | undefined): string[] {
  if (!v) return [];
  return v.split(",").map(s => s.trim()).filter(Boolean);
}

function parseTfCsv(v: string | undefined): Tf[] {
  return parseCsv(v) as Tf[];
}

// ✅ FIX: 13자리 epoch(ms) 지원
function parseDateOrSec(v: string): number {
  // epoch seconds
  if (/^\d{10}$/.test(v)) return Number(v);

  // epoch millis
  if (/^\d{13}$/.test(v)) return Math.floor(Number(v) / 1000);

  // yyyy-mm-dd => local midnight
  if (/^\d{4}-\d{2}-\d{2}$/.test(v)) {
    const [y, m, d] = v.split("-").map(Number);
    return Math.floor(new Date(y, m - 1, d, 0, 0, 0, 0).getTime() / 1000);
  }

  // ISO etc
  const t = Date.parse(v);
  if (!Number.isFinite(t)) throw new Error(`Bad date/sec: ${v}`);
  return Math.floor(t / 1000);
}

function parseDateRangeSec(start?: string, end?: string): { startSec: number; endSec: number } {
  const now = new Date();
  const today0 = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0, 0);
  const yday0 = new Date(today0.getTime() - 24 * 60 * 60 * 1000);

  const s = start ? parseDateOrSec(start) : Math.floor(yday0.getTime() / 1000);
  const e = end ? parseDateOrSec(end) : Math.floor(today0.getTime() / 1000);

  if (!(s < e)) throw new Error(`Invalid range startSec=${s} endSec=${e}`);
  return { startSec: s, endSec: e };
}

function isAllToken(arr: string[]): boolean {
  return arr.length === 0 || (arr.length === 1 && arr[0].toUpperCase() === "ALL");
}

async function main() {
  const task = (process.env.RECOVERY_TASK ?? "both") as RecoveryTask;

  const dbConfig = {
    host: must(process.env.DB_HOST, "DB_HOST"),
    port: Number(process.env.DB_PORT ?? "3306"),
    user: must(process.env.DB_USER, "DB_USER"),
    pass: must(process.env.DB_PASS, "DB_PASS"),
    name: must(process.env.DB_NAME, "DB_NAME")
  };

  const maria = await createMaria(dbConfig);

  try {
    const rawMarkets = parseCsv(process.env.MARKETS);
    const rawTfs = parseTfCsv(process.env.TFS);

    const markets = isAllToken(rawMarkets) ? await maria.getAllMarkets() : rawMarkets;
    const tfs = isAllToken(rawTfs as unknown as string[]) ? await maria.getAllTfs() : rawTfs;

    if (markets.length === 0) throw new Error("No markets resolved (upbit_candle is empty?)");
    if (tfs.length === 0) throw new Error("No tfs resolved (upbit_candle is empty?)");

    const { startSec, endSec } = parseDateRangeSec(process.env.START_DATE, process.env.END_DATE);

    const endMarginSec = Number(process.env.END_MARGIN_SEC ?? "0");
    const safeEndSec = Math.max(startSec + 1, endSec - endMarginSec);

    const noTradeMode = (process.env.NO_TRADE_MODE ?? "mark_db") as NoTradeMode;
    const noTradeMarkWholeRange = (process.env.NO_TRADE_MARK_WHOLE_RANGE ?? "1") !== "0";

    const cfg: RecoveryConfig = {
      task,
      markets,
      tfs,

      startSec,
      endSec: safeEndSec,

      maxMissingPerMarket: Number(process.env.MAX_MISSING_PER_MARKET ?? "5000"),
      restConcurrency: Number(process.env.REST_CONCURRENCY ?? "1"),
      restSleepMs: Number(process.env.REST_SLEEP_MS ?? "1200"),
      upbitMinIntervalMs: Number(process.env.UPBIT_MIN_INTERVAL_MS ?? "1200"),

      indicatorLookbackBars: Number(process.env.INDICATOR_LOOKBACK_BARS ?? "600"),

      noTradeMode,
      noTradeMarkWholeRange,

      db: dbConfig,
      upbit: { baseUrl: process.env.UPBIT_REST_BASE ?? "https://api.upbit.com" }
    };

    console.log(
      `[recovery] task=${cfg.task} range start=${cfg.startSec} end=${endSec} safeEnd=${cfg.endSec} (END_MARGIN_SEC=${endMarginSec}) markets=${cfg.markets.length} tfs=${cfg.tfs.length}`
    );

    await runRecovery(cfg);
  } finally {
    await maria.close().catch(() => {});
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
