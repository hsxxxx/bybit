import { Command } from "commander";
import { pingDb, selectDistinctMarkets } from "./db.js";
import { log } from "./logger.js";
import { DEFAULT_TFS, type Mode, type Tf } from "./config.js";
import { recoverCandles, rebuildIndicators } from "./recover.js";
import { kstIsoToUnixSec } from "./time.js";

function parseCsvList(v?: string): string[] | undefined {
  if (!v) return undefined;
  return v
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function parseTfs(v?: string): Tf[] {
  if (!v) return [...DEFAULT_TFS];
  const arr = parseCsvList(v) ?? [];
  const out = arr.map((x) => x.toLowerCase()) as Tf[];
  return out;
}

function parseMode(v?: string): Mode {
  if (v === "all" || v === "missing") return v;
  return "missing";
}

function parseKstIsoToSec(v: string): number {
  // 입력: "2026-01-01T00:00:00" (KST 가정)
  return kstIsoToUnixSec(v);
}

async function main() {
  const program = new Command();

  program
    .name("bits-recovery")
    .description("Upbit candle/indicator recovery (KST candle_date_time_kst 기준 time 통일)")
    .requiredOption("--start <kstIso>", "KST ISO start (e.g. 2026-01-01T00:00:00)")
    .requiredOption("--end <kstIso>", "KST ISO end (e.g. 2026-01-02T00:00:00)")
    .option("--mode <mode>", "all | missing", "missing")
    .option("--markets <list>", "comma separated markets. if omitted, load from DB distinct markets")
    .option("--tfs <list>", "comma separated tfs (1m,5m,15m,1h,4h)", DEFAULT_TFS.join(","))
    .option("--no-candle", "skip candle recovery")
    .option("--no-indicator", "skip indicator rebuild");

  program.parse(process.argv);
  const opts = program.opts();

  const startSec = parseKstIsoToSec(String(opts.start));
  const endSec = parseKstIsoToSec(String(opts.end));
  const mode = parseMode(String(opts.mode));
  const tfs = parseTfs(String(opts.tfs));

  await pingDb();

  let markets = parseCsvList(opts.markets);
  if (!markets || markets.length === 0) {
    markets = await selectDistinctMarkets();
    if (markets.length === 0) {
      throw new Error("No markets found in DB. Provide --markets=KRW-BTC,...");
    }
  }

  log.info(`[run] mode=${mode} markets=${markets.length} tfs=${tfs.join(",")} start=${opts.start} end=${opts.end}`);

  if (opts.candle !== false) {
    await recoverCandles({ markets, tfs, startSec, endSec, mode });
  } else {
    log.warn("[run] candle skipped");
  }

  if (opts.indicator !== false) {
    await rebuildIndicators({ markets, tfs, startSec, endSec, mode });
  } else {
    log.warn("[run] indicator skipped");
  }

  log.info("[run] done");
}

main().catch((e) => {
  log.error("fatal", e);
  process.exit(1);
});
