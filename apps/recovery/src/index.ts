// apps/recovery/src/index.ts
import "dotenv/config";

import { runRecovery } from "./recovery.js";
import { createMaria } from "./db/mariadb.js";
import type { RecoveryConfig, Tf, RecoveryMode } from "./types.js";

function must(v: string | undefined, name: string): string {
  if (!v) throw new Error(`Missing env ${name}`);
  return v;
}

function parseCsv(v: string | undefined): string[] {
  if (!v) return [];
  return v
    .split(",")
    .map(s => s.trim())
    .filter(Boolean);
}

function parseTfCsv(v: string | undefined): Tf[] {
  return parseCsv(v) as Tf[];
}

function parseDateRangeSec(start?: string, end?: string): { startSec: number; endSec: number } {
  // START/END 비면: 어제 00:00 ~ 오늘 00:00 (로컬 타임존 기준)
  const now = new Date();
  const today0 = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0, 0);
  const yday0 = new Date(today0.getTime() - 24 * 60 * 60 * 1000);

  const s = start ? parseDateOrSec(start) : Math.floor(yday0.getTime() / 1000);
  const e = end ? parseDateOrSec(end) : Math.floor(today0.getTime() / 1000);

  if (!(s < e)) throw new Error(`Invalid range startSec=${s} endSec=${e}`);
  return { startSec: s, endSec: e };
}

function parseDateOrSec(v: string): number {
  // epoch seconds
  if (/^\d{10}$/.test(v)) return Number(v);

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

function isAllToken(arr: string[]): boolean {
  return arr.length === 0 || (arr.length === 1 && arr[0].toUpperCase() === "ALL");
}

async function main() {
  const mode = (process.env.RECOVERY_MODE ?? "republish") as RecoveryMode;

  const dbConfig = {
    host: must(process.env.DB_HOST, "DB_HOST"),
    port: Number(process.env.DB_PORT ?? "3306"),
    user: must(process.env.DB_USER, "DB_USER"),
    pass: must(process.env.DB_PASS, "DB_PASS"),
    name: must(process.env.DB_NAME, "DB_NAME")
  };

  // 1) DB 연결 후 MARKETS/TFS ALL 자동확장
  const maria = await createMaria(dbConfig);

  try {
    const rawMarkets = parseCsv(process.env.MARKETS);
    const rawTfs = parseTfCsv(process.env.TFS);

    let markets: string[];
    let tfs: Tf[];

    if (isAllToken(rawMarkets)) markets = await maria.getAllMarkets();
    else markets = rawMarkets;

    if (isAllToken(rawTfs as unknown as string[])) tfs = await maria.getAllTfs();
    else tfs = rawTfs;

    if (markets.length === 0) throw new Error("No markets resolved (upbit_candle is empty?)");
    if (tfs.length === 0) throw new Error("No tfs resolved (upbit_candle is empty?)");

    // 2) 범위 파싱
    const { startSec, endSec } = parseDateRangeSec(process.env.START_DATE, process.env.END_DATE);

    // 3) end 마진 적용 (최근/경계 구간 흔들림 방지)
    // 예: END_MARGIN_SEC=86400 (1일), 43200(12시간) 등
    const endMarginSec = Number(process.env.END_MARGIN_SEC ?? "0");
    const safeEndSec = Math.max(startSec, endSec - endMarginSec);

    if (!(startSec < safeEndSec)) {
      throw new Error(`Invalid safe range after END_MARGIN_SEC=${endMarginSec}: startSec=${startSec}, endSec=${endSec}, safeEndSec=${safeEndSec}`);
    }

    // 4) Kafka 설정
    const kafkaEnabled = mode === "republish";
    const kafkaBrokers = parseCsv(process.env.KAFKA_BROKERS);

    const cfg: RecoveryConfig = {
      mode,

      markets,
      tfs,

      startSec,
      endSec: safeEndSec,

      maxMissingPerMarket: Number(process.env.MAX_MISSING_PER_MARKET ?? "5000"),
      restConcurrency: Number(process.env.REST_CONCURRENCY ?? "1"),
      restSleepMs: Number(process.env.REST_SLEEP_MS ?? "400"),

      kafka: {
        enabled: kafkaEnabled,
        brokers: kafkaBrokers,
        clientId: process.env.KAFKA_CLIENT_ID ?? "recovery",
        topicCandle: process.env.KAFKA_TOPIC_CANDLE ?? "upbit.candle",
        batchBytes: Number(process.env.KAFKA_BATCH_BYTES ?? "900000")
      },

      db: dbConfig,

      upbit: {
        baseUrl: process.env.UPBIT_REST_BASE ?? "https://api.upbit.com"
      }
    };

    console.log(
      `[recovery] range start=${cfg.startSec} end=${endSec} safeEnd=${cfg.endSec} (END_MARGIN_SEC=${endMarginSec}) markets=${cfg.markets.length} tfs=${cfg.tfs.length} mode=${cfg.mode}`
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
