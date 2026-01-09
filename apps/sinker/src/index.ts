// src/index.ts
import path from "node:path";
import fs from "node:fs";

import { config } from "./config";
import { CandleStore } from "./store/CandleStore";
import { startHttpServer } from "./http/server";
import { startWsServer } from "./ws/server";
import { startKafkaConsumers } from "./kafka";
import { parseTimeframe, unixSecNow } from "./utils/timeframe";

import { dbConfig } from "./db/config";
import { MariaDB } from "./db/mariadb";
import { SinkerDbWriter } from "./db/sinkerDbWriter";

import type { BuiltPayload, Candle } from "./types";
import { MergeJoin } from "./merge/MergeJoin";

function ensureDir(p: string) {
  fs.mkdirSync(p, { recursive: true });
}

function recentDaysFiles(allFiles: string[], warmupDays: number): string[] {
  if (warmupDays <= 0) return [];
  return allFiles.slice(Math.max(0, allFiles.length - warmupDays));
}

function toNumber(v: any): number {
  if (v == null) return 0;
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

// ms timestamp면 sec로 변환
function normalizeTsToSec(t: number): number {
  if (t > 10_000_000_000) return Math.floor(t / 1000);
  return t;
}

function pickMarket(obj: any): string | null {
  return obj?.market ?? obj?.code ?? obj?.symbol ?? obj?.ticker ?? null;
}

// candle open time(sec) 추출 (가능한 필드들)
function pickCandleTimeSec(obj: any): number | null {
  // candle payload 중첩
  if (obj?.candle?.time != null) {
    const t = normalizeTsToSec(toNumber(obj.candle.time));
    return t || null;
  }

  const raw =
    obj?.time ??
    obj?.t ??
    obj?.timestamp ??
    obj?.candle_time ??
    obj?.candleTime ??
    obj?.open_time ??
    obj?.openTime;

  if (raw == null) return null;
  const t = normalizeTsToSec(toNumber(raw));
  return t || null;
}

function normalizeBuiltPayload(tf: string, obj: any): BuiltPayload | null {
  const market = pickMarket(obj);
  if (!market) return null;

  // A) { candle: {...} }
  if (obj?.candle?.time != null) {
    const c = obj.candle;
    const time = normalizeTsToSec(toNumber(c.time));
    if (!time) return null;

    const candle: Candle = {
      time,
      open: toNumber(c.open),
      high: toNumber(c.high),
      low: toNumber(c.low),
      close: toNumber(c.close),
      volume: toNumber(c.volume)
    };

    return {
      market,
      tf: (obj.tf ?? tf) as any,
      candle,
      indicators: obj.indicators ?? undefined
    };
  }

  // B) flat candle
  const time = pickCandleTimeSec(obj);
  if (!time) return null;

  const candle: Candle = {
    time,
    open: toNumber(obj.open ?? obj.o ?? obj.opening_price),
    high: toNumber(obj.high ?? obj.h ?? obj.high_price),
    low: toNumber(obj.low ?? obj.l ?? obj.low_price),
    close: toNumber(obj.close ?? obj.c ?? obj.trade_price ?? obj.closing_price),
    volume: toNumber(obj.volume ?? obj.v ?? obj.trade_volume ?? obj.candle_acc_trade_volume)
  };

  const indicators = obj.indicators ?? obj.ind ?? undefined;

  return {
    market,
    tf: (obj.tf ?? tf) as any,
    candle,
    indicators
  };
}

// indicator 메시지 -> (market, tf, timeSec, indicators)
function normalizeIndicatorMsg(tf: string, obj: any): { market: string; tf: string; timeSec: number; indicators: Record<string, any> } | null {
  const market = pickMarket(obj);
  if (!market) return null;

  // indicator는 open_time(ms)가 핵심
  const openMs = obj?.open_time ?? obj?.openTime;
  const timeSec = openMs != null ? normalizeTsToSec(toNumber(openMs)) : pickCandleTimeSec(obj);
  if (!timeSec) return null;

  // indicators는 나머지 필드 전체(메타 필드 제외)
  const drop = new Set([
    "exchange",
    "market",
    "code",
    "symbol",
    "ticker",
    "tf",
    "time",
    "t",
    "timestamp",
    "open_time",
    "openTime",
    "close_time",
    "closeTime",
    "indicator_version"
  ]);

  const indicators: Record<string, any> = {};
  for (const [k, v] of Object.entries(obj ?? {})) {
    if (drop.has(k)) continue;
    if (v == null) indicators[k] = null;
    else if (typeof v === "number") indicators[k] = v;
    else if (typeof v === "string") {
      const n = Number(v);
      indicators[k] = Number.isFinite(n) ? n : v;
    } else {
      indicators[k] = v;
    }
  }

  if (!Object.keys(indicators).length) return null;

  return {
    market,
    tf: (obj.tf ?? tf),
    timeSec,
    indicators
  };
}

async function main() {
  const baseDir = path.resolve(config.dataDir);
  ensureDir(baseDir);

  const store = new CandleStore(baseDir, config.maxCandlesPerSeries);

  // warmup
  if (fs.existsSync(baseDir)) {
    const markets = fs
      .readdirSync(baseDir)
      .filter((f) => fs.statSync(path.join(baseDir, f)).isDirectory());

    for (const market of markets) {
      const marketDir = path.join(baseDir, market);
      const tfs = fs
        .readdirSync(marketDir)
        .filter((f) => fs.statSync(path.join(marketDir, f)).isDirectory());

      for (const tfStr of tfs) {
        try {
          const tf = parseTimeframe(tfStr);
          const files = store.getWriter().listSegmentFiles(market, tfStr);
          const pick = recentDaysFiles(files, config.warmupDays);
          store.warmup(market, tf, pick);
        } catch {
          // ignore
        }
      }
    }
  }

  const { server: httpServer } = startHttpServer(config, store);
  const ws = startWsServer(config.wsPort);

  // DB
  let db: MariaDB | null = null;
  let dbWriter: SinkerDbWriter | null = null;

  if (dbConfig.enable) {
    db = new MariaDB(dbConfig);
    await db.ping();
    dbWriter = new SinkerDbWriter(db, dbConfig);
    dbWriter.start();
    console.log("[db] enabled");
  } else {
    console.log("[db] disabled");
  }

  // merge join (TTL 5분)
  const join = new MergeJoin(5 * 60_000, 200_000);

  function emitMerged(payload: BuiltPayload) {
    // 1) store + ws 는 merged payload 기준 (viewer가 indicators까지 받음)
    store.ingest(payload);
    ws.broadcast(payload);

    // 2) DB: candle + indicator 둘 다 upsert
    if (dbWriter) {
      dbWriter.enqueue(payload);
      if (payload.indicators) {
        dbWriter.enqueueIndicator(payload.market, payload.tf, payload.candle.time, payload.indicators);
      }
    }
  }

  const { consumerCandle, consumerInd } = await startKafkaConsumers(config, {
    onCandle: (tf, obj, _meta) => {
      const payload = normalizeBuiltPayload(tf, obj);
      if (!payload) return;

      // candle은 일단 DB candle로 넣고,
      // indicator가 있으면 merge 완료 후 emitMerged에서 indicator도 같이 처리
      // (indicator가 늦게 오면 join에서 붙어서 emit)
      if (dbWriter) dbWriter.enqueue(payload);

      const merged = join.ingestCandle(payload);
      if (merged?.merged) {
        emitMerged(merged.merged);
      } else {
        // 아직 indicator 없음: viewer에는 candle만(원하면 여기서도 broadcast 가능)
        store.ingest(payload);
        ws.broadcast(payload);
      }
    },

    onIndicator: (tf, obj, _meta) => {
      const ind = normalizeIndicatorMsg(tf, obj);
      if (!ind) return;

      // indicator-only도 DB에는 들어가게
      if (dbWriter) dbWriter.enqueueIndicator(ind.market, ind.tf, ind.timeSec, ind.indicators);

      const merged = join.ingestIndicator(ind.market, ind.tf, ind.timeSec, ind.indicators);
      if (merged?.merged) {
        emitMerged(merged.merged);
      }
    }
  });

  const startedAt = unixSecNow();
  console.log(
    JSON.stringify(
      {
        service: "sinker",
        startedAt,
        http: config.port,
        ws: config.wsPort,
        db: dbConfig.enable ? { host: dbConfig.host, port: dbConfig.port, name: dbConfig.database } : null
      },
      null,
      2
    )
  );

  const shutdown = async () => {
    try {
      if (dbWriter) {
        await dbWriter.flush();
        dbWriter.stop();
      }
    } catch {}

    try {
      await consumerCandle.disconnect();
    } catch {}
    try {
      await consumerInd.disconnect();
    } catch {}

    try {
      ws.server.close();
    } catch {}
    try {
      httpServer.close();
    } catch {}

    try {
      if (db) await db.close();
    } catch {}

    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((e) => {
  console.error("[sinker] fatal", e);
  process.exit(1);
});
