import "dotenv/config";
import type { Timeframe } from "./types";
import { parseKstTextToEpochSeconds } from "./utils/time";

function env(name: string, def?: string): string {
  const v = process.env[name];
  if (v == null || v === "") {
    if (def == null) throw new Error(`Missing env: ${name}`);
    return def;
  }
  return v;
}

function envInt(name: string, def: number): number {
  const v = process.env[name];
  if (v == null || v === "") return def;
  const n = Number(v);
  if (!Number.isFinite(n)) throw new Error(`Invalid int env: ${name}=${v}`);
  return Math.floor(n);
}

function envBool(name: string, def: boolean): boolean {
  const v = process.env[name];
  if (v == null || v === "") return def;
  return ["1", "true", "yes", "y", "on"].includes(String(v).toLowerCase());
}

function parseTfList(v: string): Timeframe[] {
  const tfs = v.split(",").map((s) => s.trim()).filter(Boolean) as Timeframe[];
  return tfs;
}

export type AppConfig = {
  db: {
    host: string;
    port: number;
    user: string;
    password: string;
    database: string;
    connectionLimit: number;
  };
  marketFilter?: string;
  tfList: Timeframe[];
  startSec: number;
  endSec: number;
  concurrency: number;
  mode: "candles" | "indicators" | "all";
  indicatorLookback: number;

  // ✅ fill / verify
  fill1m: boolean;
  verify5m: boolean;
};

export function loadConfig(): AppConfig {
  const startKst = env("START_KST", "2026-01-01 00:00:00");
  const endKst = env("END_KST", "2026-01-10 23:59:59");

  return {
    db: {
      host: env("DB_HOST", "127.0.0.1"),
      port: envInt("DB_PORT", 3306),
      user: env("DB_USER", "root"),
      password: env("DB_PASSWORD", ""),
      database: env("DB_NAME", "bits"),
      connectionLimit: envInt("DB_POOL", 5)
    },
    marketFilter: process.env.MARKET_FILTER || undefined,
    tfList: parseTfList(env("TF_LIST", "1m,5m,15m,1h,4h")),
    startSec: parseKstTextToEpochSeconds(startKst),
    endSec: parseKstTextToEpochSeconds(endKst),
    concurrency: envInt("CONCURRENCY", 1),
    mode: (env("MODE", "all") as any) || "all",
    indicatorLookback: envInt("INDICATOR_LOOKBACK", 600),

    fill1m: envBool("FILL_1M", true),
    verify5m: envBool("VERIFY_5M", true)
  };
}
