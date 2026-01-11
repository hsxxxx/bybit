import dotenv from "dotenv";
import type { Timeframe } from "./types";
import { parseKstTextToEpochSeconds } from "./utils/time";

dotenv.config();

function req(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env: ${name}`);
  return v;
}

function opt(name: string, def?: string): string | undefined {
  return process.env[name] ?? def;
}

export type Config = {
  db: {
    host: string;
    port: number;
    user: string;
    password: string;
    database: string;
  };
  tfList: Timeframe[];
  marketFilter?: string;
  startSec: number;
  endSec: number;
  concurrency: number;
  mode: "candles" | "indicators" | "all";
  indicatorLookback: number;
};

const TF_ALLOWED: Timeframe[] = ["1m", "5m", "15m", "1h", "4h"];

export function loadConfig(): Config {
  const tfListRaw = (opt("TF_LIST", "1m") ?? "1m")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  const tfList = tfListRaw.map((tf) => {
    if (!TF_ALLOWED.includes(tf as Timeframe))
      throw new Error(`Unsupported tf: ${tf}`);
    return tf as Timeframe;
  });

  const startSec = parseKstTextToEpochSeconds(req("START_KST"));
  const endSec = parseKstTextToEpochSeconds(req("END_KST"));
  if (endSec < startSec) throw new Error("END_KST must be >= START_KST");

  const modeRaw = (opt("MODE", "all") ?? "all").trim() as Config["mode"];
  if (!["candles", "indicators", "all"].includes(modeRaw))
    throw new Error(`Invalid MODE: ${modeRaw}`);

  return {
    db: {
      host: req("DB_HOST"),
      port: Number(opt("DB_PORT", "3306")),
      user: req("DB_USER"),
      password: req("DB_PASSWORD"),
      database: req("DB_DATABASE")
    },
    tfList,
    marketFilter: opt("MARKET_FILTER"),
    startSec,
    endSec,
    concurrency: Number(opt("CONCURRENCY", "3")),
    mode: modeRaw,
    indicatorLookback: Number(opt("INDICATOR_LOOKBACK", "120"))
  };
}
