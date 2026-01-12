import mysql from "mysql2/promise";
import { config } from "./config.js";

export type CandleRow = {
  market: string;
  tf: string;
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

export type IndicatorRow = {
  market: string;
  tf: string;
  time: number;
  indicators: any; // JSON
};

export const pool = mysql.createPool({
  host: config.db.host,
  port: config.db.port,
  user: config.db.user,
  password: config.db.password,
  database: config.db.database,
  connectionLimit: 10,
  waitForConnections: true
});

export async function pingDb(): Promise<void> {
  const c = await pool.getConnection();
  try {
    await c.ping();
  } finally {
    c.release();
  }
}

export async function selectDistinctMarkets(): Promise<string[]> {
  const [rows] = await pool.query<any[]>(
    `SELECT DISTINCT market FROM upbit_candle ORDER BY market ASC`
  );
  return rows.map((r) => String(r.market));
}

export async function selectExistingCandleTimes(params: {
  market: string;
  tf: string;
  start: number;
  end: number;
}): Promise<Set<number>> {
  const [rows] = await pool.query<any[]>(
    `SELECT time FROM upbit_candle
     WHERE market=? AND tf=? AND time BETWEEN ? AND ?
     ORDER BY time ASC`,
    [params.market, params.tf, params.start, params.end]
  );
  return new Set(rows.map((r) => Number(r.time)));
}

export async function selectExistingIndicatorTimes(params: {
  market: string;
  tf: string;
  start: number;
  end: number;
}): Promise<Set<number>> {
  const [rows] = await pool.query<any[]>(
    `SELECT time FROM upbit_indicator
     WHERE market=? AND tf=? AND time BETWEEN ? AND ?
     ORDER BY time ASC`,
    [params.market, params.tf, params.start, params.end]
  );
  return new Set(rows.map((r) => Number(r.time)));
}

export async function selectCandlesAsc(params: {
  market: string;
  tf: string;
  start: number;
  end: number;
}): Promise<CandleRow[]> {
  const [rows] = await pool.query<any[]>(
    `SELECT market, tf, time, open, high, low, close, volume
     FROM upbit_candle
     WHERE market=? AND tf=? AND time BETWEEN ? AND ?
     ORDER BY time ASC`,
    [params.market, params.tf, params.start, params.end]
  );

  return rows.map((r) => ({
    market: String(r.market),
    tf: String(r.tf),
    time: Number(r.time),
    open: Number(r.open),
    high: Number(r.high),
    low: Number(r.low),
    close: Number(r.close),
    volume: Number(r.volume)
  }));
}

export async function upsertCandles(rows: CandleRow[], mode: "all" | "missing"): Promise<number> {
  if (rows.length === 0) return 0;

  // batch insert
  const values = rows.map((r) => [r.market, r.tf, r.time, r.open, r.high, r.low, r.close, r.volume]);

  if (mode === "missing") {
    const [res] = await pool.query<any>(
      `INSERT IGNORE INTO upbit_candle
        (market, tf, time, open, high, low, close, volume)
       VALUES ?`,
      [values]
    );
    return Number(res.affectedRows ?? 0);
  }

  const [res] = await pool.query<any>(
    `INSERT INTO upbit_candle
      (market, tf, time, open, high, low, close, volume)
     VALUES ?
     ON DUPLICATE KEY UPDATE
       open=VALUES(open),
       high=VALUES(high),
       low=VALUES(low),
       close=VALUES(close),
       volume=VALUES(volume)`,
    [values]
  );

  return Number(res.affectedRows ?? 0);
}

export async function upsertIndicators(rows: IndicatorRow[], mode: "all" | "missing"): Promise<number> {
  if (rows.length === 0) return 0;

  const values = rows.map((r) => [r.market, r.tf, r.time, JSON.stringify(r.indicators)]);

  if (mode === "missing") {
    const [res] = await pool.query<any>(
      `INSERT IGNORE INTO upbit_indicator
        (market, tf, time, indicators)
       VALUES ?`,
      [values]
    );
    return Number(res.affectedRows ?? 0);
  }

  const [res] = await pool.query<any>(
    `INSERT INTO upbit_indicator
      (market, tf, time, indicators)
     VALUES ?
     ON DUPLICATE KEY UPDATE
       indicators=VALUES(indicators)`,
    [values]
  );

  return Number(res.affectedRows ?? 0);
}
