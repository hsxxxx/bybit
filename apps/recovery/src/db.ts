import mariadb, { Pool } from "mariadb";
import type { CandleRow, IndicatorRow } from "./types";

export function createPool(cfg: {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
  connectionLimit: number;
}): Pool {
  return mariadb.createPool({
    host: cfg.host,
    port: cfg.port,
    user: cfg.user,
    password: cfg.password,
    database: cfg.database,
    connectionLimit: cfg.connectionLimit,
    timezone: "Z", // store/read unix seconds, timezone irrelevant but keep deterministic
    bigIntAsNumber: true
  });
}

// -------------------- Candle upsert --------------------

export async function upsertCandles(pool: Pool, rows: CandleRow[]): Promise<void> {
  if (!rows || rows.length === 0) return;

  const sql = `
    INSERT INTO upbit_candle
      (market, tf, time, open, high, low, close, volume)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      open = VALUES(open),
      high = VALUES(high),
      low  = VALUES(low),
      close= VALUES(close),
      volume = VALUES(volume)
  `;

  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    for (const r of rows) {
      await conn.query(sql, [
        r.market,
        r.tf,
        r.time,
        r.open,
        r.high,
        r.low,
        r.close,
        r.volume
      ]);
    }
    await conn.commit();
  } catch (e) {
    await conn.rollback();
    throw e;
  } finally {
    conn.release();
  }
}

// -------------------- Indicator upsert --------------------

export async function upsertIndicators(pool: Pool, rows: IndicatorRow[]): Promise<void> {
  if (!rows || rows.length === 0) return;

  const sql = `
    INSERT INTO upbit_indicator
      (market, tf, time, indicators)
    VALUES
      (?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      indicators = VALUES(indicators)
  `;

  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    for (const r of rows) {
      await conn.query(sql, [r.market, r.tf, r.time, JSON.stringify(r.indicators)]);
    }
    await conn.commit();
  } catch (e) {
    await conn.rollback();
    throw e;
  } finally {
    conn.release();
  }
}

// -------------------- NEW: Fill helpers --------------------

/**
 * 범위 내 캔들 오름차순 조회 (fill 단계용)
 */
export async function selectCandlesAsc(
  pool: Pool,
  market: string,
  tf: string,
  startSec: number,
  endSec: number
): Promise<CandleRow[]> {
  const sql = `
    SELECT market, tf, time, open, high, low, close, volume
    FROM upbit_candle
    WHERE market = ?
      AND tf = ?
      AND time BETWEEN ? AND ?
    ORDER BY time ASC
  `;
  const rows = await pool.query(sql, [market, tf, startSec, endSec]);
  return rows as CandleRow[];
}

/**
 * startSec 이전 가장 마지막 캔들 1개 조회 (fill seed용)
 */
export async function selectLastCandleBefore(
  pool: Pool,
  market: string,
  tf: string,
  startSec: number
): Promise<CandleRow | null> {
  const sql = `
    SELECT market, tf, time, open, high, low, close, volume
    FROM upbit_candle
    WHERE market = ?
      AND tf = ?
      AND time < ?
    ORDER BY time DESC
    LIMIT 1
  `;
  const rows = await pool.query(sql, [market, tf, startSec]);
  if (!rows || rows.length === 0) return null;
  return rows[0] as CandleRow;
}
