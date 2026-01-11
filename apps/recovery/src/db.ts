import mariadb, { Pool } from "mariadb";
import type { CandleRow, IndicatorRow } from "./types";
import { chunkArray } from "./utils/chunk";

export function createPool(cfg: {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
}): Pool {
  return mariadb.createPool({
    host: cfg.host,
    port: cfg.port,
    user: cfg.user,
    password: cfg.password,
    database: cfg.database,
    connectionLimit: 5,
    timezone: "Z" // 내부는 UTC로 처리 (epoch 기반)
  });
}

export async function upsertCandles(pool: Pool, rows: CandleRow[]): Promise<void> {
  if (rows.length === 0) return;

  const sql = `
    INSERT INTO upbit_candle (market, tf, time, open, high, low, close, volume)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      open=VALUES(open),
      high=VALUES(high),
      low=VALUES(low),
      close=VALUES(close),
      volume=VALUES(volume)
  `;

  // 너무 큰 batch 방지
  const chunks = chunkArray(rows, 500);
  for (const c of chunks) {
    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();
      for (const r of c) {
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
}

export async function upsertIndicators(
  pool: Pool,
  rows: IndicatorRow[]
): Promise<void> {
  if (rows.length === 0) return;

  const sql = `
    INSERT INTO upbit_indicator (market, tf, time, indicators)
    VALUES (?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      indicators=VALUES(indicators)
  `;

  const chunks = chunkArray(rows, 500);
  for (const c of chunks) {
    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();
      for (const r of c) {
        await conn.query(sql, [
          r.market,
          r.tf,
          r.time,
          JSON.stringify(r.indicators)
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
}

export async function selectCandlesForRange(
  pool: Pool,
  params: { market: string; tf: string; fromSec: number; toSec: number }
): Promise<
  Array<{
    time: number;
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
  }>
> {
  const conn = await pool.getConnection();
  try {
    const rows = await conn.query(
      `
      SELECT time, open, high, low, close, volume
      FROM upbit_candle
      WHERE market=? AND tf=? AND time BETWEEN ? AND ?
      ORDER BY time ASC
      `,
      [params.market, params.tf, params.fromSec, params.toSec]
    );
    return rows as any;
  } finally {
    conn.release();
  }
}
