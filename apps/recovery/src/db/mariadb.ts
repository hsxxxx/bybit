// apps/recovery/src/db/mariadb.ts  (추가 메서드만 넣는 버전)
import mysql from "mysql2/promise";
import type { Candle, Tf, IndicatorRow } from "../types.js";

export type Maria = {
  close(): Promise<void>;

  getExistingCandleTimes(params: { market: string; tf: Tf; startSec: number; endSec: number; }): Promise<Set<number>>;
  upsertCandles(candles: Candle[]): Promise<void>;
  getCandles(params: { market: string; tf: Tf; startSec: number; endSec: number; }): Promise<Candle[]>;

  getExistingIndicatorTimes(params: { market: string; tf: Tf; startSec: number; endSec: number; }): Promise<Set<number>>;
  upsertIndicators(rows: IndicatorRow[]): Promise<void>;

  // ✅ NEW: DB time offset 추정용 샘플
  sampleCandleTimes(params: { market: string; tf: Tf; startSec: number; endSec: number; limit: number; }): Promise<number[]>;

  // no-trade (있으면 유지)
  ensureNoTradeTable(): Promise<void>;
  upsertNoTradeSlots(params: { market: string; tf: Tf; times: number[] }): Promise<void>;
  getNoTradeTimes(params: { market: string; tf: Tf; startSec: number; endSec: number; }): Promise<Set<number>>;

  getAllMarkets(): Promise<string[]>;
  getAllTfs(): Promise<Tf[]>;
};

export async function createMaria(params: {
  host: string;
  port: number;
  user: string;
  pass: string;
  name: string;
}): Promise<Maria> {
  const pool = mysql.createPool({
    host: params.host,
    port: params.port,
    user: params.user,
    password: params.pass,
    database: params.name,
    connectionLimit: 8,
    enableKeepAlive: true
  });

  async function close() {
    await pool.end();
  }

  async function getExistingCandleTimes(p: { market: string; tf: Tf; startSec: number; endSec: number; }): Promise<Set<number>> {
    const [rows] = await pool.query(
      `SELECT time FROM upbit_candle WHERE market=? AND tf=? AND time>=? AND time<?`,
      [p.market, p.tf, p.startSec, p.endSec]
    );
    const set = new Set<number>();
    for (const r of rows as any[]) set.add(Number(r.time));
    return set;
  }

  async function getCandles(p: { market: string; tf: Tf; startSec: number; endSec: number; }): Promise<Candle[]> {
    const [rows] = await pool.query(
      `SELECT market, tf, time, open, high, low, close, volume
       FROM upbit_candle
       WHERE market=? AND tf=? AND time>=? AND time<?
       ORDER BY time ASC`,
      [p.market, p.tf, p.startSec, p.endSec]
    );
    return (rows as any[]).map(r => ({
      market: String(r.market),
      tf: r.tf as Tf,
      time: Number(r.time),
      open: Number(r.open),
      high: Number(r.high),
      low: Number(r.low),
      close: Number(r.close),
      volume: Number(r.volume)
    }));
  }

  async function upsertCandles(candles: Candle[]) {
    if (candles.length === 0) return;
    const sql = `
      INSERT INTO upbit_candle (market, tf, time, open, high, low, close, volume)
      VALUES ?
      ON DUPLICATE KEY UPDATE
        open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close), volume=VALUES(volume)
    `;
    const values = candles.map(c => [c.market, c.tf, c.time, c.open, c.high, c.low, c.close, c.volume]);
    await pool.query(sql, [values]);
  }

  async function getExistingIndicatorTimes(p: { market: string; tf: Tf; startSec: number; endSec: number; }): Promise<Set<number>> {
    const [rows] = await pool.query(
      `SELECT time FROM upbit_indicator WHERE market=? AND tf=? AND time>=? AND time<?`,
      [p.market, p.tf, p.startSec, p.endSec]
    );
    const set = new Set<number>();
    for (const r of rows as any[]) set.add(Number(r.time));
    return set;
  }

  async function upsertIndicators(rows: IndicatorRow[]) {
    if (rows.length === 0) return;
    const sql = `
      INSERT INTO upbit_indicator (market, tf, time, indicators)
      VALUES ?
      ON DUPLICATE KEY UPDATE indicators=VALUES(indicators)
    `;
    const values = rows.map(r => {
      const { market, tf, time, ...rest } = r as any;
      const indicators: Record<string, any> = {};
      for (const [k, v] of Object.entries(rest)) {
        if (v === undefined) continue;
        indicators[k] = v;
      }
      return [market, tf, time, JSON.stringify(indicators)];
    });
    await pool.query(sql, [values]);
  }

  async function sampleCandleTimes(p: { market: string; tf: Tf; startSec: number; endSec: number; limit: number; }): Promise<number[]> {
    const [rows] = await pool.query(
      `SELECT time FROM upbit_candle
       WHERE market=? AND tf=? AND time>=? AND time<?
       ORDER BY time ASC
       LIMIT ?`,
      [p.market, p.tf, p.startSec, p.endSec, p.limit]
    );
    return (rows as any[]).map(r => Number(r.time));
  }

  // ---- no-trade table (optional) ----
  async function ensureNoTradeTable() {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS upbit_no_trade (
        market VARCHAR(32) NOT NULL,
        tf     VARCHAR(8)  NOT NULL,
        time   BIGINT      NOT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (market, tf, time),
        INDEX idx_tf_time (tf, time),
        INDEX idx_market_time (market, time)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);
  }

  async function upsertNoTradeSlots(p: { market: string; tf: Tf; times: number[] }) {
    if (p.times.length === 0) return;
    await ensureNoTradeTable();
    const sql = `INSERT INTO upbit_no_trade (market, tf, time) VALUES ? ON DUPLICATE KEY UPDATE time=VALUES(time)`;
    const values = p.times.map(t => [p.market, p.tf, t]);
    await pool.query(sql, [values]);
  }

  async function getNoTradeTimes(p: { market: string; tf: Tf; startSec: number; endSec: number; }): Promise<Set<number>> {
    try {
      const [rows] = await pool.query(
        `SELECT time FROM upbit_no_trade WHERE market=? AND tf=? AND time>=? AND time<?`,
        [p.market, p.tf, p.startSec, p.endSec]
      );
      const set = new Set<number>();
      for (const r of rows as any[]) set.add(Number(r.time));
      return set;
    } catch {
      return new Set<number>();
    }
  }

  async function getAllMarkets(): Promise<string[]> {
    const [rows] = await pool.query(`SELECT DISTINCT market FROM upbit_candle`);
    return (rows as any[]).map(r => String(r.market));
  }

  async function getAllTfs(): Promise<Tf[]> {
    const [rows] = await pool.query(`SELECT DISTINCT tf FROM upbit_candle`);
    return (rows as any[]).map(r => r.tf as Tf);
  }

  return {
    close,
    getExistingCandleTimes,
    upsertCandles,
    getCandles,
    getExistingIndicatorTimes,
    upsertIndicators,
    sampleCandleTimes,
    ensureNoTradeTable,
    upsertNoTradeSlots,
    getNoTradeTimes,
    getAllMarkets,
    getAllTfs
  };
}
