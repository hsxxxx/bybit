// apps/recovery/src/db/mariadb.ts
import mysql from "mysql2/promise";
import type { Candle, Tf, IndicatorRow } from "../types.js";

export type Maria = {
  close(): Promise<void>;

  getExistingCandleTimes(params: { market: string; tf: Tf; startSec: number; endSec: number; }): Promise<Set<number>>;
  upsertCandles(candles: Candle[]): Promise<void>;
  getCandles(params: { market: string; tf: Tf; startSec: number; endSec: number; }): Promise<Candle[]>;

  // indicator
  getExistingIndicatorTimes(params: { market: string; tf: Tf; startSec: number; endSec: number; }): Promise<Set<number>>;
  upsertIndicators(rows: IndicatorRow[]): Promise<void>;

  // helpers
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
    const sql = `
      SELECT time
      FROM upbit_candle
      WHERE market = ?
        AND tf = ?
        AND time >= ?
        AND time < ?
    `;
    const [rows] = await pool.query(sql, [p.market, p.tf, p.startSec, p.endSec]);
    const set = new Set<number>();
    for (const r of rows as any[]) set.add(Number(r.time));
    return set;
  }

  async function getCandles(p: { market: string; tf: Tf; startSec: number; endSec: number; }): Promise<Candle[]> {
    const sql = `
      SELECT market, tf, time, open, high, low, close, volume
      FROM upbit_candle
      WHERE market = ?
        AND tf = ?
        AND time >= ?
        AND time < ?
      ORDER BY time ASC
    `;
    const [rows] = await pool.query(sql, [p.market, p.tf, p.startSec, p.endSec]);
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
      INSERT INTO upbit_candle
        (market, tf, time, open, high, low, close, volume)
      VALUES ?
      ON DUPLICATE KEY UPDATE
        open = VALUES(open),
        high = VALUES(high),
        low = VALUES(low),
        close = VALUES(close),
        volume = VALUES(volume)
    `;

    const values = candles.map(c => [
      c.market, c.tf, c.time, c.open, c.high, c.low, c.close, c.volume
    ]);

    await pool.query(sql, [values]);
  }

  async function getExistingIndicatorTimes(p: { market: string; tf: Tf; startSec: number; endSec: number; }): Promise<Set<number>> {
    // "row가 존재하는 time"만 체크. (null 컬럼까지 검사하려면 SQL 확장)
    const sql = `
      SELECT time
      FROM upbit_indicator
      WHERE market = ?
        AND tf = ?
        AND time >= ?
        AND time < ?
    `;
    const [rows] = await pool.query(sql, [p.market, p.tf, p.startSec, p.endSec]);
    const set = new Set<number>();
    for (const r of rows as any[]) set.add(Number(r.time));
    return set;
  }

  async function upsertIndicators(rows: IndicatorRow[]) {
    if (rows.length === 0) return;

    // ✅ indicator 컬럼은 테이블 스키마에 맞춰 고정
    const cols = [
      "market", "tf", "time",
      "bb_mid_20", "bb_upper_20_2", "bb_lower_20_2",
      "rsi_14", "stoch_rsi_k_14", "stoch_rsi_d_14",
      "obv", "pvt",
      "ma_10", "ma_20", "ma_60"
    ];

    const sql = `
      INSERT INTO upbit_indicator
        (${cols.join(", ")})
      VALUES ?
      ON DUPLICATE KEY UPDATE
        bb_mid_20 = VALUES(bb_mid_20),
        bb_upper_20_2 = VALUES(bb_upper_20_2),
        bb_lower_20_2 = VALUES(bb_lower_20_2),
        rsi_14 = VALUES(rsi_14),
        stoch_rsi_k_14 = VALUES(stoch_rsi_k_14),
        stoch_rsi_d_14 = VALUES(stoch_rsi_d_14),
        obv = VALUES(obv),
        pvt = VALUES(pvt),
        ma_10 = VALUES(ma_10),
        ma_20 = VALUES(ma_20),
        ma_60 = VALUES(ma_60)
    `;

    const values = rows.map(r => [
      r.market, r.tf, r.time,
      r.bb_mid_20 ?? null, r.bb_upper_20_2 ?? null, r.bb_lower_20_2 ?? null,
      r.rsi_14 ?? null, r.stoch_rsi_k_14 ?? null, r.stoch_rsi_d_14 ?? null,
      r.obv ?? null, r.pvt ?? null,
      r.ma_10 ?? null, r.ma_20 ?? null, r.ma_60 ?? null
    ]);

    await pool.query(sql, [values]);
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
    getAllMarkets,
    getAllTfs
  };
}
