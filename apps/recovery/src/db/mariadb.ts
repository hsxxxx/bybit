// apps/recovery/src/db/mariadb.ts  (JSON indicators 스키마에 맞춘 전체)
import mysql from "mysql2/promise";
import type { Candle, Tf, IndicatorRow } from "../types.js";

export type Maria = {
  close(): Promise<void>;

  getExistingCandleTimes(params: { market: string; tf: Tf; startSec: number; endSec: number; }): Promise<Set<number>>;
  upsertCandles(candles: Candle[]): Promise<void>;
  getCandles(params: { market: string; tf: Tf; startSec: number; endSec: number; }): Promise<Candle[]>;

  getExistingIndicatorTimes(params: { market: string; tf: Tf; startSec: number; endSec: number; }): Promise<Set<number>>;
  upsertIndicators(rows: IndicatorRow[]): Promise<void>;

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

  // ✅ JSON 스키마: indicators 컬럼에 row 전체(키 제외)를 넣는다
  async function upsertIndicators(rows: IndicatorRow[]) {
    if (rows.length === 0) return;

    const sql = `
      INSERT INTO upbit_indicator
        (market, tf, time, indicators)
      VALUES ?
      ON DUPLICATE KEY UPDATE
        indicators = VALUES(indicators)
    `;

    const values = rows.map(r => {
      const { market, tf, time, ...rest } = r as any;

      // indicators JSON에는 숫자/null만 들어가게 정리
      const indicators: Record<string, any> = {};
      for (const [k, v] of Object.entries(rest)) {
        if (v === undefined) continue;
        indicators[k] = v;
      }

      return [market, tf, time, JSON.stringify(indicators)];
    });

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
