// apps/recovery/src/db/mariadb.ts
import mysql from "mysql2/promise";
import type { Candle, Tf } from "../types.js";

export type Maria = {
  close(): Promise<void>;
  getExistingTimes(params: {
    market: string;
    tf: Tf;
    startSec: number;
    endSec: number;
  }): Promise<Set<number>>;
  upsertCandles(candles: Candle[]): Promise<void>;
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

  async function getExistingTimes(p: {
    market: string;
    tf: Tf;
    startSec: number;
    endSec: number;
  }): Promise<Set<number>> {
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

  return { close, getExistingTimes, upsertCandles };
}
