// src/db/mariadb.ts
import mariadb from "mariadb";
import type { Pool } from "mariadb";
import type { DbConfig } from "./config";

export class MariaDB {
  private pool: Pool;

  constructor(private cfg: DbConfig) {
    this.pool = mariadb.createPool({
      host: cfg.host,
      port: cfg.port,
      user: cfg.user,
      password: cfg.password,
      database: cfg.database,
      connectionLimit: 5
    });
  }

  async ping(): Promise<void> {
    const conn = await this.pool.getConnection();
    try {
      await conn.query("SELECT 1");
    } finally {
      conn.release();
    }
  }

  async close(): Promise<void> {
    await this.pool.end();
  }

  async exec(sql: string, params?: any[]): Promise<void> {
    const conn = await this.pool.getConnection();
    try {
      await conn.query(sql, params ?? []);
    } finally {
      conn.release();
    }
  }

  async batch(sql: string, rows: any[][]): Promise<void> {
    if (!rows.length) return;
    const conn = await this.pool.getConnection();
    try {
      await conn.batch(sql, rows);
    } finally {
      conn.release();
    }
  }
}
