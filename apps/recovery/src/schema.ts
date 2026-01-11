import type { Pool } from "mariadb";

export async function ensureTables(pool: Pool): Promise<void> {
  const conn = await pool.getConnection();
  try {
    await conn.query(`
      CREATE TABLE IF NOT EXISTS upbit_candle (
        market      VARCHAR(32) NOT NULL,
        tf          VARCHAR(8)  NOT NULL,
        time        BIGINT      NOT NULL, -- unix seconds (candle open time)
        open        DOUBLE      NOT NULL,
        high        DOUBLE      NOT NULL,
        low         DOUBLE      NOT NULL,
        close       DOUBLE      NOT NULL,
        volume      DOUBLE      NOT NULL,
        updated_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (market, tf, time),
        INDEX idx_tf_time (tf, time),
        INDEX idx_market_time (market, time)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);

    await conn.query(`
      CREATE TABLE IF NOT EXISTS upbit_indicator (
        market      VARCHAR(32) NOT NULL,
        tf          VARCHAR(8)  NOT NULL,
        time        BIGINT      NOT NULL, -- same candle.time
        indicators  JSON        NOT NULL,
        updated_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (market, tf, time),
        INDEX idx_tf_time (tf, time),
        INDEX idx_market_time (market, time)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);
  } finally {
    conn.release();
  }
}
