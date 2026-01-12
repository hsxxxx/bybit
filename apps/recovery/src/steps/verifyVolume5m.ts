import type { Pool } from "mariadb";
import { log } from "../logger";

/**
 * 5m 거래량 검증:
 * - 5m candle.volume  ~=  sum(1m volume) within same 5m bucket
 * - 1m fill은 volume=0이므로, "실제 거래량" 합산에 영향 없음.
 *
 * 주의:
 * - 업비트 캔들 집계/반올림/정합 이슈로 소수점 오차가 있을 수 있어 epsilon 적용
 */
export async function verifyVolume5m(params: {
  pool: Pool;
  market: string;
  startSec: number;
  endSec: number;
  epsilon?: number; // default 1e-6
  limit?: number;   // default 200
}): Promise<void> {
  const { pool, market, startSec, endSec } = params;
  const epsilon = params.epsilon ?? 1e-6;
  const limit = params.limit ?? 200;

  const sql = `
    SELECT
      c5.time                                         AS t5,
      c5.volume                                       AS vol_5m,
      IFNULL(SUM(c1.volume), 0)                       AS vol_1m_sum,
      (IFNULL(SUM(c1.volume), 0) - c5.volume)         AS diff
    FROM upbit_candle c5
    LEFT JOIN upbit_candle c1
      ON c1.market = c5.market
     AND c1.tf = '1m'
     AND c1.time >= c5.time
     AND c1.time <  c5.time + 300
    WHERE c5.market = ?
      AND c5.tf = '5m'
      AND c5.time BETWEEN ? AND ?
    GROUP BY c5.time, c5.volume
    HAVING ABS(diff) > ?
    ORDER BY c5.time
    LIMIT ?
  `;

  const bad = await pool.query(sql, [market, startSec, endSec, epsilon, limit]);

  if (bad.length === 0) {
    log.info(`[verify] ${market} 5m vs 1m volume OK`, { startSec, endSec });
    return;
  }

  log.warn(`[verify] ${market} volume mismatch found`, { rows: bad.length });
  // 너무 길게 찍지 않도록 일부만
  for (const r of bad.slice(0, 20)) {
    log.warn(`[verify-row]`, r);
  }
}
