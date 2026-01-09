// src/db/sinkerDbWriter.ts
import type { BuiltPayload } from "../types";
import { MariaDB } from "./mariadb";
import type { DbConfig } from "./config";

type CandleRow = [string, string, number, number, number, number, number, number];
type IndRow = [string, string, number, string];

export class SinkerDbWriter {
  private candleBuf: CandleRow[] = [];
  private indBuf: IndRow[] = [];
  private timer: NodeJS.Timeout | null = null;
  private flushing = false;

  private lastStatAt = Date.now();

  constructor(private db: MariaDB, private cfg: DbConfig) {}

  start() {
    if (!this.cfg.enable) return;
    if (this.timer) return;
    this.timer = setInterval(() => void this.flush(), this.cfg.flushMs);
  }

  stop() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  // candle(+optional indicators) payload
  enqueue(payload: BuiltPayload) {
    if (!this.cfg.enable) return;

    const market = payload?.market;
    const tf = payload?.tf;
    const candle = payload?.candle;

    if (!market || !tf || !candle?.time) return;

    this.candleBuf.push([
      market,
      tf,
      candle.time,
      candle.open,
      candle.high,
      candle.low,
      candle.close,
      candle.volume
    ]);

    const indicators = payload?.indicators;
    if (indicators && Object.keys(indicators).length) {
      this.indBuf.push([market, tf, candle.time, JSON.stringify(indicators)]);
    }

    this.maybeLogBuf();
    this.maybeFlush();
  }

  // ✅ indicator-only upsert (candle 없어도 저장)
  enqueueIndicator(market: string, tf: string, time: number, indicators: Record<string, any>) {
    if (!this.cfg.enable) return;
    if (!market || !tf || !time) return;
    if (!indicators || !Object.keys(indicators).length) return;

    this.indBuf.push([market, tf, time, JSON.stringify(indicators)]);

    this.maybeLogBuf();
    this.maybeFlush();
  }

  private maybeFlush() {
    if (this.candleBuf.length >= this.cfg.batchMax || this.indBuf.length >= this.cfg.batchMax) {
      void this.flush();
    }
  }

  private maybeLogBuf() {
    const now = Date.now();
    if (now - this.lastStatAt > 2000) {
      console.log(`[db] buf candle=${this.candleBuf.length}, ind=${this.indBuf.length}`);
      this.lastStatAt = now;
    }
  }

  async flush() {
    if (!this.cfg.enable) return;
    if (this.flushing) return;
    if (!this.candleBuf.length && !this.indBuf.length) return;

    this.flushing = true;

    const candleRows = this.candleBuf.splice(0, this.candleBuf.length);
    const indRows = this.indBuf.splice(0, this.indBuf.length);

    try {
      if (candleRows.length) {
        await this.db.batch(
          `
          INSERT INTO upbit_candle
            (market, tf, time, open, high, low, close, volume)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)
          ON DUPLICATE KEY UPDATE
            open=VALUES(open),
            high=VALUES(high),
            low=VALUES(low),
            close=VALUES(close),
            volume=VALUES(volume),
            updated_at=CURRENT_TIMESTAMP
          `,
          candleRows
        );
      }

      if (indRows.length) {
        await this.db.batch(
          `
          INSERT INTO upbit_indicator
            (market, tf, time, indicators)
          VALUES (?, ?, ?, ?)
          ON DUPLICATE KEY UPDATE
            indicators=VALUES(indicators),
            updated_at=CURRENT_TIMESTAMP
          `,
          indRows
        );
      }

      console.log(`[db] flushed candle=${candleRows.length}, ind=${indRows.length}`);
    } catch (e) {
      this.candleBuf.unshift(...candleRows);
      this.indBuf.unshift(...indRows);
      console.error("[db] flush error", e);
    } finally {
      this.flushing = false;
    }
  }
}
