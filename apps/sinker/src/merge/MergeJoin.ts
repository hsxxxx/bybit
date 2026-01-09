// src/merge/MergeJoin.ts
import type { BuiltPayload } from "../types";

type Indicators = Record<string, any>;

type MergeResult = {
  merged: BuiltPayload;
  hadCandle: boolean;
  hadIndicator: boolean;
};

function nowMs() {
  return Date.now();
}

export class MergeJoin {
  private candleMap = new Map<string, BuiltPayload>();
  private indMap = new Map<string, Indicators>();
  private tsMap = new Map<string, number>(); // last touched ms

  constructor(
    private ttlMs: number = 5 * 60_000, // 5분
    private maxKeys: number = 200_000
  ) {}

  private key(market: string, tf: string, timeSec: number) {
    return `${market}|${tf}|${timeSec}`;
  }

  private touch(k: string) {
    this.tsMap.set(k, nowMs());
    if (this.tsMap.size > this.maxKeys) this.evictOld();
  }

  private evictOld() {
    const cutoff = nowMs() - this.ttlMs;
    for (const [k, t] of this.tsMap.entries()) {
      if (t < cutoff) {
        this.tsMap.delete(k);
        this.candleMap.delete(k);
        this.indMap.delete(k);
      }
    }
  }

  ingestCandle(payload: BuiltPayload): MergeResult | null {
    const market = payload?.market;
    const tf = payload?.tf;
    const time = payload?.candle?.time;
    if (!market || !tf || !time) return null;

    const k = this.key(market, tf, time);
    this.candleMap.set(k, payload);
    this.touch(k);

    const ind = this.indMap.get(k);
    if (!ind) return null;

    const merged: BuiltPayload = {
      ...payload,
      indicators: { ...(payload.indicators ?? {}), ...ind }
    };

    // merge 완료 시 캐시 제거(중복 방지)
    this.candleMap.delete(k);
    this.indMap.delete(k);
    this.tsMap.delete(k);

    return { merged, hadCandle: true, hadIndicator: true };
  }

  ingestIndicator(market: string, tf: string, timeSec: number, indicators: Indicators): MergeResult | null {
    if (!market || !tf || !timeSec) return null;
    if (!indicators || !Object.keys(indicators).length) return null;

    const k = this.key(market, tf, timeSec);
    this.indMap.set(k, indicators);
    this.touch(k);

    const candle = this.candleMap.get(k);
    if (!candle) return null;

    const merged: BuiltPayload = {
      ...candle,
      indicators: { ...(candle.indicators ?? {}), ...indicators }
    };

    // merge 완료 시 캐시 제거
    this.candleMap.delete(k);
    this.indMap.delete(k);
    this.tsMap.delete(k);

    return { merged, hadCandle: true, hadIndicator: true };
  }
}
