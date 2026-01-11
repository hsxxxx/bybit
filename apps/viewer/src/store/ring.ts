import type { Candle, Indicator, Tf } from "./types.js";

type Key = string;

function key(market: string, tf: string) {
  return `${market}|${tf}`;
}

class Ring<T extends { time: number }> {
  private buf: T[] = [];
  constructor(private readonly cap: number) {}

  push(v: T) {
    // 시간 중복은 업데이트로 간주: 마지막 time과 같으면 교체
    const n = this.buf.length;
    if (n > 0 && this.buf[n - 1].time === v.time) {
      this.buf[n - 1] = v;
      return;
    }
    this.buf.push(v);
    if (this.buf.length > this.cap) this.buf.shift();
  }

  snapshot(limit: number): T[] {
    const n = this.buf.length;
    if (limit <= 0 || limit >= n) return [...this.buf];
    return this.buf.slice(n - limit);
  }

  latest(): T | undefined {
    return this.buf[this.buf.length - 1];
  }

  size(): number {
    return this.buf.length;
  }
}

export type RingStore = {
  putCandle(c: Candle): void;
  putIndicator(i: Indicator): void;

  getCandles(market: string, tf: Tf, limit: number): Candle[];
  getIndicators(market: string, tf: Tf, limit: number): Indicator[];

  listMarkets(): string[];
  listTfs(): string[];

  stats(): any;
};

export function createRingStore(cap: number): RingStore {
  const candles = new Map<Key, Ring<Candle>>();
  const indicators = new Map<Key, Ring<Indicator>>();

  const markets = new Set<string>();
  const tfs = new Set<string>();

  function getOrCreateCandleRing(market: string, tf: string) {
    const k = key(market, tf);
    let r = candles.get(k);
    if (!r) {
      r = new Ring<Candle>(cap);
      candles.set(k, r);
    }
    return r;
  }

  function getOrCreateIndicatorRing(market: string, tf: string) {
    const k = key(market, tf);
    let r = indicators.get(k);
    if (!r) {
      r = new Ring<Indicator>(cap);
      indicators.set(k, r);
    }
    return r;
  }

  function putCandle(c: Candle) {
    markets.add(c.market);
    tfs.add(c.tf);
    getOrCreateCandleRing(c.market, c.tf).push(c);
  }

  function putIndicator(i: Indicator) {
    markets.add(i.market);
    tfs.add(i.tf);
    getOrCreateIndicatorRing(i.market, i.tf).push(i);
  }

  function getCandles(market: string, tf: Tf, limit: number) {
    const r = candles.get(key(market, tf));
    return r ? r.snapshot(limit) : [];
  }

  function getIndicators(market: string, tf: Tf, limit: number) {
    const r = indicators.get(key(market, tf));
    return r ? r.snapshot(limit) : [];
  }

  function listMarkets() {
    return [...markets].sort();
  }

  function listTfs() {
    return [...tfs].sort();
  }

  function stats() {
    return {
      rings: {
        candle: candles.size,
        indicator: indicators.size
      },
      markets: markets.size,
      tfs: tfs.size
    };
  }

  return {
    putCandle,
    putIndicator,
    getCandles,
    getIndicators,
    listMarkets,
    listTfs,
    stats
  };
}
