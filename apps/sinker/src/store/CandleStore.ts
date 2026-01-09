// sinker/src/store/CandleStore.ts
import type { BuiltPayload, Candle, Timeframe } from "../types";
import { FileSegmentWriter } from "./FileSegmentWriter";
import { unixSecNow } from "../utils/timeframe";

type CandleItem = Candle & { indicators?: Record<string, number | null> };

function keyOf(market: string, tf: Timeframe) {
  return `${market}::${tf}`;
}

function upsertAscByTime(arr: CandleItem[], item: CandleItem, max: number) {
  // binary search insert/replace by time (asc unique)
  let lo = 0;
  let hi = arr.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    const t = arr[mid]!.time;
    if (t === item.time) {
      arr[mid] = item;
      return;
    }
    if (t < item.time) lo = mid + 1;
    else hi = mid - 1;
  }
  // insert at lo
  arr.splice(lo, 0, item);

  // trim older
  if (arr.length > max) {
    arr.splice(0, arr.length - max);
  }
}

export class CandleStore {
  private readonly mem = new Map<string, CandleItem[]>();
  private readonly writer: FileSegmentWriter;
  private lastIngestAt: number = 0;

  constructor(private readonly dataDir: string, private readonly maxCandles: number) {
    this.writer = new FileSegmentWriter(dataDir);
  }

  getLastIngestAt(): number {
    return this.lastIngestAt;
  }

  ingest(payload: BuiltPayload) {
    const { market, tf, candle, indicators } = payload;
    const k = keyOf(market, tf);

    const item: CandleItem = { ...candle };
    if (indicators) item.indicators = indicators;

    const arr = this.mem.get(k) ?? [];
    upsertAscByTime(arr, item, this.maxCandles);
    this.mem.set(k, arr);

    // persist
    this.writer.write(payload);

    this.lastIngestAt = unixSecNow();
  }

  warmup(market: string, tf: Timeframe, segmentFiles: string[]) {
    const k = keyOf(market, tf);
    const arr: CandleItem[] = [];

    for (const file of segmentFiles) {
      const rows = this.writer.readNdjsonFile(file);
      for (const r of rows) {
        if (r.market !== market || r.tf !== tf) continue;
        const item: CandleItem = { ...r.candle };
        if (r.indicators) item.indicators = r.indicators;
        upsertAscByTime(arr, item, this.maxCandles);
      }
    }

    this.mem.set(k, arr);
  }

  getSnapshot(market: string, tf: Timeframe, limit: number, from?: number, to?: number): CandleItem[] {
    const k = keyOf(market, tf);
    const arr = this.mem.get(k) ?? [];
    if (!arr.length) return [];

    let startIdx = 0;
    let endIdx = arr.length; // exclusive

    if (from != null) {
      // first index with time >= from
      let lo = 0, hi = arr.length - 1, ans = arr.length;
      while (lo <= hi) {
        const mid = (lo + hi) >> 1;
        if (arr[mid]!.time >= from) { ans = mid; hi = mid - 1; }
        else lo = mid + 1;
      }
      startIdx = ans;
    }
    if (to != null) {
      // first index with time > to
      let lo = 0, hi = arr.length - 1, ans = arr.length;
      while (lo <= hi) {
        const mid = (lo + hi) >> 1;
        if (arr[mid]!.time > to) { ans = mid; hi = mid - 1; }
        else lo = mid + 1;
      }
      endIdx = ans;
    }

    const sliced = arr.slice(startIdx, endIdx);
    if (limit <= 0) return sliced;
    return sliced.length > limit ? sliced.slice(sliced.length - limit) : sliced;
  }

  listMarketsTfs(): Array<{ market: string; tf: string; count: number }> {
    const out: Array<{ market: string; tf: string; count: number }> = [];
    for (const [k, v] of this.mem.entries()) {
      const [market, tf] = k.split("::");
      out.push({ market: market!, tf: tf!, count: v.length });
    }
    out.sort((a, b) => (a.market + a.tf).localeCompare(b.market + b.tf));
    return out;
  }

  getWriter(): FileSegmentWriter {
    return this.writer;
  }
}
