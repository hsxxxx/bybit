// src/lib/ring.ts
export class Ring<T> {
  private buf: (T | undefined)[];
  private cap: number;
  private len: number;
  private head: number; // next write index

  constructor(capacity: number) {
    this.cap = Math.max(1, capacity);
    this.buf = new Array(this.cap);
    this.len = 0;
    this.head = 0;
  }

  size() {
    return this.len;
  }

  capacity() {
    return this.cap;
  }

  /** oldest -> newest */
  toArray(): T[] {
    const out: T[] = [];
    for (let i = 0; i < this.len; i++) {
      const idx = (this.head - this.len + i + this.cap) % this.cap;
      const v = this.buf[idx];
      if (v !== undefined) out.push(v);
    }
    return out;
  }

  last(): T | undefined {
    if (this.len === 0) return undefined;
    const idx = (this.head - 1 + this.cap) % this.cap;
    return this.buf[idx];
  }

  /** append always */
  push(v: T) {
    this.buf[this.head] = v;
    this.head = (this.head + 1) % this.cap;
    if (this.len < this.cap) this.len++;
  }

  /** replace last element (if exists), else push */
  replaceLast(v: T) {
    if (this.len === 0) {
      this.push(v);
      return;
    }
    const idx = (this.head - 1 + this.cap) % this.cap;
    this.buf[idx] = v;
  }

  /**
   * Upsert by time key (monotonic stream expected):
   * - if same time as last => replace last
   * - if greater => push
   * - if smaller => ignore
   */
  upsertByTime(v: T, getTime: (x: T) => number): "replace" | "append" | "ignore" {
    const t = getTime(v);
    const last = this.last();
    if (!last) {
      this.push(v);
      return "append";
    }
    const lt = getTime(last);
    if (t === lt) {
      this.replaceLast(v);
      return "replace";
    }
    if (t > lt) {
      this.push(v);
      return "append";
    }
    // older/out-of-order
    return "ignore";
  }
}
