export class Ring<T> {
  private buf: T[] = [];
  constructor(private readonly max: number) {}

  push(v: T) {
    this.buf.push(v);
    if (this.buf.length > this.max) this.buf.shift();
  }

  toArray(limit?: number): T[] {
    if (!limit || limit >= this.buf.length) return [...this.buf];
    return this.buf.slice(this.buf.length - limit);
  }

  get size() {
    return this.buf.length;
  }
}
