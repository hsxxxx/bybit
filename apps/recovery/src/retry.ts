import { config } from "./config.js";
import { log } from "./logger.js";

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function jitter(ms: number): number {
  const j = ms * (0.2 * (Math.random() * 2 - 1)); // +/-20%
  return Math.max(0, Math.floor(ms + j));
}

export type RetryCtx = {
  label: string;
};

export async function withRetry<T>(
  fn: (attempt: number) => Promise<T>,
  shouldRetry: (e: unknown) => { retry: boolean; waitMs?: number; reason?: string },
  ctx: RetryCtx
): Promise<T> {
  const maxRetry = config.upbit.maxRetry;
  const base = config.upbit.retryBaseMs;
  const cap = config.upbit.retryMaxMs;

  for (let attempt = 0; attempt <= maxRetry; attempt++) {
    try {
      return await fn(attempt);
    } catch (e) {
      const d = shouldRetry(e);
      if (!d.retry || attempt === maxRetry) throw e;

      const exp = Math.min(cap, base * Math.pow(2, attempt));
      const waitMs = d.waitMs ?? jitter(exp);
      log.warn(`[retry] ${ctx.label} attempt=${attempt + 1}/${maxRetry} wait=${waitMs}ms reason=${d.reason ?? "n/a"}`);
      await sleep(waitMs);
    }
  }
  throw new Error("unreachable");
}

export class RateLimiter {
  private intervalMs: number;
  private nextAt = 0;

  constructor(rps: number) {
    const safe = Math.max(1, rps);
    this.intervalMs = Math.floor(1000 / safe);
  }

  async wait(): Promise<void> {
    const now = Date.now();
    const at = Math.max(now, this.nextAt);
    this.nextAt = at + this.intervalMs;
    const sleepMs = at - now;
    if (sleepMs > 0) await new Promise((r) => setTimeout(r, sleepMs));
  }
}
