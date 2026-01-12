import type { Pool } from "mariadb";
import type { CandleRow, IndicatorRow, Timeframe } from "../types";
import { selectCandlesAsc, upsertIndicators } from "../db";
import { tfStepSeconds } from "../utils/time";
import { log } from "../logger";

function sma(values: number[], period: number, idx: number): number | null {
  if (idx + 1 < period) return null;
  let s = 0;
  for (let i = idx - period + 1; i <= idx; i++) s += values[i];
  return s / period;
}

function stddev(values: number[], period: number, idx: number): number | null {
  const m = sma(values, period, idx);
  if (m == null) return null;
  let v = 0;
  for (let i = idx - period + 1; i <= idx; i++) {
    const d = values[i] - m;
    v += d * d;
  }
  return Math.sqrt(v / period);
}

function rsi(values: number[], period: number, idx: number): number | null {
  if (idx < period) return null;
  let gain = 0;
  let loss = 0;
  for (let i = idx - period + 1; i <= idx; i++) {
    const diff = values[i] - values[i - 1];
    if (diff >= 0) gain += diff;
    else loss += -diff;
  }
  const avgGain = gain / period;
  const avgLoss = loss / period;
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return 100 - 100 / (1 + rs);
}

function stoch(values: number[], period: number, idx: number): number | null {
  if (idx + 1 < period) return null;
  let lo = Infinity;
  let hi = -Infinity;
  for (let i = idx - period + 1; i <= idx; i++) {
    lo = Math.min(lo, values[i]);
    hi = Math.max(hi, values[i]);
  }
  if (hi === lo) return 0;
  return ((values[idx] - lo) / (hi - lo)) * 100;
}

function ema(values: number[], period: number): number[] {
  const k = 2 / (period + 1);
  const out: number[] = new Array(values.length);
  let prev = values[0];
  out[0] = prev;
  for (let i = 1; i < values.length; i++) {
    prev = values[i] * k + prev * (1 - k);
    out[i] = prev;
  }
  return out;
}

function obv(closes: number[], volumes: number[]): number[] {
  const out: number[] = new Array(closes.length);
  let v = 0;
  out[0] = 0;
  for (let i = 1; i < closes.length; i++) {
    if (closes[i] > closes[i - 1]) v += volumes[i];
    else if (closes[i] < closes[i - 1]) v -= volumes[i];
    out[i] = v;
  }
  return out;
}

function pvt(closes: number[], volumes: number[]): number[] {
  const out: number[] = new Array(closes.length);
  let v = 0;
  out[0] = 0;
  for (let i = 1; i < closes.length; i++) {
    const prev = closes[i - 1];
    const chg = prev === 0 ? 0 : (closes[i] - prev) / prev;
    v += chg * volumes[i];
    out[i] = v;
  }
  return out;
}

export async function rebuildIndicatorsForMarketTf(params: {
  pool: Pool;
  market: string;
  tf: Timeframe;
  startSec: number;
  endSec: number;
  lookback: number; // seconds
}): Promise<{ total: number }> {
  const { pool, market, tf, startSec, endSec, lookback } = params;

  // lookback 포함해서 읽고, 결과 저장은 start~end만
  const fromSec = Math.max(0, startSec - lookback);
  const rows: CandleRow[] = await selectCandlesAsc(pool, market, tf, fromSec, endSec);

  if (!rows || rows.length === 0) {
    log.info(`[indicators] ${market} ${tf} done`, { total: 0 });
    return { total: 0 };
  }

  // asc ordered by time already
  const closes = rows.map((r: CandleRow) => Number(r.close));
  const volumes = rows.map((r: CandleRow) => Number(r.volume));

  const ema12 = ema(closes, 12);
  const ema26 = ema(closes, 26);
  const macd = closes.map((_, i) => ema12[i] - ema26[i]);
  const macdSignal = ema(macd, 9);

  const obvArr = obv(closes, volumes);
  const pvtArr = pvt(closes, volumes);

  const step = tfStepSeconds(tf);

  const out: IndicatorRow[] = [];

  for (let i = 0; i < rows.length; i++) {
    const t = rows[i].time;

    // 저장 범위 밖이면 스킵 (lookback 구간)
    if (t < startSec || t > endSec) continue;

    const bbMid = sma(closes, 20, i);
    const bbStd = stddev(closes, 20, i);
    const bbUpper = bbMid != null && bbStd != null ? bbMid + 2 * bbStd : null;
    const bbLower = bbMid != null && bbStd != null ? bbMid - 2 * bbStd : null;

    const rsi14 = rsi(closes, 14, i);
    const stochRsi = rsi14 == null ? null : stoch(
      // stoch on RSI series (simple)
      rows.map((_, j) => (rsi(closes, 14, j) ?? NaN)),
      14,
      i
    );

    const indicators: Record<string, number | null> = {
      // Bollinger (20, 2)
      bb_mid_20: bbMid,
      bb_upper_20_2: bbUpper,
      bb_lower_20_2: bbLower,

      // RSI / StochRSI (simple)
      rsi_14: rsi14,
      stochrsi_14: Number.isFinite(stochRsi as any) ? (stochRsi as number) : null,

      // MACD
      macd: macd[i],
      macd_signal: macdSignal[i],

      // Volume flows
      obv: obvArr[i],
      pvt: pvtArr[i]
    };

    out.push({
      market,
      tf,
      time: t,
      indicators
    });
  }

  await upsertIndicators(pool, out);

  log.info(`[indicators] ${market} ${tf} done`, { total: out.length, step });
  return { total: out.length };
}
