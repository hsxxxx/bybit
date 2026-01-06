import { NextResponse } from 'next/server';
import { ensureKafkaStarted, getSnapshot } from '@/lib/kafkaStore';
import type { Timeframe } from '@/lib/types';

export const runtime = 'nodejs';

function parseTf(v: string | null): Timeframe | null {
  if (v === '1m' || v === '5m' || v === '15m' || v === '1h' || v === '4h') return v;
  return null;
}

export async function GET(req: Request) {
  await ensureKafkaStarted();

  const { searchParams } = new URL(req.url);
  const market = searchParams.get('market') ?? 'KRW-BTC';
  const tf = parseTf(searchParams.get('tf')) ?? '1m';
  const limit = Math.min(5000, Math.max(10, Number(searchParams.get('limit') ?? '500')));

  const snap = getSnapshot(market, tf, limit);
  return NextResponse.json({ market, tf, ...snap });
}
