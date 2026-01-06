import { Kafka } from 'kafkajs';
import { Ring } from './ring';
import type { Candle, Indicator, Timeframe } from './types';

type MarketKey = `${string}:${Timeframe}`; // "KRW-BTC:1m"

type Store = {
  candles: Map<MarketKey, Ring<Candle>>;
  indicators: Map<MarketKey, Ring<Indicator>>;
};

const RING_SIZE = Number(process.env.RING_SIZE ?? '2000');

function getKey(market: string, tf: Timeframe): MarketKey {
  return `${market}:${tf}`;
}

function ensureStore(): Store {
  const g = globalThis as any;
  if (!g.__BITS_VIEWER_STORE__) {
    g.__BITS_VIEWER_STORE__ = {
      candles: new Map(),
      indicators: new Map(),
    } satisfies Store;
  }
  return g.__BITS_VIEWER_STORE__ as Store;
}

function ensureRing<T>(map: Map<any, Ring<T>>, key: any): Ring<T> {
  const v = map.get(key);
  if (v) return v;
  const r = new Ring<T>(RING_SIZE);
  map.set(key, r);
  return r;
}

// ---- Public accessors used by API routes ----
export function getSnapshot(market: string, tf: Timeframe, limit: number) {
  const store = ensureStore();
  const key = getKey(market, tf);
  const c = store.candles.get(key)?.toArray(limit) ?? [];
  const i = store.indicators.get(key)?.toArray(limit) ?? [];
  return { candles: c, indicators: i };
}

// ---- Kafka consumer bootstrap (runs once per process) ----
async function startConsumerOnce() {
  const g = globalThis as any;
  if (g.__BITS_VIEWER_CONSUMER_STARTED__) return;
  g.__BITS_VIEWER_CONSUMER_STARTED__ = true;

  const brokers = (process.env.KAFKA_BROKERS ?? '').split(',').map(s => s.trim()).filter(Boolean);
  if (!brokers.length) throw new Error('Missing env: KAFKA_BROKERS');

  const kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID ?? 'bits-viewer',
    brokers,
  });

  const consumer = kafka.consumer({ groupId: process.env.KAFKA_GROUP_ID ?? 'bits-viewer-v1' });

  const TOPIC_CANDLE_1M = process.env.TOPIC_CANDLE_1M ?? 'upbit.candle.1m';
  const TOPIC_IND_1M = process.env.TOPIC_INDICATOR_1M ?? 'upbit.indicator.1m';

  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC_CANDLE_1M, fromBeginning: false });
  await consumer.subscribe({ topic: TOPIC_IND_1M, fromBeginning: false });

  const store = ensureStore();

  await consumer.run({
    autoCommit: true,
    eachMessage: async ({ topic, message }) => {
      if (!message.value) return;
      const txt = message.value.toString('utf8');
      try {
        const obj = JSON.parse(txt);

        // candle
        if (topic === TOPIC_CANDLE_1M) {
          const c = obj as Candle;
          if (!c?.market || !c?.tf) return;
          const key = getKey(c.market, c.tf);
          ensureRing(store.candles, key).push(c);
          // fanout to WS clients (optional, via wsServer singleton)
          const ws = (globalThis as any).__BITS_VIEWER_WS__;
          ws?.broadcast?.({ type: 'candle', data: c });
          return;
        }

        // indicator
        if (topic === TOPIC_IND_1M) {
          const ind = obj as Indicator;
          if (!ind?.market || !ind?.tf) return;
          const key = getKey(ind.market, ind.tf);
          ensureRing(store.indicators, key).push(ind);
          const ws = (globalThis as any).__BITS_VIEWER_WS__;
          ws?.broadcast?.({ type: 'indicator', data: ind });
          return;
        }
      } catch {
        // ignore malformed
      }
    },
  });

  console.log('[viewer] kafka consumer running');
}

export async function ensureKafkaStarted() {
  await startConsumerOnce();
}
