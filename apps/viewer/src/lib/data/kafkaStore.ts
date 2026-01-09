// apps/viewer/src/lib/data/kafkaStore.ts
import { Kafka } from "kafkajs";
import { Ring } from "./ring";
import type { Candle, Indicator, Timeframe } from "../market/types";

type MarketKey = `${string}:${Timeframe}`; // e.g. "KRW-BTC:1m"

type Store = {
  candles: Map<MarketKey, Ring<Candle>>;
  indicators: Map<MarketKey, Ring<Indicator>>;
};

const RING_SIZE = Number(process.env.RING_SIZE ?? "2000");

// -------------------------
// Event fanout (for external WS / other consumers)
// -------------------------
type Event =
  | { type: "candle"; topic: string; data: Candle }
  | { type: "indicator"; topic: string; data: Indicator };

type Listener = (evt: Event) => void;
const listeners = new Set<Listener>();

/**
 * Generic subscribe to all events (candle+indicator)
 */
export function subscribe(fn: Listener) {
  listeners.add(fn);
  return () => listeners.delete(fn);
}

/**
 * Subscribe candle events only (useful for scripts/viewer-ws.ts)
 */
export function subscribeCandleStream(fn: (tf: Timeframe, candle: Candle) => void) {
  return subscribe((evt) => {
    if (evt.type !== "candle") return;
    fn(evt.data.tf as Timeframe, evt.data);
  });
}

/**
 * Subscribe indicator events only (useful for scripts/viewer-ws.ts)
 */
export function subscribeIndicatorStream(fn: (tf: Timeframe, ind: Indicator) => void) {
  return subscribe((evt) => {
    if (evt.type !== "indicator") return;
    fn(evt.data.tf as Timeframe, evt.data);
  });
}

function emit(evt: Event) {
  for (const fn of listeners) {
    try {
      fn(evt);
    } catch {}
  }
}

// -------------------------
// Utils
// -------------------------
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

function parseTopics(envKey: string, fallback: string): string[] {
  return (process.env[envKey] ?? fallback)
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

/**
 * 서버/WS에서 ms로 줄 때가 있어서 heuristic으로 seconds로 통일
 */
function ensureSeconds(t: number): number {
  return t >= 1_000_000_000_000 ? Math.floor(t / 1000) : t;
}

/**
 * Candle time accessor (필요시 네 Candle 구조에 맞게 수정)
 */
function getCandleTime(c: Candle): number {
  // @ts-ignore
  return ensureSeconds((c as any).time);
}

/**
 * Indicator time accessor (필요시 네 Indicator 구조에 맞게 수정)
 * - Indicator에 time 필드가 없으면, 중복 제거/정렬을 skip하도록 NaN 반환
 */
function getIndicatorTime(ind: Indicator): number {
  // 대부분 indicator에도 candle time이 같이 들어감 (예: time)
  // @ts-ignore
  const t = (ind as any).time;
  return typeof t === "number" && Number.isFinite(t) ? ensureSeconds(t) : NaN;
}

/**
 * ✅ time ASC 정렬 + time 중복 제거(동일 time이면 마지막 값 유지)
 */
function normalizeAscUniqueByTime<T>(arr: T[], getTime: (x: T) => number): T[] {
  if (!arr.length) return [];

  // time이 전혀 없으면 그대로 반환
  const times = arr.map(getTime);
  if (times.every((t) => !Number.isFinite(t))) return arr;

  const items = arr
    .map((v) => ({ v, t: getTime(v) }))
    .filter((x) => Number.isFinite(x.t))
    .sort((a, b) => a.t - b.t);

  const map = new Map<number, T>();
  for (const it of items) map.set(it.t, it.v); // 동일 time이면 마지막 값이 남음

  // map values는 삽입순이지만, 중복 set 시 순서가 유지될 수 있으니 안전하게 재정렬
  return Array.from(map.entries())
    .sort((a, b) => a[0] - b[0])
    .map(([, v]) => v);
}

// ---- Public accessors used by API routes ----
/**
 * ✅ Snapshot 반환 시점에 항상
 * - time ASC 정렬
 * - time 중복 제거
 * 을 보장해서 lightweight-charts assertion을 원천 차단
 */
export function getSnapshot(market: string, tf: Timeframe, limit: number) {
  const store = ensureStore();
  const key = getKey(market, tf);

  const rawCandles = store.candles.get(key)?.toArray().slice(-limit) ?? [];
  const rawIndicators = store.indicators.get(key)?.toArray().slice(-limit) ?? [];

  const candles = normalizeAscUniqueByTime(rawCandles, getCandleTime);
  const indicators = normalizeAscUniqueByTime(rawIndicators, getIndicatorTime);

  return { candles, indicators };
}

/**
 * (옵션) 특정 market/tf의 최신 candle을 빠르게 얻고 싶을 때
 */
export function getLastCandle(market: string, tf: Timeframe): Candle | null {
  const store = ensureStore();
  const key = getKey(market, tf);
  const last = store.candles.get(key)?.toArray().slice(-1)[0] ?? null;
  return last;
}

// ---- Kafka consumer bootstrap (runs once per process) ----
async function startConsumerOnce() {
  const g = globalThis as any;
  if (g.__BITS_VIEWER_CONSUMER_STARTED__) return;
  g.__BITS_VIEWER_CONSUMER_STARTED__ = true;

  const brokers = (process.env.KAFKA_BROKERS ?? "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  if (!brokers.length) {
    throw new Error("Missing env: KAFKA_BROKERS");
  }

  const kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID ?? "bits-viewer",
    brokers,
  });

  const consumer = kafka.consumer({
    groupId: process.env.KAFKA_GROUP_ID ?? "bits-viewer-v1",
  });

  const admin = kafka.admin();

  // ✅ comma-separated topics
  const TOPICS_CANDLE = parseTopics("TOPICS_CANDLE", "upbit.candle.1m");
  const TOPICS_IND = parseTopics("TOPICS_INDICATOR", "upbit.indicator.1m");

  // backfill messages per partition (seek to high - BACKFILL)
  const BACKFILL = Math.max(0, Number(process.env.KAFKA_BACKFILL_MESSAGES ?? "0"));

  await consumer.connect();
  await admin.connect();

  for (const t of TOPICS_CANDLE) {
    await consumer.subscribe({ topic: t, fromBeginning: false });
  }
  for (const t of TOPICS_IND) {
    await consumer.subscribe({ topic: t, fromBeginning: false });
  }

  const store = ensureStore();

  // ✅ group join -> seek back N messages per partition
  const { GROUP_JOIN } = consumer.events;
  consumer.on(GROUP_JOIN, async () => {
    if (!BACKFILL) return;

    try {
      for (const topic of [...TOPICS_CANDLE, ...TOPICS_IND]) {
        const offsets = await admin.fetchTopicOffsets(topic);
        for (const o of offsets) {
          const partition = Number(o.partition);
          const high = Number(o.high); // next offset (end)
          const low = Number(o.low);
          const start = Math.max(low, high - BACKFILL);
          consumer.seek({ topic, partition, offset: String(start) });
        }
      }
      console.log(`[viewer] backfill seek applied: per-partition last ${BACKFILL} msgs`);
    } catch (e) {
      console.error("[viewer] backfill seek failed", e);
    }
  });

  await consumer.run({
    autoCommit: true,
    eachMessage: async ({ topic, message }) => {
      if (!message.value) return;

      const txt = message.value.toString("utf8");

      // ✅ 1) 어떤 토픽이 실제로 들어오는지 확인
      if (process.env.DEBUG_KAFKA === "1") {
        console.log("[viewer] recv topic =", topic);
      }

      try {
        const obj = JSON.parse(txt);

        // candle
        if (TOPICS_CANDLE.includes(topic)) {
          const raw = obj as any;
          if (!raw?.market || !raw?.tf) return;

          // ✅ time(sec) 생성: open_time(ms) -> sec
          if (typeof raw.open_time === "number" && Number.isFinite(raw.open_time)) {
            raw.time = Math.floor(raw.open_time / 1000);
          } else if (typeof raw.time === "number" && Number.isFinite(raw.time)) {
            raw.time = raw.time >= 1_000_000_000_000 ? Math.floor(raw.time / 1000) : raw.time;
          }

          const c = raw as Candle;

          const key = getKey(c.market, c.tf);
          ensureRing(store.candles, key).push(c);

          emit({ type: "candle", topic, data: c });

          const ws = (globalThis as any).__BITS_VIEWER_WS__;
          ws?.broadcast?.({ type: "candle", data: c });
          return;
        }

        // indicator
        if (TOPICS_IND.includes(topic)) {
          const raw = obj as any;
          if (!raw?.market || !raw?.tf) return;

          // ✅ indicator도 time(sec) 생성: open_time(ms) -> sec
          if (typeof raw.open_time === "number" && Number.isFinite(raw.open_time)) {
            raw.time = Math.floor(raw.open_time / 1000);
          } else if (typeof raw.time === "number" && Number.isFinite(raw.time)) {
            raw.time = raw.time >= 1_000_000_000_000 ? Math.floor(raw.time / 1000) : raw.time;
          }

          const ind = raw as Indicator;

          const key = getKey(ind.market, ind.tf);
          ensureRing(store.indicators, key).push(ind);

          emit({ type: "indicator", topic, data: ind });

          const ws = (globalThis as any).__BITS_VIEWER_WS__;
          ws?.broadcast?.({ type: "indicator", data: ind });
          return;
        }

      } catch {
        // ignore malformed json
      }
    },
  });

  console.log("[viewer] kafka consumer running");
}

export async function ensureKafkaStarted() {
  await startConsumerOnce();
}
