import { Kafka } from "kafkajs";
import type { RingStore } from "../store/ring.js";
import type { Candle, Indicator, Tf } from "../store/types.js";
import { tfFromTopic } from "./topics.js";
import { log } from "../lib/logger.js";
import type { WsHub } from "../ws.js";

export async function startKafkaConsumers(params: {
  brokers: string[];
  clientId: string;
  groupId: string;
  topicsCandle: string[];
  topicsIndicator: string[];
  backfillMessages: number;
  store: RingStore;
  wsHub: WsHub;
}) {
  const kafka = new Kafka({
    clientId: params.clientId,
    brokers: params.brokers
  });

  const admin = kafka.admin();
  await admin.connect();

  // backfill: 각 토픽/파티션의 end offset에서 N만큼 뒤로 seek
  const backfillPlan = await buildBackfillPlan(admin, [...params.topicsCandle, ...params.topicsIndicator], params.backfillMessages);
  await admin.disconnect();

  const consumer = kafka.consumer({ groupId: params.groupId });
  await consumer.connect();

  for (const t of params.topicsCandle) await consumer.subscribe({ topic: t, fromBeginning: false });
  for (const t of params.topicsIndicator) await consumer.subscribe({ topic: t, fromBeginning: false });

  consumer.on(consumer.events.GROUP_JOIN, async () => {
    // seek는 run 전에 해도 되지만, join 이후가 가장 안전
    for (const p of backfillPlan) {
      try {
        consumer.seek({ topic: p.topic, partition: p.partition, offset: String(p.offset) });
      } catch (e) {
        // ignore
      }
    }
    log.info(`[kafka] seek backfill partitions=${backfillPlan.length} messages=${params.backfillMessages}`);
  });

  await consumer.run({
    autoCommit: true,
    eachMessage: async ({ topic, message }) => {
      const v = message.value?.toString("utf8");
      if (!v) return;

      const tf = tfFromTopic(topic);
      if (!tf) return;

      if (topic.includes(".candle.")) {
        const c = safeParseCandle(v, tf);
        if (!c) return;
        params.store.putCandle(c);
        params.wsHub.broadcast("candle", c);
        return;
      }

      if (topic.includes(".indicator.")) {
        const i = safeParseIndicator(v, tf);
        if (!i) return;
        params.store.putIndicator(i);
        params.wsHub.broadcast("indicator", i);
        return;
      }
    }
  });

  log.info(`[kafka] consuming candle=${params.topicsCandle.length} indicator=${params.topicsIndicator.length}`);
}

async function buildBackfillPlan(
  admin: ReturnType<Kafka["admin"]>,
  topics: string[],
  backfillMessages: number
): Promise<Array<{ topic: string; partition: number; offset: bigint }>> {
  if (!backfillMessages || backfillMessages <= 0) return [];

  const plan: Array<{ topic: string; partition: number; offset: bigint }> = [];

  for (const topic of topics) {
    try {
      const offsets = await admin.fetchTopicOffsets(topic);
      for (const o of offsets) {
        const hi = BigInt(o.high);
        const lo = BigInt(o.low);
        const bf = BigInt(backfillMessages);

        let start = hi > bf ? hi - bf : lo;
        if (start < lo) start = lo;

        plan.push({ topic, partition: o.partition, offset: start });
      }
    } catch (e) {
      // 토픽이 아직 없거나 권한 문제면 그냥 무시
    }
  }

  return plan;
}

function safeParseCandle(json: string, tf: Tf): Candle | null {
  try {
    const o = JSON.parse(json);
    const market = String(o.market ?? "");
    const time = Number(o.time ?? o.open_time ?? 0);
    if (!market || !Number.isFinite(time)) return null;

    return {
      market,
      tf: (o.tf ?? tf) as Tf,
      time,
      open: Number(o.open ?? o.opening_price ?? 0),
      high: Number(o.high ?? o.high_price ?? 0),
      low: Number(o.low ?? o.low_price ?? 0),
      close: Number(o.close ?? o.trade_price ?? 0),
      volume: Number(o.volume ?? o.candle_acc_trade_volume ?? 0)
    };
  } catch {
    return null;
  }
}

function safeParseIndicator(json: string, tf: Tf): Indicator | null {
  try {
    const o = JSON.parse(json);
    const market = String(o.market ?? "");
    const time = Number(o.time ?? 0);
    const indicators = (o.indicators ?? o) as Record<string, unknown>;
    if (!market || !Number.isFinite(time)) return null;

    // payload가 {market,tf,time, ...indicators} 형태면 indicators만 분리하고 싶으면 여기서 조정
    return {
      market,
      tf: (o.tf ?? tf) as Tf,
      time,
      indicators: o.indicators ? indicators : stripMeta(o)
    };
  } catch {
    return null;
  }
}

function stripMeta(o: any): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(o)) {
    if (k === "market" || k === "tf" || k === "time") continue;
    out[k] = v;
  }
  return out;
}
