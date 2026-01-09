// src/kafka.ts
import { Kafka, logLevel, type Consumer } from "kafkajs";
import type { SinkerConfig } from "./config";

function toKafkaLogLevel(level: string) {
  const v = (level ?? "").toLowerCase();
  if (v === "debug") return logLevel.DEBUG;
  if (v === "warn" || v === "warning") return logLevel.WARN;
  if (v === "error") return logLevel.ERROR;
  if (v === "nothing" || v === "silent") return logLevel.NOTHING;
  return logLevel.INFO;
}

export type KafkaHandlers = {
  onCandle: (tf: string, obj: any, meta: { topic: string; partition: number; offset: string }) => void;
  onIndicator: (tf: string, obj: any, meta: { topic: string; partition: number; offset: string }) => void;
};

function buildTopicToTf(map: Record<string, string>) {
  const out = new Map<string, string>();
  for (const [tf, topic] of Object.entries(map)) out.set(topic, tf);
  return out;
}

export async function startKafkaConsumers(
  cfg: SinkerConfig,
  handlers: KafkaHandlers
): Promise<{ consumerCandle: Consumer; consumerInd: Consumer }> {
  const kafka = new Kafka({
    clientId: cfg.kafkaClientId,
    brokers: cfg.kafkaBrokers,
    logLevel: toKafkaLogLevel(cfg.logLevel)
  });

  const candleTopicToTf = buildTopicToTf(cfg.candleTopics);
  const indTopicToTf = buildTopicToTf(cfg.indTopics);

  // -----------------------
  // Candle consumer
  // -----------------------
  const consumerCandle = kafka.consumer({ groupId: `${cfg.kafkaGroupId}-candle` });
  consumerCandle.on(consumerCandle.events.CRASH, (e) => console.error("[kafka][candle] CRASH", e.payload?.error));
  consumerCandle.on(consumerCandle.events.GROUP_JOIN, (e) =>
    console.log("[kafka][candle] GROUP_JOIN", e.payload?.groupId, "member", e.payload?.memberId)
  );

  await consumerCandle.connect();

  for (const [tf, topic] of Object.entries(cfg.candleTopics)) {
    console.log("[kafka][candle] subscribe", tf, topic);
    await consumerCandle.subscribe({ topic, fromBeginning: false });
  }

  await consumerCandle.run({
    autoCommit: true,
    eachMessage: async ({ topic, partition, message }) => {
      const offset = message.offset;
      // const len = message.value?.length ?? 0;
      // console.log("[kafka][candle] recv", topic, "p", partition, "o", offset, "bytes", len);

      const tf = candleTopicToTf.get(topic);
      if (!tf) {
        console.warn("[kafka][candle] drop(no tf mapping) topic=", topic);
        return;
      }

      try {
        const v = message.value?.toString("utf8");
        if (!v) return;
        const obj = JSON.parse(v);
        handlers.onCandle(tf, obj, { topic, partition, offset });
      } catch (e) {
        console.error("[kafka][candle] parse error", e);
      }
    }
  });

  // -----------------------
  // Indicator consumer
  // -----------------------
  const consumerInd = kafka.consumer({ groupId: `${cfg.kafkaGroupId}-indicator` });
  consumerInd.on(consumerInd.events.CRASH, (e) => console.error("[kafka][ind] CRASH", e.payload?.error));
  consumerInd.on(consumerInd.events.GROUP_JOIN, (e) =>
    console.log("[kafka][ind] GROUP_JOIN", e.payload?.groupId, "member", e.payload?.memberId)
  );

  await consumerInd.connect();

  for (const [tf, topic] of Object.entries(cfg.indTopics)) {
    console.log("[kafka][ind] subscribe", tf, topic);
    await consumerInd.subscribe({ topic, fromBeginning: false });
  }

  await consumerInd.run({
    autoCommit: true,
    eachMessage: async ({ topic, partition, message }) => {
      const offset = message.offset; // ✅ 여기 추가/선언
      // const len = message.value?.length ?? 0;
      // console.log("[kafka][ind] recv", topic, "p", partition, "o", offset, "bytes", len);

      const tf = indTopicToTf.get(topic);
      if (!tf) {
        console.warn("[kafka][ind] drop(no tf mapping) topic=", topic);
        return;
      }

      try {
        const v = message.value?.toString("utf8");
        if (!v) return;
        const obj = JSON.parse(v);
        handlers.onIndicator(tf, obj, { topic, partition, offset }); // ✅ offset 사용 OK
      } catch (e) {
        console.error("[kafka][ind] parse error", e);
      }
    }
  });

  return { consumerCandle, consumerInd };
}
