import { Kafka, logLevel, type Consumer, type Producer } from "kafkajs";
import { config } from "./config.js";
import type { Candle } from "./candle.js";
import type { Indicator } from "./indicator.js";

/**
 * Bybit builder 입력은 collector가 만든 "확정 1m 캔들(Candle)"이 기본값.
 * (이전 upbit 구조의 RawCandleMessage는 builder에서 더 이상 사용하지 않음)
 */
function isCandle(x: any): x is Candle {
  return (
    x &&
    typeof x === "object" &&
    typeof x.market === "string" &&
    typeof x.tf === "string" &&
    typeof x.open_time === "number" &&
    typeof x.close_time === "number" &&
    typeof x.open === "number" &&
    typeof x.high === "number" &&
    typeof x.low === "number" &&
    typeof x.close === "number" &&
    typeof x.volume === "number" &&
    typeof x.is_closed === "boolean"
  );
}

export class KafkaIO {
  private consumer: Consumer;
  private producer: Producer;

  constructor() {
    const kafka = new Kafka({
      clientId: config.kafka.clientId,
      brokers: config.kafka.brokers,
      logLevel: logLevel.NOTHING,
    });

    this.consumer = kafka.consumer({ groupId: config.kafka.groupId });
    this.producer = kafka.producer({ allowAutoTopicCreation: true });
  }

  async connect() {
    await this.producer.connect();
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: config.kafka.inRaw1m, fromBeginning: false });
  }

  async disconnect() {
    await this.consumer.disconnect();
    await this.producer.disconnect();
  }

  async run(onCandle: (c: Candle) => Promise<void>) {
    await this.consumer.run({
      autoCommit: true,
      eachMessage: async ({ message }) => {
        if (!message.value) return;
        const obj = JSON.parse(message.value.toString("utf8")) as unknown;

        if (!isCandle(obj)) return; // 포맷 다르면 스킵
        await onCandle(obj);
      },
    });
  }

  async emitCandle(c: Candle) {
    const topic =
      c.tf === "1m"
        ? config.kafka.out1m
        : c.tf === "5m"
        ? config.kafka.out5m
        : c.tf === "15m"
        ? config.kafka.out15m
        : c.tf === "1h"
        ? config.kafka.out1h
        : config.kafka.out4h;

    await this.producer.send({
      topic,
      messages: [{ key: c.market, value: JSON.stringify(c) }],
    });
  }

  async emitIndicator(ind: Indicator) {
    const topic =
      ind.tf === "1m"
        ? config.kafka.outInd1m
        : ind.tf === "5m"
        ? config.kafka.outInd5m
        : ind.tf === "15m"
        ? config.kafka.outInd15m
        : ind.tf === "1h"
        ? config.kafka.outInd1h
        : config.kafka.outInd4h;

    await this.producer.send({
      topic,
      messages: [{ key: ind.market, value: JSON.stringify(ind) }],
    });
  }
}
