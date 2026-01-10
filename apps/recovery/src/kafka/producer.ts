// apps/recovery/src/kafka/producer.ts
import { Kafka, CompressionTypes, type Producer } from "kafkajs";
import type { Candle } from "../types.js";

export type CandleProducer = {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  sendCandles(topic: string, candles: Candle[], batchBytes: number): Promise<void>;
};

export function createCandleProducer(params: {
  brokers: string[];
  clientId: string;
}): CandleProducer {
  const kafka = new Kafka({ brokers: params.brokers, clientId: params.clientId });
  const producer: Producer = kafka.producer({ allowAutoTopicCreation: true });

  async function connect() {
    await producer.connect();
  }

  async function disconnect() {
    await producer.disconnect();
  }

  async function sendCandles(topic: string, candles: Candle[], batchBytes: number) {
    // sinker가 기존에 받는 candle JSON 포맷에 맞춰야 함:
    // { market, tf, time, open, high, low, close, volume, ...optional }
    let buf: { value: string }[] = [];
    let size = 0;

    async function flush() {
      if (buf.length === 0) return;
      await producer.send({
        topic,
        compression: CompressionTypes.GZIP,
        messages: buf
      });
      buf = [];
      size = 0;
    }

    for (const c of candles) {
      const s = JSON.stringify(c);
      size += Buffer.byteLength(s, "utf8");
      buf.push({ value: s });
      if (size >= batchBytes) await flush();
    }
    await flush();
  }

  return { connect, disconnect, sendCandles };
}
