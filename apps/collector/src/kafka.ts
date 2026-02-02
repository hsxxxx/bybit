import { Kafka, logLevel, type Producer } from "kafkajs";
import { config } from "./config";

export type RawCandleMessage = {
  exchange: "bybit";
  type: "kline.1m";
  symbol: string;      // BTCUSDT
  recv_ts: number;     // local receive timestamp (ms)
  payload: unknown;    // raw Bybit WS message
};

export class KafkaOut {
  private producer: Producer;

  constructor() {
    const kafka = new Kafka({
      clientId: config.kafka.clientId,
      brokers: config.kafka.brokers,
      logLevel: logLevel.NOTHING,
    });

    this.producer = kafka.producer({
      allowAutoTopicCreation: true,
      idempotent: false,
    });
  }

  async connect() {
    await this.producer.connect();
  }

  async disconnect() {
    await this.producer.disconnect();
  }

  async sendRaw1m(msg: RawCandleMessage) {
    await this.producer.send({
      topic: config.kafka.topicRaw1m,
      messages: [
        {
          key: msg.symbol,
          value: JSON.stringify(msg),
        },
      ],
    });
  }
}
