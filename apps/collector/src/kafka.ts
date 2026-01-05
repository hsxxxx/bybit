import { Kafka, logLevel, type Producer } from 'kafkajs';
import { config } from './config.js';

export type RawCandleMessage = {
  exchange: 'upbit';
  type: 'candle.1m';
  market: string;           // KRW-BTC
  recv_ts: number;          // local receive timestamp (ms)
  payload: unknown;         // raw Upbit WS message
};

export class KafkaOut {
  private producer: Producer;

  constructor() {
    const kafka = new Kafka({
      clientId: config.kafka.clientId,
      brokers: config.kafka.brokers,
      logLevel: logLevel.NOTHING
    });
    this.producer = kafka.producer({
      allowAutoTopicCreation: true,
      idempotent: false
    });
  }

  async connect() {
    await this.producer.connect();
  }

  async disconnect() {
    await this.producer.disconnect();
  }

  async sendRaw1m(msg: RawCandleMessage) {
    const value = JSON.stringify(msg);
    await this.producer.send({
      topic: config.kafka.topicRaw1m,
      messages: [
        {
          key: msg.market,
          value
        }
      ]
    });
  }
}
