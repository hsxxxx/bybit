import { Kafka, logLevel, type Consumer, type Producer } from 'kafkajs';
import { config } from './config.js';
import type { Candle } from './candle.js';

export type RawCandleMessage = {
  exchange: 'upbit';
  type: 'candle.1m';
  market: string;
  recv_ts: number;
  payload: any;
};

export class KafkaIO {
  private consumer: Consumer;
  private producer: Producer;

  constructor() {
    const kafka = new Kafka({
      clientId: config.kafka.clientId,
      brokers: config.kafka.brokers,
      logLevel: logLevel.NOTHING
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

  async run(onRaw: (msg: RawCandleMessage) => Promise<void>) {
    await this.consumer.run({
      autoCommit: true,
      eachMessage: async ({ message }) => {
        if (!message.value) return;
        const raw = JSON.parse(message.value.toString('utf8')) as RawCandleMessage;
        await onRaw(raw);
      }
    });
  }

  async emitCandle(c: Candle) {
    const topic =
      c.tf === '1m' ? config.kafka.out1m :
      c.tf === '5m' ? config.kafka.out5m :
      c.tf === '15m' ? config.kafka.out15m :
      c.tf === '1h' ? config.kafka.out1h :
      config.kafka.out4h;

    await this.producer.send({
      topic,
      messages: [
        {
          key: c.market,
          value: JSON.stringify(c)
        }
      ]
    });
  }
}
