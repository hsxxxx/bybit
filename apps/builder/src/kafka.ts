import { Kafka, logLevel, type Consumer, type Producer, type KafkaConfig, type LogEntry } from 'kafkajs';
import { config } from './config.js';
import type { Candle } from './candle.js';
import type { Indicator } from './indicator.js';

export type RawCandleMessage = {
  exchange: 'upbit';
  type: 'candle.1m';
  market: string;
  recv_ts: number;
  payload: any;
};

type LogLv = 'debug' | 'info' | 'warn' | 'error';
const LOG_LEVEL = (process.env.LOG_LEVEL ?? 'info').toLowerCase() as LogLv;

function lvRank(lv: LogLv) {
  return lv === 'debug' ? 10 : lv === 'info' ? 20 : lv === 'warn' ? 30 : 40;
}
function canLog(lv: LogLv) {
  return lvRank(lv) >= lvRank(LOG_LEVEL);
}
function nowIso() {
  return new Date().toISOString();
}
function log(lv: LogLv, msg: string, extra?: unknown) {
  if (!canLog(lv)) return;
  if (extra !== undefined) {
    // eslint-disable-next-line no-console
    console.log(`${nowIso()} [builder][kafka][${lv}] ${msg}`, extra);
  } else {
    // eslint-disable-next-line no-console
    console.log(`${nowIso()} [builder][kafka][${lv}] ${msg}`);
  }
}

function toKafkaJsLevel(lv: LogLv) {
  // kafkajs: NOTHING(0), ERROR(1), WARN(2), INFO(4), DEBUG(5)
  if (lv === 'debug') return logLevel.DEBUG;
  if (lv === 'info') return logLevel.INFO;
  if (lv === 'warn') return logLevel.WARN;
  return logLevel.ERROR;
}

function logCreator() {
  return ({ namespace, level, label, log: l }: LogEntry) => {
    // kafkajs 내부 로그도 우리 prefix로 통일
    // label: INFO/WARN/ERROR/DEBUG
    const msg = `[kafkajs:${namespace}] ${label} ${l.message}`;
    if (label === 'ERROR') log('error', msg, l);
    else if (label === 'WARN') log('warn', msg, l);
    else if (label === 'INFO') log('info', msg);
    else log('debug', msg);
  };
}

export class KafkaIO {
  private consumer: Consumer;
  private producer: Producer;

  private consumeCnt = 0;
  private lastStatTs = Date.now();

  constructor() {
    const kafkaCfg: KafkaConfig = {
      clientId: config.kafka.clientId,
      brokers: config.kafka.brokers,
      logLevel: toKafkaJsLevel(LOG_LEVEL),
      logCreator
    };

    const kafka = new Kafka(kafkaCfg);

    this.consumer = kafka.consumer({
      groupId: config.kafka.groupId
    });

    this.producer = kafka.producer({
      allowAutoTopicCreation: true
    });
  }

  async connect() {
    log('info', `connect producer... brokers=${config.kafka.brokers.join(',')}`);
    await this.producer.connect();
    log('info', 'producer connected');

    log('info', `connect consumer... groupId=${config.kafka.groupId}`);
    await this.consumer.connect();
    log('info', `consumer connected`);

    log('info', `subscribe topic=${config.kafka.inRaw1m} fromBeginning=false`);
    await this.consumer.subscribe({ topic: config.kafka.inRaw1m, fromBeginning: false });
    log('info', 'subscribed');
  }

  async disconnect() {
    log('info', 'disconnect consumer...');
    await this.consumer.disconnect();
    log('info', 'consumer disconnected');

    log('info', 'disconnect producer...');
    await this.producer.disconnect();
    log('info', 'producer disconnected');
  }

  async run(onRaw: (msg: RawCandleMessage) => Promise<void>) {
    log('info', `consumer.run() start topic=${config.kafka.inRaw1m}`);

    await this.consumer.run({
      autoCommit: true,
      eachMessage: async ({ topic, partition, message }) => {
        if (!message.value) return;

        this.consumeCnt++;

        // 주기적 소비 통계 (기본 10초)
        const now = Date.now();
        const elapsed = now - this.lastStatTs;
        const statEveryMs = Number(process.env.KAFKA_STAT_EVERY_MS ?? '10000');
        if (elapsed >= statEveryMs) {
          const rps = (this.consumeCnt / (elapsed / 1000)).toFixed(1);
          log('info', `consume stat: cnt=${this.consumeCnt} elapsed=${elapsed}ms rps=${rps}`);
          this.consumeCnt = 0;
          this.lastStatTs = now;
        }

        const rawText = message.value.toString('utf8');

        try {
          const raw = JSON.parse(rawText) as RawCandleMessage;

          if (LOG_LEVEL === 'debug') {
            log('debug', `recv ${topic}[${partition}] offset=${message.offset} key=${message.key?.toString() ?? ''}`);
          }

          await onRaw(raw);
        } catch (e) {
          log('error', `failed to parse/handle message topic=${topic} partition=${partition} offset=${message.offset}`, {
            err: String(e),
            sample: rawText.slice(0, 300)
          });
        }
      }
    });

    log('info', 'consumer.run() started');
  }

  async emitCandle(c: Candle) {
    const topic =
      c.tf === '1m' ? config.kafka.out1m :
      c.tf === '5m' ? config.kafka.out5m :
      c.tf === '15m' ? config.kafka.out15m :
      c.tf === '1h' ? config.kafka.out1h :
      config.kafka.out4h;

    if (LOG_LEVEL === 'debug') {
      log('debug', `emit candle -> topic=${topic} market=${c.market} tf=${c.tf} open=${c.open_time} close=${c.close_time}`);
    }

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

  async emitIndicator(ind: Indicator) {
    const topic =
      ind.tf === '1m' ? config.kafka.outInd1m :
      ind.tf === '5m' ? config.kafka.outInd5m :
      ind.tf === '15m' ? config.kafka.outInd15m :
      ind.tf === '1h' ? config.kafka.outInd1h :
      config.kafka.outInd4h;

    if (LOG_LEVEL === 'debug') {
      log('debug', `emit indicator -> topic=${topic} market=${ind.market} tf=${ind.tf} time=${ind.time}`);
    }

    await this.producer.send({
      topic,
      messages: [{ key: ind.market, value: JSON.stringify(ind) }]
    });
  }
}
