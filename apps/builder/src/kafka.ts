import { Kafka, logLevel, type Consumer, type Producer } from "kafkajs";
import { config } from "./config.js";
import type { Candle } from "./candle.js";
import type { Indicator } from "./indicator.js";

/* ---------- logging ---------- */
type LogLevel = "debug" | "info" | "warn" | "error";
const LOG_LEVEL = (process.env.LOG_LEVEL ?? "info") as LogLevel;

const rank: Record<LogLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

function log(lv: LogLevel, msg: string, extra?: any) {
  if (rank[lv] < rank[LOG_LEVEL]) return;
  const ts = new Date().toISOString();
  if (extra !== undefined) console.log(`${ts} [builder][kafka][${lv}] ${msg}`, extra);
  else console.log(`${ts} [builder][kafka][${lv}] ${msg}`);
}
/* ----------------------------- */

/**
 * builder 입력 토픽(bybit.kline1m.raw)은 collector가 만든 raw wrapper 메시지
 * (exchange/bybit, type/kline.1m, payload.data[0].confirm=true ...)
 *
 * => 여기서는 Candle 여부를 체크하지 않고 raw를 그대로 index.ts로 넘긴다.
 */
export class KafkaIO {
  private consumer: Consumer;
  private producer: Producer;

  // recv stat
  private recvCnt = 0;
  private lastStatTs = Date.now();
  private statEveryMs = Number(process.env.KAFKA_STAT_EVERY_MS ?? "10000"); // 10s

  // duplicate skip (optional)
  // - 기본: 켜짐 (중복 offset/동일 payload를 과도하게 처리하지 않도록)
  // - 필요 없으면 KAFKA_DEDUP=0
  private dedupEnabled = (process.env.KAFKA_DEDUP ?? "1") !== "0";
  private lastOffsetByPartition = new Map<number, string>(); // partition -> last offset
  private lastPayloadHashByPartition = new Map<number, string>(); // partition -> last hash

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
    log("info", `producer connect ${config.kafka.brokers.join(",")}`);
    await this.producer.connect();

    log("info", `consumer connect group=${config.kafka.groupId}`);
    await this.consumer.connect();

    log("info", `subscribe ${config.kafka.inRaw1m}`);
    await this.consumer.subscribe({ topic: config.kafka.inRaw1m, fromBeginning: false });
  }

  async disconnect() {
    log("info", "disconnect consumer...");
    await this.consumer.disconnect();
    log("info", "disconnect producer...");
    await this.producer.disconnect();
  }

  /**
   * raw 메시지를 그대로 전달한다.
   * index.ts에서 rawToClosed1mCandle(raw)로 confirm=true만 변환/처리
   */
  async run(onRaw: (raw: any) => Promise<void>) {
    log(
      "info",
      `consumer.run start topic=${config.kafka.inRaw1m} dedup=${this.dedupEnabled ? "on" : "off"}`
    );

    await this.consumer.run({
      autoCommit: true,
      eachMessage: async ({ topic, partition, message }) => {
        if (!message.value) return;

        this.recvCnt++;
        this.maybeLogRecvStat();

        const offset = String(message.offset ?? "");
        const buf = message.value;
        const text = buf.toString("utf8");

        // (optional) 파티션 단위 offset 중복 스킵
        if (this.dedupEnabled) {
          const lastOff = this.lastOffsetByPartition.get(partition);
          if (lastOff === offset) {
            log("debug", `skip dup offset topic=${topic} p=${partition} off=${offset}`);
            return;
          }
          this.lastOffsetByPartition.set(partition, offset);

          // payload hash로 중복 방지 (동일 메시지 재전송/리밸런싱에서 중복 처리 완화)
          const h = fastHash(text);
          const lastH = this.lastPayloadHashByPartition.get(partition);
          if (lastH === h) {
            log("debug", `skip dup payload topic=${topic} p=${partition} off=${offset}`);
            return;
          }
          this.lastPayloadHashByPartition.set(partition, h);
        }

        try {
          const obj = JSON.parse(text);
          if (LOG_LEVEL === "debug") {
            log("debug", `recv raw topic=${topic} p=${partition} off=${offset}`, {
              exchange: obj?.exchange,
              type: obj?.type,
              symbol: obj?.symbol,
              recv_ts: obj?.recv_ts,
            });
          }
          await onRaw(obj);
        } catch (e) {
          // 파싱 실패는 샘플만 남김(너무 길면 로그 폭발)
          log("error", `parse/handler error topic=${topic} p=${partition} off=${offset}`, {
            err: String(e),
            sample: text.slice(0, 400),
          });
        }
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

    if (LOG_LEVEL === "debug") {
      log("debug", `emit candle -> ${topic}`, {
        market: c.market,
        tf: c.tf,
        open_time: c.open_time,
        close_time: c.close_time,
      });
    }

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

    if (LOG_LEVEL === "debug") {
      log("debug", `emit indicator -> ${topic}`, {
        market: ind.market,
        tf: ind.tf,
      });
    }

    await this.producer.send({
      topic,
      messages: [{ key: ind.market, value: JSON.stringify(ind) }],
    });
  }

  private maybeLogRecvStat() {
    const now = Date.now();
    if (now - this.lastStatTs < this.statEveryMs) return;

    const elapsed = now - this.lastStatTs;
    const rps = (this.recvCnt / (elapsed / 1000)).toFixed(2);

    log("info", `recv stat cnt=${this.recvCnt} elapsed=${elapsed}ms rps=${rps}`);

    this.recvCnt = 0;
    this.lastStatTs = now;
  }
}

/**
 * 매우 가벼운 문자열 해시 (중복 payload 감지용)
 * crypto 모듈 없이 빠르게.
 */
function fastHash(s: string): string {
  let h = 2166136261; // FNV-1a base
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  // unsigned
  return (h >>> 0).toString(16);
}
