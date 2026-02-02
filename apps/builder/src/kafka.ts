import { Kafka, logLevel, type Consumer, type Producer } from "kafkajs";
import { config } from "./config.js";
import type { Candle } from "./candle.js";
import type { Indicator } from "./indicator.js";

/* ---------- logging ---------- */
type LogLevel = "debug" | "info" | "warn" | "error";
const LOG_LEVEL = (process.env.LOG_LEVEL ?? "info") as LogLevel;
const rank: Record<LogLevel, number> = { debug: 10, info: 20, warn: 30, error: 40 };

function log(lv: LogLevel, msg: string, extra?: any) {
  if (rank[lv] < rank[LOG_LEVEL]) return;
  const ts = new Date().toISOString();
  extra ? console.log(`${ts} [builder][kafka][${lv}] ${msg}`, extra)
        : console.log(`${ts} [builder][kafka][${lv}] ${msg}`);
}
/* ----------------------------- */

function isCandle(x: any): x is Candle {
  return (
    x &&
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

  private recvCnt = 0;
  private lastStatTs = Date.now();

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
    await this.consumer.subscribe({
      topic: config.kafka.inRaw1m,
      fromBeginning: false,
    });
  }

  async disconnect() {
    log("info", "disconnect");
    await this.consumer.disconnect();
    await this.producer.disconnect();
  }

  async run(onCandle: (c: Candle) => Promise<void>) {
    log("info", "consumer.run start");

    await this.consumer.run({
      autoCommit: true,
      eachMessage: async ({ message, partition }) => {
        if (!message.value) return;

        this.recvCnt++;

        // 10초마다 수신 통계
        const now = Date.now();
        if (now - this.lastStatTs > 10_000) {
          log("info", `recv stat cnt=${this.recvCnt}`);
          this.recvCnt = 0;
          this.lastStatTs = now;
        }

        try {
          const obj = JSON.parse(message.value.toString("utf8"));

          if (!isCandle(obj)) {
            log("debug", "skip non-candle message");
            return;
          }

          log("debug", `recv candle ${obj.market} ${obj.tf} ${obj.open_time}`);
          await onCandle(obj);
        } catch (e) {
          log("error", "consumer message error", e);
        }
      },
    });
  }

  async emitCandle(c: Candle) {
    const topic =
      c.tf === "1m" ? config.kafka.out1m :
      c.tf === "5m" ? config.kafka.out5m :
      c.tf === "15m" ? config.kafka.out15m :
      c.tf === "1h" ? config.kafka.out1h :
      config.kafka.out4h;

    log("debug", `emit candle ${topic} ${c.market} ${c.tf} ${c.open_time}`);

    await this.producer.send({
      topic,
      messages: [{ key: c.market, value: JSON.stringify(c) }],
    });
  }

  async emitIndicator(ind: Indicator) {
    const topic =
      ind.tf === "1m" ? config.kafka.outInd1m :
      ind.tf === "5m" ? config.kafka.outInd5m :
      ind.tf === "15m" ? config.kafka.outInd15m :
      ind.tf === "1h" ? config.kafka.outInd1h :
      config.kafka.outInd4h;

    log("debug", `emit indicator ${topic} ${ind.market} ${ind.tf}`);

    await this.producer.send({
      topic,
      messages: [{ key: ind.market, value: JSON.stringify(ind) }],
    });
  }
}
