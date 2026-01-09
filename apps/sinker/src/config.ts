// src/config.ts
import dotenv from "dotenv";
dotenv.config();

export type SinkerConfig = {
  nodeEnv: string;
  port: number;
  wsPort: number;
  corsOrigin: string;
  dataDir: string;

  kafkaClientId: string;
  kafkaBrokers: string[];
  kafkaGroupId: string;
  logLevel: string;

  // topic map
  candleTopics: Record<string, string>; // tf -> topic
  indTopics: Record<string, string>; // tf -> topic

  maxCandlesPerSeries: number;
  warmupDays: number;
};

function must(v: string | undefined, name: string) {
  if (!v) throw new Error(`Missing env: ${name}`);
  return v;
}

function opt(v: string | undefined, fallback = "") {
  return (v ?? fallback).trim();
}

export const config: SinkerConfig = {
  nodeEnv: opt(process.env.NODE_ENV, "development"),
  port: Number(opt(process.env.SINKER_PORT, "3110")),
  wsPort: Number(opt(process.env.SINKER_WS_PORT, "3111")),
  corsOrigin: opt(process.env.SINKER_CORS_ORIGIN, "*"),
  dataDir: opt(process.env.SINKER_DATA_DIR, "./data/sinker"),

  kafkaClientId: opt(process.env.KAFKA_CLIENT_ID, "bits-sinker"),
  kafkaBrokers: opt(process.env.KAFKA_BROKERS, "localhost:9092")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean),
  kafkaGroupId: opt(process.env.KAFKA_GROUP_ID, "bits-sinker-v1"),
  logLevel: opt(process.env.LOG_LEVEL, "info"),

  candleTopics: {
    "1m": must(process.env.KAFKA_TOPIC_IN_1M, "KAFKA_TOPIC_IN_1M"),
    "5m": must(process.env.KAFKA_TOPIC_IN_5M, "KAFKA_TOPIC_IN_5M"),
    "15m": must(process.env.KAFKA_TOPIC_IN_15M, "KAFKA_TOPIC_IN_15M"),
    "1h": must(process.env.KAFKA_TOPIC_IN_1H, "KAFKA_TOPIC_IN_1H"),
    "4h": must(process.env.KAFKA_TOPIC_IN_4H, "KAFKA_TOPIC_IN_4H")
  },

  indTopics: {
    "1m": must(process.env.KAFKA_TOPIC_IN_IND_1M, "KAFKA_TOPIC_IN_IND_1M"),
    "5m": must(process.env.KAFKA_TOPIC_IN_IND_5M, "KAFKA_TOPIC_IN_IND_5M"),
    "15m": must(process.env.KAFKA_TOPIC_IN_IND_15M, "KAFKA_TOPIC_IN_IND_15M"),
    "1h": must(process.env.KAFKA_TOPIC_IN_IND_1H, "KAFKA_TOPIC_IN_IND_1H"),
    "4h": must(process.env.KAFKA_TOPIC_IN_IND_4H, "KAFKA_TOPIC_IN_IND_4H")
  },

  maxCandlesPerSeries: Number(opt(process.env.SINKER_MAX_CANDLES, "5000")),
  warmupDays: Number(opt(process.env.SINKER_WARMUP_DAYS, "7"))
};
