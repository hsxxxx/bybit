import "dotenv/config";

function req(name: string, fallback?: string): string {
  const v = process.env[name] ?? fallback;
  if (!v) throw new Error(`Missing env: ${name}`);
  return v;
}

export const config = {
  kafka: {
    brokers: req("KAFKA_BROKERS").split(",").map((s) => s.trim()),
    clientId: req("KAFKA_CLIENT_ID", "bybit-builder"),
    groupId: req("KAFKA_GROUP_ID", "bybit-builder-v1"),

    // collector가 confirm=true 1m 확정봉만 publish 하는 입력 토픽
    inRaw1m: req("KAFKA_TOPIC_IN_RAW_1M", "bybit.kline1m.raw"),

    // builder가 표준화/집계 후 내보내는 출력 토픽들
    out1m: req("KAFKA_TOPIC_OUT_1M", "bybit.candle.1m"),
    out5m: req("KAFKA_TOPIC_OUT_5M", "bybit.candle.5m"),
    out15m: req("KAFKA_TOPIC_OUT_15M", "bybit.candle.15m"),
    out1h: req("KAFKA_TOPIC_OUT_1H", "bybit.candle.1h"),
    out4h: req("KAFKA_TOPIC_OUT_4H", "bybit.candle.4h"),

    outInd1m: req("KAFKA_TOPIC_OUT_IND_1M", "bybit.indicator.1m"),
    outInd5m: req("KAFKA_TOPIC_OUT_IND_5M", "bybit.indicator.5m"),
    outInd15m: req("KAFKA_TOPIC_OUT_IND_15M", "bybit.indicator.15m"),
    outInd1h: req("KAFKA_TOPIC_OUT_IND_1H", "bybit.indicator.1h"),
    outInd4h: req("KAFKA_TOPIC_OUT_IND_4H", "bybit.indicator.4h"),
  },
  logLevel: req("LOG_LEVEL", "info"),
};
