function must(v: string | undefined, name: string): string {
  if (!v) throw new Error(`Missing env: ${name}`);
  return v;
}

function csv(v: string | undefined): string[] {
  if (!v) return [];
  return v.split(",").map(s => s.trim()).filter(Boolean);
}

export function getEnv() {
  const kafkaBrokers = csv(must(process.env.KAFKA_BROKERS, "KAFKA_BROKERS"));
  const kafkaClientId = must(process.env.KAFKA_CLIENT_ID, "KAFKA_CLIENT_ID");
  const kafkaGroupId = must(process.env.KAFKA_GROUP_ID, "KAFKA_GROUP_ID");

  const topicsCandle = csv(must(process.env.TOPICS_CANDLE, "TOPICS_CANDLE"));
  const topicsIndicator = csv(must(process.env.TOPICS_INDICATOR, "TOPICS_INDICATOR"));

  const kafkaBackfillMessages = Number(process.env.KAFKA_BACKFILL_MESSAGES ?? "30000");
  const ringSize = Number(process.env.RING_SIZE ?? "2000");

  const portApi = Number(process.env.PORT ?? "3100");
  const portWs = Number(process.env.PORT_WS ?? "3101");

  const corsOrigins = csv(process.env.CORS_ORIGINS);

  return {
    kafkaBrokers,
    kafkaClientId,
    kafkaGroupId,
    topicsCandle,
    topicsIndicator,
    kafkaBackfillMessages,
    ringSize,
    portApi,
    portWs,
    corsOrigins
  };
}
