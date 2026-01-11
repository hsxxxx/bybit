import "dotenv/config";
import { createRingStore } from "./store/ring.js";
import { startApiServer } from "./api.js";
import { startWsServer } from "./ws.js";
import { startKafkaConsumers } from "./kafka/consumer.js";
import { getEnv } from "./lib/env.js";
import { log } from "./lib/logger.js";

async function main() {
  const env = getEnv();

  const store = createRingStore(env.ringSize);

  // 1) WS 먼저 (실시간 브로드캐스트용)
  const wsHub = startWsServer({
    port: env.portWs,
    store
  });

  // 2) API
  startApiServer({
    port: env.portApi,
    store
  });

  // 3) Kafka consume → store 적재 + WS broadcast
  await startKafkaConsumers({
    brokers: env.kafkaBrokers,
    clientId: env.kafkaClientId,
    groupId: env.kafkaGroupId,
    topicsCandle: env.topicsCandle,
    topicsIndicator: env.topicsIndicator,
    backfillMessages: env.kafkaBackfillMessages,
    store,
    wsHub
  });

  log.info(
    `[viewer] up api=:${env.portApi} ws=:${env.portWs} ring=${env.ringSize} backfill=${env.kafkaBackfillMessages}`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
