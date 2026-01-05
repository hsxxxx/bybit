import { KafkaOut } from './kafka.js';
import { UpbitCollector } from './upbit.js';

async function main() {
  const kafka = new KafkaOut();
  await kafka.connect();
  console.log('[collector] kafka connected');

  const collector = new UpbitCollector(kafka);
  await collector.start();

  const shutdown = async (sig: string) => {
    console.log(`[collector] shutting down (${sig})...`);
    await collector.stop();
    await kafka.disconnect();
    process.exit(0);
  };

  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
}

main().catch((e) => {
  console.error('[collector] fatal', e);
  process.exit(1);
});
