import 'dotenv/config';

function req(name: string, fallback?: string): string {
  const v = process.env[name] ?? fallback;
  if (!v) throw new Error(`Missing env: ${name}`);
  return v;
}

export const config = {
  kafka: {
    brokers: req('KAFKA_BROKERS').split(',').map(s => s.trim()),
    clientId: req('KAFKA_CLIENT_ID', 'bits-builder'),
    groupId: req('KAFKA_GROUP_ID', 'bits-builder-v1'),
    inRaw1m: req('KAFKA_TOPIC_IN_RAW_1M', 'upbit.candle1m.raw'),
    out1m: req('KAFKA_TOPIC_OUT_1M', 'upbit.candle.1m'),
    out5m: req('KAFKA_TOPIC_OUT_5M', 'upbit.candle.5m'),
    out15m: req('KAFKA_TOPIC_OUT_15M', 'upbit.candle.15m'),
    out1h: req('KAFKA_TOPIC_OUT_1H', 'upbit.candle.1h'),
    out4h: req('KAFKA_TOPIC_OUT_4H', 'upbit.candle.4h'),
    outFeature1m: req('KAFKA_TOPIC_OUT_FEATURE_1M', 'upbit.feature.1m')
  },
  logLevel: req('LOG_LEVEL', 'info')
};
