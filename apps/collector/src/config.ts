import 'dotenv/config';

function req(name: string, fallback?: string): string {
  const v = process.env[name] ?? fallback;
  if (!v) throw new Error(`Missing env: ${name}`);
  return v;
}

export const config = {
  kafka: {
    brokers: req('KAFKA_BROKERS').split(',').map(s => s.trim()),
    clientId: req('KAFKA_CLIENT_ID', 'bits-collector'),
    topicRaw1m: req('KAFKA_TOPIC_RAW_1M', 'upbit.candle1m.raw')
  },
  upbit: {
    wsUrl: req('UPBIT_WS_URL', 'wss://api.upbit.com/websocket/v1'),
    restUrl: req('UPBIT_REST_URL', 'https://api.upbit.com/v1'),
    marketRefreshSec: Number(req('UPBIT_MARKET_REFRESH_SEC', '1800'))
  },
  ws: {
    ticket: req('WS_TICKET', 'bits-collector')
  },
  logLevel: req('LOG_LEVEL', 'info')
};
