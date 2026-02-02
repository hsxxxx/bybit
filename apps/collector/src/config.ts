import "dotenv/config";

function req(name: string, fallback?: string): string {
  const v = process.env[name] ?? fallback;
  if (!v) throw new Error(`Missing env: ${name}`);
  return v;
}

function list(name: string, fallbackCsv: string) {
  return req(name, fallbackCsv)
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

export const config = {
  kafka: {
    brokers: req("KAFKA_BROKERS").split(",").map((s) => s.trim()),
    clientId: req("KAFKA_CLIENT_ID", "bits-collector"),
    topicRaw1m: req("KAFKA_TOPIC_RAW_1M", "bybit.kline1m.raw"),
  },

  bybit: {
    wsUrl: req("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/linear"),
    symbols: list("BYBIT_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT"),
    interval: req("BYBIT_INTERVAL", "1"), // "1" = 1m
    pingSec: Number(req("BYBIT_PING_SEC", "20")),
  },

  ws: {
    ticket: req("WS_TICKET", "bits-collector"),
  },

  logLevel: req("LOG_LEVEL", "info"),
};
