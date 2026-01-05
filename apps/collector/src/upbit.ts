import WebSocket from 'ws';
import { request } from 'undici';
import { config } from './config.js';
import type { KafkaOut } from './kafka.js';

type UpbitMarket = {
  market: string;       // KRW-BTC
  korean_name: string;
  english_name: string;
};

function sleep(ms: number) {
  return new Promise(res => setTimeout(res, ms));
}

function decodeWsData(data: WebSocket.RawData): string {
  // Upbit WS often sends binary frames. ws gives Buffer by default.
  if (typeof data === 'string') return data;
  if (data instanceof Buffer) return data.toString('utf8');
  if (Array.isArray(data)) return Buffer.concat(data).toString('utf8');
  return Buffer.from(data as ArrayBuffer).toString('utf8');
}

export async function fetchKrwMarkets(): Promise<string[]> {
  const url = `${config.upbit.restUrl}/market/all?isDetails=true`;
  const res = await request(url, { method: 'GET' });
  if (res.statusCode !== 200) {
    const body = await res.body.text();
    throw new Error(`Upbit market/all failed: ${res.statusCode} ${body}`);
  }
  const json = (await res.body.json()) as UpbitMarket[];
  return json
    .map(x => x.market)
    .filter(m => m.startsWith('KRW-'))
    .sort();
}

type CollectorState = {
  markets: string[];
  ws?: WebSocket;
  stopped: boolean;
};

export class UpbitCollector {
  private state: CollectorState = { markets: [], stopped: false };
  private refreshTimer?: NodeJS.Timeout;

  constructor(private kafka: KafkaOut) {}

  async start() {
    this.state.stopped = false;

    // initial markets
    this.state.markets = await fetchKrwMarkets();
    console.log(`[collector] KRW markets loaded: ${this.state.markets.length}`);

    // start periodic refresh (optional)
    const refreshMs = Math.max(60, config.upbit.marketRefreshSec) * 1000;
    this.refreshTimer = setInterval(() => {
      void this.refreshMarketsAndResubscribe().catch(err => {
        console.error('[collector] market refresh error', err);
      });
    }, refreshMs);

    // connect loop
    void this.connectLoop();
  }

  async stop() {
    this.state.stopped = true;
    if (this.refreshTimer) clearInterval(this.refreshTimer);

    const ws = this.state.ws;
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.close(1000, 'shutdown');
    }
  }

  private async refreshMarketsAndResubscribe() {
    const next = await fetchKrwMarkets();
    if (next.join(',') === this.state.markets.join(',')) return;

    console.log(`[collector] markets changed: ${this.state.markets.length} -> ${next.length}`);
    this.state.markets = next;

    // If connected, resubscribe with a single message (rate-limit friendly)
    const ws = this.state.ws;
    if (ws && ws.readyState === WebSocket.OPEN) {
      this.sendSubscribe(ws, this.state.markets);
      console.log('[collector] resubscribed with refreshed market list');
    }
  }

  private async connectLoop() {
    let attempt = 0;

    while (!this.state.stopped) {
      try {
        attempt += 1;
        await this.openWsOnce();
        attempt = 0; // reset after a clean open (we’ll increment again if it closes)
      } catch (err) {
        const backoff = Math.min(30_000, 1_000 * Math.pow(2, Math.min(attempt, 5)));
        console.error(`[collector] ws error, retry in ${backoff}ms`, err);
        await sleep(backoff);
      }
    }
  }

  private async openWsOnce(): Promise<void> {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(config.upbit.wsUrl, {
        handshakeTimeout: 10_000
      });
      this.state.ws = ws;

      let opened = false;

      ws.on('open', () => {
        opened = true;
        console.log('[collector] ws connected');
        this.sendSubscribe(ws, this.state.markets);
      });

      ws.on('message', async (data) => {
        const recvTs = Date.now();
        try {
          const text = decodeWsData(data);
          const payload = JSON.parse(text) as any;

          // Upbit candle payload includes "code": "KRW-BTC"
          const market = payload?.code;
          if (!market || typeof market !== 'string') return;

          await this.kafka.sendRaw1m({
            exchange: 'upbit',
            type: 'candle.1m',
            market,
            recv_ts: recvTs,
            payload
          });
        } catch (e) {
          // parsing or kafka send failure
          console.error('[collector] message handling error', e);
        }
      });

      ws.on('close', (code, reason) => {
        console.warn(`[collector] ws closed code=${code} reason=${reason.toString()}`);
        // trigger reconnect by resolving (clean) or rejecting (if never opened)
        if (!opened) reject(new Error('ws closed before open'));
        else resolve();
      });

      ws.on('error', (err) => {
        console.error('[collector] ws error event', err);
        // error will typically be followed by close; but if not opened yet, reject now
        if (!opened) reject(err);
      });

      // keepalive (ws library automatically responds to pings; we can still set a timer if needed later)
    });
  }

  private sendSubscribe(ws: WebSocket, markets: string[]) {
    // Subscribe to candle.1m for all KRW markets in one message.
    const msg = [
      { ticket: config.ws.ticket },
      { type: 'candle.1m', codes: markets },
      { format: 'DEFAULT' }
    ];
    ws.send(JSON.stringify(msg));
    console.log(`[collector] subscribed candle.1m codes=${markets.length}`);
  }
}
