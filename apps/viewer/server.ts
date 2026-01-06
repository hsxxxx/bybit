import http from 'http';
import next from 'next';
import { WebSocketServer } from 'ws';
import { ensureKafkaStarted } from './src/lib/kafkaStore';

const port = Number(process.env.PORT ?? '3100');
const dev = process.env.NODE_ENV !== 'production';
const app = next({ dev, port });
const handle = app.getRequestHandler();

type WSMessage = { type: string; data: any };

async function main() {
  await app.prepare();

  const server = http.createServer((req, res) => handle(req, res));

  const wss = new WebSocketServer({ server, path: '/ws' });

  const sockets = new Set<any>();

  wss.on('connection', (ws) => {
    sockets.add(ws);
    ws.on('close', () => sockets.delete(ws));
  });

  (globalThis as any).__BITS_VIEWER_WS__ = {
    broadcast: (msg: WSMessage) => {
      const payload = JSON.stringify(msg);
      for (const ws of sockets) {
        if (ws.readyState === ws.OPEN) ws.send(payload);
      }
    }
  };

  await ensureKafkaStarted();

  server.listen(port, () => {
    console.log(`[viewer] server listening on http://localhost:${port}`);
    console.log(`[viewer] ws endpoint ws://localhost:${port}/ws`);
  });
}

main().catch((e) => {
  console.error('[viewer] fatal', e);
  process.exit(1);
});
