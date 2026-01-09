import "dotenv/config";
import http from "http";
import httpProxy from "http-proxy";

const PORT_PROXY = Number(process.env.PORT || 3100);
const NEXT_PORT = Number(process.env.PORT_NEXT || 3102);
const WS_PORT = Number(process.env.PORT_WS || 3101);

const NEXT_HTTP_TARGET = `http://127.0.0.1:${NEXT_PORT}`;
const WS_HTTP_TARGET = `http://127.0.0.1:${WS_PORT}`;
const WS_WS_TARGET = `ws://127.0.0.1:${WS_PORT}`;

const proxy = httpProxy.createProxyServer({
  ws: true,
  changeOrigin: true,
  xfwd: true,
});

proxy.on("error", (err, req: any, res: any) => {
  console.error(`[proxy:error] ${req?.method} ${req?.url} -> ${err?.message || err}`);
  if (res && typeof res.writeHead === "function") {
    try {
      res.writeHead(502, { "Content-Type": "text/plain; charset=utf-8" });
      res.end("Bad gateway");
    } catch {}
  }
});

proxy.on("proxyReqWs", (_proxyReq, req) => {
  console.log("[proxy] proxyReqWs", req.url);
});

const server = http.createServer((req, res) => {
  const url = req.url ?? "/";

  // viewer-ws로 보내야 하는 HTTP
  if (url.startsWith("/api/")) {
    return proxy.web(req, res, { target: WS_HTTP_TARGET });
  }

  // /ws 로 HTTP가 들어오는 경우(거의 없음)도 viewer-ws로
  if (url.startsWith("/ws")) {
    return proxy.web(req, res, { target: WS_HTTP_TARGET });
  }

  // 나머지 Next
  return proxy.web(req, res, { target: NEXT_HTTP_TARGET });
});

server.on("upgrade", (req, socket, head) => {
  const url = req.url ?? "/";
  console.log("[proxy] upgrade", url);

  // viewer ws
  if (url.startsWith("/ws")) {
    return proxy.ws(req, socket, head, { target: WS_WS_TARGET });
  }

  // next hmr & others
  return proxy.ws(req, socket, head, { target: NEXT_HTTP_TARGET });
});

server.listen(PORT_PROXY, () => {
  console.log(`[proxy] http://localhost:${PORT_PROXY}`);
  console.log(`[proxy]  ├─ /ws      -> ${WS_WS_TARGET}`);
  console.log(`[proxy]  ├─ /api/*   -> ${WS_HTTP_TARGET}`);
  console.log(`[proxy]  └─ /*       -> ${NEXT_HTTP_TARGET}`);
});
