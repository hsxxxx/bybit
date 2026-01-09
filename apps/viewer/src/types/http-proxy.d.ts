declare module "http-proxy" {
  import type { IncomingMessage, ServerResponse } from "http";

  export interface ProxyOptions {
    target?: string | { host: string; port: number; protocol?: string };
    ws?: boolean;
    changeOrigin?: boolean;
    secure?: boolean;
    xfwd?: boolean;
    prependPath?: boolean;
    ignorePath?: boolean;
    toProxy?: boolean;
    hostRewrite?: string;
    autoRewrite?: boolean;
    protocolRewrite?: string;
    cookieDomainRewrite?: string | Record<string, string>;
    cookiePathRewrite?: string | Record<string, string>;
    headers?: Record<string, string>;
    timeout?: number;
    proxyTimeout?: number;
    followRedirects?: boolean;
    selfHandleResponse?: boolean;
    localAddress?: string;
  }

  export interface Server {
    web(req: IncomingMessage, res: ServerResponse, options?: ProxyOptions): void;
    ws(req: IncomingMessage, socket: any, head: any, options?: ProxyOptions): void;

    on(event: "error", listener: (err: Error, req: IncomingMessage, res?: ServerResponse) => void): this;
    on(
      event: "proxyReq",
      listener: (proxyReq: any, req: IncomingMessage, res: ServerResponse, options: ProxyOptions) => void
    ): this;
    on(
      event: "proxyRes",
      listener: (proxyRes: any, req: IncomingMessage, res: ServerResponse, options: ProxyOptions) => void
    ): this;
    on(event: string, listener: (...args: any[]) => void): this;
  }

  export function createProxyServer(options?: ProxyOptions): Server;

  const _default: {
    createProxyServer: typeof createProxyServer;
  };

  export default _default;
}
