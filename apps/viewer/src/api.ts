import express from "express";
import { makeCors } from "./lib/cors.js";
import { getEnv } from "./lib/env.js";
import type { RingStore } from "./store/ring.js";
import { routeHealth } from "./routes/health.js";
import { routeMarkets } from "./routes/markets.js";
import { routeSnapshot } from "./routes/snapshot.js";

export function startApiServer(params: { port: number; store: RingStore }) {
  const env = getEnv();
  const app = express();

  app.disable("x-powered-by");
  app.use(express.json({ limit: "1mb" }));
  app.use(makeCors(env.corsOrigins));

  app.use("/health", routeHealth());
  app.use("/api/markets", routeMarkets(params.store));
  app.use("/api/snapshot", routeSnapshot(params.store));

  app.listen(params.port, () => {
    console.log(`[api] listening on :${params.port}`);
  });
}
