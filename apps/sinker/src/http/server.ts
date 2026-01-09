// sinker/src/http/server.ts
import express from "express";
import cors from "cors";
import type { CandleStore } from "../store/CandleStore";
import type { SinkerConfig } from "../config";
import { parseTimeframe } from "../utils/timeframe";
import type { SnapshotResponse } from "../types";

export function startHttpServer(cfg: SinkerConfig, store: CandleStore) {
  const app = express();

  app.use(cors({ origin: cfg.corsOrigin === "*" ? true : cfg.corsOrigin }));
  app.use(express.json({ limit: "2mb" }));

  app.get("/health", (_req, res) => {
    res.json({
      ok: true,
      lastIngestAt: store.getLastIngestAt(),
      series: store.listMarketsTfs().length
    });
  });

  app.get("/series", (_req, res) => {
    res.json(store.listMarketsTfs());
  });

  // GET /snapshot?market=KRW-BTC&tf=15m&limit=2000&from=...&to=...
  app.get("/snapshot", (req, res) => {
    try {
      const market = String(req.query.market ?? "");
      const tf = parseTimeframe(String(req.query.tf ?? ""));
      const limit = Number(req.query.limit ?? 2000);

      const from = req.query.from != null ? Number(req.query.from) : undefined;
      const to = req.query.to != null ? Number(req.query.to) : undefined;

      if (!market) return res.status(400).json({ error: "market required" });

      const candles = store.getSnapshot(market, tf, limit, from, to);

      const out: SnapshotResponse = {
        market,
        tf,
        candles,
        from,
        to,
        count: candles.length
      };

      res.json(out);
    } catch (e: any) {
      res.status(400).json({ error: e?.message ?? "bad request" });
    }
  });

  const server = app.listen(cfg.port);
  return { app, server };
}
