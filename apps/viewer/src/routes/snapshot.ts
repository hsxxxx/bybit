import { Router } from "express";
import type { RingStore } from "../store/ring.js";

export function routeSnapshot(store: RingStore) {
  const r = Router();

  // /api/snapshot?kind=candle|indicator&market=KRW-BTC&tf=1m&limit=500
  r.get("/", (req, res) => {
    const kind = String(req.query.kind ?? "candle");
    const market = String(req.query.market ?? "");
    const tf = String(req.query.tf ?? "");
    const limit = Math.min(5000, Math.max(1, Number(req.query.limit ?? "500")));

    if (!market || !tf) {
      res.status(400).json({ error: "market, tf required" });
      return;
    }

    if (kind === "indicator") {
      const items = store.getIndicators(market, tf as any, limit);
      res.json({ kind, market, tf, limit, items });
      return;
    }

    const candles = store.getCandles(market, tf as any, limit);
    res.json({ kind: "candle", market, tf, limit, items: candles });
  });

  return r;
}
