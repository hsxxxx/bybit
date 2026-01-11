import { Router } from "express";
import type { RingStore } from "../store/ring.js";

export function routeMarkets(store: RingStore) {
  const r = Router();

  r.get("/", (_req, res) => {
    res.json({
      markets: store.listMarkets(),
      tfs: store.listTfs(),
      stats: store.stats()
    });
  });

  return r;
}
