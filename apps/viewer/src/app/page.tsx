"use client";

import { useState } from "react";
import type { Timeframe } from "@/lib/market/types";
import { useMarketFeed } from "@/hooks/useMarketFeed";
import MultiPanelChart from "@/components/charts/MultiPanelChart";
import TimeframeSelector from "@/components/controls/TimeframeSelector";

export default function Page() {
  const [tf, setTf] = useState<Timeframe>("1m");
  const { candles, status } = useMarketFeed(tf);

  return (
    <div style={{ minHeight: "100vh", background: "#070a12" }}>
      <TimeframeSelector value={tf} onChange={setTf} status={status} />
      <div style={{ padding: 12 }}>
        <MultiPanelChart candles={candles} />
      </div>
    </div>
  );
}
