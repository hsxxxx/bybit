"use client";

import React from "react";
import type { Timeframe } from "@/lib/market/types";

type Props = {
  value: Timeframe;
  onChange: (v: Timeframe) => void;
  status?: string;
};

const TFS: Timeframe[] = ["5m", "15m", "1h", "4h"];

export default function TimeframeSelector({ value, onChange, status }: Props) {
  return (
    <div style={{ display: "flex", gap: 12, alignItems: "center", padding: 12 }}>
      <div style={{ fontWeight: 700, color: "#e2e8f0" }}>Viewer</div>

      <div style={{ display: "flex", gap: 8, marginLeft: 12 }}>
        {TFS.map((t) => (
          <button
            key={t}
            onClick={() => onChange(t)}
            style={{
              padding: "6px 10px",
              borderRadius: 8,
              border: "1px solid rgba(148,163,184,0.2)",
              background: value === t ? "rgba(56,189,248,0.15)" : "transparent",
              color: "#e2e8f0",
              cursor: "pointer",
            }}
          >
            {t}
          </button>
        ))}
      </div>

      <div style={{ marginLeft: "auto", fontSize: 12, opacity: 0.85, color: "#cbd5e1" }}>
        {status ?? ""}
      </div>
    </div>
  );
}
