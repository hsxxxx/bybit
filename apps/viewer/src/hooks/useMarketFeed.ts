"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import type { Candle, SnapshotResponse, Timeframe, WsMessage } from "@/lib/market/types";
import { normalizeCandlesAscUnique, upsertCandleAscUnique } from "@/lib/market/candles";
import { isFiniteNumber } from "@/utils/time";

type UseMarketFeedResult = {
  candles: Candle[];
  status: string;
};

function defaultWsUrl() {
  // env가 있으면 우선 사용
  const env = process.env.NEXT_PUBLIC_WS_URL;
  if (env) return env;

  // 없으면 현재 host 기반 추정 (reverse proxy 구성에 따라 수정 가능)
  if (typeof window !== "undefined") {
    const proto = window.location.protocol === "https:" ? "wss" : "ws";
    return `${proto}://${window.location.host}/ws`;
  }
  return "ws://localhost:3100/ws";
}

export function useMarketFeed(tf: Timeframe): UseMarketFeedResult {
  const [candles, setCandles] = useState<Candle[]>([]);
  const [status, setStatus] = useState("idle");

  const wsUrl = useMemo(() => defaultWsUrl(), []);
  const wsRef = useRef<WebSocket | null>(null);

  // 최신 candles를 ws 핸들러에서 참조하기 위한 ref
  const candlesRef = useRef<Candle[]>([]);
  useEffect(() => {
    candlesRef.current = candles;
  }, [candles]);

  async function loadSnapshot(currentTf: Timeframe) {
    setStatus(`loading snapshot (${currentTf})...`);
    try {
      const res = await fetch(`/api/snapshot?market=KRW-BTC&tf=${tf}&limit=1000`, { cache: "no-store" });
      if (!res.ok) {
        setStatus(`snapshot error: ${res.status}`);
        return;
      }

      const data = (await res.json()) as SnapshotResponse;
      const safe = normalizeCandlesAscUnique(data.candles ?? []);

      setCandles(safe);
      setStatus(`snapshot loaded (${currentTf})`);
    } catch {
      setStatus("snapshot fetch failed");
    }
  }

  function ensureWsConnected() {
    // 이미 열려 있으면 재사용
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) return;

    // 기존 연결 정리
    if (wsRef.current) {
      try {
        wsRef.current.close();
      } catch {}
      wsRef.current = null;
    }

    setStatus("connecting ws...");

    const ws = new WebSocket(wsUrl);
    wsRef.current = ws;

    ws.onopen = () => {
      setStatus("ws connected");
      try {
        ws.send(JSON.stringify({ type: "subscribe", tf }));
      } catch {}
    };

    ws.onclose = () => {
      setStatus("ws disconnected");
    };

    ws.onerror = () => {
      setStatus("ws error");
    };

    ws.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data) as WsMessage;

        if (msg.type !== "candle") return;
        if ((msg as any).tf !== tf) return;

        const incoming = (msg as any).candle as Candle;
        if (
          !incoming ||
          !isFiniteNumber(incoming.time) ||
          !isFiniteNumber(incoming.open) ||
          !isFiniteNumber(incoming.high) ||
          !isFiniteNumber(incoming.low) ||
          !isFiniteNumber(incoming.close)
        ) {
          return;
        }

        const next = upsertCandleAscUnique(candlesRef.current, incoming);

        // ✅ 여기서도 혹시 모를 중복/역순 방어 (정말 안전하게)
        const safe = normalizeCandlesAscUnique(next);

        setCandles(safe);
      } catch {
        // ignore non-json or parse errors
      }
    };
  }

  // TF 바뀔 때: snapshot 재로드 + ws 구독 변경
  useEffect(() => {
    loadSnapshot(tf);
    ensureWsConnected();

    // 열린 ws가 있으면 subscribe 갱신
    const ws = wsRef.current;
    if (ws && ws.readyState === WebSocket.OPEN) {
      try {
        ws.send(JSON.stringify({ type: "subscribe", tf }));
      } catch {}
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tf]);

  // unmount cleanup
  useEffect(() => {
    return () => {
      if (wsRef.current) {
        try {
          wsRef.current.close();
        } catch {}
        wsRef.current = null;
      }
    };
  }, []);

  return { candles, status };
}
