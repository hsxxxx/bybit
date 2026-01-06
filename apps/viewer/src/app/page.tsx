// apps/viewer/src/app/page.tsx
'use client';

import { useEffect, useRef, useState } from 'react';
import {
  createChart,
  CandlestickSeries,
  LineSeries,
  type CandlestickData,
  type LineData,
  type ISeriesApi,
} from 'lightweight-charts';

type Candle = {
  exchange: 'upbit';
  market: string;
  tf: string; // '1m' for now
  open_time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

type Indicator = {
  exchange: 'upbit';
  market: string;
  tf: string; // '1m' for now
  open_time: number;

  ma_50: number | null;
  ma_200: number | null;

  bb_upper_20_2: number | null;
  bb_mid_20: number | null;
  bb_lower_20_2: number | null;
};

type SnapshotResponse = {
  market: string;
  tf: string;
  candles: Candle[];
  indicators: Indicator[];
};

type WSMessage =
  | { type: 'candle'; data: Candle }
  | { type: 'indicator'; data: Indicator };

export default function Page() {
  const [market, setMarket] = useState('KRW-BTC');
  const tf = '1m';

  const containerRef = useRef<HTMLDivElement | null>(null);

  const chartRef = useRef<ReturnType<typeof createChart> | null>(null);
  const candleRef = useRef<ISeriesApi<'Candlestick'> | null>(null);

  const ma50Ref = useRef<ISeriesApi<'Line'> | null>(null);
  const ma200Ref = useRef<ISeriesApi<'Line'> | null>(null);

  const bbURef = useRef<ISeriesApi<'Line'> | null>(null);
  const bbMRef = useRef<ISeriesApi<'Line'> | null>(null);
  const bbLRef = useRef<ISeriesApi<'Line'> | null>(null);

  const wsRef = useRef<WebSocket | null>(null);

  // --- init chart once ---
  useEffect(() => {
    if (!containerRef.current) return;

    const chart = createChart(containerRef.current, {
      height: 560,
      autoSize: true,
    });

    const candle = chart.addSeries(CandlestickSeries, {});
    const ma50 = chart.addSeries(LineSeries, {});
    const ma200 = chart.addSeries(LineSeries, {});
    const bbU = chart.addSeries(LineSeries, {});
    const bbM = chart.addSeries(LineSeries, {});
    const bbL = chart.addSeries(LineSeries, {});

    chartRef.current = chart;
    candleRef.current = candle;

    ma50Ref.current = ma50;
    ma200Ref.current = ma200;
    bbURef.current = bbU;
    bbMRef.current = bbM;
    bbLRef.current = bbL;

    // Handle resize
    const ro = new ResizeObserver(() => {
      if (!containerRef.current) return;
      chart.applyOptions({ width: containerRef.current.clientWidth });
    });
    ro.observe(containerRef.current);

    return () => {
      ro.disconnect();
      wsRef.current?.close();
      chart.remove();
      chartRef.current = null;
      candleRef.current = null;
      ma50Ref.current = null;
      ma200Ref.current = null;
      bbURef.current = null;
      bbMRef.current = null;
      bbLRef.current = null;
    };
  }, []);

  // --- load snapshot whenever market changes ---
  useEffect(() => {
    let cancelled = false;

    (async () => {
      const res = await fetch(
        `/api/snapshot?market=${encodeURIComponent(market)}&tf=${encodeURIComponent(tf)}&limit=500`
      );
      const json = (await res.json()) as SnapshotResponse;
      if (cancelled) return;

      const candles = json.candles ?? [];
      const indicators = json.indicators ?? [];

      // Candle series
      const cdata: CandlestickData[] = candles.map((c) => ({
        time: Math.floor(c.open_time / 1000),
        open: c.open,
        high: c.high,
        low: c.low,
        close: c.close,
      }));
      candleRef.current?.setData(cdata);

      // Build indicator map by open_time
      const imap = new Map<number, Indicator>();
      for (const i of indicators) imap.set(i.open_time, i);

      const ma50Data: LineData[] = [];
      const ma200Data: LineData[] = [];
      const bbUData: LineData[] = [];
      const bbMData: LineData[] = [];
      const bbLData: LineData[] = [];

      for (const c of candles) {
        const i = imap.get(c.open_time);
        const t = Math.floor(c.open_time / 1000);

        if (i?.ma_50 != null) ma50Data.push({ time: t, value: i.ma_50 });
        if (i?.ma_200 != null) ma200Data.push({ time: t, value: i.ma_200 });

        if (i?.bb_upper_20_2 != null) bbUData.push({ time: t, value: i.bb_upper_20_2 });
        if (i?.bb_mid_20 != null) bbMData.push({ time: t, value: i.bb_mid_20 });
        if (i?.bb_lower_20_2 != null) bbLData.push({ time: t, value: i.bb_lower_20_2 });
      }

      ma50Ref.current?.setData(ma50Data);
      ma200Ref.current?.setData(ma200Data);

      bbURef.current?.setData(bbUData);
      bbMRef.current?.setData(bbMData);
      bbLRef.current?.setData(bbLData);

      chartRef.current?.timeScale().fitContent();
    })().catch((e) => console.error('[viewer] snapshot error', e));

    return () => {
      cancelled = true;
    };
  }, [market]);

  // --- realtime websocket ---
  useEffect(() => {
    // close previous
    wsRef.current?.close();

    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    const ws = new WebSocket(`${proto}://${location.host}/ws`);
    wsRef.current = ws;

    ws.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data) as WSMessage;

        if (msg.type === 'candle') {
          const c = msg.data;
          if (c.market !== market || c.tf !== tf) return;

          const t = Math.floor(c.open_time / 1000);
          candleRef.current?.update({
            time: t,
            open: c.open,
            high: c.high,
            low: c.low,
            close: c.close,
          });
        }

        if (msg.type === 'indicator') {
          const i = msg.data;
          if (i.market !== market || i.tf !== tf) return;

          const t = Math.floor(i.open_time / 1000);

          if (i.ma_50 != null) ma50Ref.current?.update({ time: t, value: i.ma_50 });
          if (i.ma_200 != null) ma200Ref.current?.update({ time: t, value: i.ma_200 });

          if (i.bb_upper_20_2 != null) bbURef.current?.update({ time: t, value: i.bb_upper_20_2 });
          if (i.bb_mid_20 != null) bbMRef.current?.update({ time: t, value: i.bb_mid_20 });
          if (i.bb_lower_20_2 != null) bbLRef.current?.update({ time: t, value: i.bb_lower_20_2 });
        }
      } catch {
        // ignore
      }
    };

    ws.onerror = (e) => {
      console.error('[viewer] ws error', e);
    };

    return () => {
      ws.close();
    };
  }, [market]);

  return (
    <div style={{ padding: 16 }}>
      <div style={{ display: 'flex', gap: 12, alignItems: 'center', marginBottom: 12 }}>
        <div style={{ fontWeight: 700 }}>bits viewer</div>

        <label style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          Market
          <input
            value={market}
            onChange={(e) => setMarket(e.target.value)}
            style={{ width: 140, padding: 6 }}
            placeholder="KRW-BTC"
          />
        </label>

        <div style={{ opacity: 0.7 }}>tf: {tf}</div>
      </div>

      <div
        ref={containerRef}
        style={{
          width: '100%',
          border: '1px solid rgba(255,255,255,0.1)',
          borderRadius: 8,
          overflow: 'hidden',
        }}
      />

      <div style={{ marginTop: 10, opacity: 0.7, fontSize: 12 }}>
        Overlay: MA50/MA200 + BB(20,2). (RSI/OBV/PVT 패널은 다음 단계로 확장)
      </div>
    </div>
  );
}
