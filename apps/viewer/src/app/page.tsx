'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import { createChart, type ISeriesApi, type CandlestickData, type LineData } from 'lightweight-charts';

type Candle = {
  market: string;
  tf: string;
  open_time: number;
  open: number; high: number; low: number; close: number;
};

type Indicator = {
  market: string;
  tf: string;
  open_time: number;
  ma_50: number | null;
  ma_200: number | null;
  bb_upper_20_2: number | null;
  bb_mid_20: number | null;
  bb_lower_20_2: number | null;
};

export default function Page() {
  const [market, setMarket] = useState('KRW-BTC');
  const tf = '1m';
  const chartRef = useRef<HTMLDivElement | null>(null);

  const seriesRef = useRef<{
    candle?: ISeriesApi<'Candlestick'>;
    ma50?: ISeriesApi<'Line'>;
    ma200?: ISeriesApi<'Line'>;
    bbU?: ISeriesApi<'Line'>;
    bbM?: ISeriesApi<'Line'>;
    bbL?: ISeriesApi<'Line'>;
  }>({});

  // init chart once
  useEffect(() => {
    if (!chartRef.current) return;

    const chart = createChart(chartRef.current, { height: 520 });
    const candle = chart.addCandlestickSeries();
    const ma50 = chart.addLineSeries();
    const ma200 = chart.addLineSeries();
    const bbU = chart.addLineSeries();
    const bbM = chart.addLineSeries();
    const bbL = chart.addLineSeries();

    seriesRef.current = { candle, ma50, ma200, bbU, bbM, bbL };

    return () => chart.remove();
  }, []);

  // load snapshot
  useEffect(() => {
    let cancelled = false;
    (async () => {
      const res = await fetch(`/api/snapshot?market=${encodeURIComponent(market)}&tf=${tf}&limit=500`);
      const json = await res.json();

      if (cancelled) return;

      const candles: Candle[] = json.candles ?? [];
      const inds: Indicator[] = json.indicators ?? [];

      const cdata: CandlestickData[] = candles.map(c => ({
        time: Math.floor(c.open_time / 1000),
        open: c.open,
        high: c.high,
        low: c.low,
        close: c.close,
      }));

      // map indicator by open_time
      const map = new Map<number, Indicator>();
      for (const i of inds) map.set(i.open_time, i);

      const ma50Data: LineData[] = [];
      const ma200Data: LineData[] = [];
      const bbUData: LineData[] = [];
      const bbMData: LineData[] = [];
      const bbLData: LineData[] = [];

      for (const c of candles) {
        const i = map.get(c.open_time);
        const t = Math.floor(c.open_time / 1000);
        if (i?.ma_50 != null) ma50Data.push({ time: t, value: i.ma_50 });
        if (i?.ma_200 != null) ma200Data.push({ time: t, value: i.ma_200 });
        if (i?.bb_upper_20_2 != null) bbUData.push({ time: t, value: i.bb_upper_20_2 });
        if (i?.bb_mid_20 != null) bbMData.push({ time: t, value: i.bb_mid_20 });
        if (i?.bb_lower_20_2 != null) bbLData.push({ time: t, value: i.bb_lower_20_2 });
      }

      seriesRef.current.candle?.setData(cdata);
      seriesRef.current.ma50?.setData(ma50Data);
      seriesRef.current.ma200?.setData(ma200Data);
      seriesRef.current.bbU?.setData(bbUData);
      seriesRef.current.bbM?.setData(bbMData);
      seriesRef.current.bbL?.setData(bbLData);
    })();

    return () => { cancelled = true; };
  }, [market]);

  // realtime ws
  useEffect(() => {
    const ws = new WebSocket(`ws://${location.host}/ws`);

    ws.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        if (!msg?.type || !msg?.data) return;

        // only 1m for now
        if (msg.type === 'candle') {
          const c = msg.data as Candle;
          if (c.market !== market || c.tf !== tf) return;
          seriesRef.current.candle?.update({
            time: Math.floor(c.open_time / 1000),
            open: c.open,
            high: c.high,
            low: c.low,
            close: c.close
          });
        }

        if (msg.type === 'indicator') {
          const i = msg.data as Indicator;
          if (i.market !== market || i.tf !== tf) return;
          const t = Math.floor(i.open_time / 1000);

          if (i.ma_50 != null) seriesRef.current.ma50?.update({ time: t, value: i.ma_50 });
          if (i.ma_200 != null) seriesRef.current.ma200?.update({ time: t, value: i.ma_200 });

          if (i.bb_upper_20_2 != null) seriesRef.current.bbU?.update({ time: t, value: i.bb_upper_20_2 });
          if (i.bb_mid_20 != null) seriesRef.current.bbM?.update({ time: t, value: i.bb_mid_20 });
          if (i.bb_lower_20_2 != null) seriesRef.current.bbL?.update({ time: t, value: i.bb_lower_20_2 });
        }
      } catch {}
    };

    return () => ws.close();
  }, [market]);

  return (
    <div style={{ padding: 16 }}>
      <div style={{ display: 'flex', gap: 12, alignItems: 'center', marginBottom: 12 }}>
        <div style={{ fontWeight: 700 }}>bits viewer</div>
        <label>
          Market&nbsp;
          <input value={market} onChange={(e) => setMarket(e.target.value)} style={{ width: 140 }} />
        </label>
        <div style={{ opacity: 0.7 }}>tf: {tf}</div>
      </div>

      <div ref={chartRef} />
      <div style={{ marginTop: 10, opacity: 0.7, fontSize: 12 }}>
        Overlay: MA50/MA200 + BB(20,2). (RSI/OBV/PVT 패널은 다음 단계로 확장)
      </div>
    </div>
  );
}
