"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";
import {
  ColorType,
  CrosshairMode,
  TickMarkType,
  createChart,
  CandlestickSeries,
  HistogramSeries,
  LineSeries,
  type IChartApi,
  type ISeriesApi,
  type Time,
  type CandlestickData,
  type HistogramData,
  type LineData,
} from "lightweight-charts";

import type { Candle, Timeframe } from "@/lib/market/types";
import { normalizeCandlesAscUnique, upsertCandleAscUnique } from "@/lib/market/candles";
import { calcRSI, calcStochRSI, calcOBV, calcPVT } from "@/lib/market/indicators";
import { safeRemoveChart } from "./chartUtils";

const TF_OPTIONS: Timeframe[] = ["1m", "5m", "15m", "1h", "4h"];

type SnapshotResponse = {
  candles: Candle[];
};

function tfToLabel(tf: Timeframe) {
  switch (tf) {
    case "1m":
      return "1m";
    case "5m":
      return "5m";
    case "15m":
      return "15m";
    case "1h":
      return "1h";
    case "4h":
      return "4h";
  }
}

/**
 * lightweight-charts v5: time은 UNIX seconds(number) 권장
 */
function toUnixSec(t: number) {
  return t >= 1_000_000_000_000 ? Math.floor(t / 1000) : Math.floor(t);
}

/**
 * v5: tickMarkFormatter 시그니처 (time, tickMarkType, locale)
 * - Date/Time/DateTime 처리
 */
function tickMarkFormatter(time: Time, tickMarkType: TickMarkType, _locale?: string) {
  const ts = typeof time === "number" ? time : (time as any).timestamp;
  if (!ts) return "";

  const d = new Date(ts * 1000);
  const pad = (n: number) => String(n).padStart(2, "0");

  // intraday
  if (tickMarkType === TickMarkType.Time) {
    return `${pad(d.getHours())}:${pad(d.getMinutes())}`;
  }

  // date (fallback)
  return `${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
}

export default function Page() {
  // 단일 마켓 viewer면 여기 고정값으로 두면 됨
  const MARKET = "KRW-BTC";

  const [tf, setTf] = useState<Timeframe>("1m");
  const [candles, setCandles] = useState<Candle[]>([]);
  const wsRef = useRef<WebSocket | null>(null);

  // layout refs
  const wrapRef = useRef<HTMLDivElement | null>(null);
  const mainRef = useRef<HTMLDivElement | null>(null);
  const ind1Ref = useRef<HTMLDivElement | null>(null);
  const ind2Ref = useRef<HTMLDivElement | null>(null);

  // charts
  const chartMainRef = useRef<IChartApi | null>(null);
  const chartInd1Ref = useRef<IChartApi | null>(null);
  const chartInd2Ref = useRef<IChartApi | null>(null);

  // series
  const candleSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volSeriesRef = useRef<ISeriesApi<"Histogram"> | null>(null);

  const rsiSeriesRef = useRef<ISeriesApi<"Line"> | null>(null);
  const stochKSeriesRef = useRef<ISeriesApi<"Line"> | null>(null);
  const stochDSeriesRef = useRef<ISeriesApi<"Line"> | null>(null);

  const obvSeriesRef = useRef<ISeriesApi<"Line"> | null>(null);
  const pvtSeriesRef = useRef<ISeriesApi<"Line"> | null>(null);

  /**
   * indicators.ts 계약 유지:
   * - RSI/Stoch: (number|null)[]
   * - OBV/PVT: Candle[] -> (number|null)[]
   */
  const indicatorData = useMemo(() => {
    const cs = normalizeCandlesAscUnique(candles);
    const closes = cs.map((c) => c.close);

    const rsi = calcRSI(closes, 14);
    const stoch = calcStochRSI(rsi, 14, 3, 3);

    const obv = calcOBV(cs);
    const pvt = calcPVT(cs);

    return { cs, rsi, stoch, obv, pvt };
  }, [candles]);

  // snapshot + ws
  useEffect(() => {
    let mounted = true;

    async function loadSnapshot() {
      const res = await fetch(`/api/snapshot?tf=${encodeURIComponent(tf)}`, { cache: "no-store" });
      const json = (await res.json()) as SnapshotResponse;
      if (!mounted) return;
      setCandles(normalizeCandlesAscUnique(json.candles));
    }

    function connectWs() {
      try {
        wsRef.current?.close();
      } catch {}

      const proto = location.protocol === "https:" ? "wss" : "ws";

      // 서버가 market 파라미터 지원하면 같이 넘기는게 가장 정확
      // const url = `${proto}://${location.host}/ws?market=${encodeURIComponent(MARKET)}&tf=${encodeURIComponent(tf)}`;
      const url = `${proto}://${location.host}/ws?tf=${encodeURIComponent(tf)}`;

      const ws = new WebSocket(url);
      wsRef.current = ws;

      ws.onmessage = (ev) => {
        try {
          const msg = JSON.parse(ev.data);

          const c: Candle = {
            market: msg.market ?? MARKET,
            tf: msg.tf ?? tf,
            time: msg.time ?? msg.open_time ?? msg.close_time,
            open: msg.open,
            high: msg.high,
            low: msg.low,
            close: msg.close,
            volume: msg.volume ?? msg.trade_volume ?? 0,
          };

          setCandles((prev) => upsertCandleAscUnique(prev, c));
        } catch {}
      };
    }

    loadSnapshot().catch(() => {});
    connectWs();

    return () => {
      mounted = false;
      try {
        wsRef.current?.close();
      } catch {}
      wsRef.current = null;
    };
  }, [tf]);

  // create charts (1회)
  useEffect(() => {
    const mainEl = mainRef.current;
    const ind1El = ind1Ref.current;
    const ind2El = ind2Ref.current;
    if (!mainEl || !ind1El || !ind2El) return;

    safeRemoveChart(chartMainRef.current);
    safeRemoveChart(chartInd1Ref.current);
    safeRemoveChart(chartInd2Ref.current);

    chartMainRef.current = null;
    chartInd1Ref.current = null;
    chartInd2Ref.current = null;

    const baseOptions = {
      layout: {
        background: { type: ColorType.Solid, color: "#0b0f1a" },
        textColor: "rgba(220, 220, 220, 0.85)",
        fontSize: 12,
      },
      grid: {
        vertLines: { color: "rgba(255,255,255,0.06)" },
        horzLines: { color: "rgba(255,255,255,0.06)" },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: { labelVisible: false },
        horzLine: { labelVisible: true },
      },
      rightPriceScale: {
        borderVisible: false,
        minimumWidth: 74,
      },
      localization: { locale: "ko-KR" },
    } as const;

    const mainChart = createChart(mainEl, {
      ...baseOptions,
      timeScale: {
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 5,
        tickMarkFormatter,
      },
      handleScale: { mouseWheel: true, pinch: true, axisPressedMouseMove: true },
      handleScroll: { mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true, vertTouchDrag: false },
    });

    const ind1Chart = createChart(ind1El, {
      ...baseOptions,
      timeScale: {
        visible: false,
        timeVisible: true,
        secondsVisible: false,
        tickMarkFormatter,
      },
      // ✅ 패널이 따로 움직이는 문제 방지
      handleScale: { mouseWheel: false, pinch: false, axisPressedMouseMove: false },
      handleScroll: { mouseWheel: false, pressedMouseMove: false, horzTouchDrag: false, vertTouchDrag: false },
    });

    const ind2Chart = createChart(ind2El, {
      ...baseOptions,
      timeScale: {
        visible: true,
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 5,
        tickMarkFormatter,
      },
      handleScale: { mouseWheel: false, pinch: false, axisPressedMouseMove: false },
      handleScroll: { mouseWheel: false, pressedMouseMove: false, horzTouchDrag: false, vertTouchDrag: false },
    });

    chartMainRef.current = mainChart;
    chartInd1Ref.current = ind1Chart;
    chartInd2Ref.current = ind2Chart;

    // --- main series ---
    candleSeriesRef.current = mainChart.addSeries(CandlestickSeries, {
      priceScaleId: "right",
      wickUpColor: "#26a69a",
      wickDownColor: "#ef5350",
      upColor: "#26a69a",
      downColor: "#ef5350",
      borderVisible: false,
    });

    volSeriesRef.current = mainChart.addSeries(HistogramSeries, {
      priceScaleId: "vol",
      priceFormat: { type: "volume" },
    });

    mainChart.priceScale("vol").applyOptions({ scaleMargins: { top: 0.8, bottom: 0.0 } });
    mainChart.priceScale("right").applyOptions({ scaleMargins: { top: 0.1, bottom: 0.2 } });

    // --- ind1 series (RSI + Stoch K/D) ---
    rsiSeriesRef.current = ind1Chart.addSeries(LineSeries, { priceScaleId: "right", lineWidth: 2 });
    stochKSeriesRef.current = ind1Chart.addSeries(LineSeries, { priceScaleId: "right", lineWidth: 2 });
    stochDSeriesRef.current = ind1Chart.addSeries(LineSeries, { priceScaleId: "right", lineWidth: 2 });
    ind1Chart.priceScale("right").applyOptions({ scaleMargins: { top: 0.15, bottom: 0.15 } });

    // --- ind2 series (OBV + PVT) ---
    obvSeriesRef.current = ind2Chart.addSeries(LineSeries, { priceScaleId: "right", lineWidth: 2 });
    pvtSeriesRef.current = ind2Chart.addSeries(LineSeries, { priceScaleId: "right", lineWidth: 2 });
    ind2Chart.priceScale("right").applyOptions({ scaleMargins: { top: 0.15, bottom: 0.15 } });

    // ✅ timeScale sync main -> sub
    const syncFromMain = () => {
      const range = mainChart.timeScale().getVisibleLogicalRange();
      if (!range) return;
      ind1Chart.timeScale().setVisibleLogicalRange(range);
      ind2Chart.timeScale().setVisibleLogicalRange(range);
    };

    const onRangeChange = () => syncFromMain();
    mainChart.timeScale().subscribeVisibleLogicalRangeChange(onRangeChange);

    // ✅ resize: 스크롤 방지 + 패널 높이 고정
    const ro = new ResizeObserver(() => {
      const w = wrapRef.current?.clientWidth ?? mainEl.clientWidth;
      const totalH = wrapRef.current?.clientHeight ?? 800;

      const mainH = Math.max(240, Math.floor(totalH * 0.62));
      const ind1H = Math.max(140, Math.floor(totalH * 0.20));
      const ind2H = Math.max(140, totalH - mainH - ind1H);

      mainChart.applyOptions({ width: w, height: mainH });
      ind1Chart.applyOptions({ width: w, height: ind1H });
      ind2Chart.applyOptions({ width: w, height: ind2H });

      syncFromMain();
    });

    if (wrapRef.current) ro.observe(wrapRef.current);

    syncFromMain();

    return () => {
      try {
        mainChart.timeScale().unsubscribeVisibleLogicalRangeChange(onRangeChange);
      } catch {}
      try {
        ro.disconnect();
      } catch {}

      safeRemoveChart(chartMainRef.current);
      safeRemoveChart(chartInd1Ref.current);
      safeRemoveChart(chartInd2Ref.current);

      chartMainRef.current = null;
      chartInd1Ref.current = null;
      chartInd2Ref.current = null;
    };
  }, []);

  // set data (candles 변화마다)
  useEffect(() => {
    const { cs, rsi, stoch, obv, pvt } = indicatorData;

    const candleSeries = candleSeriesRef.current;
    const volSeries = volSeriesRef.current;
    const rsiSeries = rsiSeriesRef.current;
    const stochK = stochKSeriesRef.current;
    const stochD = stochDSeriesRef.current;
    const obvSeries = obvSeriesRef.current;
    const pvtSeries = pvtSeriesRef.current;

    if (!candleSeries || !volSeries || !rsiSeries || !stochK || !stochD || !obvSeries || !pvtSeries) return;

    const candleData: CandlestickData<Time>[] = cs.map((c) => ({
      time: toUnixSec(c.time) as unknown as Time,
      open: c.open,
      high: c.high,
      low: c.low,
      close: c.close,
    }));

    const volData: HistogramData<Time>[] = cs.map((c) => ({
      time: toUnixSec(c.time) as unknown as Time,
      value: c.volume ?? 0,
    }));

    // indicators.ts: null -> NaN
    const rsiData: LineData<Time>[] = cs.map((c, i) => ({
      time: toUnixSec(c.time) as unknown as Time,
      value: rsi[i] ?? NaN,
    }));

    const stochKData: LineData<Time>[] = cs.map((c, i) => ({
      time: toUnixSec(c.time) as unknown as Time,
      value: stoch.k[i] ?? NaN,
    }));

    const stochDData: LineData<Time>[] = cs.map((c, i) => ({
      time: toUnixSec(c.time) as unknown as Time,
      value: stoch.d[i] ?? NaN,
    }));

    const finite = (arr: (number | null)[]) => arr.filter((v): v is number => v !== null && Number.isFinite(v));

    const obvF = finite(obv);
    const pvtF = finite(pvt);

    const obvMin = obvF.length ? Math.min(...obvF) : 0;
    const obvMax = obvF.length ? Math.max(...obvF) : 1;
    const pvtMin = pvtF.length ? Math.min(...pvtF) : 0;
    const pvtMax = pvtF.length ? Math.max(...pvtF) : 1;

    const norm = (v: number, min: number, max: number) => (max === min ? 0 : ((v - min) / (max - min)) * 100);

    const obvData: LineData<Time>[] = cs.map((c, i) => {
      const v = obv[i];
      return {
        time: toUnixSec(c.time) as unknown as Time,
        value: v === null ? NaN : norm(v, obvMin, obvMax),
      };
    });

    const pvtData: LineData<Time>[] = cs.map((c, i) => {
      const v = pvt[i];
      return {
        time: toUnixSec(c.time) as unknown as Time,
        value: v === null ? NaN : norm(v, pvtMin, pvtMax),
      };
    });

    candleSeries.setData(candleData);
    volSeries.setData(volData);
    rsiSeries.setData(rsiData);
    stochK.setData(stochKData);
    stochD.setData(stochDData);
    obvSeries.setData(obvData);
    pvtSeries.setData(pvtData);

    chartMainRef.current?.timeScale().fitContent();
  }, [indicatorData]);

  return (
    <div className="w-full h-[calc(100vh-16px)] p-2 overflow-hidden">
      {/* TF tabs */}
      <div className="flex gap-2 items-center mb-2">
        {TF_OPTIONS.map((t) => (
          <button
            key={t}
            onClick={() => setTf(t)}
            className={[
              "px-3 py-1 rounded-md text-sm",
              t === tf ? "bg-white/15 text-white" : "bg-white/5 text-white/70 hover:bg-white/10",
            ].join(" ")}
          >
            {tfToLabel(t)}
          </button>
        ))}
        <div className="ml-auto text-xs text-white/50">
          {MARKET} / TF: {tfToLabel(tf)}
        </div>
      </div>

      {/* charts */}
      <div ref={wrapRef} className="w-full h-[calc(100%-36px)] overflow-hidden flex flex-col gap-2">
        <div ref={mainRef} className="w-full flex-1 overflow-hidden rounded-lg border border-white/5" />
        <div ref={ind1Ref} className="w-full overflow-hidden rounded-lg border border-white/5" style={{ height: "22%" }} />
        <div ref={ind2Ref} className="w-full overflow-hidden rounded-lg border border-white/5" style={{ height: "22%" }} />
      </div>
    </div>
  );
}
