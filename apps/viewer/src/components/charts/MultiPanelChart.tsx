"use client";

import React, { useEffect, useMemo, useRef } from "react";
import {
  ColorType,
  CrosshairMode,
  createChart,
  CandlestickSeries,
  LineSeries,
  HistogramSeries,
  type IChartApi,
  type ISeriesApi,
  type Time,
  type CandlestickData,
  type HistogramData,
  type LineData,
  type ITimeScaleApi,
} from "lightweight-charts";

import type { Candle } from "@/lib/market/types";
import { calcRSI, calcStochRSI, calcOBV, calcPVT } from "@/lib/market/indicators";
import { safeRemoveChart } from "./chartUtils";
import { normalizeCandlesAscUnique } from "@/lib/market/candles";

type Props = {
  candles: Candle[];
};

function toCandleData(c: Candle): CandlestickData<Time> {
  return { time: c.time as unknown as Time, open: c.open, high: c.high, low: c.low, close: c.close };
}
function toVolumeData(c: Candle): HistogramData<Time> {
  return { time: c.time as unknown as Time, value: c.volume ?? 0 };
}
function toLineData(time: number, value: number): LineData<Time> {
  return { time: time as unknown as Time, value };
}

export default function MultiPanelChart({ candles }: Props) {
  const mainEl = useRef<HTMLDivElement | null>(null);
  const rsiEl = useRef<HTMLDivElement | null>(null);
  const flowEl = useRef<HTMLDivElement | null>(null);

  const mainChartRef = useRef<IChartApi | null>(null);
  const rsiChartRef = useRef<IChartApi | null>(null);
  const flowChartRef = useRef<IChartApi | null>(null);

  const candleSeriesRef = useRef<ISeriesApi<any> | null>(null);
  const volSeriesRef = useRef<ISeriesApi<any> | null>(null);

  const rsiSeriesRef = useRef<ISeriesApi<any> | null>(null);
  const stochKSeriesRef = useRef<ISeriesApi<any> | null>(null);
  const stochDSeriesRef = useRef<ISeriesApi<any> | null>(null);

  const obvSeriesRef = useRef<ISeriesApi<any> | null>(null);
  const pvtSeriesRef = useRef<ISeriesApi<any> | null>(null);

  // ✅ timeScale sync cleanup handlers
  const timeSyncUnsubsRef = useRef<Array<() => void>>([]);

  // ✅ fitContent를 매번 호출하면 사용자 pan/zoom이 계속 리셋되어 "떨림"이 생김 -> 초기 1회만
  const didInitialFitRef = useRef(false);

  const commonOptions = useMemo(
    () => ({
      layout: {
        background: { type: ColorType.Solid as const, color: "#0b0f19" },
        textColor: "#cbd5e1",
      },
      grid: {
        vertLines: { color: "rgba(148,163,184,0.10)" },
        horzLines: { color: "rgba(148,163,184,0.10)" },
      },
      crosshair: { mode: CrosshairMode.Normal },
      rightPriceScale: { borderColor: "rgba(148,163,184,0.20)" },
      timeScale: { borderColor: "rgba(148,163,184,0.20)" },
      // 기본은 메인에서 인터랙션 허용
      handleScroll: { mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true, vertTouchDrag: false },
      handleScale: { mouseWheel: true, pinch: true, axisPressedMouseMove: true },
    }),
    []
  );

  const panelNoInteract = useMemo(
    () => ({
      // ✅ 패널(보조 차트)은 사용자 스크롤/줌을 막고, 메인 범위만 따라가게 함(떨림/핑퐁 방지)
      handleScroll: { mouseWheel: false, pressedMouseMove: false, horzTouchDrag: false, vertTouchDrag: false },
      handleScale: { mouseWheel: false, pinch: false, axisPressedMouseMove: false },
    }),
    []
  );

  function clearTimeSync() {
    const unsubs = timeSyncUnsubsRef.current;
    timeSyncUnsubsRef.current = [];
    for (const u of unsubs) {
      try {
        u();
      } catch {}
    }
  }

  // ✅ v5 안전 동기화: range null / disposed / 초기화 타이밍 방어 + rAF
  function syncVisibleRange(source: ITimeScaleApi<any>, targets: ITimeScaleApi<any>[]) {
    let lock = false;
    let rafId: number | null = null;

    const onChange = (range: any) => {
      if (lock) return;
      if (!range) return;

      // time range / logical range 모두 방어
      const hasRange =
        (range.from != null && range.to != null) || (range.left != null && range.right != null);
      if (!hasRange) return;

      lock = true;

      if (rafId != null) cancelAnimationFrame(rafId);
      rafId = requestAnimationFrame(() => {
        try {
          for (const t of targets) {
            try {
              t.setVisibleRange(range);
            } catch {
              // disposed / no data / 초기화 타이밍 -> 무시
            }
          }
        } finally {
          lock = false;
        }
      });
    };

    source.subscribeVisibleTimeRangeChange(onChange);

    return () => {
      try {
        source.unsubscribeVisibleTimeRangeChange(onChange);
      } catch {}
      if (rafId != null) {
        try {
          cancelAnimationFrame(rafId);
        } catch {}
      }
    };
  }

  function buildCharts() {
    // cleanup (dev strictmode 안전)
    clearTimeSync();
    safeRemoveChart(mainChartRef);
    safeRemoveChart(rsiChartRef);
    safeRemoveChart(flowChartRef);

    candleSeriesRef.current = null;
    volSeriesRef.current = null;
    rsiSeriesRef.current = null;
    stochKSeriesRef.current = null;
    stochDSeriesRef.current = null;
    obvSeriesRef.current = null;
    pvtSeriesRef.current = null;

    // 차트 재생성 시 초기 fit도 다시 허용
    didInitialFitRef.current = false;

    if (!mainEl.current || !rsiEl.current || !flowEl.current) return;

    // Main (인터랙션 허용)
    const main = createChart(mainEl.current, commonOptions);
    mainChartRef.current = main;

    candleSeriesRef.current = main.addSeries(CandlestickSeries, {});
    volSeriesRef.current = main.addSeries(HistogramSeries, { priceFormat: { type: "volume" }, priceScaleId: "" });
    volSeriesRef.current.priceScale().applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });

    // RSI (인터랙션 비활성)
    const rsiC = createChart(rsiEl.current, { ...commonOptions, ...panelNoInteract });
    rsiChartRef.current = rsiC;

    rsiSeriesRef.current = rsiC.addSeries(LineSeries, {});
    stochKSeriesRef.current = rsiC.addSeries(LineSeries, {});
    stochDSeriesRef.current = rsiC.addSeries(LineSeries, {});

    // Flow (인터랙션 비활성)
    const flow = createChart(flowEl.current, { ...commonOptions, ...panelNoInteract });
    flowChartRef.current = flow;

    obvSeriesRef.current = flow.addSeries(LineSeries, {});
    pvtSeriesRef.current = flow.addSeries(LineSeries, {});

    // ✅ timeScale sync: 메인 -> (RSI, Flow) 단방향만 (핑퐁/떨림 방지)
    const mainTs = main.timeScale();
    const rsiTs = rsiC.timeScale();
    const flowTs = flow.timeScale();

    timeSyncUnsubsRef.current.push(syncVisibleRange(mainTs, [rsiTs, flowTs]));
  }

  function renderAll(raw: Candle[]) {
    const safe = normalizeCandlesAscUnique(raw);

    const candleSeries = candleSeriesRef.current;
    const volSeries = volSeriesRef.current;
    if (!candleSeries || !volSeries) return;

    candleSeries.setData(safe.map(toCandleData));
    volSeries.setData(safe.map(toVolumeData));

    // indicators
    const closes = safe.map((c) => c.close);
    const rsi = calcRSI(closes, 14);
    const { k, d } = calcStochRSI(rsi, 14, 3, 3);
    const obv = calcOBV(safe);
    const pvt = calcPVT(safe);

    const rsiData: LineData<Time>[] = [];
    const kData: LineData<Time>[] = [];
    const dData: LineData<Time>[] = [];
    const obvData: LineData<Time>[] = [];
    const pvtData: LineData<Time>[] = [];

    for (let i = 0; i < safe.length; i++) {
      const t = safe[i].time;
      if (rsi[i] !== null) rsiData.push(toLineData(t, rsi[i]!));
      if (k[i] !== null) kData.push(toLineData(t, k[i]!));
      if (d[i] !== null) dData.push(toLineData(t, d[i]!));
      if (obv[i] !== null) obvData.push(toLineData(t, obv[i]!));
      if (pvt[i] !== null) pvtData.push(toLineData(t, pvt[i]!));
    }

    rsiSeriesRef.current?.setData(rsiData);
    stochKSeriesRef.current?.setData(kData);
    stochDSeriesRef.current?.setData(dData);

    obvSeriesRef.current?.setData(obvData);
    pvtSeriesRef.current?.setData(pvtData);

    // ✅ 초기 1회만 fitContent (계속 호출하면 pan/zoom이 매번 리셋되어 떨림)
    if (!didInitialFitRef.current) {
      didInitialFitRef.current = true;
      requestAnimationFrame(() => {
        try {
          mainChartRef.current?.timeScale().fitContent();
        } catch {}
        try {
          rsiChartRef.current?.timeScale().fitContent();
        } catch {}
        try {
          flowChartRef.current?.timeScale().fitContent();
        } catch {}
      });
    }
  }

  // init once
  useEffect(() => {
    buildCharts();

    const onResize = () => {
      try {
        if (mainEl.current && mainChartRef.current) {
          mainChartRef.current.applyOptions({ width: mainEl.current.clientWidth, height: mainEl.current.clientHeight });
        }
      } catch {}
      try {
        if (rsiEl.current && rsiChartRef.current) {
          rsiChartRef.current.applyOptions({ width: rsiEl.current.clientWidth, height: rsiEl.current.clientHeight });
        }
      } catch {}
      try {
        if (flowEl.current && flowChartRef.current) {
          flowChartRef.current.applyOptions({ width: flowEl.current.clientWidth, height: flowEl.current.clientHeight });
        }
      } catch {}
    };

    window.addEventListener("resize", onResize);
    onResize();

    return () => {
      window.removeEventListener("resize", onResize);

      clearTimeSync();
      safeRemoveChart(mainChartRef);
      safeRemoveChart(rsiChartRef);
      safeRemoveChart(flowChartRef);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // candles update -> render
  useEffect(() => {
    if (!candles || candles.length === 0) return;
    renderAll(candles);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [candles]);

  return (
    <div style={{ display: "grid", gridTemplateRows: "1fr 220px 220px", gap: 10, height: "calc(100vh - 56px)" }}>
      <div
        ref={mainEl}
        style={{
          borderRadius: 12,
          overflow: "hidden",
          border: "1px solid rgba(148,163,184,0.12)",
          background: "#0b0f19",
        }}
      />
      <div
        ref={rsiEl}
        style={{
          borderRadius: 12,
          overflow: "hidden",
          border: "1px solid rgba(148,163,184,0.12)",
          background: "#0b0f19",
        }}
      />
      <div
        ref={flowEl}
        style={{
          borderRadius: 12,
          overflow: "hidden",
          border: "1px solid rgba(148,163,184,0.12)",
          background: "#0b0f19",
        }}
      />
    </div>
  );
}
