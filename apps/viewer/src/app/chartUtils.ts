import type { IChartApi } from "lightweight-charts";

export function safeRemoveChart(chart: IChartApi | null) {
  if (!chart) return;
  try {
    chart.remove();
  } catch {
    // ignore
  }
}
