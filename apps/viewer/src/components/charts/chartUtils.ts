import type { IChartApi } from "lightweight-charts";

export function safeRemoveChart(ref: React.MutableRefObject<IChartApi | null>) {
  const chart = ref.current;
  if (!chart) return;

  try {
    chart.remove(); // v5 dispose
  } catch {
    // dev(StrictMode/fast refresh)에서 "Object is disposed"가 흔함 -> 무시
  } finally {
    ref.current = null;
  }
}
