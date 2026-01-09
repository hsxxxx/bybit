import { NextResponse } from "next/server";

export const runtime = "nodejs";

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);

  const market = searchParams.get("market") ?? "KRW-BTC";
  const tf = searchParams.get("tf") ?? "1m";
  const limit = searchParams.get("limit") ?? "1000";

  // viewer-ws가 제공하는 HTTP snapshot으로 프록시
  const base = process.env.VIEWER_WS_HTTP ?? "http://localhost:3101";
  const url = `${base}/snapshot?market=${encodeURIComponent(market)}&tf=${encodeURIComponent(tf)}&limit=${encodeURIComponent(limit)}`;

  const r = await fetch(url, { cache: "no-store" });
  const data = await r.json();

  return NextResponse.json(data, { status: r.status });
}
