// src/index.ts
import { runRecovery } from "./recovery.js";
import { TF_SEC, type TfKey } from "./timeframes.js";
import { fetchUpbitMarkets } from "./exchange/upbit.js";

function assert(cond: unknown, msg: string): asserts cond {
  if (!cond) throw new Error(msg);
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function parseArgs(argv: string[]) {
  // 지원 포맷:
  // 1) node dist/index.js recovery MARKETS=ALL TFS=ALL 2026-01-01T00:00:00 2026-01-10T23:59:59
  // 2) node dist/index.js recovery MARKETS=KRW-BTC,KRW-ETH TFS=1m,5m 2026-01-01T00:00:00 2026-01-10T23:59:59
  // 3) node dist/index.js recovery KRW-BTC 1m 2026-01-01T00:00:00 2026-01-10T23:59:59  (기존 호환)

  const [, , cmd, a1, a2, a3, a4] = argv;
  assert(cmd, "missing cmd");

  if (cmd !== "recovery") throw new Error(`Unknown cmd=${cmd}`);

  assert(a1 && a2 && a3 && a4, "missing args");

  const looksKV = (s: string) => s.includes("=");

  if (looksKV(a1) || looksKV(a2)) {
    // KV 모드
    const kv1 = a1;
    const kv2 = a2;
    const fromIso = a3;
    const toIso = a4;

    const parseKV = (kv: string) => {
      const [k, v] = kv.split("=", 2);
      assert(k && v, `bad arg: ${kv}`);
      return { k: k.toUpperCase(), v };
    };

    const p1 = parseKV(kv1);
    const p2 = parseKV(kv2);

    const get = (key: string) => {
      if (p1.k === key) return p1.v;
      if (p2.k === key) return p2.v;
      throw new Error(`missing ${key}=...`);
    };

    return {
      cmd,
      marketsExpr: get("MARKETS"),
      tfsExpr: get("TFS"),
      fromIso,
      toIso,
    };
  }

  // legacy 모드: <market> <tf> <fromIso> <toIso>
  return {
    cmd,
    marketsExpr: a1,
    tfsExpr: a2,
    fromIso: a3,
    toIso: a4,
  };
}

function parseTfList(expr: string): TfKey[] {
  if (expr.toUpperCase() === "ALL") {
    // 원하는 TF만 골라서 ALL로 취급 (필요시 여기 수정)
    return Object.keys(TF_SEC) as TfKey[];
  }

  // "1m,5m,15m"
  const parts = expr.split(",").map((s) => s.trim()).filter(Boolean);
  assert(parts.length > 0, "empty TFS");

  const out: TfKey[] = [];
  for (const p of parts) {
    const tf = p as TfKey;
    assert(tf in TF_SEC, `Unsupported tf=${p}`);
    out.push(tf);
  }
  return Array.from(new Set(out));
}

async function resolveMarkets(expr: string): Promise<string[]> {
  const upper = expr.toUpperCase();

  if (upper === "ALL" || upper === "KRW" || upper === "BTC" || upper === "USDT") {
    const quote = (upper === "ALL" ? "ALL" : (upper as "KRW" | "BTC" | "USDT")) as
      | "KRW"
      | "BTC"
      | "USDT"
      | "ALL";
    const rows = await fetchUpbitMarkets({ quote });
    return rows.map((r) => r.market);
  }

  // "KRW-BTC,KRW-ETH"
  const parts = expr.split(",").map((s) => s.trim()).filter(Boolean);
  assert(parts.length > 0, "empty MARKETS");

  // 중복 제거
  return Array.from(new Set(parts));
}

async function main() {
  const args = parseArgs(process.argv);

  const tfs = parseTfList(args.tfsExpr);
  const markets = await resolveMarkets(args.marketsExpr);

  // 운영 파라미터 (429 방지)
  const throttleBetweenJobsMs = Number(process.env.THROTTLE_JOB_MS ?? "80"); // 작업 사이
  const throttleBetweenMarketsMs = Number(process.env.THROTTLE_MARKET_MS ?? "200"); // 마켓 단위
  const limitPerReq = Number(process.env.LIMIT_PER_REQ ?? "200");
  const throttleApiMs = Number(process.env.THROTTLE_API_MS ?? "120"); // upbit.ts 내부 호출용

  let ok = 0;
  let fail = 0;

  console.log(
    `[recovery] markets=${markets.length} tfs=${tfs.join(",")} from=${args.fromIso} to=${args.toIso}`
  );

  // 이중 루프: TF -> Market (원하면 순서 바꿔도 됨)
  for (const tf of tfs) {
    console.log(`\n[tf] ${tf} START`);

    for (let i = 0; i < markets.length; i++) {
      const market = markets[i];
      const jobId = `${tf} ${market} (${i + 1}/${markets.length})`;

      try {
        const rows = await runRecovery({
          market,
          tf,
          fromIsoKst: args.fromIso,
          toIsoKst: args.toIso,
          limitPerReq,
          throttleMs: throttleApiMs,
        });

        // 지금은 stdout(jsonl)로만 출력 (기존 흐름 유지)
        // DB upsert는 너의 파이프라인에서 처리하면 됨.
        for (const r of rows) process.stdout.write(JSON.stringify(r) + "\n");

        ok++;
        console.log(`[ok] ${jobId} rows=${rows.length}`);
      } catch (e) {
        fail++;
        console.error(`[fail] ${jobId}`, e);
      }

      if (throttleBetweenJobsMs > 0) await sleep(throttleBetweenJobsMs);
    }

    if (throttleBetweenMarketsMs > 0) await sleep(throttleBetweenMarketsMs);
    console.log(`[tf] ${tf} END`);
  }

  console.log(`\n[done] ok=${ok} fail=${fail}`);
  if (fail > 0) process.exitCode = 2;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
