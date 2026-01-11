// src/index.ts
import { runRecovery } from "./recovery.js";
import { TF_SEC } from "./timeframes.js";

function assert(cond: unknown, msg: string): asserts cond {
  if (!cond) throw new Error(msg);
}

// 사용 예:
// node dist/index.js recovery KRW-BTC 1m 2026-01-01T00:00:00 2026-01-01T03:00:00
const [, , cmd, market, tfStr, fromIso, toIso] = process.argv;

if (!cmd) {
  console.log("usage:");
  console.log("  node dist/index.js recovery <market> <tf> <fromIsoKst> <toIsoKst>");
  process.exit(0);
}

if (cmd === "recovery") {
  assert(market && tfStr && fromIso && toIso, "missing args");

  const tf = tfStr as keyof typeof TF_SEC;
  assert(tf in TF_SEC, `Unsupported tf=${tfStr}`);

  const rows = await runRecovery({
    market,
    tf,
    fromIsoKst: fromIso,
    toIsoKst: toIso,
    limitPerReq: 200,
    throttleMs: 120,
  });

  for (const r of rows) {
    process.stdout.write(JSON.stringify(r) + "\n");
  }
} else {
  throw new Error(`Unknown cmd=${cmd}`);
}
