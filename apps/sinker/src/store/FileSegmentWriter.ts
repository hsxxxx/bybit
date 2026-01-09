// sinker/src/store/FileSegmentWriter.ts
import fs from "node:fs";
import path from "node:path";
import { yyyymmddFromUnixSec } from "../utils/timeframe";
import type { BuiltPayload } from "../types";

export class FileSegmentWriter {
  constructor(private readonly baseDir: string) {}

  private ensureDir(p: string) {
    fs.mkdirSync(p, { recursive: true });
  }

  write(payload: BuiltPayload) {
    const { market, tf, candle } = payload;
    const day = yyyymmddFromUnixSec(candle.time);

    const dir = path.join(this.baseDir, market, tf);
    this.ensureDir(dir);

    const file = path.join(dir, `${day}.ndjson`);
    const line = JSON.stringify(payload) + "\n";
    fs.appendFileSync(file, line, "utf8");
  }

  listSegmentFiles(market: string, tf: string): string[] {
    const dir = path.join(this.baseDir, market, tf);
    if (!fs.existsSync(dir)) return [];
    const files = fs
      .readdirSync(dir)
      .filter((f) => f.endsWith(".ndjson"))
      .map((f) => path.join(dir, f))
      .sort(); // YYYYMMDD 정렬
    return files;
  }

  readNdjsonFile(filePath: string): BuiltPayload[] {
    if (!fs.existsSync(filePath)) return [];
    const txt = fs.readFileSync(filePath, "utf8");
    if (!txt.trim()) return [];
    const lines = txt.split("\n").filter(Boolean);
    const out: BuiltPayload[] = [];
    for (const line of lines) {
      try {
        const obj = JSON.parse(line) as BuiltPayload;
        if (obj?.market && obj?.tf && obj?.candle?.time != null) out.push(obj);
      } catch {
        // ignore broken line
      }
    }
    return out;
  }
}
