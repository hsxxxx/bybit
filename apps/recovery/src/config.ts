import path from "node:path";
import dotenv from "dotenv";

dotenv.config({ path: path.resolve(process.cwd(), ".env") });

export type Mode = "all" | "missing";

export const config = {
  db: {
    host: process.env.DB_HOST ?? "127.0.0.1",
    port: Number(process.env.DB_PORT ?? "3306"),
    user: process.env.DB_USER ?? "root",
    password: process.env.DB_PASS ?? "",
    database: process.env.DB_NAME ?? "bits"
  },
  upbit: {
    rps: Number(process.env.UPBIT_RPS ?? "8"),
    maxRetry: Number(process.env.UPBIT_MAX_RETRY ?? "8"),
    retryBaseMs: Number(process.env.UPBIT_RETRY_BASE_MS ?? "800"),
    retryMaxMs: Number(process.env.UPBIT_RETRY_MAX_MS ?? "15000")
  }
} as const;

export const DEFAULT_TFS = ["1m", "5m", "15m", "1h", "4h"] as const;
export type Tf = (typeof DEFAULT_TFS)[number];
