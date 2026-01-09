// src/db/config.ts
import dotenv from "dotenv";
dotenv.config(); // <- 중요: index.ts import 순서/경로에 의존하지 않게 여기서도 로드

export type DbConfig = {
  enable: boolean;
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;

  flushMs: number;
  batchMax: number;
};

function opt(v: string | undefined, fallback = "") {
  return (v ?? fallback).trim();
}

export const dbConfig: DbConfig = {
  enable: opt(process.env.SINKER_DB_ENABLE, "0") === "1",
  host: opt(process.env.DB_HOST, "localhost"),
  port: Number(opt(process.env.DB_PORT, "3306")),
  user: opt(process.env.DB_USER, "root"),
  password: opt(process.env.DB_PASS, ""),
  database: opt(process.env.DB_NAME, "bits"),

  flushMs: Number(opt(process.env.SINKER_DB_FLUSH_MS, "1000")),
  batchMax: Number(opt(process.env.SINKER_DB_BATCH_MAX, "2000"))
};
