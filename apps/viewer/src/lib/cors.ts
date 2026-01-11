import cors from "cors";

export function makeCors(origins: string[]) {
  if (!origins || origins.length === 0) {
    return cors({ origin: true, credentials: true });
  }

  return cors({
    origin(origin, cb) {
      if (!origin) return cb(null, true);
      if (origins.includes(origin)) return cb(null, true);
      return cb(new Error(`CORS blocked: ${origin}`));
    },
    credentials: true
  });
}
