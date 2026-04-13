import * as duckdb from "@duckdb/duckdb-wasm";

// Use local file URLs for both the worker and the WASM binary so nothing
// is fetched from the CDN. The worker needs { type: "module" } because Deno
// does not support classic workers.
const pkgDist = new URL(
  "file:///Users/tyler/code/work/duckdb-wasm/packages/duckdb-wasm/dist/",
);

const workerUrl = new URL("duckdb-browser-eh.worker.mjs", pkgDist);
const wasmUrl = new URL("duckdb-eh.wasm", pkgDist).href;

const worker = new Worker(workerUrl, { type: "module" });

const logger = new duckdb.VoidLogger();
const db = new duckdb.AsyncDuckDB(logger, worker);
await db.instantiate(wasmUrl, null);

const conn = await db.connect();
try {
  const result = await conn.query(
    "SELECT 42 AS answer, version() AS version",
  );
  console.log(result.toArray().map((r) => r.toJSON()));
} finally {
  await conn.close();
  await db.terminate();
  worker.terminate();
}
