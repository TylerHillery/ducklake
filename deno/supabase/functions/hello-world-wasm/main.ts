import { serveDir } from "@std/http";
import * as duckdb from "@duckdb/duckdb-wasm";

const userPagePattern = new URLPattern({ pathname: "/users/:id" });
const staticPathPattern = new URLPattern({ pathname: "/static/*" });

async function getDuckDBData() {
  // duckdb-wasm currently ships only a classic worker (importScripts), which
  // Deno and the Supabase edge runtime reject. We need a module worker.
  //
  // Pending: @duckdb/duckdb-wasm publishing an ESM worker bundle. Once that is
  // available on npm, update the import map version and switch to:
  //
  //   const pkgDist = new URL(
  //     "npm:@duckdb/duckdb-wasm@<version>/dist/",
  //     import.meta.url,
  //   );
  //   const worker = new Worker(
  //     new URL("duckdb-browser-eh.worker.mjs", pkgDist),
  //     { type: "module" },
  //   );
  //   await db.instantiate(new URL("duckdb-eh.wasm", pkgDist).href, null);
  //
  // For now, try the classic worker to document the failure mode:
  const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
  const worker_url = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker!}");`], {
      type: "text/javascript",
    }),
  );
  const worker = new Worker(worker_url);
  const logger = new duckdb.ConsoleLogger();
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(worker_url);
  const connection = await db.connect();

  try {
    const table1 = await connection.query(
      "select 10 as num, 'foo' as text",
    );
    const rows1 = table1.toArray().map((row) => row.toJSON());

    const table2 = await connection.query(
      "select 20 as num, 'bar' as text",
    );
    const rows2 = table2.toArray().map((row) => row.toJSON());

    const prepared = await connection.prepare("select $1 as num, $2 as text");
    const table3 = await prepared.query(30, "baz");
    const rows3 = table3.toArray().map((row) => row.toJSON());
    await prepared.close();

    return {
      query1: rows1,
      query2: rows2,
      prepared_query: rows3,
      success: true,
      method: "duckdb-wasm",
    };
  } finally {
    await connection.close();
    await db.terminate();
    worker.terminate();
  }
}

export default {
  async fetch(req) {
    const url = new URL(req.url);

    if (url.pathname === "/hello-world-wasm") {
      try {
        const data = await getDuckDBData();
        return new Response(JSON.stringify(data, null, 2), {
          headers: { "Content-Type": "application/json" },
        });
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        return new Response(JSON.stringify({ error: message }), {
          status: 500,
          headers: { "Content-Type": "application/json" },
        });
      }
    }

    const userPageMatch = userPagePattern.exec(url);
    if (userPageMatch) {
      return new Response(userPageMatch.pathname.groups.id);
    }

    if (staticPathPattern.test(url)) {
      return serveDir(req);
    }

    return new Response("Not found", { status: 404 });
  },
} satisfies Deno.ServeDefaultExport;
