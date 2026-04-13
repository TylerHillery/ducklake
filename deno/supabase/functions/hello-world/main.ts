import { serveDir } from "@std/http";
import { DuckDBInstance } from "npm:@duckdb/node-api";

const userPagePattern = new URLPattern({ pathname: "/users/:id" });
const staticPathPattern = new URLPattern({ pathname: "/static/*" });

async function getDuckDBData() {
  const instance = await DuckDBInstance.create(":memory:");
  const connection = await instance.connect();

  try {
    // Execute simple queries like in duckdb.ts
    const reader1 = await connection.runAndReadAll(
      "select 10 as num, 'foo' as text",
    );
    const rows1 = reader1.getRows();

    const reader2 = await connection.runAndReadAll(
      "select 20 as num, 'bar' as text",
    );
    const rows2 = reader2.getRows();

    // Also test prepared statements
    const prepared = await connection.prepare("select $1 as num, $2 as text");
    prepared.bindInteger(1, 30);
    prepared.bindVarchar(2, "baz");
    const reader3 = await prepared.runAndReadAll();
    const rows3 = reader3.getRows();

    return {
      query1: rows1,
      query2: rows2,
      prepared_query: rows3,
      success: true,
      method: "node-api",
    };
  } finally {
    // Manual cleanup
    connection.closeSync();
    instance.closeSync();
  }
}

export default {
  async fetch(req) {
    const url = new URL(req.url);

    if (url.pathname === "/hello-world") {
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
