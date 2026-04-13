import { DuckDBInstance } from "@duckdb/node-api";

const instance = await DuckDBInstance.create(":memory:");
const connection = await instance.connect();

try {
  const reader = await connection.runAndReadAll(
    "select 42 as answer, version() as version",
  );
  console.log(reader.getRows());
} finally {
  connection.closeSync();
  instance.closeSync();
}
