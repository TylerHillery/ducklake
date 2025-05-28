import { DuckDBInstance } from "npm:@duckdb/node-api";

console.log("Deno version:", Deno.version.deno);

const instance = await DuckDBInstance.create(":memory:");
const connection = await instance.connect();

try {
  const reader = await connection.runAndReadAll("select 10, 'foo'");
  const rows = reader.getRows();
  
  console.debug(rows); // [ [ 10, "foo" ] ]
  
  const prepared = await connection.prepare("select $1, $2");
  prepared.bindInteger(1, 20);
  prepared.bindVarchar(2, "bar");
  const reader2 = await prepared.runAndReadAll();
  const rows2 = reader2.getRows();
  
  console.debug(rows2); // [ [ 20, "bar" ] ]
} finally {
  // Manual cleanup
  connection.closeSync();
  instance.closeSync();
}