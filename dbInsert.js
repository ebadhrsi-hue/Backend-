import fs from "fs";
import { poolPromise, sql } from "../config/db.js";

/**
 * Insert a file blob into a specific whitelisted table.
 * tableKey must exist in tableMap to prevent SQL injection on table name.
 */
const tableMap = {
  table_file1: "table_file1",
  table_file2: "table_file2",
  table_file3: "table_file3",
  table_file4: "table_file4",
  table_processed_multi: "table_processed_multi",
  table_file_single: "table_file_single",
  table_processed_single: "table_processed_single"
};

export async function insertFileBlob(tableKey, fileName, filePathOrBuffer) {
  const tableName = tableMap[tableKey];
  if (!tableName) throw new Error(`Disallowed table: ${tableKey}`);

  const pool = await poolPromise;

  // Read buffer (accepts a path or a Buffer)
  const buffer =
    Buffer.isBuffer(filePathOrBuffer)
      ? filePathOrBuffer
      : fs.readFileSync(filePathOrBuffer);

  const request = pool.request();
  request.input("file_name", sql.NVarChar(255), fileName);
  request.input("file_data", sql.VarBinary(sql.MAX), buffer);

  await request.query(`
    INSERT INTO ${tableName} (file_name, file_data, inserted_at)
    VALUES (@file_name, @file_data, GETDATE())
  `);
}
