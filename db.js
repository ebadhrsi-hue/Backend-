import sql from "mssql";

const poolPromise = new sql.ConnectionPool({
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  server: process.env.DB_SERVER,
  database: process.env.DB_DATABASE,
  options: {
    encrypt: String(process.env.DB_ENCRYPT).toLowerCase() === "true",
    trustServerCertificate: String(process.env.DB_TRUST_CERT).toLowerCase() !== "false"
  },
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000
  }
})
  .connect()
  .then(pool => {
    console.log("Connected to SQL Server");
    return pool;
  })
  .catch(err => {
    console.error("SQL Server connection failed:", err);
    throw err;
  });

export { sql, poolPromise };
