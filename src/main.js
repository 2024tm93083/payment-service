// payment-service/src/main.js
import express from "express";
import pkg from "pg";
import dotenv from "dotenv";
import { v4 as uuidv4 } from "uuid";

dotenv.config();
const { Pool } = pkg;

const DB_HOST = process.env.POSTGRES_HOST || "payment-db";
const DB_PORT = parseInt(process.env.POSTGRES_PORT || "5432", 10);
const DB_USER = process.env.POSTGRES_USER || "postgres";
const DB_PASS = process.env.POSTGRES_PASSWORD || "2491";
const DB_NAME = process.env.PAYMENT_DB || "payment_db";

const pool = new Pool({
  host: DB_HOST,
  port: DB_PORT,
  user: DB_USER,
  password: DB_PASS,
  database: DB_NAME,
  max: 8,
});

async function waitForDb(retries = 12, delayMs = 2000) {
  for (let i = 0; i < retries; i++) {
    try {
      const c = await pool.connect();
      c.release();
      return;
    } catch {
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
  throw new Error("payment DB unavailable");
}

async function runQuery(sql, params = []) {
  const c = await pool.connect();
  try {
    return await c.query(sql, params);
  } finally {
    c.release();
  }
}

(async () => {
  await waitForDb();

  const app = express();
  app.use(express.json());

  app.get("/healthz", (req, res) => res.json({ status: "ok" }));

  // Idempotent charge endpoint
  app.post("/v1/payments/charge", async (req, res) => {
    const idemp = req.header("Idempotency-Key");
    if (!idemp) return res.status(400).json({ error: "Idempotency-Key header required" });

    // check idempotency_keys table
    const existing = await runQuery("SELECT idempotency_key, response_snapshot FROM payment_schema.idempotency_keys WHERE idempotency_key=$1", [idemp]);
    if (existing.rowCount > 0) {
      const snap = existing.rows[0].response_snapshot;
      // stored as json string in DB; attempt parse then return
      try {
        return res.status(200).json(JSON.parse(snap));
      } catch {
        return res.status(200).json(snap);
      }
    }

    const { order_id, amount, method } = req.body;
    // simple success rule: succeed for <= 10000
    const status = Number(amount) > 10000 ? "FAILED" : "SUCCESS";
    const reference = uuidv4();
    const resp = { payment_id: Date.now(), order_id, amount, method, status, reference, created_at: new Date().toISOString() };

    // persist payment and idempotency snapshot
    try {
      await runQuery(
        "INSERT INTO payment_schema.payments(payment_id,order_id,amount,method,status,reference,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7)",
        [resp.payment_id, order_id, amount, method, status, reference, resp.created_at]
      );
      await runQuery(
        "INSERT INTO payment_schema.idempotency_keys(idempotency_key,response_snapshot) VALUES ($1,$2)",
        [idemp, JSON.stringify(resp)]
      );
    } catch (e) {
      console.error("DB error in payment persist:", e);
      return res.status(500).json({ error: "payment persistence failed" });
    }

    return res.status(200).json(resp);
  });

  const PORT = process.env.PORT || 6000;
  app.listen(PORT, () => console.log(`Payment stub running on port ${PORT}`));
})();
