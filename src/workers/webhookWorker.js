import "dotenv/config";
import { Worker, UnrecoverableError } from "bullmq";
import pool from "../config/db.js";
import crypto from "node:crypto";

const worker = new Worker(
  "webhook-queue",
  async (job) => {
    const dbresult = await pool.query(
      `SELECT
      events.id AS event_id,
      events.type,
      events.data,
      webhook_endpoints.secret,
      webhook_endpoints.url
      FROM events
      JOIN webhook_endpoints
      ON events.endpoint_id = webhook_endpoints.id
      WHERE events.id = $1`,
      [job.data.eventId],
    );

    const event = dbresult.rows[0];

    console.log("START:", job.id, "Attempt:", job.attemptsMade + 1, new Date().toISOString());

    const attemptResult = await pool.query(
      `INSERT INTO webhook_delivery_attempts
       (event_id, attempt, status, started_at)
       VALUES ($1, $2, $3, $4)
       RETURNING *`,
      [
        event.event_id,
        job.attemptsMade + 1,
        "processing",
        new Date(),
      ]
    );

    console.log("Attempt DB row:", attemptResult.rows[0]);

    let status_code = null;
    let httpAcknowledged = false;



    try {
      const payload = JSON.stringify({
        type: event.type,
        data: event.data,
      });

      const signature = crypto
        .createHmac("sha256", event.secret)
        .update(payload)
        .digest("hex");

      const response = await fetch(event.url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Webhook-Signature": signature,
        },
        body: payload,
        signal: AbortSignal.timeout(5000),
      });
      console.log("Customer response status:", response.status);

      status_code = response.status;

      if (response.status >= 400 && response.status < 500 && response.status !== 429) {
        throw new UnrecoverableError(
          `Customer API failed with status ${response.status}`
        );
      }

      if (!response.ok) {
        throw new Error(`Customer API failed with status ${response.status}`);
      }

      httpAcknowledged = true

      const client = await pool.connect();

      try {
        await client.query("BEGIN")

        await client.query(
          `UPDATE webhook_delivery_attempts
          SET status = $1,
         status_code = $2,
         finished_at = $3
         WHERE id = $4`,
          [
            "success",
            response.status,
            new Date(),
            attemptResult.rows[0].id,
          ]
        );

        await client.query(`UPDATE deliveries SET attempt=$1,
        status =$2
        WHERE event_id= $3`, [
          job.attemptsMade + 1,
          "delivered",
          event.event_id
        ])
        await client.query("COMMIT")

        console.log("END:", job.id, new Date().toISOString());
      } catch (dbError) {
        console.error("DATABASE TRANSACTION FAILED:", {
          eventId: event.event_id,
          attempt: job.attemptsMade + 1,
          message: dbError.message,
        });

        try {
          await client.query("ROLLBACK");
          console.log("DATABASE ROLLBACK COMPLETED");
        } catch (rollbackError) {
          console.error("DATABASE ROLLBACK FAILED:", rollbackError);
        }

        throw dbError;
      } finally {
        client.release()
      }

    } catch (error) {
      if (httpAcknowledged) {
        console.error("DATABASE/PERSISTENCE FAILURE AFTER HTTP SUCCESS:", {
          eventId: event.event_id,
          attempt: job.attemptsMade + 1,
          httpStatus: status_code,
          message: error.message,
        });
      } else {
        console.error("HTTP DELIVERY FAILURE:", {
          eventId: event.event_id,
          attempt: job.attemptsMade + 1,
          httpStatus: status_code,
          message: error.message,
        });
      }
      await pool.query(
        `UPDATE webhook_delivery_attempts
         SET status = $1,
         status_code =$2,
         error = $3,
         finished_at = $4
         WHERE id = $5`,
        [
          "failed",
          status_code,
          error.message,
          new Date(),
          attemptResult.rows[0].id
        ]
      );
      const currentAttempt = job.attemptsMade + 1;
      const maxAttempt = job.opts.attempts;

      if (currentAttempt === maxAttempt || error instanceof UnrecoverableError) {
        await pool.query(
          `UPDATE deliveries
           SET status = $1,
           attempt = $2
           WHERE event_id = $3`,
          [
            "failed",
            currentAttempt,
            event.event_id
          ]
        );
      }

      throw error;

    }


  },
  {
    connection: {
      host: "127.0.0.1",
      port: 6379,
    },
    concurrency: 5,
  },
);

worker.on("failed", (job, err) => {
  console.log("JOB FAILED:", job.id);
  console.log("ERROR:", err.message);
})