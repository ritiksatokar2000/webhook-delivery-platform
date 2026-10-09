import "dotenv/config";
import express from "express"
import crypto from "node:crypto";

const app = express()
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET;

app.use(express.json({
  verify: (req, res, buf) => {
    req.rawBody = Buffer.from(buf);
  }
}))

app.post("/webhook", async (req, res) => {
  const receivedSignature = req.get("X-Webhook-Signature");

  if (!WEBHOOK_SECRET) {
    console.error("Webhook secret is not configured");
    return res.status(500).json({
      success: false,
      message: "Webhook verification is not configured",
    });
  }

  const expectedSignature = crypto
    .createHmac("sha256", WEBHOOK_SECRET)
    .update(req.rawBody)
    .digest("hex");

  const receivedBuffer = Buffer.from(receivedSignature || "", "hex");
  const expectedBuffer = Buffer.from(expectedSignature, "hex");

  const isValid =
    receivedSignature?.length === expectedSignature.length &&
    receivedBuffer.length === expectedBuffer.length &&
    crypto.timingSafeEqual(receivedBuffer, expectedBuffer);

  if (!isValid) {
    console.log("INVALID WEBHOOK SIGNATURE");

    return res.status(401).json({
      success: false,
      message: "Invalid webhook signature",
    });
  }

  console.log("WEBHOOK SIGNATURE VERIFIED");
  console.log("Received event:", req.body);

  return res.status(200).json({
    success: true,
    message: "Webhook received and verified",
  });
});

const PORT = 4000;
app.listen(PORT, () => {
  console.log(`server is running on 4000`)
})