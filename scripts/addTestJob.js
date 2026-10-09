import queue from "../src/queues/webhookQueue.js";

const job = await queue.add("deliver-webhook",{
  eventId:13
})

console.log(job)

await queue.close();