import { Queue } from "bullmq";

const queue = new Queue("webhook-queue",{
  connection:{
    host:"127.0.0.1",
    port:6379,
  }
})

export default queue;