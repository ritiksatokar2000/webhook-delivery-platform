import express from "express";
import pool from "./src/config/db.js";
import queue from "./src/queues/webhookQueue.js"

const app = express();

app.use(express.json());

app.get("/health", (req, res) => {
  res.status(200).json({
    status: "ok",
  });
});

app.post("/api/events", async (req, res) => {
 try {
   const { endpointId, type, data } = req.body;
 
   if (!type || !data || !endpointId) {
     return res.status(400).json({
       success: false,
       message: "endpointId, type and data are required",
     });
   }
 
   const result = await pool.query(
     `INSERT INTO events (endpoint_id, type, data) VALUES($1,$2,$3) 
     RETURNING *`,
     [endpointId, type, data],
   );
 
   await pool.query(`INSERT INTO deliveries (event_id,attempt,status) VALUES($1,$2,$3) RETURNING *`, [result.rows[0].id, 0, "pending"])
 
   const job = await queue.add("deliver-webhook", {
     eventId: result.rows[0].id
   }, {
     attempts: 4,
     backoff: {
       type: "exponential",
       delay: 5000,
     }
   })
 
   console.log(type);
   console.log(data);
 
   res.status(201).json({
     success: true,
     message: "Event received",
     data: result.rows[0],
   });
 } catch (error) {
  console.error("EVENT CREATION FAILED:",error);

  return res.status(500).json({
    success:false,
    message:"Internal server error",
  })
  
 }
});


app.get("/api/events/:eventId/delivery",async(req,res) =>{
  try {
    const {eventId} =req.params;
    const parseId = parseInt(eventId,10);
  
    if(Number.isNaN(parseId)){
       return res.status(400).json({
      success: false,
      message: "Invalid event ID"})
    }
  
    if(parseId.toString() !==eventId){
      return res.status(400).json({
      success: false,
      message: "Invalid event ID"})
    }
  
    const eventResult = await pool.query(`SELECT * FROM events WHERE id = $1`,[parseId])
  
    if(eventResult.rows.length === 0){
      return res.status(404).json({
          success: false,
          message: "Event not found"
      });
    }
  
    const deliveryResult = await pool.query(
      `SELECT * FROM deliveries WHERE event_id =$1`,[parseId]
    )
  
    const attemptResult = await pool.query(
      `SELECT * FROM webhook_delivery_attempts WHERE event_id =$1 ORDER BY attempt`,[parseId]
    )
  
    return res.status(200).json({
      success:true,
        "event":eventResult.rows[0],
        "delivery":deliveryResult.rows[0],
        "attempts":attemptResult.rows
    })
  } catch (error) {
    console.error("Error",error)
    return res.status(500).json({
      success:false,
      message:"Internal server error"
    })
  }
})

export default app;
