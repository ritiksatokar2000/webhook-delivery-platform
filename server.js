import "dotenv/config"

import app from "./app.js";
import pool from "./src/config/db.js";

const PORT = 3000;

const startServer = async () => {
  try {
    const result = await pool.query("SELECT NOW()");
    console.log(result.rows);
    app.listen(PORT, () => {
      console.log(`Server is connected to the port ${PORT}`);
    });
  } catch (error) {
    console.error(error);
  }
};

startServer();
