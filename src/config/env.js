import dotenv from "dotenv";
dotenv.config();

export const env = {
  port: Number(process.env.PORT || 3007),
  nodeEnv: process.env.NODE_ENV || "development",
  db: {
    host: process.env.DB_HOST || "localhost",
    port: Number(process.env.DB_PORT || 5432),
    database: process.env.DB_NAME || "postgres",
    user: process.env.DB_USER || "postgres",
    password: process.env.DB_PASSWORD || "",
    ssl: String(process.env.DB_SSL || "false") === "true"
      ? { rejectUnauthorized: false }
      : false
  }
};
