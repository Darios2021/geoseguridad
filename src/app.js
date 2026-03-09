import express from "express";
import cors from "cors";
import helmet from "helmet";
import morgan from "morgan";
import geoRoutes from "./modules/geo/routes/geo.routes.js";
import { errorHandler, notFoundHandler } from "./middlewares/errorHandler.js";

const app = express();

app.use(helmet());
app.use(cors());
app.use(morgan("dev"));
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true }));

app.get("/", (req, res) => {
  res.json({
    ok: true,
    app: "GeoSeguridad Backend",
    version: "1.0.0"
  });
});

app.use("/api/geo", geoRoutes);

app.use(notFoundHandler);
app.use(errorHandler);

export default app;
