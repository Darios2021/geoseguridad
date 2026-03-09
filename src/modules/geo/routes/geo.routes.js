import { Router } from "express";
import {
  health,
  importKmz,
  importKmzMiddleware,
  listFeatures,
  listLayers
} from "../controllers/geo.controller.js";

const router = Router();

router.get("/health", health);
router.get("/layers", listLayers);
router.get("/features", listFeatures);
router.post("/import/kmz", importKmzMiddleware, importKmz);

export default router;