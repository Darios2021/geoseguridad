import { Router } from "express";
import { health, listFeatures, listLayers } from "../controllers/geo.controller.js";

const router = Router();

router.get("/health", health);
router.get("/layers", listLayers);
router.get("/features", listFeatures);

export default router;
