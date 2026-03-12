import { Router } from "express";
import { getCisemCamerasGeoJSON } from "../controllers/cisem.controller.js";

const router = Router();

router.get("/camaras/geojson", getCisemCamerasGeoJSON);

export default router;