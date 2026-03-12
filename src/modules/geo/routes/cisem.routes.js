import { Router } from "express";
import {
  getAllCisemCameras,
  getCisemCameras,
  getCisemCamerasGeoJson
} from "../controllers/cisem.controller.js";

const router = Router();

router.get("/camaras", getCisemCameras);
router.get("/camaras/all", getAllCisemCameras);
router.get("/camaras/geojson", getCisemCamerasGeoJson);

export default router;