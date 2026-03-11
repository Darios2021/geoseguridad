import { Router } from "express";
import {
  geoTree,
  health,
  importKmz,
  importKmzPreview,
  importKmzMiddleware,
  listFeatures,
  listLayers
} from "../controllers/geo.controller.js";

import {
  importCameraCatalog,
  importCameraCatalogMiddleware,
  previewCameraCatalog
} from "../controllers/geo-camera-catalog-import.controller.js";

const router = Router();

router.get("/health", health);
router.get("/layers", listLayers);
router.get("/features", listFeatures);
router.get("/tree", geoTree);

router.post("/import/preview", importKmzMiddleware, importKmzPreview);
router.post("/import/kmz", importKmzMiddleware, importKmz);

router.post(
  "/import/cameras-csv/preview",
  importCameraCatalogMiddleware,
  previewCameraCatalog
);

router.post(
  "/import/cameras-csv",
  importCameraCatalogMiddleware,
  importCameraCatalog
);

export default router;