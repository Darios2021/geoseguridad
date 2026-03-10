import multer from "multer";
import { asyncHandler } from "../../../utils/asyncHandler.js";
import {
  importKmzToDatabase,
  previewKmzImport
} from "../services/geo-import.service.js";
import { getFeatures, getHealth, getLayers } from "../services/geo.service.js";

const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 50 * 1024 * 1024
  }
});

export const importKmzMiddleware = upload.single("file");

export const health = asyncHandler(async (req, res) => {
  const data = await getHealth();
  return res.json({ ok: true, data });
});

export const listLayers = asyncHandler(async (req, res) => {
  const data = await getLayers();
  return res.json({ ok: true, data });
});

export const listFeatures = asyncHandler(async (req, res) => {
  const data = await getFeatures({
    layer: req.query.layer,
    department: req.query.department,
    dependency: req.query.dependency,
    status: req.query.status,
    limit: req.query.limit
  });

  return res.json({
    ok: true,
    type: "FeatureCollection",
    count: data.length,
    features: data
  });
});

export const importKmzPreview = asyncHandler(async (req, res) => {
  if (!req.file) {
    return res.status(400).json({
      ok: false,
      message: "Debes enviar un archivo KML/KMZ en el campo 'file'."
    });
  }

  const replaceExisting =
    String(req.body.replaceExisting ?? "true").toLowerCase() !== "false";

  const importProfile =
    req.body?.importProfile && String(req.body.importProfile).trim()
      ? String(req.body.importProfile).trim()
      : null;

  const result = await previewKmzImport({
    buffer: req.file.buffer,
    filename: req.file.originalname,
    replaceExisting,
    importProfile
  });

  return res.json(result);
});

export const importKmz = asyncHandler(async (req, res) => {
  if (!req.file) {
    return res.status(400).json({
      ok: false,
      message: "Debes enviar un archivo KML/KMZ en el campo 'file'."
    });
  }

  const replaceExisting =
    String(req.body.replaceExisting ?? "true").toLowerCase() !== "false";

  const importProfile =
    req.body?.importProfile && String(req.body.importProfile).trim()
      ? String(req.body.importProfile).trim()
      : null;

  const result = await importKmzToDatabase({
    buffer: req.file.buffer,
    filename: req.file.originalname,
    replaceExisting,
    importProfile
  });

  return res.json(result);
});