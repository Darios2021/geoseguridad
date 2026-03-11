import multer from "multer";
import { asyncHandler } from "../../../utils/asyncHandler.js";
import {
  previewCameraCatalogImport,
  importCameraCatalogToDatabase
} from "../services/geo-camera-catalog-import.service.js";

const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 20 * 1024 * 1024
  }
});

export const importCameraCatalogMiddleware = upload.single("file");

export const previewCameraCatalog = asyncHandler(async (req, res) => {
  console.log("CSV PREVIEW req.file:", {
    exists: !!req.file,
    fieldname: req.file?.fieldname,
    originalname: req.file?.originalname,
    mimetype: req.file?.mimetype,
    size: req.file?.size
  });

  if (!req.file) {
    return res.status(400).json({
      ok: false,
      message: "Debes enviar un archivo CSV en el campo 'file'."
    });
  }

  const result = await previewCameraCatalogImport({
    buffer: req.file.buffer,
    filename: req.file.originalname || "camaras.csv"
  });

  return res.json(result);
});

export const importCameraCatalog = asyncHandler(async (req, res) => {
  console.log("CSV IMPORT req.file:", {
    exists: !!req.file,
    fieldname: req.file?.fieldname,
    originalname: req.file?.originalname,
    mimetype: req.file?.mimetype,
    size: req.file?.size
  });

  if (!req.file) {
    return res.status(400).json({
      ok: false,
      message: "Debes enviar un archivo CSV en el campo 'file'."
    });
  }

  const replaceExisting =
    String(req.body?.replaceExisting ?? "true").toLowerCase() !== "false";

  const result = await importCameraCatalogToDatabase({
    buffer: req.file.buffer,
    filename: req.file.originalname || "camaras.csv",
    replaceExisting
  });

  return res.json(result);
});