import multer from "multer";
import { asyncHandler } from "../../../utils/asyncHandler.js";
import {
  importKmzToDatabase,
  previewKmzImport
} from "../services/geo-import.service.js";
import {
  getFeatures,
  getGeoTree,
  getHealth,
  getLayers
} from "../services/geo.service.js";

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

export async function getGeoTree(filters = {}) {
  const values = [];
  let where = "";
  let idx = 1;

  if (filters.status) {
    where += ` AND f.status = $${idx++}`;
    values.push(filters.status);
  }

  const sql = `
    SELECT
      f.id,
      l.code AS layer_code,
      f.code,
      f.name,
      f.department_name,
      f.dependency_name,
      f.jurisdiction_name,
      f.feature_type
    FROM geo_features f
    INNER JOIN geo_layers l ON l.id = f.layer_id
    WHERE f.is_active = TRUE
    ${where}
    ORDER BY
      f.department_name,
      f.dependency_name,
      f.jurisdiction_name,
      l.code,
      f.name
  `;

  const { rows } = await pool.query(sql, values);

  const departments = new Map();

  const ensureDepartment = (name) => {
    const key = name || "SIN DEPARTAMENTAL";

    if (!departments.has(key)) {
      departments.set(key, {
        id: `department:${key}`,
        type: "department",
        name: key,
        children: new Map()
      });
    }

    return departments.get(key);
  };

  const ensureDependency = (dept, name) => {
    const key = name || "SIN DEPENDENCIA";

    if (!dept.children.has(key)) {
      dept.children.set(key, {
        id: `dependency:${dept.name}:${key}`,
        type: "dependency",
        name: key,
        children: new Map()
      });
    }

    return dept.children.get(key);
  };

  const ensureJurisdiction = (dep, name) => {
    const key = name || "JURISDICCION";

    if (!dep.children.has(key)) {
      dep.children.set(key, {
        id: `jurisdiction:${dep.id}:${key}`,
        type: "jurisdiction",
        name: key,
        children: {
          cuadrantes: [],
          camaras: []
        }
      });
    }

    return dep.children.get(key);
  };

  for (const row of rows) {
    const dept = ensureDepartment(row.department_name);
    const dep = ensureDependency(dept, row.dependency_name);
    const jur = ensureJurisdiction(dep, row.jurisdiction_name);

    const feature = {
      id: row.id,
      name: row.name,
      layerCode: row.layer_code,
      code: row.code,
      featureType: row.feature_type
    };

    if (row.layer_code === "cuadrantes") {
      jur.children.cuadrantes.push({
        id: `quadrant:${row.id}`,
        type: "quadrant",
        name: row.name,
        feature
      });
    }

    if (row.layer_code === "camaras") {
      jur.children.camaras.push({
        id: `camera:${row.id}`,
        type: "camera",
        name: row.name,
        feature
      });
    }
  }

  return [...departments.values()].map((dept) => ({
    ...dept,
    children: [...dept.children.values()].map((dep) => ({
      ...dep,
      children: [...dep.children.values()].map((jur) => ({
        id: jur.id,
        type: jur.type,
        name: jur.name,
        children: [
          {
            id: `${jur.id}:cuadrantes`,
            type: "group",
            name: "Cuadrantes",
            children: jur.children.cuadrantes
          },
          {
            id: `${jur.id}:camaras`,
            type: "group",
            name: "Cámaras",
            children: jur.children.camaras
          }
        ]
      }))
    }))
  }));
}

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