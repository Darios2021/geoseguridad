import {
  fetchCisemCameras,
  fetchAllCisemCameras,
  fetchCisemCamerasAsFeatureCollection
} from "../services/cisem.api.js";

function parseCsvParam(value, fallback = []) {
  if (!value) return fallback;
  return String(value)
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function parseNumberParam(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

export async function getCisemCameras(req, res, next) {
  try {
    const page = parseNumberParam(req.query.page, 1);
    const limit = parseNumberParam(req.query.limit, 500);
    const sort = req.query.sort === "desc" ? "desc" : "asc";
    const include = parseCsvParam(req.query.include, [
      "id_departamento.id_cuadrante",
      "id_estado"
    ]);
    const fields = parseCsvParam(req.query.fields);

    const data = await fetchCisemCameras({
      page,
      limit,
      sort,
      include,
      fields
    });

    res.json({
      ok: true,
      source: "cisem",
      ...data
    });
  } catch (error) {
    next(error);
  }
}

export async function getAllCisemCameras(req, res, next) {
  try {
    const limit = parseNumberParam(req.query.limit, 500);
    const sort = req.query.sort === "desc" ? "desc" : "asc";
    const include = parseCsvParam(req.query.include, [
      "id_departamento.id_cuadrante",
      "id_estado"
    ]);
    const fields = parseCsvParam(req.query.fields);

    const rows = await fetchAllCisemCameras({
      limit,
      sort,
      include,
      fields
    });

    res.json({
      ok: true,
      source: "cisem",
      count: rows.length,
      data: rows
    });
  } catch (error) {
    next(error);
  }
}

export async function getCisemCamerasGeoJson(req, res, next) {
  try {
    const limit = parseNumberParam(req.query.limit, 500);
    const sort = req.query.sort === "desc" ? "desc" : "asc";
    const include = parseCsvParam(req.query.include, [
      "id_departamento.id_cuadrante",
      "id_estado"
    ]);
    const fields = parseCsvParam(req.query.fields);

    const featureCollection = await fetchCisemCamerasAsFeatureCollection({
      limit,
      sort,
      include,
      fields
    });

    res.json(featureCollection);
  } catch (error) {
    next(error);
  }
}