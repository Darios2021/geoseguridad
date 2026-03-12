import fetch from "node-fetch";
const DEFAULT_INCLUDE = ["id_departamento.id_cuadrante", "id_estado"];

function getConfig() {
  const base = String(process.env.CISEM_API_BASE || "")
    .trim()
    .replace(/\/+$/, "");
  const apiKey = String(process.env.CISEM_API_KEY || "").trim();

  if (!base) {
    throw new Error("Falta configurar CISEM_API_BASE en el .env");
  }

  if (!apiKey) {
    throw new Error("Falta configurar CISEM_API_KEY en el .env");
  }

  return { base, apiKey };
}

function buildUrl(path, params = {}) {
  const { base } = getConfig();
  const url = new URL(`${base}${path}`);

  if (params.page) {
    url.searchParams.set("page", String(params.page));
  }

  if (params.limit) {
    url.searchParams.set("limit", String(params.limit));
  }

  if (params.sort) {
    url.searchParams.set("sort", String(params.sort));
  }

  if (Array.isArray(params.include) && params.include.length) {
    url.searchParams.set("include", params.include.join(","));
  }

  if (Array.isArray(params.fields) && params.fields.length) {
    url.searchParams.set("fields", params.fields.join(","));
  }

  return url.toString();
}

async function fetchCisemCameras(params = {}) {
  const { apiKey } = getConfig();

  const url = buildUrl("/api/v1/cisem/camaras", {
    page: params.page ?? 1,
    limit: params.limit ?? 500,
    sort: params.sort ?? "asc",
    include: params.include ?? DEFAULT_INCLUDE,
    fields: params.fields ?? undefined
  });

console.log("CISEM REQUEST:", url);

const response = await fetch(url, {
  method: "GET",
  headers: {
    Accept: "application/json",
    "x-api-key": apiKey
  }
});

console.log("CISEM STATUS:", response.status);

  const text = await response.text();

  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = null;
  }

  if (!response.ok) {
    const detail = json ? JSON.stringify(json) : text;
    throw new Error(
      `Error CISem ${response.status} ${response.statusText}${detail ? ` - ${detail}` : ""}`
    );
  }

  return json || {};
}

async function fetchAllCisemCameras(params = {}) {
  const pageSize = Math.min(Number(params.limit || 500), 1000);

  const firstPage = await fetchCisemCameras({
    page: 1,
    limit: pageSize,
    sort: params.sort ?? "asc",
    include: params.include ?? DEFAULT_INCLUDE,
    fields: params.fields
  });

  const firstRows = Array.isArray(firstPage.data) ? firstPage.data : [];
  const totalPages = Number(firstPage.pagination?.totalPages || 1);

  if (totalPages <= 1) {
    return firstRows;
  }

  const pages = Array.from({ length: totalPages - 1 }, (_, i) => i + 2);

  const results = await Promise.all(
    pages.map((page) =>
      fetchCisemCameras({
        page,
        limit: pageSize,
        sort: params.sort ?? "asc",
        include: params.include ?? DEFAULT_INCLUDE,
        fields: params.fields
      })
    )
  );

  const rows = [...firstRows];

  for (const result of results) {
    if (Array.isArray(result.data)) {
      rows.push(...result.data);
    }
  }

  return rows;
}

function toNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function normalizeTipo(tipo) {
  const raw = String(tipo || "").trim().toLowerCase();

  if (!raw) return "camera";
  if (raw.includes("domo")) return "camera_dome";
  if (raw.includes("ptz")) return "camera_ptz";
  if (raw.includes("fija")) return "camera_fixed";

  return "camera";
}

function normalizeEstadoNombre(estado) {
  if (!estado) return "Sin estado";
  if (typeof estado === "number") return `Estado ${estado}`;
  return String(estado.nombre || "").trim() || "Sin estado";
}

function normalizeEstadoColor(estado) {
  if (!estado || typeof estado === "number") return null;
  return String(estado.color_hex || "").trim() || null;
}

function cameraRecordToFeature(camera) {
  const lat = toNumber(camera.latitud);
  const lng = toNumber(camera.longitud);

  if (lat === null || lng === null) {
    return null;
  }

  const estadoNombre = normalizeEstadoNombre(camera.id_estado ?? null);
  const estadoColor = normalizeEstadoColor(camera.id_estado ?? null);
  const departamento = camera.id_departamento?.nombre?.trim() || null;
  const cuadrante = camera.id_departamento?.id_cuadrante?.nombre?.trim() || null;
  const title =
    String(camera.codigo_busqueda || "").trim() ||
    String(camera.nro_camara || "").trim() ||
    `Cámara ${camera.id_camara}`;

  return {
    type: "Feature",
    id: `cisem-camera-${camera.id_camara}`,
    geometry: {
      type: "Point",
      coordinates: [lng, lat]
    },
    properties: {
      id: `cisem-camera-${camera.id_camara}`,
      source: "cisem",
      sourceId: camera.id_camara,
      layerCode: "camaras",
      featureType: normalizeTipo(camera.tipo),
      title,
      name: title,
      subtitle: String(camera.calle || "").trim() || null,
      code: String(camera.codigo_busqueda || "").trim() || null,
      cameraNumber: String(camera.nro_camara || "").trim() || null,
      address: String(camera.calle || "").trim() || null,
      cameraType: String(camera.tipo || "").trim() || null,
      status: estadoNombre,
      statusColor: estadoColor,
      department: departamento,
      quadrant: cuadrante,
      ip: camera.ip ?? null,
      mac: camera.mac ?? null,
      onu: camera.onu ?? null,
      poe: camera.poe ?? null,
      ups: camera.ups ?? null,
      fuente: camera.fuente ?? null,
      contrato: camera.contrato ?? null,
      proyecto: camera.proyecto ?? null,
      serialNumber: camera.nro_serie ?? null,
      model: camera.modelo ?? null,
      description: camera.descripcion ?? null,
      createdAt: camera.creado ?? null,
      updatedAt: camera.actualizado ?? null,
      raw: camera
    }
  };
}

async function fetchCisemCamerasAsFeatureCollection(params = {}) {
  const records = await fetchAllCisemCameras(params);

  const features = records.map(cameraRecordToFeature).filter(Boolean);

  return {
    type: "FeatureCollection",
    features
  };
}

export {
  fetchCisemCameras,
  fetchAllCisemCameras,
  fetchCisemCamerasAsFeatureCollection
};