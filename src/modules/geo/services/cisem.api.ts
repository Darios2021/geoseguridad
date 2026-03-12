const API_BASE = String(process.env.CISEM_API_BASE || "").replace(/\/+$/, "");
const API_KEY = String(process.env.CISEM_API_KEY || "").trim();

function assertConfig() {
  if (!API_BASE) throw new Error("CISEM_API_BASE no configurado");
  if (!API_KEY) throw new Error("CISEM_API_KEY no configurado");
}

async function fetchCisemPage(page = 1, limit = 500) {
  assertConfig();

  const url = new URL(`${API_BASE}/api/v1/cisem/camaras`);
  url.searchParams.set("include", "id_departamento.id_cuadrante,id_estado");
  url.searchParams.set("page", page);
  url.searchParams.set("limit", limit);
  url.searchParams.set("sort", "asc");

  const response = await fetch(url.toString(), {
    headers: {
      "x-api-key": API_KEY,
      Accept: "application/json"
    }
  });

  if (!response.ok) {
    const txt = await response.text();
    throw new Error(`CISEM ${response.status} ${txt}`);
  }

  return response.json();
}

function toNumber(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function normalizeTipo(tipo) {
  const t = String(tipo || "").toLowerCase();
  if (t.includes("domo")) return "camera_dome";
  if (t.includes("ptz")) return "camera_ptz";
  if (t.includes("fija")) return "camera_fixed";
  return "camera";
}

function recordToFeature(cam) {
  const lat = toNumber(cam.latitud);
  const lng = toNumber(cam.longitud);

  if (lat === null || lng === null) return null;

  return {
    type: "Feature",
    id: `cisem-${cam.id_camara}`,
    geometry: {
      type: "Point",
      coordinates: [lng, lat]
    },
    properties: {
      id: `cisem-${cam.id_camara}`,
      source: "cisem",
      layerCode: "camaras",
      title: cam.codigo_busqueda || `Cam ${cam.nro_camara}`,
      subtitle: cam.calle || "",
      department: cam.id_departamento?.nombre || null,
      quadrant: cam.id_departamento?.id_cuadrante?.nombre || null,
      status: cam.id_estado?.nombre || null,
      statusColor: cam.id_estado?.color_hex || null,
      tipo: normalizeTipo(cam.tipo),
      raw: cam
    }
  };
}

export async function fetchCisemGeoJSON() {
  const first = await fetchCisemPage(1, 500);

  const rows = [...(first.data || [])];
  const pages = first.pagination?.totalPages || 1;

  if (pages > 1) {
    const jobs = [];

    for (let i = 2; i <= pages; i++) {
      jobs.push(fetchCisemPage(i, 500));
    }

    const results = await Promise.all(jobs);

    for (const r of results) {
      rows.push(...(r.data || []));
    }
  }

  const features = rows
    .map(recordToFeature)
    .filter(Boolean);

  return {
    type: "FeatureCollection",
    features
  };
}