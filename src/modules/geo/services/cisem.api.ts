import type { FeatureCollection } from "../types/geo";

type CisemEstado =
  | number
  | {
      nombre?: string;
      descripcion?: string;
      color_hex?: string;
      color_nombre?: string;
      requiere_tecnico?: number;
    }
  | null;

type CisemCuadrante = {
  id_cuadrante?: number;
  nombre?: string;
  descripcion?: string;
  creado?: string | null;
  actualizado?: string | null;
  creado_por?: number | null;
  modificado_por?: number | null;
  eliminado?: number;
} | null;

type CisemDepartamento = {
  nombre?: string;
  id_cuadrante?: CisemCuadrante;
  creado?: string | null;
  actualizado?: string | null;
  creado_por?: number | null;
  modificado_por?: number | null;
  eliminado?: number;
} | null;

export type CisemCameraRecord = {
  id_camara: number;
  nro_camara?: string | null;
  codigo_busqueda?: string | null;
  id_departamento?: CisemDepartamento;
  calle?: string | null;
  id_estado?: CisemEstado;
  tipo?: string | null;
  ip?: string | null;
  mac?: string | null;
  onu?: string | null;
  poe?: string | null;
  ups?: string | null;
  fuente?: string | null;
  contrato?: string | null;
  proyecto?: string | null;
  nro_serie?: string | null;
  modelo?: string | null;
  latitud?: string | number | null;
  longitud?: string | number | null;
  descripcion?: string | null;
  creado?: string | null;
  actualizado?: string | null;
  creado_por?: number | null;
  modificado_por?: number | null;
  eliminado?: number;
  [key: string]: unknown;
};

type CisemApiResponse = {
  message?: string;
  data?: CisemCameraRecord[];
  pagination?: {
    totalRecords?: number;
    totalPages?: number;
    currentPage?: number;
    limit?: number;
  };
  includes?: string[];
  cached?: boolean;
};

export type FetchCisemCamerasParams = {
  page?: number;
  limit?: number;
  sort?: "asc" | "desc";
  include?: string[];
  fields?: string[];
};

const API_BASE = (import.meta.env.VITE_CISEM_API_BASE || "").replace(/\/+$/, "");
const API_KEY = import.meta.env.VITE_CISEM_API_KEY || "";

function assertConfig() {
  if (!API_BASE) {
    throw new Error("Falta VITE_CISEM_API_BASE en el .env");
  }

  if (!API_KEY) {
    throw new Error("Falta VITE_CISEM_API_KEY en el .env");
  }
}

function buildUrl(path: string, params?: FetchCisemCamerasParams) {
  const url = new URL(`${API_BASE}${path}`);

  if (params?.page) {
    url.searchParams.set("page", String(params.page));
  }

  if (params?.limit) {
    url.searchParams.set("limit", String(params.limit));
  }

  if (params?.sort) {
    url.searchParams.set("sort", params.sort);
  }

  if (params?.include?.length) {
    url.searchParams.set("include", params.include.join(","));
  }

  if (params?.fields?.length) {
    url.searchParams.set("fields", params.fields.join(","));
  }

  return url.toString();
}

function toNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;

  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function normalizeTipo(tipo?: string | null) {
  const raw = String(tipo || "").trim().toLowerCase();

  if (!raw) return "camera";
  if (raw.includes("domo")) return "camera_dome";
  if (raw.includes("ptz")) return "camera_ptz";
  if (raw.includes("fija")) return "camera_fixed";

  return "camera";
}

function normalizeEstadoNombre(estado: CisemEstado): string {
  if (!estado) return "Sin estado";
  if (typeof estado === "number") return `Estado ${estado}`;
  return estado.nombre?.trim() || "Sin estado";
}

function normalizeEstadoColor(estado: CisemEstado): string | null {
  if (!estado || typeof estado === "number") return null;
  return estado.color_hex?.trim() || null;
}

function cameraRecordToFeature(camera: CisemCameraRecord) {
  const lat = toNumber(camera.latitud);
  const lng = toNumber(camera.longitud);

  if (lat === null || lng === null) {
    return null;
  }

  const estadoNombre = normalizeEstadoNombre(camera.id_estado ?? null);
  const estadoColor = normalizeEstadoColor(camera.id_estado ?? null);
  const departamento = camera.id_departamento?.nombre?.trim() || null;
  const cuadrante = camera.id_departamento?.id_cuadrante?.nombre?.trim() || null;

  return {
    type: "Feature",
    geometry: {
      type: "Point",
      coordinates: [lng, lat]
    },
    properties: {
      id: `cisem-camera-${camera.id_camara}`,
      source: "cisem",
      sourceId: camera.id_camara,
      layerCode: "cameras",
      type: normalizeTipo(camera.tipo),
      title:
        camera.codigo_busqueda?.trim() ||
        camera.nro_camara?.trim() ||
        `Cámara ${camera.id_camara}`,
      subtitle: camera.calle?.trim() || null,
      code: camera.codigo_busqueda?.trim() || null,
      cameraNumber: camera.nro_camara?.trim() || null,
      address: camera.calle?.trim() || null,
      cameraType: camera.tipo?.trim() || null,
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

export async function fetchCisemCameras(
  params: FetchCisemCamerasParams = {}
): Promise<CisemApiResponse> {
  assertConfig();

  const url = buildUrl("/api/v1/cisem/camaras", {
    page: params.page ?? 1,
    limit: params.limit ?? 1000,
    sort: params.sort ?? "asc",
    include:
      params.include ?? ["id_departamento.id_cuadrante", "id_estado"],
    fields: params.fields
  });

  const response = await fetch(url, {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": API_KEY
    }
  });

  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(
      `Error consultando CISem (${response.status} ${response.statusText}) ${text}`.trim()
    );
  }

  const json = (await response.json()) as CisemApiResponse;
  return json;
}

export async function fetchAllCisemCameras(
  pageSize = 1000
): Promise<CisemCameraRecord[]> {
  const firstPage = await fetchCisemCameras({
    page: 1,
    limit: pageSize,
    sort: "asc",
    include: ["id_departamento.id_cuadrante", "id_estado"]
  });

  const firstData = Array.isArray(firstPage.data) ? firstPage.data : [];
  const totalPages = Number(firstPage.pagination?.totalPages || 1);

  if (totalPages <= 1) {
    return firstData;
  }

  const pages = Array.from({ length: totalPages - 1 }, (_, index) => index + 2);

  const results = await Promise.all(
    pages.map((page) =>
      fetchCisemCameras({
        page,
        limit: pageSize,
        sort: "asc",
        include: ["id_departamento.id_cuadrante", "id_estado"]
      })
    )
  );

  const all = [...firstData];

  for (const result of results) {
    if (Array.isArray(result.data)) {
      all.push(...result.data);
    }
  }

  return all;
}

export async function fetchCisemCamerasAsFeatureCollection(): Promise<FeatureCollection> {
  const records = await fetchAllCisemCameras(1000);

  const features = records
    .map(cameraRecordToFeature)
    .filter(Boolean) as FeatureCollection["features"];

  return {
    type: "FeatureCollection",
    features
  };
}