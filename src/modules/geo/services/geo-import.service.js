import AdmZip from "adm-zip";
import { XMLParser } from "fast-xml-parser";
import { pool } from "../../../config/db.js";

function asArray(value) {
  if (!value) return [];
  return Array.isArray(value) ? value : [value];
}

function textValue(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") return value.trim();
  if (typeof value === "number") return String(value);
  if (typeof value === "object" && "#text" in value) {
    return String(value["#text"]).trim();
  }
  return String(value).trim();
}

function normalizeWhitespace(value) {
  return String(value || "")
    .replace(/\u00A0/g, " ")
    .replace(/\u200B/g, "")
    .replace(/\u200C/g, "")
    .replace(/\u200D/g, "")
    .replace(/\uFEFF/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeName(value) {
  const clean = normalizeWhitespace(textValue(value) || "");
  return clean || null;
}

function normalizeDepartmentName(value) {
  const raw = normalizeName(value);
  if (!raw) return null;

  const upper = raw.toUpperCase();
  const match = upper.match(/DEPARTAMENTAL\s*(?:N[º°O]\s*)?(\d+)/i);
  if (!match) return upper;

  return `DEPARTAMENTAL Nº ${Number(match[1])}`;
}

function normalizeDependencyName(value) {
  const raw = normalizeName(value);
  if (!raw) return null;

  let upper = raw.toUpperCase();

  upper = upper
    .replace(/\bCRIA\.?\b/g, "COMISARIA")
    .replace(/\bCRÍA\.?\b/g, "COMISARIA")
    .replace(/\bCOMISAR[IÍ]A\b/g, "COMISARIA")
    .replace(/\bSUBCOMISAR[IÍ]A\b/g, "SUB COMISARIA")
    .replace(/\bSUB\.?\s*COMISAR[IÍ]A\b/g, "SUB COMISARIA")
    .replace(/\bSUB\.?\s*CRIA\.?\b/g, "SUB COMISARIA")
    .replace(/\bSUB\.?\s*CRÍA\.?\b/g, "SUB COMISARIA")
    .replace(/\bSTA\.?\b/g, "SANTA")
    .replace(/\bSTA\b/g, "SANTA");

  let match = upper.match(/\bCOMISARIA\.?\s*(\d+)\b/i);
  if (match) {
    return `COMISARIA ${String(Number(match[1])).padStart(2, "0")}`;
  }

  match = upper.match(/\bSUB COMISARIA\.?\s*(.+)$/i);
  if (match) {
    return `SUB COMISARIA ${normalizeWhitespace(match[1]).toUpperCase()}`;
  }

  return normalizeWhitespace(upper);
}

function normalizeJurisdictionName(value, fallbackDependencyName = null) {
  const raw = normalizeName(value);
  if (!raw) return fallbackDependencyName || null;

  const normalizedDependency = normalizeDependencyName(raw);

  if (/^(COMISARIA|SUB COMISARIA)\b/i.test(normalizedDependency || "")) {
    return normalizedDependency;
  }

  return normalizeWhitespace(raw).toUpperCase();
}

function slugify(value) {
  return normalizeWhitespace(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-zA-Z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toUpperCase();
}

function buildFeatureCode({
  layerCode,
  departmentName,
  dependencyName,
  jurisdictionName,
  name
}) {
  const parts = [layerCode];

  if (departmentName) parts.push(departmentName);
  if (dependencyName) parts.push(dependencyName);

  if (layerCode === "jurisdicciones" && jurisdictionName) {
    parts.push(jurisdictionName);
  } else if (name) {
    parts.push(name);
  }

  const code = parts.map(slugify).filter(Boolean).join("__");
  return code.length <= 120 ? code : code.slice(0, 120);
}

function parseCoordinatePair(pair) {
  if (!pair) return null;

  const parts = String(pair).trim().split(",");
  if (parts.length < 2) return null;

  const lon = Number(parts[0]);
  const lat = Number(parts[1]);

  if (!Number.isFinite(lon) || !Number.isFinite(lat)) return null;

  return [lon, lat];
}

function parseCoordinatesList(coordsText) {
  if (!coordsText) return [];

  return String(coordsText)
    .trim()
    .split(/\s+/)
    .map(parseCoordinatePair)
    .filter(Boolean);
}

function ensureClosedRing(coords) {
  if (!Array.isArray(coords) || coords.length < 3) return coords;

  const first = coords[0];
  const last = coords[coords.length - 1];

  if (!first || !last) return coords;
  if (first[0] === last[0] && first[1] === last[1]) return coords;

  return [...coords, first];
}

function pointGeometryFromPlacemark(placemark) {
  const coords = textValue(placemark?.Point?.coordinates);
  const pair = parseCoordinatePair(coords);
  if (!pair) return null;

  return {
    type: "Point",
    coordinates: pair
  };
}

function polygonGeometryFromPlacemark(placemark) {
  const outer =
    placemark?.Polygon?.outerBoundaryIs?.LinearRing?.coordinates ||
    placemark?.Polygon?.outerBoundaryIs?.coordinates;

  const outerCoords = ensureClosedRing(parseCoordinatesList(textValue(outer)));
  if (outerCoords.length < 4) return null;

  const rings = [outerCoords];
  const innerBoundaries = asArray(placemark?.Polygon?.innerBoundaryIs);

  for (const inner of innerBoundaries) {
    const innerCoords = ensureClosedRing(
      parseCoordinatesList(
        textValue(inner?.LinearRing?.coordinates || inner?.coordinates)
      )
    );
    if (innerCoords.length >= 4) {
      rings.push(innerCoords);
    }
  }

  return {
    type: "Polygon",
    coordinates: rings
  };
}

function linestringGeometryFromPlacemark(placemark) {
  const coords = parseCoordinatesList(textValue(placemark?.LineString?.coordinates));
  if (coords.length < 2) return null;

  return {
    type: "LineString",
    coordinates: coords
  };
}

function getGeometryFromPlacemark(placemark) {
  if (placemark?.Point) return pointGeometryFromPlacemark(placemark);
  if (placemark?.Polygon) return polygonGeometryFromPlacemark(placemark);
  if (placemark?.LineString) return linestringGeometryFromPlacemark(placemark);
  return null;
}

function folderNameLooksLikeDepartment(name) {
  return /departamental/i.test(name || "");
}

function folderNameLooksLikeCameras(name) {
  return /camaras?/i.test(name || "");
}

function looksLikeFileName(name) {
  const clean = normalizeWhitespace(name || "");
  return /\.(KML|KMZ)$/i.test(clean);
}

function looksLikeContainerFolder(name) {
  const clean = normalizeWhitespace(name || "").toUpperCase();

  if (!clean) return false;
  if (looksLikeFileName(clean)) return true;

  return (
    /\bCUADRANTE(S)?\b/i.test(clean) ||
    /\bCAMARA(S)?\b/i.test(clean) ||
    /\bJURISDICCION(ES)?\b/i.test(clean) ||
    /\bOPERATIVO\b/i.test(clean) ||
    /\bACTUALIZADO(S)?\b/i.test(clean) ||
    /\bAGOSTO\b|\bSEPTIEMBRE\b|\bOCTUBRE\b|\bNOVIEMBRE\b|\bDICIEMBRE\b|\bENERO\b|\bFEBRERO\b|\bMARZO\b|\bABRIL\b|\bMAYO\b|\bJUNIO\b|\bJULIO\b/i.test(
      clean
    )
  );
}

function folderNameLooksLikeDependency(name) {
  const clean = normalizeWhitespace(name || "").toUpperCase();

  if (!clean) return false;
  if (looksLikeContainerFolder(clean)) return false;

  return /\b(COMISARIA|COMISARÍA|CRIA|CRÍA|SUB\s*COMISARIA|SUB\s*COMISARÍA|SUB\s*CRIA|SUB\s*CRÍA|SECCIONAL|UNIDAD|DEPENDENCIA|BRIGADA)\b/i.test(
    clean
  );
}

function nameLooksLikeQuadrant(name) {
  return /^CUADRANTE\s+\d+\s*-\s*\d+$/i.test(normalizeWhitespace(name || ""));
}

function nameLooksLikeDepartment(name) {
  return /departamental/i.test(name || "");
}

function nameLooksLikeJurisdiction(name) {
  const clean = normalizeWhitespace(name || "").toUpperCase();
  if (!clean) return false;

  if (nameLooksLikeQuadrant(clean)) return false;
  if (nameLooksLikeDepartment(clean)) return false;

  return /\b(COMISARIA|COMISARÍA|CRIA|CRÍA|SUB\s*COMISARIA|SUB\s*COMISARÍA|SUB\s*CRIA|SUB\s*CRÍA)\b/i.test(
    clean
  );
}

function nameLooksLikeCamera(name) {
  const clean = normalizeWhitespace(name || "");
  return /^(C\d+|C-\d+|CAM\s*\d+|CAMARA\s*\d+)$/i.test(clean);
}

function inferQuadrantDependencyName(quadrantName) {
  const clean = normalizeWhitespace(quadrantName || "");
  const match = clean.match(/^CUADRANTE\s+(\d+)\s*-\s*\d+$/i);
  if (!match) return null;

  return `COMISARIA ${String(Number(match[1])).padStart(2, "0")}`;
}

function placemarkDescription(placemark) {
  return textValue(placemark?.description) || null;
}

function extractContext(folderPath) {
  let departmentName = null;
  let dependencyName = null;

  const normalizedPath = folderPath.map((part) => normalizeName(part)).filter(Boolean);

  for (const part of normalizedPath) {
    if (!departmentName && folderNameLooksLikeDepartment(part)) {
      departmentName = normalizeDepartmentName(part);
    }
  }

  for (let i = normalizedPath.length - 1; i >= 0; i -= 1) {
    const part = normalizedPath[i];

    if (!part) continue;
    if (looksLikeContainerFolder(part)) continue;

    if (folderNameLooksLikeDependency(part)) {
      dependencyName = normalizeDependencyName(part);
      break;
    }
  }

  return {
    departmentName,
    dependencyName
  };
}

function autoDetectImportProfile(filename = "") {
  const clean = normalizeWhitespace(filename).toLowerCase();

  if (/departamento(s|_)/.test(clean)) {
    return "departamentos";
  }

  if (/cuadrante|departamental/.test(clean)) {
    return "departamentales_cuadrantes";
  }

  if (/camara|cámara|jurisdic/.test(clean)) {
    return "operativo";
  }

  return "operativo";
}

function detectLayerCode({ profile, geometry, folderPath, placemarkName }) {
  const joinedPath = folderPath.join(" / ");
  const cleanName = normalizeWhitespace(placemarkName || "");

  if (profile === "departamentos") {
    if (geometry?.type !== "Polygon" && geometry?.type !== "MultiPolygon") {
      return null;
    }
    return "departamentos";
  }

  if (profile === "departamentales_cuadrantes") {
    if (geometry?.type !== "Polygon") return null;

    if (nameLooksLikeDepartment(cleanName)) {
      return "departamentales";
    }

    if (nameLooksLikeQuadrant(cleanName)) {
      return "cuadrantes";
    }

    if (nameLooksLikeJurisdiction(cleanName)) {
      return "jurisdicciones";
    }

    return null;
  }

  if (profile === "operativo") {
    if (geometry?.type === "Polygon") {
      return "jurisdicciones";
    }

    if (geometry?.type === "Point") {
      if (folderNameLooksLikeCameras(joinedPath) || nameLooksLikeCamera(cleanName)) {
        return "camaras";
      }
      return "dependencias";
    }

    return null;
  }

  return null;
}

function buildWarnings(feature) {
  const warnings = [];

  if (!feature.layerCode) warnings.push("Sin layerCode detectado");
  if (!feature.featureType) warnings.push("Sin featureType detectado");
  if (!feature.geometry?.type) warnings.push("Sin geometría");
  if (!feature.departmentName && feature.layerCode !== "departamentos") {
    warnings.push("Sin departamental");
  }

  if (
    ["camaras", "dependencias", "jurisdicciones", "cuadrantes"].includes(
      feature.layerCode
    ) &&
    !feature.dependencyName
  ) {
    warnings.push("Sin dependencia");
  }

  return warnings;
}

function buildFeatureFromPlacemark(placemark, folderPath, profile) {
  const geometry = getGeometryFromPlacemark(placemark);
  if (!geometry) return null;

  const rawName = normalizeName(placemark?.name) || "Sin nombre";
  const description = placemarkDescription(placemark);
  const context = extractContext(folderPath);

  const layerCode = detectLayerCode({
    profile,
    geometry,
    folderPath,
    placemarkName: rawName
  });

  if (!layerCode) return null;

  const featureTypeMap = {
    camaras: "camera",
    dependencias: "dependency",
    jurisdicciones: "jurisdiction",
    cuadrantes: "quadrant",
    departamentales: "departmental",
    departamentos: "departamento"
  };

  let name = rawName;
  let departmentName = context.departmentName;
  let dependencyName = context.dependencyName;
  let jurisdictionName = null;

  if (layerCode === "departamentales") {
    name = normalizeDepartmentName(rawName) || departmentName || rawName;
    departmentName = normalizeDepartmentName(rawName) || departmentName;
  }

  if (layerCode === "departamentos") {
    name = normalizeWhitespace(rawName).toUpperCase();
    departmentName = name;
    dependencyName = null;
    jurisdictionName = null;
  }

  if (layerCode === "cuadrantes") {
    name = normalizeWhitespace(rawName).toUpperCase();

    const inferredDependency = inferQuadrantDependencyName(name);
    dependencyName = dependencyName || inferredDependency || null;
    jurisdictionName = null;
  }

  if (layerCode === "jurisdicciones") {
    name = normalizeWhitespace(rawName).toUpperCase();
    dependencyName =
      dependencyName ||
      normalizeDependencyName(rawName) ||
      normalizeDependencyName(jurisdictionName);
    jurisdictionName = normalizeJurisdictionName(name, dependencyName);
  }

  if (layerCode === "camaras") {
    name = normalizeWhitespace(rawName).toUpperCase();
    jurisdictionName = dependencyName || null;
  }

  if (layerCode === "dependencias") {
    dependencyName = dependencyName || normalizeDependencyName(name);
    jurisdictionName = dependencyName || null;
    name = dependencyName || normalizeWhitespace(rawName).toUpperCase();
  }

  const feature = {
    layerCode,
    code: buildFeatureCode({
      layerCode,
      departmentName,
      dependencyName,
      jurisdictionName,
      name
    }),
    name,
    description,
    featureType: featureTypeMap[layerCode] || null,
    status: "active",
    departmentName,
    dependencyName,
    jurisdictionName,
    geometry,
    properties: {
      source_folder_path: folderPath,
      original_name: rawName,
      normalized_name: name,
      import_profile: profile
    }
  };

  feature.warnings = buildWarnings(feature);
  return feature;
}

function walkFolder(folder, profile, folderPath = [], out = []) {
  const currentName = normalizeName(folder?.name);
  const nextPath = currentName ? [...folderPath, currentName] : [...folderPath];

  for (const placemark of asArray(folder?.Placemark)) {
    const feature = buildFeatureFromPlacemark(placemark, nextPath, profile);
    if (feature) out.push(feature);
  }

  for (const child of asArray(folder?.Folder)) {
    walkFolder(child, profile, nextPath, out);
  }

  for (const childDocument of asArray(folder?.Document)) {
    walkDocument(childDocument, profile, nextPath, out);
  }

  return out;
}

function walkDocument(documentNode, profile, folderPath = [], out = []) {
  for (const placemark of asArray(documentNode?.Placemark)) {
    const feature = buildFeatureFromPlacemark(placemark, folderPath, profile);
    if (feature) out.push(feature);
  }

  for (const folder of asArray(documentNode?.Folder)) {
    walkFolder(folder, profile, folderPath, out);
  }

  for (const innerDoc of asArray(documentNode?.Document)) {
    walkDocument(innerDoc, profile, folderPath, out);
  }

  return out;
}

function extractKmlTextFromBuffer(buffer, filename = "archivo.kmz") {
  const lower = String(filename || "").toLowerCase();

  if (lower.endsWith(".kml")) {
    return buffer.toString("utf8");
  }

  const zip = new AdmZip(buffer);
  const entries = zip.getEntries();

  const docEntry =
    entries.find((e) => /(^|\/)doc\.kml$/i.test(e.entryName)) ||
    entries.find((e) => /\.kml$/i.test(e.entryName));

  if (!docEntry) {
    throw new Error("No se encontró ningún archivo KML dentro del KMZ.");
  }

  return docEntry.getData().toString("utf8");
}

function parseFileFeatures(buffer, filename, profile) {
  const kmlText = extractKmlTextFromBuffer(buffer, filename);

  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: "@_",
    trimValues: true
  });

  const parsed = parser.parse(kmlText);
  const rootDoc = parsed?.kml?.Document || parsed?.Document;

  if (!rootDoc) {
    throw new Error("No se pudo interpretar el documento KML.");
  }

  return walkDocument(rootDoc, profile, [], []);
}

function dedupeCodes(features) {
  const seen = new Set();

  for (const feature of features) {
    const key = `${feature.layerCode}::${feature.code || ""}`;

    if (!feature.code) continue;

    if (seen.has(key)) {
      feature.properties = {
        ...(feature.properties || {}),
        duplicate_code_original: feature.code
      };
      feature.warnings = [...(feature.warnings || []), "Código duplicado"];
      feature.code = null;
      continue;
    }

    seen.add(key);
  }

  return features;
}

function buildDependencyFeaturesFromJurisdictions(features, profile = "operativo") {
  const created = [];
  const seen = new Set();

  for (const feature of features) {
    if (feature.layerCode !== "jurisdicciones") continue;

    const departmentName = feature.departmentName || null;
    const dependencyName =
      feature.dependencyName ||
      normalizeDependencyName(feature.jurisdictionName) ||
      normalizeDependencyName(feature.name);

    if (!dependencyName) continue;

    const key = `${departmentName || ""}::${dependencyName}`;
    if (seen.has(key)) continue;
    seen.add(key);

    created.push({
      layerCode: "dependencias",
      code: buildFeatureCode({
        layerCode: "dependencias",
        departmentName,
        dependencyName,
        jurisdictionName: feature.jurisdictionName || dependencyName,
        name: dependencyName
      }),
      name: dependencyName,
      description: `Punto generado automáticamente desde jurisdicción ${feature.name}`,
      featureType: "dependency",
      status: "active",
      departmentName,
      dependencyName,
      jurisdictionName: feature.jurisdictionName || dependencyName,
      geometry: feature.geometry,
      geometryFromJurisdiction: true,
      properties: {
        source_folder_path: feature.properties?.source_folder_path || [],
        original_name: dependencyName,
        normalized_name: dependencyName,
        generated_from_jurisdiction: true,
        jurisdiction_source_name: feature.name,
        import_profile: profile
      },
      warnings: ["Dependencia generada automáticamente desde jurisdicción"]
    });
  }

  return created;
}

function summarizeFeatures(features) {
  return features.reduce(
    (acc, feature) => {
      if (feature.layerCode === "camaras") acc.camaras += 1;
      if (feature.layerCode === "dependencias") acc.dependencias += 1;
      if (feature.layerCode === "jurisdicciones") acc.jurisdicciones += 1;
      if (feature.layerCode === "cuadrantes") acc.cuadrantes += 1;
      if (feature.layerCode === "departamentales") acc.departamentales += 1;
      if (feature.layerCode === "departamentos") acc.departamentos += 1;
      return acc;
    },
    {
      total: features.length,
      camaras: 0,
      dependencias: 0,
      jurisdicciones: 0,
      cuadrantes: 0,
      departamentales: 0,
      departamentos: 0
    }
  );
}

function buildGlobalWarnings(features) {
  const warnings = [];

  const missingDepartment = features.filter((f) => !f.departmentName).length;
  const missingDependency = features.filter(
    (f) =>
      ["camaras", "dependencias", "jurisdicciones", "cuadrantes"].includes(
        f.layerCode
      ) && !f.dependencyName
  ).length;
  const duplicateCodes = features.filter((f) => !f.code).length;

  if (missingDepartment > 0) {
    warnings.push(`Se detectaron ${missingDepartment} registros sin departamental.`);
  }

  if (missingDependency > 0) {
    warnings.push(`Se detectaron ${missingDependency} registros sin dependencia.`);
  }

  if (duplicateCodes > 0) {
    warnings.push(`Se detectaron ${duplicateCodes} registros con código duplicado.`);
  }

  return warnings;
}

function buildPreviewRows(features, limit = 200) {
  return features.slice(0, limit).map((feature, index) => ({
    index: index + 1,
    layerCode: feature.layerCode || null,
    featureType: feature.featureType || null,
    code: feature.code || null,
    name: feature.name || null,
    departmentName: feature.departmentName || null,
    dependencyName: feature.dependencyName || null,
    jurisdictionName: feature.jurisdictionName || null,
    geometryType: feature.geometry?.type || null,
    warnings: feature.warnings || []
  }));
}

async function getLayerIds(client) {
  const requiredCodes = [
    "departamentales",
    "departamentos",
    "cuadrantes",
    "jurisdicciones",
    "dependencias",
    "camaras"
  ];

  const { rows } = await client.query(
    `
      SELECT id, code
      FROM geo_layers
      WHERE code = ANY($1::text[])
    `,
    [requiredCodes]
  );

  const map = new Map(rows.map((r) => [r.code, r.id]));

  for (const code of requiredCodes) {
    if (!map.has(code)) {
      throw new Error(`No existe la capa requerida: ${code}`);
    }
  }

  return map;
}

async function clearImportedLayers(client, layerIds, features) {
  const usedLayerIds = [
    ...new Set(
      features
        .map((feature) => layerIds.get(feature.layerCode))
        .filter(Boolean)
    )
  ];

  if (!usedLayerIds.length) return;

  await client.query(
    `
      DELETE FROM geo_features
      WHERE layer_id = ANY($1::uuid[])
    `,
    [usedLayerIds]
  );
}

async function insertFeature(client, layerId, feature, sourceFilename) {
  if (feature.layerCode === "dependencias" && feature.geometryFromJurisdiction) {
    await client.query(
      `
        INSERT INTO geo_features (
          layer_id,
          code,
          name,
          description,
          feature_type,
          status,
          department_name,
          dependency_name,
          jurisdiction_name,
          geom,
          properties,
          source_type,
          source_reference,
          external_id,
          is_active
        )
        VALUES (
          $1,
          $2,
          $3,
          $4,
          $5,
          $6,
          $7,
          $8,
          $9,
          ST_PointOnSurface(ST_SetSRID(ST_GeomFromGeoJSON($10), 4326)),
          $11::jsonb,
          'kmz',
          $12,
          $13,
          TRUE
        )
      `,
      [
        layerId,
        feature.code,
        feature.name,
        feature.description,
        feature.featureType,
        feature.status,
        feature.departmentName,
        feature.dependencyName,
        feature.jurisdictionName,
        JSON.stringify(feature.geometry),
        JSON.stringify(feature.properties || {}),
        sourceFilename,
        null
      ]
    );
    return;
  }

  await client.query(
    `
      INSERT INTO geo_features (
        layer_id,
        code,
        name,
        description,
        feature_type,
        status,
        department_name,
        dependency_name,
        jurisdiction_name,
        geom,
        properties,
        source_type,
        source_reference,
        external_id,
        is_active
      )
      VALUES (
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9,
        ST_SetSRID(ST_GeomFromGeoJSON($10), 4326),
        $11::jsonb,
        'kmz',
        $12,
        $13,
        TRUE
      )
    `,
    [
      layerId,
      feature.code,
      feature.name,
      feature.description,
      feature.featureType,
      feature.status,
      feature.departmentName,
      feature.dependencyName,
      feature.jurisdictionName,
      JSON.stringify(feature.geometry),
      JSON.stringify(feature.properties || {}),
      sourceFilename,
      null
    ]
  );
}

function prepareFeatures({ buffer, filename, importProfile }) {
  const profile = importProfile || autoDetectImportProfile(filename);

  let features = parseFileFeatures(buffer, filename, profile);

  if (["operativo", "departamentales_cuadrantes"].includes(profile)) {
    const generatedDependencies = buildDependencyFeaturesFromJurisdictions(
      features,
      profile
    );

    const existingDependencyKeys = new Set(
      features
        .filter((f) => f.layerCode === "dependencias")
        .map((f) => `${f.departmentName || ""}::${f.dependencyName || ""}`)
    );

    for (const dep of generatedDependencies) {
      const key = `${dep.departmentName || ""}::${dep.dependencyName || ""}`;
      if (!existingDependencyKeys.has(key)) {
        features.push(dep);
        existingDependencyKeys.add(key);
      }
    }
  }

  if (!features.length) {
    throw new Error("El archivo no contiene features compatibles para importar.");
  }

  features = dedupeCodes(features);

  return {
    profile,
    features
  };
}

export async function previewKmzImport({
  buffer,
  filename = "archivo.kmz",
  replaceExisting = true,
  importProfile = null
}) {
  const { profile, features } = prepareFeatures({
    buffer,
    filename,
    importProfile
  });

  const summary = summarizeFeatures(features);
  const warnings = buildGlobalWarnings(features);
  const preview = buildPreviewRows(features, 200);

  return {
    ok: true,
    filename,
    replaceExisting,
    importProfile: profile,
    summary,
    warnings,
    preview
  };
}

export async function importKmzToDatabase({
  buffer,
  filename = "archivo.kmz",
  replaceExisting = true,
  importProfile = null
}) {
  const { profile, features } = prepareFeatures({
    buffer,
    filename,
    importProfile
  });

  const summary = summarizeFeatures(features);
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const layerIds = await getLayerIds(client);

    if (replaceExisting) {
      await clearImportedLayers(client, layerIds, features);
    }

    for (const feature of features) {
      const layerId = layerIds.get(feature.layerCode);
      if (!layerId) continue;
      await insertFeature(client, layerId, feature, filename);
    }

    await client.query("COMMIT");

    return {
      ok: true,
      filename,
      replaceExisting,
      importProfile: profile,
      summary
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}