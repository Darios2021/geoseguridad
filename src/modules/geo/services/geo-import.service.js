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

function normalizeName(value) {
  return textValue(value)?.replace(/\s+/g, " ").trim() || null;
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

  const outerCoords = parseCoordinatesList(textValue(outer));
  if (outerCoords.length < 4) return null;

  const rings = [outerCoords];

  const innerBoundaries = asArray(placemark?.Polygon?.innerBoundaryIs);

  for (const inner of innerBoundaries) {
    const innerCoords = parseCoordinatesList(
      textValue(inner?.LinearRing?.coordinates || inner?.coordinates)
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

function folderNameLooksLikeDependency(name) {
  return /(comisaria|comisaría|subcomisaria|subcomisaría|seccional|unidad|dependencia|brigada|departamento)/i.test(
    name || ""
  );
}

function detectLayerCode({ geometry, folderPath, placemarkName }) {
  const joinedPath = folderPath.join(" / ");

  if (geometry?.type === "Polygon") {
    return "jurisdicciones";
  }

  if (geometry?.type === "Point") {
    if (
      folderNameLooksLikeCameras(joinedPath) ||
      /^c\d+/i.test(placemarkName || "")
    ) {
      return "camaras";
    }
    return "dependencias";
  }

  return null;
}

function extractContext(folderPath) {
  let departmentName = null;
  let dependencyName = null;

  for (const part of folderPath) {
    if (!departmentName && folderNameLooksLikeDepartment(part)) {
      departmentName = part;
      continue;
    }

    if (!dependencyName && folderNameLooksLikeDependency(part)) {
      dependencyName = part;
    }
  }

  return {
    departmentName,
    dependencyName
  };
}

function sanitizeCode(name) {
  const clean = normalizeName(name);
  if (!clean) return null;
  return clean.length <= 120 ? clean : clean.slice(0, 120);
}

function placemarkDescription(placemark) {
  return textValue(placemark?.description) || null;
}

function buildFeatureFromPlacemark(placemark, folderPath) {
  const geometry = getGeometryFromPlacemark(placemark);
  if (!geometry) return null;

  const name = normalizeName(placemark?.name) || "Sin nombre";
  const description = placemarkDescription(placemark);
  const { departmentName, dependencyName } = extractContext(folderPath);

  const layerCode = detectLayerCode({
    geometry,
    folderPath,
    placemarkName: name
  });

  if (!layerCode) return null;

  const featureTypeMap = {
    camaras: "camera",
    dependencias: "dependency",
    jurisdicciones: "jurisdiction"
  };

  let jurisdictionName = null;

  if (layerCode === "jurisdicciones") {
    jurisdictionName = name;
  } else if (layerCode === "camaras" || layerCode === "dependencias") {
    jurisdictionName = dependencyName || null;
  }

  return {
    layerCode,
    code: sanitizeCode(name),
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
      original_name: name
    }
  };
}

function walkFolder(folder, folderPath = [], out = []) {
  const currentName = normalizeName(folder?.name);
  const nextPath = currentName ? [...folderPath, currentName] : [...folderPath];

  for (const placemark of asArray(folder?.Placemark)) {
    const feature = buildFeatureFromPlacemark(placemark, nextPath);
    if (feature) out.push(feature);
  }

  for (const child of asArray(folder?.Folder)) {
    walkFolder(child, nextPath, out);
  }

  for (const childDocument of asArray(folder?.Document)) {
    walkDocument(childDocument, nextPath, out);
  }

  return out;
}

function walkDocument(documentNode, folderPath = [], out = []) {
  for (const placemark of asArray(documentNode?.Placemark)) {
    const feature = buildFeatureFromPlacemark(placemark, folderPath);
    if (feature) out.push(feature);
  }

  for (const folder of asArray(documentNode?.Folder)) {
    walkFolder(folder, folderPath, out);
  }

  for (const innerDoc of asArray(documentNode?.Document)) {
    walkDocument(innerDoc, folderPath, out);
  }

  return out;
}

function extractKmlTextFromKmzBuffer(buffer) {
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

function parseKmzFeatures(buffer) {
  const kmlText = extractKmlTextFromKmzBuffer(buffer);

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

  return walkDocument(rootDoc, [], []);
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
      feature.code = null;
      continue;
    }

    seen.add(key);
  }

  return features;
}

async function getLayerIds(client) {
  const { rows } = await client.query(`
    SELECT id, code
    FROM geo_layers
    WHERE code IN ('camaras', 'dependencias', 'jurisdicciones')
  `);

  const map = new Map(rows.map((r) => [r.code, r.id]));

  for (const code of ["camaras", "dependencias", "jurisdicciones"]) {
    if (!map.has(code)) {
      throw new Error(`No existe la capa requerida: ${code}`);
    }
  }

  return map;
}

async function clearImportedLayers(client, layerIds) {
  await client.query(
    `
      DELETE FROM geo_features
      WHERE layer_id = ANY($1::uuid[])
    `,
    [[...layerIds.values()]]
  );
}

async function insertFeature(client, layerId, feature, sourceFilename) {
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

export async function importKmzToDatabase({
  buffer,
  filename = "archivo.kmz",
  replaceExisting = true
}) {
  let features = parseKmzFeatures(buffer);

  if (!features.length) {
    throw new Error("El KMZ no contiene features compatibles para importar.");
  }

  features = dedupeCodes(features);

  const summary = {
    total: features.length,
    camaras: 0,
    dependencias: 0,
    jurisdicciones: 0
  };

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const layerIds = await getLayerIds(client);

    if (replaceExisting) {
      await clearImportedLayers(client, layerIds);
    }

    for (const feature of features) {
      const layerId = layerIds.get(feature.layerCode);
      await insertFeature(client, layerId, feature, filename);

      if (feature.layerCode === "camaras") summary.camaras += 1;
      if (feature.layerCode === "dependencias") summary.dependencias += 1;
      if (feature.layerCode === "jurisdicciones") summary.jurisdicciones += 1;
    }

    await client.query("COMMIT");

    return {
      ok: true,
      filename,
      replaceExisting,
      summary
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}