import { pool } from "../../../config/db.js";

const REQUIRED_HEADERS = [
  "CAMARA",
  "UBICACION",
  "DESCRIPCION",
  "DEPARTAMENTO",
  "ESTADO",
  "TIPO",
  "LATITUD",
  "LONGITUD",
  "ACTIVA"
];

function normalizeWhitespace(value) {
  return String(value ?? "")
    .replace(/\u00A0/g, " ")
    .replace(/\u200B/g, "")
    .replace(/\u200C/g, "")
    .replace(/\u200D/g, "")
    .replace(/\uFEFF/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeHeader(value) {
  return normalizeWhitespace(value).toUpperCase();
}

function normalizeCameraCode(value) {
  return normalizeWhitespace(value).toUpperCase();
}

function normalizeText(value) {
  const clean = normalizeWhitespace(value);
  return clean || null;
}

function parseDecimal(value) {
  const clean = normalizeWhitespace(value);
  if (!clean) return null;

  let normalized = clean;

  if (normalized.includes(",") && normalized.includes(".")) {
    normalized = normalized.replace(/\./g, "").replace(",", ".");
  } else if (normalized.includes(",")) {
    normalized = normalized.replace(",", ".");
  }

  const number = Number(normalized);
  if (!Number.isFinite(number)) return null;

  return number;
}

function parseBooleanActive(value) {
  const clean = normalizeWhitespace(value).toUpperCase();

  if (!clean) return true;

  if (["SI", "S", "TRUE", "1", "ACTIVA", "ACTIVO"].includes(clean)) return true;
  if (["NO", "N", "FALSE", "0", "INACTIVA", "INACTIVO"].includes(clean)) return false;

  return true;
}

function splitCsvLine(line, delimiter = ",") {
  const result = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];
    const next = line[i + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === delimiter && !inQuotes) {
      result.push(current);
      current = "";
      continue;
    }

    current += char;
  }

  result.push(current);
  return result.map((value) => value.trim());
}

function detectDelimiter(firstLine) {
  const commaCount = (firstLine.match(/,/g) || []).length;
  const semicolonCount = (firstLine.match(/;/g) || []).length;
  return semicolonCount > commaCount ? ";" : ",";
}

function looksLikeHeaderRow(row) {
  const normalized = row.map(normalizeHeader);
  return REQUIRED_HEADERS.every((header) => normalized.includes(header));
}

function mapRowByStandardHeaders(headerRow, values) {
  const row = {};

  headerRow.forEach((header, idx) => {
    row[header] = values[idx] ?? "";
  });

  return row;
}

function mapRowByLegacyPositions(values) {
  return {
    CAMARA: values[0] ?? "",
    UBICACION: values[1] ?? "",
    DESCRIPCION: values[2] ?? "",
    DEPARTAMENTO: values[3] ?? "",
    ESTADO: values[4] ?? "",
    TIPO: values[5] ?? "",
    LATITUD: values[6] ?? "",
    LONGITUD: values[7] ?? "",
    ACTIVA: values[8] ?? "SI"
  };
}

function ensureValidBuffer(buffer) {
  if (!buffer) {
    throw new Error("No se recibió archivo para procesar.");
  }

  if (!Buffer.isBuffer(buffer)) {
    throw new Error("El archivo recibido no tiene un buffer válido.");
  }

  if (!buffer.length) {
    throw new Error("El archivo recibido está vacío.");
  }
}

function parseCsv(buffer) {
  ensureValidBuffer(buffer);

  const text = buffer
    .toString("utf8")
    .replace(/^\uFEFF/, "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n");

  const rawLines = text
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);

  if (!rawLines.length) {
    throw new Error("El CSV está vacío.");
  }

  const delimiter = detectDelimiter(rawLines[0]);
  const firstRow = splitCsvLine(rawLines[0], delimiter);
  const rows = [];
  let detectedFormat = "legacy_without_headers";

  if (looksLikeHeaderRow(firstRow)) {
    detectedFormat = "standard_with_headers";

    const headerRow = firstRow.map(normalizeHeader);

    for (const header of REQUIRED_HEADERS) {
      if (!headerRow.includes(header)) {
        throw new Error(`Falta la columna requerida: ${header}`);
      }
    }

    for (let i = 1; i < rawLines.length; i += 1) {
      const values = splitCsvLine(rawLines[i], delimiter);

      if (!values.some((v) => normalizeWhitespace(v))) continue;

      rows.push(mapRowByStandardHeaders(headerRow, values));
    }

    return {
      rows,
      detectedFormat,
      delimiter
    };
  }

  for (let i = 0; i < rawLines.length; i += 1) {
    const values = splitCsvLine(rawLines[i], delimiter);

    if (!values.some((v) => normalizeWhitespace(v))) continue;

    rows.push(mapRowByLegacyPositions(values));
  }

  return {
    rows,
    detectedFormat,
    delimiter
  };
}

function buildCatalogRecord(rawRow, index, hasHeader = true) {
  const cameraCode = normalizeCameraCode(rawRow.CAMARA);
  const latitude = parseDecimal(rawRow.LATITUD);
  const longitude = parseDecimal(rawRow.LONGITUD);

  const warnings = [];

  if (!cameraCode) warnings.push("Sin código de cámara");
  if (latitude === null) warnings.push("Latitud inválida o vacía");
  if (longitude === null) warnings.push("Longitud inválida o vacía");

  return {
    rowNumber: hasHeader ? index + 2 : index + 1,
    cameraCode,
    locationText: normalizeText(rawRow.UBICACION),
    description: normalizeText(rawRow.DESCRIPCION),
    departmentCatalog: normalizeText(rawRow.DEPARTAMENTO),
    statusCatalog: normalizeText(rawRow.ESTADO),
    cameraType: normalizeText(rawRow.TIPO),
    latitude,
    longitude,
    isActive: parseBooleanActive(rawRow.ACTIVA),
    rawPayload: rawRow,
    warnings
  };
}

function summarizeRecords(records) {
  return {
    total: records.length,
    active: records.filter((r) => r.isActive).length,
    inactive: records.filter((r) => !r.isActive).length,
    withCoordinates: records.filter(
      (r) => r.latitude !== null && r.longitude !== null
    ).length,
    withoutCoordinates: records.filter(
      (r) => r.latitude === null || r.longitude === null
    ).length,
    withWarnings: records.filter((r) => r.warnings.length > 0).length
  };
}

function buildPreview(records, limit = 100) {
  return records.slice(0, limit).map((r) => ({
    rowNumber: r.rowNumber,
    cameraCode: r.cameraCode,
    locationText: r.locationText,
    departmentCatalog: r.departmentCatalog,
    statusCatalog: r.statusCatalog,
    cameraType: r.cameraType,
    latitude: r.latitude,
    longitude: r.longitude,
    isActive: r.isActive,
    warnings: r.warnings
  }));
}

function prepareCameraCatalogImport(buffer) {
  const parsed = parseCsv(buffer);
  const hasHeader = parsed.detectedFormat === "standard_with_headers";

  const records = parsed.rows.map((row, idx) =>
    buildCatalogRecord(row, idx, hasHeader)
  );

  if (!records.length) {
    throw new Error("El CSV no contiene registros.");
  }

  return {
    detectedFormat: parsed.detectedFormat,
    delimiter: parsed.delimiter,
    records,
    summary: summarizeRecords(records),
    preview: buildPreview(records)
  };
}

async function upsertCatalogRecord(client, record, sourceReference) {
  await client.query(
    `
      INSERT INTO geo_camera_catalog (
        camera_code,
        location_text,
        description,
        department_catalog,
        status_catalog,
        camera_type,
        latitude,
        longitude,
        is_active,
        source_reference,
        raw_payload,
        updated_at
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, now()
      )
      ON CONFLICT ((upper(camera_code)))
      DO UPDATE SET
        location_text = EXCLUDED.location_text,
        description = EXCLUDED.description,
        department_catalog = EXCLUDED.department_catalog,
        status_catalog = EXCLUDED.status_catalog,
        camera_type = EXCLUDED.camera_type,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        is_active = EXCLUDED.is_active,
        source_reference = EXCLUDED.source_reference,
        raw_payload = EXCLUDED.raw_payload,
        updated_at = now()
    `,
    [
      record.cameraCode,
      record.locationText,
      record.description,
      record.departmentCatalog,
      record.statusCatalog,
      record.cameraType,
      record.latitude,
      record.longitude,
      record.isActive,
      sourceReference,
      JSON.stringify(record.rawPayload || {})
    ]
  );
}

async function syncCatalogIntoGeoFeatures(client) {
  const sql = `
    UPDATE geo_features gf
    SET
      properties = COALESCE(gf.properties, '{}'::jsonb)
        || jsonb_build_object(
          'camera_catalog_code', gcc.camera_code,
          'camera_location_text', gcc.location_text,
          'camera_description', gcc.description,
          'camera_department_catalog', gcc.department_catalog,
          'camera_status_catalog', gcc.status_catalog,
          'camera_type', gcc.camera_type,
          'camera_latitude_catalog', gcc.latitude,
          'camera_longitude_catalog', gcc.longitude,
          'camera_active', gcc.is_active,
          'camera_catalog_source', gcc.source_reference
        ),
      status = CASE
        WHEN gcc.is_active THEN 'active'
        ELSE 'inactive'
      END,
      updated_at = now()
    FROM geo_camera_catalog gcc
    WHERE gf.feature_type = 'camera'
      AND (
        upper(gf.name) = upper(gcc.camera_code)
        OR upper(gf.code) LIKE '%' || upper(gcc.camera_code) || '%'
      )
  `;

  await client.query(sql);
}

async function getSyncStats(client) {
  const { rows } = await client.query(`
    SELECT
      COUNT(*) FILTER (
        WHERE feature_type = 'camera'
          AND properties ? 'camera_catalog_code'
      ) AS matched_cameras,
      COUNT(*) FILTER (
        WHERE feature_type = 'camera'
          AND NOT (properties ? 'camera_catalog_code')
      ) AS unmatched_cameras
    FROM geo_features
  `);

  return rows[0] || {
    matched_cameras: 0,
    unmatched_cameras: 0
  };
}

export async function previewCameraCatalogImport({
  buffer,
  filename = "camaras.csv"
}) {
  const { detectedFormat, delimiter, records, summary, preview } =
    prepareCameraCatalogImport(buffer);

  return {
    ok: true,
    filename,
    detectedFormat,
    delimiter,
    summary,
    preview,
    warnings: records
      .filter((r) => r.warnings.length > 0)
      .slice(0, 100)
      .map((r) => ({
        rowNumber: r.rowNumber,
        cameraCode: r.cameraCode,
        warnings: r.warnings
      }))
  };
}

export async function importCameraCatalogToDatabase({
  buffer,
  filename = "camaras.csv",
  replaceExisting = true
}) {
  const { detectedFormat, delimiter, records, summary } =
    prepareCameraCatalogImport(buffer);

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    if (replaceExisting) {
      await client.query(`DELETE FROM geo_camera_catalog`);
    }

    for (const record of records) {
      await upsertCatalogRecord(client, record, filename);
    }

    await syncCatalogIntoGeoFeatures(client);
    const syncStats = await getSyncStats(client);

    await client.query("COMMIT");

    return {
      ok: true,
      filename,
      detectedFormat,
      delimiter,
      replaceExisting,
      summary,
      syncStats
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}