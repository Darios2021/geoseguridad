import { pool } from "../../../config/db.js";
import { GEO_QUERIES } from "../queries/geo.queries.js";

export async function getLayers() {
  const result = await pool.query(GEO_QUERIES.layers);
  return result.rows;
}

function buildStandardFeature(row) {
  return {
    type: "Feature",
    id: row.id,
    geometry: row.geometry,
    properties: {
      id: row.id,
      layer_id: row.layer_id,
      layer_code: row.layer_code,
      layer_name: row.layer_name,
      code: row.code,
      name: row.name,
      description: row.description,
      feature_type: row.feature_type,
      status: row.status,
      priority: row.priority,
      department_name: row.department_name,
      dependency_name: row.dependency_name,
      jurisdiction_name: row.jurisdiction_name,
      source_type: row.source_type,
      source_reference: row.source_reference,
      external_id: row.external_id,
      is_active: row.is_active,
      created_at: row.created_at,
      updated_at: row.updated_at,
      ...(row.properties || {})
    }
  };
}

async function getCameraFeatures(filters = {}) {
  const values = [];
  const where = [];
  let idx = 1;

  where.push(`gcc.latitude IS NOT NULL`);
  where.push(`gcc.longitude IS NOT NULL`);

  if (filters.department) {
    where.push(`
      COALESCE(
        dep_poly_rel.department_name,
        jur_rel.department_name,
        quad_rel.department_name,
        dep_rel.department_name,
        gcc.department_catalog,
        'SIN DEPARTAMENTAL'
      ) = $${idx++}
    `);
    values.push(filters.department);
  }

  if (filters.dependency) {
    where.push(`
      COALESCE(
        dep_rel.dependency_name,
        jur_rel.dependency_name,
        quad_rel.dependency_name,
        'SIN DEPENDENCIA'
      ) = $${idx++}
    `);
    values.push(filters.dependency);
  }

  if (filters.status) {
    if (String(filters.status).toLowerCase() === "active") {
      where.push(`gcc.is_active = TRUE`);
    } else if (String(filters.status).toLowerCase() === "inactive") {
      where.push(`gcc.is_active = FALSE`);
    } else {
      where.push(`
        CASE
          WHEN gcc.is_active THEN 'active'
          ELSE 'inactive'
        END = $${idx++}
      `);
      values.push(filters.status);
    }
  }

  const limit = Math.min(Number(filters.limit || 1000), 10000);
  values.push(limit);

  const sql = `
    SELECT
      gcc.id,
      gl.id AS layer_id,
      gl.code AS layer_code,
      gl.name AS layer_name,
      gcc.camera_code AS code,

      CASE
        WHEN gcc.location_text IS NOT NULL AND btrim(gcc.location_text) <> ''
        THEN gcc.camera_code || ' - ' || gcc.location_text
        ELSE gcc.camera_code
      END AS name,

      gcc.description,
      'camera' AS feature_type,
      CASE
        WHEN gcc.is_active THEN 'active'
        ELSE 'inactive'
      END AS status,
      NULL::integer AS priority,

      COALESCE(
        dep_poly_rel.department_name,
        jur_rel.department_name,
        quad_rel.department_name,
        dep_rel.department_name,
        gcc.department_catalog,
        'SIN DEPARTAMENTAL'
      ) AS department_name,

      COALESCE(
        dep_rel.dependency_name,
        jur_rel.dependency_name,
        quad_rel.dependency_name,
        'SIN DEPENDENCIA'
      ) AS dependency_name,

      COALESCE(
        jur_rel.jurisdiction_name,
        quad_rel.jurisdiction_name,
        dep_rel.jurisdiction_name,
        'SIN JURISDICCIÓN'
      ) AS jurisdiction_name,

      ST_AsGeoJSON(
        ST_SetSRID(
          ST_MakePoint(
            gcc.longitude::double precision,
            gcc.latitude::double precision
          ),
          4326
        )
      )::json AS geometry,

      jsonb_build_object(
        'camera_catalog_id', gcc.id,
        'camera_catalog_code', gcc.camera_code,
        'camera_location_text', gcc.location_text,
        'camera_description', gcc.description,
        'camera_department_catalog', gcc.department_catalog,
        'camera_status_catalog', gcc.status_catalog,
        'camera_type', gcc.camera_type,
        'camera_latitude_catalog', gcc.latitude,
        'camera_longitude_catalog', gcc.longitude,
        'camera_active', gcc.is_active,
        'camera_catalog_source', gcc.source_reference,
        'quadrant_name', quad_rel.quadrant_name,
        'quadrant_code', quad_rel.quadrant_code,
        'departmental_name', COALESCE(
          dep_poly_rel.department_name,
          jur_rel.department_name,
          quad_rel.department_name,
          dep_rel.department_name,
          gcc.department_catalog,
          'SIN DEPARTAMENTAL'
        ),
        'departmental_code', dep_poly_rel.departmental_code,
        'jurisdiction_code', jur_rel.jurisdiction_code,
        'dependency_code', dep_rel.dependency_code,
        'matched_by', COALESCE(
          dep_poly_rel.matched_by,
          jur_rel.matched_by,
          quad_rel.matched_by,
          dep_rel.matched_by,
          'catalog_only'
        ),
        'raw_payload', gcc.raw_payload
      ) AS properties,

      'camera_catalog' AS source_type,
      gcc.source_reference AS source_reference,
      gcc.id AS external_id,
      gcc.is_active,
      gcc.created_at,
      gcc.updated_at

    FROM geo_camera_catalog gcc
    INNER JOIN geo_layers gl
      ON gl.code = 'camaras'
     AND gl.is_active = TRUE

    LEFT JOIN LATERAL (
      SELECT
        f.name AS department_name,
        f.code AS departmental_code,
        'spatial_departmental'::text AS matched_by
      FROM geo_features f
      INNER JOIN geo_layers l ON l.id = f.layer_id
      WHERE f.is_active = TRUE
        AND l.code = 'departamentales'
        AND ST_Intersects(
          f.geom,
          ST_SetSRID(
            ST_MakePoint(
              gcc.longitude::double precision,
              gcc.latitude::double precision
            ),
            4326
          )
        )
      ORDER BY f.name ASC
      LIMIT 1
    ) dep_poly_rel ON TRUE

    LEFT JOIN LATERAL (
      SELECT
        f.department_name,
        f.dependency_name,
        f.jurisdiction_name,
        f.code AS dependency_code,
        'spatial_dependency'::text AS matched_by
      FROM geo_features f
      INNER JOIN geo_layers l ON l.id = f.layer_id
      WHERE f.is_active = TRUE
        AND l.code = 'dependencias'
        AND ST_Intersects(
          f.geom,
          ST_SetSRID(
            ST_MakePoint(
              gcc.longitude::double precision,
              gcc.latitude::double precision
            ),
            4326
          )
        )
      ORDER BY ST_Area(ST_Envelope(f.geom)) ASC NULLS LAST, f.name ASC
      LIMIT 1
    ) dep_rel ON TRUE

    LEFT JOIN LATERAL (
      SELECT
        f.department_name,
        f.dependency_name,
        f.jurisdiction_name,
        f.code AS jurisdiction_code,
        'spatial_jurisdiction'::text AS matched_by
      FROM geo_features f
      INNER JOIN geo_layers l ON l.id = f.layer_id
      WHERE f.is_active = TRUE
        AND l.code = 'jurisdicciones'
        AND ST_Intersects(
          f.geom,
          ST_SetSRID(
            ST_MakePoint(
              gcc.longitude::double precision,
              gcc.latitude::double precision
            ),
            4326
          )
        )
      ORDER BY ST_Area(ST_Envelope(f.geom)) ASC NULLS LAST, f.name ASC
      LIMIT 1
    ) jur_rel ON TRUE

    LEFT JOIN LATERAL (
      SELECT
        f.name AS quadrant_name,
        f.code AS quadrant_code,
        f.department_name,
        f.dependency_name,
        f.jurisdiction_name,
        'spatial_quadrant'::text AS matched_by
      FROM geo_features f
      INNER JOIN geo_layers l ON l.id = f.layer_id
      WHERE f.is_active = TRUE
        AND l.code = 'cuadrantes'
        AND ST_Intersects(
          f.geom,
          ST_SetSRID(
            ST_MakePoint(
              gcc.longitude::double precision,
              gcc.latitude::double precision
            ),
            4326
          )
        )
      ORDER BY f.name ASC
      LIMIT 1
    ) quad_rel ON TRUE

    WHERE ${where.join(" AND ")}
    ORDER BY gcc.camera_code ASC
    LIMIT $${idx}
  `;

  const result = await pool.query(sql, values);
  return result.rows.map(buildStandardFeature);
}

async function getStructuralFeatures(filters = {}) {
  const values = [];
  let where = "";
  let idx = 1;

  if (filters.layer) {
    where += ` AND l.code = $${idx++}`;
    values.push(filters.layer);
  }

  if (filters.department) {
    where += ` AND f.department_name = $${idx++}`;
    values.push(filters.department);
  }

  if (filters.dependency) {
    where += ` AND f.dependency_name = $${idx++}`;
    values.push(filters.dependency);
  }

  if (filters.status) {
    where += ` AND f.status = $${idx++}`;
    values.push(filters.status);
  }

  const limit = Math.min(Number(filters.limit || 1000), 5000);
  values.push(limit);

  const sql = `
    ${GEO_QUERIES.featuresBase}
    ${where}
    ORDER BY f.name ASC
    LIMIT $${idx}
  `;

  const result = await pool.query(sql, values);
  return result.rows.map(buildStandardFeature);
}

export async function getFeatures(filters = {}) {
  if (filters.layer === "camaras") {
    return getCameraFeatures(filters);
  }

  return getStructuralFeatures(filters);
}

async function getTreeStructuralRows(filters = {}) {
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
      AND l.code <> 'camaras'
      ${where}
    ORDER BY
      COALESCE(f.department_name, 'ZZZ') ASC,
      COALESCE(f.dependency_name, 'ZZZ') ASC,
      COALESCE(f.jurisdiction_name, 'ZZZ') ASC,
      l.code ASC,
      f.name ASC
  `;

  const { rows } = await pool.query(sql, values);
  return rows;
}

async function getTreeCameraRows(filters = {}) {
  const cameraFeatures = await getCameraFeatures({
    status: filters.status,
    limit: 100000
  });

  return cameraFeatures.map((feature) => {
    const props = feature.properties || {};
    const departmentCatalog =
      props.camera_department_catalog &&
      String(props.camera_department_catalog).trim()
        ? String(props.camera_department_catalog).trim()
        : "SIN DEPARTAMENTO";

    return {
      id: props.id,
      layer_code: props.layer_code,
      code: props.code,
      name: props.name,
      department_name: props.department_name || "SIN DEPARTAMENTAL",
      dependency_name: props.dependency_name || "SIN DEPENDENCIA",
      jurisdiction_name: props.jurisdiction_name || "SIN JURISDICCIÓN",
      feature_type: props.feature_type,
      quadrant_name:
        props.quadrant_name && String(props.quadrant_name).trim()
          ? String(props.quadrant_name).trim()
          : null,
      quadrant_code:
        props.quadrant_code && String(props.quadrant_code).trim()
          ? String(props.quadrant_code).trim()
          : null,
      camera_department_catalog: departmentCatalog
    };
  });
}

function buildStructuralTree(structuralRows) {
  const departments = new Map();

  function makeFeature(row, fallbackName) {
    return {
      id: row.id,
      layerCode: row.layer_code,
      name: row.name || row.code || fallbackName || row.id,
      code: row.code,
      featureType: row.feature_type
    };
  }

  function ensureDepartment(name) {
    const key = String(name || "SIN DEPARTAMENTAL").trim();

    if (!departments.has(key)) {
      departments.set(key, {
        id: `department:${key}`,
        type: "department",
        name: key,
        feature: null,
        dependencyMap: new Map()
      });
    }

    return departments.get(key);
  }

  function ensureDependency(departmentNode, name) {
    const key = String(name || "SIN DEPENDENCIA").trim();

    if (!departmentNode.dependencyMap.has(key)) {
      departmentNode.dependencyMap.set(key, {
        id: `dependency:${departmentNode.name}:${key}`,
        type: "dependency",
        name: key,
        feature: null,
        jurisdictionNode: null,
        quadrantGroup: {
          id: `dependency:${departmentNode.name}:${key}:group:cuadrantes`,
          type: "group",
          name: "Cuadrantes",
          groupKind: "cuadrantes",
          children: []
        }
      });
    }

    return departmentNode.dependencyMap.get(key);
  }

  for (const row of structuralRows) {
    const layerCode = row.layer_code;
    const departmentName = row.department_name || "SIN DEPARTAMENTAL";
    const dependencyName =
      row.dependency_name || row.jurisdiction_name || "SIN DEPENDENCIA";

    const departmentNode = ensureDepartment(departmentName);
    const dependencyNode = ensureDependency(departmentNode, dependencyName);

    if (layerCode === "departamentales") {
      departmentNode.feature = makeFeature(row, departmentNode.name);
      continue;
    }

    if (layerCode === "dependencias") {
      dependencyNode.feature = makeFeature(row, dependencyNode.name);
      continue;
    }

    if (layerCode === "jurisdicciones") {
      const rawJurisdictionName = String(
        row.jurisdiction_name || row.name || "Jurisdicción"
      ).trim();

      const jurisdictionDisplayName =
        !rawJurisdictionName || rawJurisdictionName === dependencyNode.name
          ? "Jurisdicción"
          : rawJurisdictionName;

      dependencyNode.jurisdictionNode = {
        id: `jurisdiction:${dependencyNode.id}`,
        type: "jurisdiction",
        name: jurisdictionDisplayName,
        feature: makeFeature(row, jurisdictionDisplayName)
      };
      continue;
    }

    if (layerCode === "cuadrantes") {
      dependencyNode.quadrantGroup.children.push({
        id: `quadrant:${row.id}`,
        type: "quadrant",
        name: row.name,
        feature: makeFeature(row, row.name)
      });
    }
  }

  const tree = [...departments.values()].map((departmentNode) => {
    const dependencies = [...departmentNode.dependencyMap.values()].map(
      (dependencyNode) => {
        const children = [];

        if (dependencyNode.jurisdictionNode) {
          children.push(dependencyNode.jurisdictionNode);
        }

        if (dependencyNode.quadrantGroup.children.length) {
          dependencyNode.quadrantGroup.children.sort((a, b) =>
            String(a.name).localeCompare(String(b.name))
          );
          children.push(dependencyNode.quadrantGroup);
        }

        return {
          id: dependencyNode.id,
          type: dependencyNode.type,
          name: dependencyNode.name,
          feature: dependencyNode.feature || null,
          children
        };
      }
    );

    return {
      id: departmentNode.id,
      type: departmentNode.type,
      name: departmentNode.name,
      feature: departmentNode.feature || null,
      children: dependencies.sort((a, b) =>
        String(a.name).localeCompare(String(b.name))
      )
    };
  });

  return tree.sort((a, b) => String(a.name).localeCompare(String(b.name)));
}

function buildCameraCatalogTree(cameraRows) {
  const cameraDepartments = new Map();

  for (const row of cameraRows) {
    const departmentKey = String(
      row.camera_department_catalog || "SIN DEPARTAMENTO"
    ).trim();

    if (!cameraDepartments.has(departmentKey)) {
      cameraDepartments.set(departmentKey, {
        id: `camera-catalog-department:${departmentKey}`,
        type: "group",
        name: departmentKey,
        groupKind: "camaras",
        children: []
      });
    }

    cameraDepartments.get(departmentKey).children.push({
      id: `camera-catalog:${row.id}`,
      type: "camera",
      name: row.name || row.code || "Cámara",
      feature: {
        id: row.id,
        layerCode: row.layer_code,
        name: row.name || row.code || "Cámara",
        code: row.code,
        featureType: row.feature_type
      }
    });
  }

  const departmentNodes = [...cameraDepartments.values()]
    .map((node) => ({
      ...node,
      children: node.children.sort((a, b) =>
        String(a.name).localeCompare(String(b.name))
      )
    }))
    .sort((a, b) => String(a.name).localeCompare(String(b.name)));

  if (!departmentNodes.length) {
    return null;
  }

  return {
    id: "group:camaras-root",
    type: "group",
    name: "Cámaras",
    groupKind: "camaras",
    children: departmentNodes
  };
}

export async function getGeoTree(filters = {}) {
  const [structuralRows, cameraRows] = await Promise.all([
    getTreeStructuralRows(filters),
    getTreeCameraRows(filters)
  ]);

  const structuralTree = buildStructuralTree(structuralRows);
  const cameraRoot = buildCameraCatalogTree(cameraRows);

  return cameraRoot ? [...structuralTree, cameraRoot] : structuralTree;
}

export async function getHealth() {
  const result = await pool.query(`
    SELECT
      current_database() AS database,
      current_user AS user_name,
      PostGIS_Version() AS postgis_version,
      NOW() AS server_time
  `);
  return result.rows[0];
}