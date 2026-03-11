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

/* ============================================================
   CAMERAS FROM geo_camera_catalog
============================================================ */

async function getCameraFeatures(filters = {}) {

  const values = [];
  const where = [];
  let idx = 1;

  where.push(`gcc.latitude IS NOT NULL`);
  where.push(`gcc.longitude IS NOT NULL`);

  if (filters.status) {

    if (filters.status === "active")
      where.push(`gcc.is_active = TRUE`);

    else if (filters.status === "inactive")
      where.push(`gcc.is_active = FALSE`);
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
    gcc.camera_code AS name,
    gcc.description,

    'camera' AS feature_type,

    CASE
      WHEN gcc.is_active THEN 'active'
      ELSE 'inactive'
    END AS status,

    NULL::integer AS priority,

    /* ======================================
       DEPARTAMENTAL RESUELTA POR GEOMETRIA
    ====================================== */

    COALESCE(
      dep_poly.name,
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
      'matched_by', COALESCE(
        dep_poly.matched_by,
        jur_rel.matched_by,
        quad_rel.matched_by,
        dep_rel.matched_by,
        'catalog_only'
      ),
      'raw_payload', gcc.raw_payload
    ) AS properties,

    'camera_catalog' AS source_type,
    gcc.source_reference,
    gcc.id AS external_id,
    gcc.is_active,
    gcc.created_at,
    gcc.updated_at

  FROM geo_camera_catalog gcc

  INNER JOIN geo_layers gl
    ON gl.code = 'camaras'
   AND gl.is_active = TRUE

  /* ===============================
     DEPARTAMENTAL POLICIAL
  =============================== */

  LEFT JOIN LATERAL (
    SELECT
      f.name,
      'spatial_departmental' AS matched_by
    FROM geo_features f
    JOIN geo_layers l ON l.id = f.layer_id
    WHERE l.code = 'departamentales'
      AND ST_Intersects(
        f.geom,
        ST_SetSRID(ST_MakePoint(gcc.longitude, gcc.latitude),4326)
      )
    LIMIT 1
  ) dep_poly ON TRUE

  /* ===============================
     DEPENDENCIA
  =============================== */

  LEFT JOIN LATERAL (
    SELECT
      f.department_name,
      f.dependency_name,
      f.jurisdiction_name,
      'spatial_dependency' AS matched_by
    FROM geo_features f
    JOIN geo_layers l ON l.id = f.layer_id
    WHERE l.code = 'dependencias'
      AND ST_Intersects(
        f.geom,
        ST_SetSRID(ST_MakePoint(gcc.longitude, gcc.latitude),4326)
      )
    LIMIT 1
  ) dep_rel ON TRUE

  /* ===============================
     JURISDICCION
  =============================== */

  LEFT JOIN LATERAL (
    SELECT
      f.department_name,
      f.dependency_name,
      f.jurisdiction_name,
      'spatial_jurisdiction' AS matched_by
    FROM geo_features f
    JOIN geo_layers l ON l.id = f.layer_id
    WHERE l.code = 'jurisdicciones'
      AND ST_Intersects(
        f.geom,
        ST_SetSRID(ST_MakePoint(gcc.longitude, gcc.latitude),4326)
      )
    LIMIT 1
  ) jur_rel ON TRUE

  /* ===============================
     CUADRANTE
  =============================== */

  LEFT JOIN LATERAL (
    SELECT
      f.name AS quadrant_name,
      f.code AS quadrant_code,
      f.department_name,
      f.dependency_name,
      f.jurisdiction_name,
      'spatial_quadrant' AS matched_by
    FROM geo_features f
    JOIN geo_layers l ON l.id = f.layer_id
    WHERE l.code = 'cuadrantes'
      AND ST_Intersects(
        f.geom,
        ST_SetSRID(ST_MakePoint(gcc.longitude, gcc.latitude),4326)
      )
    LIMIT 1
  ) quad_rel ON TRUE

  WHERE ${where.join(" AND ")}

  ORDER BY gcc.camera_code ASC

  LIMIT $${idx}
  `;

  const result = await pool.query(sql, values);

  return result.rows.map(buildStandardFeature);
}

/* ============================================================
   STRUCTURAL FEATURES
============================================================ */

async function getStructuralFeatures(filters = {}) {

  const values = [];
  let where = "";
  let idx = 1;

  if (filters.layer) {
    where += ` AND l.code = $${idx++}`;
    values.push(filters.layer);
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

  if (filters.layer === "camaras")
    return getCameraFeatures(filters);

  return getStructuralFeatures(filters);
}

/* ============================================================
   TREE
============================================================ */

export async function getGeoTree(filters = {}) {

  const structural = await getStructuralFeatures({});
  const cameras = await getCameraFeatures({});

  const rows = [
    ...structural.map(r => r.properties),
    ...cameras.map(r => r.properties)
  ];

  const departments = new Map();

  function makeFeature(row) {
    return {
      id: row.id,
      layerCode: row.layer_code,
      name: row.name,
      code: row.code,
      featureType: row.feature_type
    };
  }

  function ensureDepartment(name) {

    const key = name || "SIN DEPARTAMENTAL";

    if (!departments.has(key)) {

      departments.set(key, {
        id:`department:${key}`,
        type:"department",
        name:key,
        feature:null,
        dependencyMap:new Map()
      });
    }

    return departments.get(key);
  }

  function ensureDependency(depNode,name){

    const key = name || "SIN DEPENDENCIA";

    if(!depNode.dependencyMap.has(key)){

      depNode.dependencyMap.set(key,{
        id:`dependency:${depNode.name}:${key}`,
        type:"dependency",
        name:key,
        feature:null,
        children:[]
      });

    }

    return depNode.dependencyMap.get(key);
  }

  for(const row of rows){

    const departmentNode = ensureDepartment(row.department_name);
    const dependencyNode = ensureDependency(departmentNode,row.dependency_name);

    if(row.feature_type === "camera"){

      dependencyNode.children.push({
        id:`camera:${row.id}`,
        type:"camera",
        name:row.name,
        feature:makeFeature(row)
      });

    }

  }

  const tree = [...departments.values()].map(dep=>({

    id:dep.id,
    type:"department",
    name:dep.name,
    children:[...dep.dependencyMap.values()]

  }));

  return tree.sort((a,b)=>a.name.localeCompare(b.name));
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