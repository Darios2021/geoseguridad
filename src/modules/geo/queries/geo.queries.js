export const GEO_QUERIES = {
  layers: `
    SELECT
      id,
      code,
      name,
      description,
      geometry_type,
      icon,
      color,
      sort_order,
      visible_default,
      is_active,
      created_at,
      updated_at
    FROM geo_layers
    WHERE is_active = TRUE
    ORDER BY sort_order ASC, name ASC
  `,

  featuresBase: `
    SELECT
      f.id,
      f.layer_id,
      l.code AS layer_code,
      l.name AS layer_name,
      f.code,
      f.name,
      f.description,
      f.feature_type,
      f.status,
      f.priority,
      f.department_name,
      f.dependency_name,
      f.jurisdiction_name,
      ST_AsGeoJSON(f.geom)::json AS geometry,
      f.properties,
      f.source_type,
      f.source_reference,
      f.external_id,
      f.is_active,
      f.created_at,
      f.updated_at
    FROM geo_features f
    INNER JOIN geo_layers l ON l.id = f.layer_id
    WHERE f.is_active = TRUE
      AND l.code <> 'camaras'
  `,

  cameraFeaturesBase: `
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
      COALESCE(dep_rel.department_name, dep_catalog.department_name, gcc.department_catalog) AS department_name,
      COALESCE(dep_rel.dependency_name, jur_rel.dependency_name, quad_rel.dependency_name, dep_catalog.dependency_name) AS dependency_name,
      COALESCE(jur_rel.jurisdiction_name, dep_rel.jurisdiction_name, quad_rel.jurisdiction_name) AS jurisdiction_name,
      ST_AsGeoJSON(
        ST_SetSRID(ST_MakePoint(gcc.longitude::double precision, gcc.latitude::double precision), 4326)
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
        'departmental_name', COALESCE(dep_rel.department_name, dep_catalog.department_name, gcc.department_catalog),
        'departmental_code', dep_rel.departmental_code,
        'jurisdiction_code', jur_rel.jurisdiction_code,
        'dependency_code', dep_rel.dependency_code,
        'matched_by', COALESCE(
          dep_rel.matched_by,
          jur_rel.matched_by,
          quad_rel.matched_by,
          dep_catalog.matched_by,
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
        f.department_name,
        f.dependency_name,
        f.jurisdiction_name,
        f.code AS dependency_code,
        NULL::text AS departmental_code,
        'spatial_dependency'::text AS matched_by
      FROM geo_features f
      INNER JOIN geo_layers l ON l.id = f.layer_id
      WHERE f.is_active = TRUE
        AND l.code = 'dependencias'
        AND gcc.latitude IS NOT NULL
        AND gcc.longitude IS NOT NULL
        AND ST_Intersects(
          f.geom,
          ST_SetSRID(ST_MakePoint(gcc.longitude::double precision, gcc.latitude::double precision), 4326)
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
        AND gcc.latitude IS NOT NULL
        AND gcc.longitude IS NOT NULL
        AND ST_Intersects(
          f.geom,
          ST_SetSRID(ST_MakePoint(gcc.longitude::double precision, gcc.latitude::double precision), 4326)
        )
      ORDER BY ST_Area(ST_Envelope(f.geom)) ASC NULLS LAST, f.name ASC
      LIMIT 1
    ) jur_rel ON TRUE

    LEFT JOIN LATERAL (
      SELECT
        f.name AS quadrant_name,
        f.code AS quadrant_code,
        f.dependency_name,
        f.jurisdiction_name,
        'spatial_quadrant'::text AS matched_by
      FROM geo_features f
      INNER JOIN geo_layers l ON l.id = f.layer_id
      WHERE f.is_active = TRUE
        AND l.code = 'cuadrantes'
        AND gcc.latitude IS NOT NULL
        AND gcc.longitude IS NOT NULL
        AND ST_Intersects(
          f.geom,
          ST_SetSRID(ST_MakePoint(gcc.longitude::double precision, gcc.latitude::double precision), 4326)
        )
      ORDER BY f.name ASC
      LIMIT 1
    ) quad_rel ON TRUE

    LEFT JOIN LATERAL (
      SELECT
        f.name AS department_name,
        f.code AS departmental_code,
        'spatial_departmental'::text AS matched_by
      FROM geo_features f
      INNER JOIN geo_layers l ON l.id = f.layer_id
      WHERE f.is_active = TRUE
        AND l.code = 'departamentales'
        AND gcc.latitude IS NOT NULL
        AND gcc.longitude IS NOT NULL
        AND ST_Intersects(
          f.geom,
          ST_SetSRID(ST_MakePoint(gcc.longitude::double precision, gcc.latitude::double precision), 4326)
        )
      ORDER BY f.name ASC
      LIMIT 1
    ) dep_poly_rel ON TRUE

    LEFT JOIN LATERAL (
      SELECT
        d.department_name,
        d.dependency_name,
        d.code AS dependency_code,
        'catalog_department_match'::text AS matched_by
      FROM geo_features d
      INNER JOIN geo_layers l ON l.id = d.layer_id
      WHERE d.is_active = TRUE
        AND l.code = 'dependencias'
        AND gcc.department_catalog IS NOT NULL
        AND upper(coalesce(d.department_name, '')) = upper(coalesce(gcc.department_catalog, ''))
      ORDER BY d.name ASC
      LIMIT 1
    ) dep_catalog ON TRUE
  `
};