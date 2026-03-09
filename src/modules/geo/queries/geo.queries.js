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
  `
};
