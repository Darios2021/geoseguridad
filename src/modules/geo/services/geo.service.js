import { pool } from "../../../config/db.js";
import { GEO_QUERIES } from "../queries/geo.queries.js";

export async function getLayers() {
  const result = await pool.query(GEO_QUERIES.layers);
  return result.rows;
}

export async function getFeatures(filters = {}) {
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

  return result.rows.map((row) => ({
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
  }));
}

import { pool } from "../../../config/db.js";
import { GEO_QUERIES } from "../queries/geo.queries.js";

export async function getLayers() {
  const result = await pool.query(GEO_QUERIES.layers);
  return result.rows;
}

export async function getFeatures(filters = {}) {
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

  return result.rows.map((row) => ({
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
  }));
}

export async function getGeoTree(filters = {}) {
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
    ${where}
    ORDER BY
      COALESCE(f.department_name, 'ZZZ') ASC,
      COALESCE(f.dependency_name, 'ZZZ') ASC,
      COALESCE(f.jurisdiction_name, 'ZZZ') ASC,
      l.code ASC,
      f.name ASC
  `;

  const { rows } = await pool.query(sql, values);

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
        jurisdictionMap: new Map()
      });
    }

    return departmentNode.dependencyMap.get(key);
  }

  function ensureJurisdiction(dependencyNode, rawName) {
    const raw = String(rawName || "").trim();
    const key = raw || "JURISDICCIÓN";

    if (!dependencyNode.jurisdictionMap.has(key)) {
      const displayName =
        !raw || raw === dependencyNode.name ? "Jurisdicción" : raw;

      dependencyNode.jurisdictionMap.set(key, {
        id: `jurisdiction:${dependencyNode.id}:${key}`,
        type: "jurisdiction",
        name: displayName,
        feature: null,
        groupsMap: new Map()
      });
    }

    return dependencyNode.jurisdictionMap.get(key);
  }

  function ensureGroup(jurisdictionNode, groupKind) {
    const key = groupKind;
    const groupName = groupKind === "camaras" ? "Cámaras" : "Cuadrantes";

    if (!jurisdictionNode.groupsMap.has(key)) {
      jurisdictionNode.groupsMap.set(key, {
        id: `${jurisdictionNode.id}:group:${key}`,
        type: "group",
        name: groupName,
        groupKind,
        children: []
      });
    }

    return jurisdictionNode.groupsMap.get(key);
  }

  for (const row of rows) {
    const layerCode = row.layer_code;
    const departmentName = row.department_name || "SIN DEPARTAMENTAL";
    const dependencyName =
      row.dependency_name || row.jurisdiction_name || "SIN DEPENDENCIA";
    const jurisdictionName =
      row.jurisdiction_name || dependencyName || "JURISDICCIÓN";

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

    const jurisdictionNode = ensureJurisdiction(
      dependencyNode,
      jurisdictionName
    );

    if (layerCode === "jurisdicciones") {
      jurisdictionNode.feature = makeFeature(row, jurisdictionNode.name);
      continue;
    }

    if (layerCode === "cuadrantes") {
      const groupNode = ensureGroup(jurisdictionNode, "cuadrantes");
      groupNode.children.push({
        id: `quadrant:${row.id}`,
        type: "quadrant",
        name: row.name,
        feature: makeFeature(row, row.name)
      });
      continue;
    }

    if (layerCode === "camaras") {
      const groupNode = ensureGroup(jurisdictionNode, "camaras");
      groupNode.children.push({
        id: `camera:${row.id}`,
        type: "camera",
        name: row.name,
        feature: makeFeature(row, row.name)
      });
    }
  }

  const tree = [...departments.values()].map((departmentNode) => {
    const dependencies = [...departmentNode.dependencyMap.values()].map(
      (dependencyNode) => {
        const jurisdictions = [...dependencyNode.jurisdictionMap.values()].map(
          (jurisdictionNode) => {
            const groups = [...jurisdictionNode.groupsMap.values()]
              .map((groupNode) => ({
                id: groupNode.id,
                type: groupNode.type,
                name: groupNode.name,
                groupKind: groupNode.groupKind,
                children: [...groupNode.children].sort((a, b) =>
                  String(a.name).localeCompare(String(b.name))
                )
              }))
              .sort((a, b) => String(a.name).localeCompare(String(b.name)));

            return {
              id: jurisdictionNode.id,
              type: jurisdictionNode.type,
              name: jurisdictionNode.name,
              feature: jurisdictionNode.feature || null,
              children: groups
            };
          }
        );

        return {
          id: dependencyNode.id,
          type: dependencyNode.type,
          name: dependencyNode.name,
          feature: dependencyNode.feature || null,
          children: jurisdictions.sort((a, b) =>
            String(a.name).localeCompare(String(b.name))
          )
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