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
      l.name AS layer_name,
      f.code,
      f.name,
      f.department_name,
      f.dependency_name,
      f.jurisdiction_name,
      f.feature_type,
      f.status,
      f.properties
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

  const result = await pool.query(sql, values);
  const rows = result.rows;

  const departmentMap = new Map();

  function ensureDepartment(name) {
    const key = String(name || "SIN DEPARTAMENTAL").trim();

    if (!departmentMap.has(key)) {
      departmentMap.set(key, {
        id: `department:${key}`,
        type: "department",
        name: key,
        counts: {
          dependencias: 0,
          jurisdicciones: 0,
          cuadrantes: 0,
          camaras: 0
        },
        childrenMap: new Map()
      });
    }

    return departmentMap.get(key);
  }

  function ensureDependency(departmentNode, name) {
    const key = String(name || "SIN DEPENDENCIA").trim();

    if (!departmentNode.childrenMap.has(key)) {
      departmentNode.childrenMap.set(key, {
        id: `dependency:${departmentNode.name}:${key}`,
        type: "dependency",
        name: key,
        counts: {
          jurisdicciones: 0,
          cuadrantes: 0,
          camaras: 0
        },
        childrenMap: new Map(),
        cameraChildren: []
      });
    }

    return departmentNode.childrenMap.get(key);
  }

  function ensureJurisdiction(dependencyNode, name) {
    const key = String(name || dependencyNode.name || "SIN JURISDICCION").trim();

    if (!dependencyNode.childrenMap.has(key)) {
      dependencyNode.childrenMap.set(key, {
        id: `jurisdiction:${dependencyNode.id}:${key}`,
        type: "jurisdiction",
        name: key,
        counts: {
          cuadrantes: 0,
          camaras: 0
        },
        quadrantChildren: [],
        cameraChildren: []
      });
    }

    return dependencyNode.childrenMap.get(key);
  }

  function pushUniqueById(list, item) {
    if (!list.some((entry) => entry.id === item.id)) {
      list.push(item);
    }
  }

  for (const row of rows) {
    const layerCode = row.layer_code;
    const featureName = row.name || row.code || row.id;
    const departmentName = row.department_name || "SIN DEPARTAMENTAL";
    const dependencyName =
      row.dependency_name ||
      row.jurisdiction_name ||
      "SIN DEPENDENCIA";
    const jurisdictionName =
      row.jurisdiction_name ||
      row.dependency_name ||
      dependencyName;

    const departmentNode = ensureDepartment(departmentName);
    const dependencyNode = ensureDependency(departmentNode, dependencyName);

    if (layerCode === "dependencias") {
      departmentNode.counts.dependencias += 1;
    }

    if (layerCode === "jurisdicciones") {
      const jurisdictionNode = ensureJurisdiction(
        dependencyNode,
        jurisdictionName
      );

      departmentNode.counts.jurisdicciones += 1;
      dependencyNode.counts.jurisdicciones += 1;

      jurisdictionNode.feature = {
        id: row.id,
        layerCode,
        name: featureName,
        code: row.code,
        featureType: row.feature_type
      };

      continue;
    }

    if (layerCode === "cuadrantes") {
      const jurisdictionNode = ensureJurisdiction(
        dependencyNode,
        jurisdictionName
      );

      const quadrantItem = {
        id: `quadrant:${row.id}`,
        type: "quadrant",
        name: featureName,
        feature: {
          id: row.id,
          layerCode,
          name: featureName,
          code: row.code,
          featureType: row.feature_type
        }
      };

      departmentNode.counts.cuadrantes += 1;
      dependencyNode.counts.cuadrantes += 1;
      jurisdictionNode.counts.cuadrantes += 1;

      pushUniqueById(jurisdictionNode.quadrantChildren, quadrantItem);
      continue;
    }

    if (layerCode === "camaras") {
      const jurisdictionNode = ensureJurisdiction(
        dependencyNode,
        jurisdictionName
      );

      const cameraItem = {
        id: `camera:${row.id}`,
        type: "camera",
        name: featureName,
        feature: {
          id: row.id,
          layerCode,
          name: featureName,
          code: row.code,
          featureType: row.feature_type
        }
      };

      departmentNode.counts.camaras += 1;
      dependencyNode.counts.camaras += 1;
      jurisdictionNode.counts.camaras += 1;

      pushUniqueById(jurisdictionNode.cameraChildren, cameraItem);
      continue;
    }

    if (layerCode === "departamentales") {
      departmentNode.feature = {
        id: row.id,
        layerCode,
        name: featureName,
        code: row.code,
        featureType: row.feature_type
      };
    }
  }

  const tree = [...departmentMap.values()].map((departmentNode) => {
    const dependencies = [...departmentNode.childrenMap.values()].map(
      (dependencyNode) => {
        const jurisdictions = [...dependencyNode.childrenMap.values()].map(
          (jurisdictionNode) => ({
            id: jurisdictionNode.id,
            type: jurisdictionNode.type,
            name: jurisdictionNode.name,
            counts: jurisdictionNode.counts,
            feature: jurisdictionNode.feature || null,
            children: [
              ...jurisdictionNode.quadrantChildren,
              ...jurisdictionNode.cameraChildren
            ].sort((a, b) => String(a.name).localeCompare(String(b.name)))
          })
        );

        return {
          id: dependencyNode.id,
          type: dependencyNode.type,
          name: dependencyNode.name,
          counts: dependencyNode.counts,
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
      counts: departmentNode.counts,
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