import { asyncHandler } from "../../../utils/asyncHandler.js";
import { getFeatures, getHealth, getLayers } from "../services/geo.service.js";

export const health = asyncHandler(async (req, res) => {
  const data = await getHealth();
  return res.json({ ok: true, data });
});

export const listLayers = asyncHandler(async (req, res) => {
  const data = await getLayers();
  return res.json({ ok: true, data });
});

export const listFeatures = asyncHandler(async (req, res) => {
  const data = await getFeatures({
    layer: req.query.layer,
    department: req.query.department,
    dependency: req.query.dependency,
    status: req.query.status,
    limit: req.query.limit
  });

  return res.json({
    ok: true,
    type: "FeatureCollection",
    count: data.length,
    features: data
  });
});