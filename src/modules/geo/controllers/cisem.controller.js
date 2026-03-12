import { fetchCisemGeoJSON } from "../services/cisem.api.js";

export async function getCisemCamerasGeoJSON(req, res, next) {
  try {
    const data = await fetchCisemGeoJSON();

    res.json(data);
  } catch (err) {
    next(err);
  }
}