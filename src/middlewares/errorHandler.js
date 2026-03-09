export function notFoundHandler(req, res) {
  return res.status(404).json({
    ok: false,
    message: `Ruta no encontrada: ${req.method} ${req.originalUrl}`
  });
}

export function errorHandler(err, req, res, next) {
  console.error("❌ Error:", err);

  return res.status(err.status || 500).json({
    ok: false,
    message: err.message || "Error interno del servidor",
    ...(process.env.NODE_ENV !== "production" ? { stack: err.stack } : {})
  });
}
