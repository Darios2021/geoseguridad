export function notFoundHandler(req, res, next) {
  return res.status(404).json({
    ok: false,
    message: `Ruta no encontrada: ${req.method} ${req.originalUrl}`
  });
}

export function errorHandler(err, req, res, next) {
  console.error("GLOBAL ERROR:", {
    method: req.method,
    url: req.originalUrl,
    message: err?.message,
    stack: err?.stack
  });

  if (err?.name === "MulterError") {
    return res.status(400).json({
      ok: false,
      message:
        err.code === "LIMIT_FILE_SIZE"
          ? "El archivo excede el tamaño máximo permitido."
          : err.message || "Error procesando el archivo enviado."
    });
  }

  return res.status(err?.status || 500).json({
    ok: false,
    message: err?.message || "Internal Server Error"
  });
}