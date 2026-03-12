import app from "./app.js";
import { env } from "./config/env.js";
import { testDbConnection } from "./config/db.js";

async function bootstrap() {
  try {
    const dbInfo = await testDbConnection();
    console.log("✅ DB conectada");
    console.log("DB time:", dbInfo.now);
    console.log("DB version:", dbInfo.version.split("\n")[0]);

    app.listen(env.port, () => {
      console.log(`GeoSeguridad backend corriendo en http://localhost:${env.port}`);
    });
  } catch (error) {
    console.error("❌ No se pudo iniciar el backend:", error.message);
    process.exit(1);
  }
}

bootstrap();