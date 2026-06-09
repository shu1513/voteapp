import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { createAddressApiServer, type AddressResolutionDiagnostics } from "../api/addressApiServer.js";
import { resolveAddressToDistricts } from "../pipeline/address/addressResolverService.js";
import {
  DEFAULT_CENSUS_ADDRESS_GEOCODER_BENCHMARK,
  DEFAULT_CENSUS_ADDRESS_GEOCODER_LAYERS,
  DEFAULT_CENSUS_ADDRESS_GEOCODER_TIMEOUT_MS,
  DEFAULT_CENSUS_ADDRESS_GEOCODER_VINTAGE,
} from "../pipeline/address/censusAddressGeocoder.js";

function readEnv(name: string, fallback?: string): string {
  const value = process.env[name]?.trim() || fallback;
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function readPort(): number {
  const raw = process.env.ADDRESS_API_PORT ?? "3001";
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isInteger(parsed) || parsed <= 0 || parsed > 65535) {
    throw new Error(`Invalid ADDRESS_API_PORT: ${raw}`);
  }
  return parsed;
}

function readPositiveIntegerEnv(name: string, fallback: number): number {
  const raw = process.env[name]?.trim();
  if (!raw) {
    return fallback;
  }
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Invalid ${name}: ${raw}`);
  }
  return parsed;
}

function readAllowedOrigins(): string[] {
  return (process.env.ADDRESS_API_ALLOWED_ORIGINS ?? "")
    .split(",")
    .map((origin) => origin.trim())
    .filter((origin) => origin.length > 0);
}

function logAddressResolutionDiagnostics(diagnostics: AddressResolutionDiagnostics): void {
  if (diagnostics.missing_district_keys.length === 0 && diagnostics.warnings.length === 0) {
    return;
  }
  console.warn(
    JSON.stringify({
      type: "address_resolution_diagnostics",
      ts: new Date().toISOString(),
      address_match_count: diagnostics.address_match_count,
      district_key_count: diagnostics.district_keys.length,
      missing_district_keys: diagnostics.missing_district_keys,
      warnings: diagnostics.warnings,
    })
  );
}

async function main(): Promise<void> {
  loadProjectEnv();
  const pool = new Pool({ connectionString: readEnv("DATABASE_URL", "postgresql://localhost:5432/voteapp") });
  const host = process.env.ADDRESS_API_HOST?.trim() || "127.0.0.1";
  const port = readPort();
  const allowedOrigins = readAllowedOrigins();

  const server = createAddressApiServer({
    allowedOrigins,
    logDiagnostics: logAddressResolutionDiagnostics,
    resolveAddress: (address) =>
      resolveAddressToDistricts(pool, address, {
        geocoderOptions: {
          benchmark: readEnv("CENSUS_ADDRESS_GEOCODER_BENCHMARK", DEFAULT_CENSUS_ADDRESS_GEOCODER_BENCHMARK),
          vintage: readEnv("CENSUS_ADDRESS_GEOCODER_VINTAGE", DEFAULT_CENSUS_ADDRESS_GEOCODER_VINTAGE),
          layers: readEnv("CENSUS_ADDRESS_GEOCODER_LAYERS", DEFAULT_CENSUS_ADDRESS_GEOCODER_LAYERS),
          timeoutMs: readPositiveIntegerEnv(
            "CENSUS_ADDRESS_GEOCODER_TIMEOUT_MS",
            DEFAULT_CENSUS_ADDRESS_GEOCODER_TIMEOUT_MS
          ),
        },
      }),
  });

  await new Promise<void>((resolve) => {
    server.listen(port, host, resolve);
  });

  console.log(
    `address API server listening on http://${host}:${port} allowed_origins=${
      allowedOrigins.length > 0 ? allowedOrigins.join(",") : "none"
    }`
  );

  const shutdown = async (): Promise<void> => {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve();
      });
    });
    await pool.end();
  };

  const handleShutdown = (signal: NodeJS.Signals): void => {
    void shutdown()
      .then(() => {
        console.log(`address API server stopped after ${signal}`);
        process.exit(0);
      })
      .catch((error) => {
        console.error("address API server shutdown failed:", error);
        process.exit(1);
      });
  };

  process.on("SIGINT", handleShutdown);
  process.on("SIGTERM", handleShutdown);
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error("address API server failed:", message);
  process.exitCode = 1;
});
