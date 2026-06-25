import type { Server } from "node:http";
import { Pool } from "pg";
import { createClient } from "redis";

import { createTrustedUserIdResolver } from "../api/addressApiAuth.js";
import { createTrustedClientIpResolver } from "../api/addressApiClientIp.js";
import type { AddressResolutionDiagnostics } from "../api/addressApiResponses.js";
import { createApiApp } from "../api/apiServer.js";
import { loadProjectEnv } from "../config/env.js";
import {
  createInMemoryAddressApiRateLimiter,
  DEFAULT_ADDRESS_API_RATE_LIMIT_MAX_BUCKETS,
  DEFAULT_ADDRESS_API_RATE_LIMIT_MAX_REQUESTS,
  DEFAULT_ADDRESS_API_RATE_LIMIT_WINDOW_MS,
} from "../api/addressApiRateLimiter.js";
import { lookupBallotSummariesByDistrictIds, lookupElectionDetailById } from "../pipeline/address/ballotLookup.js";
import { resolveAddressToDistricts } from "../pipeline/address/addressResolverService.js";
import { DEFAULT_ADDRESS_LOOKUP_CACHE_TTL_SECONDS } from "../pipeline/address/addressResolutionCache.js";
import {
  DEFAULT_CENSUS_ADDRESS_GEOCODER_BENCHMARK,
  DEFAULT_CENSUS_ADDRESS_GEOCODER_LAYERS,
  DEFAULT_CENSUS_ADDRESS_GEOCODER_TIMEOUT_MS,
  DEFAULT_CENSUS_ADDRESS_GEOCODER_VINTAGE,
} from "../pipeline/address/censusAddressGeocoder.js";
import { updateAuthenticatedAddressDistricts } from "../pipeline/users/userAddressDistrictUpdater.js";
import { listUserCandidateFollows, setUserCandidateFollow } from "../pipeline/users/userCandidateFollows.js";
import { initializeUserDistricts } from "../pipeline/users/userDistrictInitializer.js";
import { listUserDistrictIds } from "../pipeline/users/userDistrictReader.js";
import { replaceUserDistricts } from "../pipeline/users/userDistrictReplacer.js";
import {
  listSelectableResearchAreas,
  listUserResearchAreaPreferences,
  replaceUserResearchAreaPreferences,
} from "../pipeline/users/userResearchAreaPreferences.js";

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

function readBooleanEnv(name: string, fallback: boolean): boolean {
  const raw = process.env[name]?.trim().toLowerCase();
  if (!raw) {
    return fallback;
  }
  if (["1", "true", "yes", "on"].includes(raw)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(raw)) {
    return false;
  }
  throw new Error(`Invalid ${name}: ${process.env[name]}`);
}

function readAllowedOrigins(): string[] {
  return (process.env.ADDRESS_API_ALLOWED_ORIGINS ?? "")
    .split(",")
    .map((origin) => origin.trim())
    .filter((origin) => origin.length > 0);
}

function readOptionalEnv(name: string): string | null {
  const value = process.env[name]?.trim();
  return value && value.length > 0 ? value : null;
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
  // Security boundary: only trust a client-IP header when a trusted proxy/gateway
  // owns that header and strips client-supplied copies. Otherwise rate limiting uses
  // the direct socket IP.
  const trustedClientIpHeader = readOptionalEnv("ADDRESS_API_TRUSTED_CLIENT_IP_HEADER");
  if (trustedClientIpHeader) {
    console.warn(
      `address API trusting client IP header "${trustedClientIpHeader}"; ensure the edge proxy strips client-supplied copies`
    );
  }
  // Security boundary: only trust a user-id header when an authenticated gateway
  // injects it and strips client-supplied copies. If unset, authenticated routes
  // fail closed with 401.
  const trustedUserIdHeader = readOptionalEnv("API_TRUSTED_USER_ID_HEADER");
  if (trustedUserIdHeader) {
    console.warn(
      `API trusting authenticated user header "${trustedUserIdHeader}"; ensure the edge proxy authenticates requests and strips client-supplied copies`
    );
  }
  const addressCacheEnabled = readBooleanEnv("ADDRESS_LOOKUP_CACHE_ENABLED", true);
  const rateLimitEnabled = readBooleanEnv("ADDRESS_API_RATE_LIMIT_ENABLED", true);
  const rateLimitWindowMs = readPositiveIntegerEnv(
    "ADDRESS_API_RATE_LIMIT_WINDOW_MS",
    DEFAULT_ADDRESS_API_RATE_LIMIT_WINDOW_MS
  );
  const rateLimitMaxRequests = readPositiveIntegerEnv(
    "ADDRESS_API_RATE_LIMIT_MAX_REQUESTS",
    DEFAULT_ADDRESS_API_RATE_LIMIT_MAX_REQUESTS
  );
  const rateLimitMaxBuckets = readPositiveIntegerEnv(
    "ADDRESS_API_RATE_LIMIT_MAX_BUCKETS",
    DEFAULT_ADDRESS_API_RATE_LIMIT_MAX_BUCKETS
  );
  const rateLimit = rateLimitEnabled
    ? createInMemoryAddressApiRateLimiter({
        windowMs: rateLimitWindowMs,
        maxRequests: rateLimitMaxRequests,
        maxBuckets: rateLimitMaxBuckets,
      })
    : undefined;
  const addressCacheTtlSeconds = readPositiveIntegerEnv(
    "ADDRESS_LOOKUP_CACHE_TTL_SECONDS",
    DEFAULT_ADDRESS_LOOKUP_CACHE_TTL_SECONDS
  );
  const redis = addressCacheEnabled ? createClient({ url: readEnv("REDIS_URL", "redis://localhost:6379") }) : null;
  const buildAddressResolverOptions = () => ({
    cache: redis?.isOpen ? redis : undefined,
    cacheTtlSeconds: addressCacheTtlSeconds,
    geocoderOptions: {
      benchmark: readEnv("CENSUS_ADDRESS_GEOCODER_BENCHMARK", DEFAULT_CENSUS_ADDRESS_GEOCODER_BENCHMARK),
      vintage: readEnv("CENSUS_ADDRESS_GEOCODER_VINTAGE", DEFAULT_CENSUS_ADDRESS_GEOCODER_VINTAGE),
      layers: readEnv("CENSUS_ADDRESS_GEOCODER_LAYERS", DEFAULT_CENSUS_ADDRESS_GEOCODER_LAYERS),
      timeoutMs: readPositiveIntegerEnv("CENSUS_ADDRESS_GEOCODER_TIMEOUT_MS", DEFAULT_CENSUS_ADDRESS_GEOCODER_TIMEOUT_MS),
    },
  });
  if (redis) {
    redis.on("error", (error) => {
      console.warn("address lookup cache Redis error; continuing without failing requests", error);
    });
    try {
      await redis.connect();
    } catch (error) {
      console.warn("address lookup cache disabled: failed to connect to Redis", error);
    }
  }

  const app = createApiApp({
    allowedOrigins,
    rateLimit,
    resolveClientIp: createTrustedClientIpResolver(trustedClientIpHeader),
    resolveAuthenticatedUserId: createTrustedUserIdResolver(trustedUserIdHeader),
    logDiagnostics: logAddressResolutionDiagnostics,
    lookupBallotSummaries: (districtIds) => lookupBallotSummariesByDistrictIds(pool, districtIds),
    lookupAuthenticatedBallotSummaries: async (userId) => {
      const districtIds = await listUserDistrictIds(pool, userId);
      return lookupBallotSummariesByDistrictIds(pool, districtIds);
    },
    lookupElectionDetail: (electionId) => lookupElectionDetailById(pool, electionId),
    listResearchAreas: () => listSelectableResearchAreas(pool),
    listAuthenticatedCandidateFollows: (userId) => listUserCandidateFollows(pool, userId),
    setAuthenticatedCandidateFollow: (userId, input) => setUserCandidateFollow(pool, userId, input),
    listAuthenticatedResearchAreaPreferences: (userId) => listUserResearchAreaPreferences(pool, userId),
    replaceAuthenticatedResearchAreaPreferences: (userId, preferences) =>
      replaceUserResearchAreaPreferences(pool, userId, preferences),
    updateAuthenticatedAddressDistricts: (userId, address) =>
      updateAuthenticatedAddressDistricts(
        {
          resolveAddressToDistricts: (inputAddress) =>
            resolveAddressToDistricts(pool, inputAddress, buildAddressResolverOptions()),
          replaceUserDistricts: (inputUserId, districtIds) => replaceUserDistricts(pool, inputUserId, districtIds),
          lookupBallotSummariesByDistrictIds: (districtIds) => lookupBallotSummariesByDistrictIds(pool, districtIds),
        },
        userId,
        address
      ),
    initializeUserDistricts: ({ userId, districtIds }) => initializeUserDistricts(pool, userId, districtIds),
    resolveAddress: (address) =>
      resolveAddressToDistricts(pool, address, buildAddressResolverOptions()),
  });

  let server: Server | null = null;
  await new Promise<void>((resolve, reject) => {
    const startedServer = app.listen(port, host, () => {
      startedServer.off("error", reject);
      resolve();
    });
    startedServer.once("error", reject);
    server = startedServer;
  });

  console.log(
    `address API server listening on http://${host}:${port} allowed_origins=${
      allowedOrigins.length > 0 ? allowedOrigins.join(",") : "none"
    }`
  );

  const shutdown = async (): Promise<void> => {
    await new Promise<void>((resolve, reject) => {
      if (!server) {
        resolve();
        return;
      }
      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve();
      });
    });
    await pool.end();
    if (redis?.isOpen) {
      await redis.quit();
    }
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
