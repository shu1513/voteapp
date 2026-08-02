import { parse as parsePostgresConnectionString } from "pg-connection-string";

const DEFAULT_LOCAL_DATABASE_HOSTS = new Set([
  "",
  "localhost",
  "127.0.0.1",
  "::1",
  "[::1]",
  "host.docker.internal",
  "host.containers.internal",
  "gateway.docker.internal",
  "172.17.0.1",
]);

function normalizeHost(host: string): string {
  return host.trim().toLowerCase();
}

function isLocalDatabaseHost(host: string, allowedHosts: Set<string>): boolean {
  const normalized = normalizeHost(host);
  return normalized.startsWith("/") || allowedHosts.has(normalized);
}

function readAllowedLocalDatabaseHosts(): Set<string> {
  const hosts = new Set(DEFAULT_LOCAL_DATABASE_HOSTS);
  for (const host of (process.env.LOCAL_DATABASE_ALLOWED_HOSTS ?? "").split(",")) {
    const normalized = normalizeHost(host);
    if (normalized.length > 0) {
      hosts.add(normalized);
    }
  }
  return hosts;
}

/**
 * Same local-only guard for Redis: manual staging scripts publish pipeline
 * messages, and a remote REDIS_URL would let a manual run enqueue work on a
 * production stream even when the database target is local.
 */
export function requireLocalRedisTarget(redisUrl = process.env.REDIS_URL ?? ""): void {
  if (process.env.ALLOW_REMOTE_REDIS_WRITES?.trim() === "1") {
    return;
  }

  const trimmed = redisUrl.trim();
  if (!trimmed) {
    throw new Error("REDIS_URL is required");
  }

  let parsed: URL;
  try {
    parsed = new URL(trimmed);
  } catch {
    throw new Error("Refusing manual write: REDIS_URL must be a redis:// or rediss:// URL");
  }

  if (parsed.protocol !== "redis:" && parsed.protocol !== "rediss:") {
    throw new Error(`Refusing manual write: unsupported REDIS_URL protocol ${parsed.protocol}`);
  }

  const hosts = new Set(DEFAULT_LOCAL_DATABASE_HOSTS);
  for (const host of (process.env.LOCAL_REDIS_ALLOWED_HOSTS ?? "").split(",")) {
    const normalized = normalizeHost(host);
    if (normalized.length > 0) {
      hosts.add(normalized);
    }
  }

  if (!isLocalDatabaseHost(parsed.hostname, hosts)) {
    throw new Error(
      `Refusing manual write to non-local REDIS_URL host "${parsed.hostname}". ` +
        "Use LOCAL_REDIS_ALLOWED_HOSTS for reviewed local aliases, or set ALLOW_REMOTE_REDIS_WRITES=1 only for an intentional remote write."
    );
  }
}

export function requireLocalDatabaseTarget(databaseUrl = process.env.DATABASE_URL ?? ""): void {
  if (process.env.ALLOW_REMOTE_DB_WRITES?.trim() === "1") {
    return;
  }

  const trimmed = databaseUrl.trim();
  if (!trimmed) {
    throw new Error("DATABASE_URL is required");
  }

  let parsed: URL;
  try {
    parsed = new URL(trimmed);
  } catch {
    throw new Error("Refusing manual write: DATABASE_URL must be a postgres:// or postgresql:// URL");
  }

  if (parsed.protocol !== "postgres:" && parsed.protocol !== "postgresql:") {
    throw new Error(`Refusing manual write: unsupported DATABASE_URL protocol ${parsed.protocol}`);
  }

  const effectiveHost = String(parsePostgresConnectionString(trimmed).host ?? "");
  if (!isLocalDatabaseHost(effectiveHost, readAllowedLocalDatabaseHosts())) {
    throw new Error(
      `Refusing manual write to non-local DATABASE_URL host "${effectiveHost}". ` +
        "Use LOCAL_DATABASE_ALLOWED_HOSTS for reviewed local aliases, or set ALLOW_REMOTE_DB_WRITES=1 only for an intentional remote write."
    );
  }
}
