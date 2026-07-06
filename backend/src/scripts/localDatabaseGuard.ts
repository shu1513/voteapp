const LOOPBACK_HOSTS = new Set(["", "localhost", "127.0.0.1", "::1", "[::1]"]);

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

  const host = parsed.hostname.trim().toLowerCase();
  if (!LOOPBACK_HOSTS.has(host)) {
    throw new Error(
      `Refusing manual write to non-local DATABASE_URL host "${parsed.hostname}". ` +
        "Set ALLOW_REMOTE_DB_WRITES=1 only for an intentional, reviewed remote write."
    );
  }
}
