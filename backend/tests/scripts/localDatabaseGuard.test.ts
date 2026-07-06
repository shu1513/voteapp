import { afterEach, describe, expect, it } from "vitest";

import { requireLocalDatabaseTarget } from "../../src/scripts/localDatabaseGuard.js";

describe("requireLocalDatabaseTarget", () => {
  const originalOverride = process.env.ALLOW_REMOTE_DB_WRITES;
  const originalAllowedHosts = process.env.LOCAL_DATABASE_ALLOWED_HOSTS;

  afterEach(() => {
    if (originalOverride === undefined) {
      delete process.env.ALLOW_REMOTE_DB_WRITES;
    } else {
      process.env.ALLOW_REMOTE_DB_WRITES = originalOverride;
    }
    if (originalAllowedHosts === undefined) {
      delete process.env.LOCAL_DATABASE_ALLOWED_HOSTS;
    } else {
      process.env.LOCAL_DATABASE_ALLOWED_HOSTS = originalAllowedHosts;
    }
  });

  it("allows loopback postgres urls", () => {
    expect(() => requireLocalDatabaseTarget("postgresql://localhost:5432/voteapp")).not.toThrow();
    expect(() => requireLocalDatabaseTarget("postgres://127.0.0.1/voteapp")).not.toThrow();
    expect(() => requireLocalDatabaseTarget("postgresql://[::1]:5432/voteapp")).not.toThrow();
  });

  it("allows common Docker-local postgres hosts", () => {
    expect(() => requireLocalDatabaseTarget("postgresql://host.docker.internal:5432/voteapp")).not.toThrow();
    expect(() => requireLocalDatabaseTarget("postgresql://172.17.0.1:5432/voteapp")).not.toThrow();
  });

  it("allows local effective hosts from query params and Unix sockets", () => {
    expect(() => requireLocalDatabaseTarget("postgresql://db.example.com:5432/voteapp?host=localhost")).not.toThrow();
    expect(() => requireLocalDatabaseTarget("postgresql:///voteapp?host=/var/run/postgresql")).not.toThrow();
  });

  it("allows reviewed local host aliases from env", () => {
    process.env.LOCAL_DATABASE_ALLOWED_HOSTS = "dev-postgres.local";

    expect(() => requireLocalDatabaseTarget("postgresql://dev-postgres.local:5432/voteapp")).not.toThrow();
  });

  it("rejects remote postgres urls unless explicitly overridden", () => {
    expect(() => requireLocalDatabaseTarget("postgresql://db.example.com:5432/voteapp")).toThrow(
      "Refusing manual write to non-local DATABASE_URL host"
    );

    process.env.ALLOW_REMOTE_DB_WRITES = "1";
    expect(() => requireLocalDatabaseTarget("postgresql://db.example.com:5432/voteapp")).not.toThrow();
  });

  it("rejects remote effective hosts from query params", () => {
    expect(() => requireLocalDatabaseTarget("postgresql://localhost:5432/voteapp?host=db.example.com")).toThrow(
      'Refusing manual write to non-local DATABASE_URL host "db.example.com"'
    );
  });

  it("rejects malformed and non-postgres urls", () => {
    expect(() => requireLocalDatabaseTarget("not-a-url")).toThrow("DATABASE_URL must be a postgres://");
    expect(() => requireLocalDatabaseTarget("mysql://localhost/voteapp")).toThrow("unsupported DATABASE_URL protocol");
  });

  it("rejects empty database urls", () => {
    expect(() => requireLocalDatabaseTarget("")).toThrow("DATABASE_URL is required");
    expect(() => requireLocalDatabaseTarget("   ")).toThrow("DATABASE_URL is required");
  });
});
