import { afterEach, describe, expect, it } from "vitest";

import { requireLocalDatabaseTarget } from "../../src/scripts/localDatabaseGuard.js";

describe("requireLocalDatabaseTarget", () => {
  const originalOverride = process.env.ALLOW_REMOTE_DB_WRITES;

  afterEach(() => {
    if (originalOverride === undefined) {
      delete process.env.ALLOW_REMOTE_DB_WRITES;
    } else {
      process.env.ALLOW_REMOTE_DB_WRITES = originalOverride;
    }
  });

  it("allows loopback postgres urls", () => {
    expect(() => requireLocalDatabaseTarget("postgresql://localhost:5432/voteapp")).not.toThrow();
    expect(() => requireLocalDatabaseTarget("postgres://127.0.0.1/voteapp")).not.toThrow();
    expect(() => requireLocalDatabaseTarget("postgresql://[::1]:5432/voteapp")).not.toThrow();
  });

  it("rejects remote postgres urls unless explicitly overridden", () => {
    expect(() => requireLocalDatabaseTarget("postgresql://db.example.com:5432/voteapp")).toThrow(
      "Refusing manual write to non-local DATABASE_URL host"
    );

    process.env.ALLOW_REMOTE_DB_WRITES = "1";
    expect(() => requireLocalDatabaseTarget("postgresql://db.example.com:5432/voteapp")).not.toThrow();
  });

  it("rejects malformed and non-postgres urls", () => {
    expect(() => requireLocalDatabaseTarget("not-a-url")).toThrow("DATABASE_URL must be a postgres://");
    expect(() => requireLocalDatabaseTarget("mysql://localhost/voteapp")).toThrow("unsupported DATABASE_URL protocol");
  });
});
