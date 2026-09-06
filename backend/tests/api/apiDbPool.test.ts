import { describe, expect, it, vi } from "vitest";

import {
  API_DB_QUERY_TIMEOUT_MARGIN_MS,
  attachApiDbPoolErrorHandler,
  buildApiDbPoolConfig,
} from "../../src/api/apiDbPool.js";

describe("buildApiDbPoolConfig", () => {
  it("bounds client acquisition and statement execution, with the client read timeout behind the server deadline", () => {
    const config = buildApiDbPoolConfig({
      connectionString: "postgresql://localhost:5432/voteapp",
      connectionTimeoutMs: 10_000,
      statementTimeoutMs: 30_000,
    });
    expect(config).toEqual({
      connectionString: "postgresql://localhost:5432/voteapp",
      connectionTimeoutMillis: 10_000,
      statement_timeout: 30_000,
      query_timeout: 30_000 + API_DB_QUERY_TIMEOUT_MARGIN_MS,
    });
    // pg's query_timeout errors locally without sending a cancel, so the
    // server-side statement_timeout must always fire first.
    expect(config.query_timeout).toBeGreaterThan(config.statement_timeout);
  });

  it("rejects non-positive or fractional timeouts", () => {
    for (const bad of [0, -1, 1.5, Number.NaN]) {
      expect(() =>
        buildApiDbPoolConfig({ connectionString: "x", connectionTimeoutMs: bad, statementTimeoutMs: 1 })
      ).toThrow("connectionTimeoutMs must be a positive integer");
      expect(() =>
        buildApiDbPoolConfig({ connectionString: "x", connectionTimeoutMs: 1, statementTimeoutMs: bad })
      ).toThrow("statementTimeoutMs must be a positive integer");
    }
  });
});

describe("attachApiDbPoolErrorHandler", () => {
  it("logs and captures an idle-client error instead of letting it escape as an unhandled 'error' event", () => {
    const listeners = new Map<string, (error: unknown) => void>();
    const pool = { on: vi.fn((event: string, listener: (error: unknown) => void) => listeners.set(event, listener)) };
    const capture = vi.fn();
    const log = vi.fn();

    attachApiDbPoolErrorHandler(pool as never, capture, log);

    const failure = new Error("terminating connection due to administrator command");
    expect(() => listeners.get("error")?.(failure)).not.toThrow();
    expect(log).toHaveBeenCalledWith(expect.stringContaining("idle database client error"), failure);
    expect(capture).toHaveBeenCalledWith(failure, { source: "pg_pool_idle_client" });
  });

  it("never throws out of the listener when monitoring itself fails", () => {
    let listener: ((error: unknown) => void) | undefined;
    const pool = { on: vi.fn((_event: string, fn: (error: unknown) => void) => (listener = fn)) };
    attachApiDbPoolErrorHandler(
      pool as never,
      () => {
        throw new Error("sentry down");
      },
      () => undefined
    );
    expect(() => listener?.(new Error("boom"))).not.toThrow();
  });
});
