import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { buildApiDbPoolConfig } from "../../src/api/apiDbPool.js";

/**
 * Live round-trip for the API pool deadlines: an exhausted pool and a
 * blocked statement both fail within their bounds, and the pool keeps
 * serving afterwards. Whether Postgres actually cancels the statement (and
 * hands the client back reusable) is server behaviour, not something the
 * config object shows.
 *
 * Needs a live Postgres (DATABASE_URL). CI runs it in the migrate job, which
 * provides one; the unit-test job skips it.
 */

const databaseUrl = process.env.DATABASE_URL;

describe.skipIf(!databaseUrl)("API DB pool deadlines (requires DATABASE_URL)", () => {
  let pool: Pool;

  beforeAll(() => {
    pool = new Pool({
      ...buildApiDbPoolConfig({ connectionString: databaseUrl!, connectionTimeoutMs: 300, statementTimeoutMs: 300 }),
      max: 1,
    });
  });

  afterAll(async () => {
    await pool.end();
  });

  it("fails a checkout within the acquisition bound when the pool is exhausted, then serves again", async () => {
    const held = await pool.connect();
    try {
      const started = Date.now();
      await expect(pool.query("SELECT 1")).rejects.toThrow(/timeout exceeded when trying to connect/);
      expect(Date.now() - started).toBeLessThan(2_000);
    } finally {
      held.release();
    }
    await expect(pool.query<{ ok: number }>("SELECT 1 AS ok")).resolves.toMatchObject({ rows: [{ ok: 1 }] });
  });

  it("cancels a statement server-side at the statement deadline and leaves the same client reusable", async () => {
    // Hold ONE client: pool.query() would discard an errored client and open a
    // replacement, so only a held client can show that the cancel left this
    // connection usable. The BEGIN/ROLLBACK mirrors the authService
    // transaction path, where the timeout fires mid-transaction.
    const client = await pool.connect();
    try {
      const { rows: before } = await client.query<{ pid: number }>("SELECT pg_backend_pid() AS pid");
      await client.query("BEGIN");
      const started = Date.now();
      await expect(client.query("SELECT pg_sleep(5)")).rejects.toMatchObject({ code: "57014" }); // query_canceled
      expect(Date.now() - started).toBeLessThan(2_000);
      await client.query("ROLLBACK");
      const { rows: after } = await client.query<{ pid: number }>("SELECT pg_backend_pid() AS pid");
      expect(after[0].pid).toBe(before[0].pid);
    } finally {
      client.release();
    }
    await expect(pool.query<{ ok: number }>("SELECT 2 AS ok")).resolves.toMatchObject({ rows: [{ ok: 2 }] });
    expect(pool.totalCount).toBe(1);
  });
});
