import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { replaceNewMexicoCandidateFinanceSnapshot } from "../../../src/pipeline/newMexicoFinance/newMexicoFinanceWriter.js";

/**
 * Live round-trip for the snapshot writer's pool boundary. Before the guard,
 * a real PoolClient (which has both connect and release) slipped past the
 * "pool?" check into the direct branch, and the writer issued BEGIN/COMMIT
 * on the caller's client — committing whatever transaction the caller had
 * open. That is only observable with a real connection: a mock cannot show
 * that the caller's own work was committed out from under it.
 *
 * Needs a live Postgres (DATABASE_URL). CI runs it in the migrate job.
 */

const databaseUrl = process.env.DATABASE_URL;

describe.skipIf(!databaseUrl)("New Mexico snapshot writer pool boundary (requires DATABASE_URL)", () => {
  let pool: Pool;

  beforeAll(() => {
    pool = new Pool({ connectionString: databaseUrl, max: 2 });
  });

  afterAll(async () => {
    await pool.end();
  });

  it("rejects a caller's transaction client and leaves that transaction untouched", async () => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      // Work the caller has done inside its own transaction; it must still be
      // rollback-able after the writer refuses the client.
      await client.query("CREATE TEMP TABLE caller_tx_marker (x int) ON COMMIT PRESERVE ROWS");
      await client.query("INSERT INTO caller_tx_marker VALUES (1)");

      await expect(
        replaceNewMexicoCandidateFinanceSnapshot({
          db: client,
          link: {
            candidateId: "11111111-1111-1111-1111-111111111111",
            electionId: "22222222-2222-2222-2222-222222222222",
            electionYear: 2026,
            candidateNameNormalized: "POOL BOUNDARY",
            officeName: "Governor",
            committeeId: "1001",
            committeeName: "Boundary for New Mexico",
            linkSource: "cfis_bulk",
          },
          summary: { totalReceipts: 1 },
        })
      ).rejects.toThrow("New Mexico finance snapshot writes must receive a Pool, not a PoolClient");

      // Still inside the caller's transaction: the marker is visible here…
      const inside = await client.query<{ n: string }>("SELECT count(*)::text AS n FROM caller_tx_marker");
      expect(inside.rows[0]?.n).toBe("1");
      await client.query("ROLLBACK");
      // …and gone after the caller's ROLLBACK, i.e. nothing committed it.
      const after = await client.query<{ present: string | null }>("SELECT to_regclass('pg_temp.caller_tx_marker')::text AS present");
      expect(after.rows[0]?.present).toBeNull();
    } finally {
      await client.query("ROLLBACK").catch(() => undefined);
      client.release();
    }
  });
});
