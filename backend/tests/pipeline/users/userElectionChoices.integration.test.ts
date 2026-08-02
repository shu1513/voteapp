import { Pool, type PoolClient } from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";

import { setUserElectionChoice } from "../../../src/pipeline/users/userElectionChoices.js";

// Opt-in like userDistrictInitializer.integration.test.ts: needs a real
// Postgres because it exercises the same-statement eligibility gate against
// a concurrent catalog write — semantics mocks cannot reproduce.
const integrationEnabled = process.env.USER_CHOICES_INTEGRATION === "true";
const integrationDatabaseUrl = process.env.USER_CHOICES_INTEGRATION_DATABASE_URL;
const describeIntegration = integrationEnabled && integrationDatabaseUrl ? describe : describe.skip;

const userId = "aaaaaaaa-aaaa-4aaa-8aaa-abcabcabcab1";
const districtId = "bbbbbbbb-bbbb-4bbb-8bbb-abcabcabcab2";
const electionId = "cccccccc-cccc-4ccc-8ccc-abcabcabcab3";
const candidateId = "dddddddd-dddd-4ddd-8ddd-abcabcabcab4";

async function cleanup(pool: Pool): Promise<void> {
  await pool.query("DELETE FROM public.user_election_choices WHERE user_id = $1", [userId]);
  await pool.query("DELETE FROM public.users WHERE id = $1", [userId]);
  await pool.query("DELETE FROM public.candidate_elections WHERE election_id = $1", [electionId]);
  await pool.query("DELETE FROM public.candidates WHERE id = $1", [candidateId]);
  await pool.query("DELETE FROM public.elections WHERE id = $1", [electionId]);
  await pool.query("DELETE FROM public.districts WHERE id = $1", [districtId]);
}

async function seed(pool: Pool): Promise<void> {
  await pool.query(
    `
      INSERT INTO public.users (id, first_name, email, password_hash)
      VALUES ($1, 'Integration', $2, 'not-a-real-hash')
    `,
    [userId, `user-choices-${process.pid}@example.test`]
  );
  await pool.query(
    `
      INSERT INTO public.districts (
        id, geoid_compact, name, state, state_fips, district_type, population, representation_power_score
      )
      VALUES ($1, '99901', 'Choices Integration County', 'CA', '99', 'county', 100, 1.00)
    `,
    [districtId]
  );
  await pool.query(
    `
      INSERT INTO public.elections (
        id, district_id, official_ballot_title, official_ballot_title_key,
        election_date, sources, race_type
      )
      VALUES ($1, $2, 'Choices Integration Race', 'choices integration race',
              CURRENT_DATE + 30, '["https://example.test/choices-integration"]'::jsonb, 'office')
    `,
    [electionId, districtId]
  );
  await pool.query(
    `
      INSERT INTO public.candidates (id, first_name, last_name, party, state)
      VALUES ($1, 'Race', 'Condition', 'Independent', 'CA')
    `,
    [candidateId]
  );
  await pool.query(
    `
      INSERT INTO public.candidate_elections (candidate_id, election_id, status)
      VALUES ($1, $2, 'declared')
    `,
    [candidateId, electionId]
  );
}

/**
 * A Pool facade whose client runs `sabotage` right before the first statement
 * matching `trigger` — the write lands after validation has already passed.
 *
 * Scope: the sabotage COMMITS before the gated statement runs, which is the
 * interleaving the same-statement gate is built to refuse. A catalog
 * transaction still uncommitted when the gate takes its snapshot is
 * deliberately NOT covered — that residual is accepted, not defended
 * (see the comments on the gated INSERTs in userElectionChoices.ts).
 */
function sabotagedDb(pool: Pool, trigger: string, sabotage: () => Promise<void>) {
  return {
    connect: async () => {
      const client: PoolClient = await pool.connect();
      let fired = false;
      const facade = {
        query: async (text: string, values?: unknown[]) => {
          if (!fired && text.includes(trigger)) {
            fired = true;
            await sabotage();
          }
          return client.query(text, values as never);
        },
        release: () => client.release(),
      };
      return facade as unknown as PoolClient;
    },
  };
}

describeIntegration("setUserElectionChoice concurrency", () => {
  let pool: Pool;

  beforeAll(() => {
    pool = new Pool({ connectionString: integrationDatabaseUrl });
  });

  beforeEach(async () => {
    await cleanup(pool);
    await seed(pool);
  });

  afterAll(async () => {
    if (pool) {
      await cleanup(pool);
      await pool.end();
    }
  });

  it("records a pick for an active candidacy", async () => {
    const result = await setUserElectionChoice(pool, userId, { electionId, candidateId, chosen: true });
    expect(result.choice.picks.map((pick) => pick.candidate_id)).toEqual([candidateId]);
  });

  it("refuses a pick when the candidacy is withdrawn after validation, before the write", async () => {
    const db = sabotagedDb(pool, "INSERT INTO public.user_election_choices", async () => {
      await pool.query(
        "UPDATE public.candidate_elections SET status = 'withdrawn' WHERE candidate_id = $1 AND election_id = $2",
        [candidateId, electionId]
      );
    });

    await expect(
      setUserElectionChoice(db as never, userId, { electionId, candidateId, chosen: true })
    ).rejects.toMatchObject({ code: "candidacy_not_available" });

    const rows = await pool.query("SELECT 1 FROM public.user_election_choices WHERE user_id = $1", [userId]);
    expect(rows.rowCount).toBe(0);
  });

  it("refuses a pick when the election closes after validation, before the write", async () => {
    const db = sabotagedDb(pool, "INSERT INTO public.user_election_choices", async () => {
      await pool.query("UPDATE public.elections SET election_date = CURRENT_DATE - 1 WHERE id = $1", [electionId]);
    });

    await expect(
      setUserElectionChoice(db as never, userId, { electionId, candidateId, chosen: true })
    ).rejects.toMatchObject({ code: "election_closed" });

    const rows = await pool.query("SELECT 1 FROM public.user_election_choices WHERE user_id = $1", [userId]);
    expect(rows.rowCount).toBe(0);
  });
});
