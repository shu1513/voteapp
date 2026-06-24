import { Pool } from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";

import { initializeUserDistricts } from "../../../src/pipeline/users/userDistrictInitializer.js";
import { listUserDistrictIds } from "../../../src/pipeline/users/userDistrictReader.js";

const integrationEnabled = process.env.USER_DISTRICTS_INTEGRATION === "true";
const integrationDatabaseUrl = process.env.USER_DISTRICTS_INTEGRATION_DATABASE_URL;
const describeIntegration = integrationEnabled && integrationDatabaseUrl ? describe : describe.skip;

const userId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const districtIdA = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
const districtIdB = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
const districtIdC = "dddddddd-dddd-4ddd-8ddd-dddddddddddd";
const districtIdD = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee";

async function cleanup(pool: Pool): Promise<void> {
  await pool.query("DELETE FROM public.user_districts WHERE user_id = $1", [userId]);
  await pool.query("DELETE FROM public.users WHERE id = $1", [userId]);
  await pool.query("DELETE FROM public.districts WHERE id = ANY($1::uuid[])", [
    [districtIdA, districtIdB, districtIdC, districtIdD],
  ]);
}

async function seedUserAndDistricts(pool: Pool): Promise<void> {
  await pool.query(
    `
      INSERT INTO public.users (id, first_name, email, password_hash)
      VALUES ($1, 'Integration', $2, 'not-a-real-hash')
    `,
    [userId, `user-districts-${Date.now()}@example.test`]
  );
  await pool.query(
    `
      INSERT INTO public.districts (
        id,
        geoid_compact,
        name,
        state,
        state_fips,
        district_type,
        population,
        representation_power_score
      )
      VALUES
        ($1, '06037', 'Los Angeles County', 'CA', '06', 'county', 100, 1.00),
        ($2, '0631', 'California Congressional District 31', 'CA', '06', 'us_house', 100, 1.00),
        ($3, '06022', 'California State Senate District 22', 'CA', '06', 'state_upper', 100, 1.00),
        ($4, '06048', 'California Assembly District 48', 'CA', '06', 'state_lower', 100, 1.00)
    `,
    [districtIdA, districtIdB, districtIdC, districtIdD]
  );
}

describeIntegration("initializeUserDistricts integration", () => {
  let pool: Pool;

  beforeAll(() => {
    pool = new Pool({ connectionString: integrationDatabaseUrl });
  });

  beforeEach(async () => {
    await cleanup(pool);
    await seedUserAndDistricts(pool);
  });

  afterAll(async () => {
    if (pool) {
      await cleanup(pool);
      await pool.end();
    }
  });

  it("serializes concurrent first-time initialization and stores only one district set", async () => {
    const [left, right] = await Promise.all([
      initializeUserDistricts(pool, userId, [districtIdA, districtIdB]),
      initializeUserDistricts(pool, userId, [districtIdC, districtIdD]),
    ]);

    const statuses = [left.status, right.status].sort();
    expect(statuses).toEqual(["already_initialized", "initialized"]);

    const saved = await pool.query<{ district_id: string; district_type: string }>(
      `
        SELECT district_id::text, district_type
        FROM public.user_districts
        WHERE user_id = $1
        ORDER BY district_id
      `,
      [userId]
    );

    const savedIds = saved.rows.map((row) => row.district_id).sort();
    expect([
      [districtIdA, districtIdB].sort(),
      [districtIdC, districtIdD].sort(),
    ]).toContainEqual(savedIds);
    expect(saved.rows).toHaveLength(2);
    expect(new Set(saved.rows.map((row) => row.district_type)).size).toBe(2);
  });

  it("returns initialized saved districts through the user district reader", async () => {
    await initializeUserDistricts(pool, userId, [districtIdA, districtIdB, districtIdA.toUpperCase()]);

    const savedDistrictIds = await listUserDistrictIds(pool, userId);

    expect(savedDistrictIds).toHaveLength(2);
    expect([...savedDistrictIds].sort()).toEqual([districtIdA, districtIdB].sort());
  });
});
