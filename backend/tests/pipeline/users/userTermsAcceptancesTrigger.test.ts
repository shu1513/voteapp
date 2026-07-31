import { Client } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

/**
 * Live round-trip for the append-only guard on user_terms_acceptances
 * (migration 201).
 *
 * The guard is doing something subtle enough that reading the SQL is not
 * enough to believe it: UPDATE must always fail, a direct DELETE must fail,
 * and yet the DELETE that cascades from removing the account must succeed —
 * otherwise account deletion, which the privacy policy promises, breaks. It
 * separates the two by checking whether the parent users row still exists,
 * which depends on Postgres firing the referential action after the parent
 * row is gone. That is behaviour, not text, so it needs a real database.
 *
 * Needs a live Postgres (DATABASE_URL) with migrations applied. CI runs it in
 * the migrate job, which provides one; the unit-test job skips it.
 */

const databaseUrl = process.env.DATABASE_URL;

const TEST_EMAIL = "terms-acceptance-trigger@example.test";

describe.skipIf(!databaseUrl)("user_terms_acceptances append-only guard (requires DATABASE_URL)", () => {
  let client: Client;
  let userId: string;

  beforeAll(async () => {
    client = new Client({ connectionString: databaseUrl });
    await client.connect();
    // Left over from a previous interrupted run; the cascade clears its rows.
    await client.query("DELETE FROM public.users WHERE email = $1::citext", [TEST_EMAIL]);

    const inserted = await client.query<{ id: string }>(
      `
        INSERT INTO public.users (first_name, email, password_hash, accepted_terms_version, accepted_terms_at)
        VALUES ('Trigger', $1::citext, 'not-a-real-hash', '1.1', now())
        RETURNING id::text AS id
      `,
      [TEST_EMAIL]
    );
    userId = inserted.rows[0]!.id;

    await client.query(
      `
        INSERT INTO public.user_terms_acceptances (user_id, terms_version, context)
        VALUES ($1::uuid, '1.1', 'registration')
      `,
      [userId]
    );
  });

  afterAll(async () => {
    if (client) {
      await client.query("DELETE FROM public.users WHERE email = $1::citext", [TEST_EMAIL]);
      await client.end();
    }
  });

  it("rejects UPDATE, so a recorded acceptance cannot be rewritten", async () => {
    await expect(
      client.query("UPDATE public.user_terms_acceptances SET terms_version = '9.9' WHERE user_id = $1::uuid", [
        userId,
      ])
    ).rejects.toThrow(/append-only/);
  });

  it("rejects a direct DELETE, so the evidence cannot be quietly removed", async () => {
    await expect(
      client.query("DELETE FROM public.user_terms_acceptances WHERE user_id = $1::uuid", [userId])
    ).rejects.toThrow(/removed only by deleting the account/);
  });

  it("still lets the rows go when the account itself is deleted", async () => {
    const before = await client.query<{ count: string }>(
      "SELECT count(*)::text AS count FROM public.user_terms_acceptances WHERE user_id = $1::uuid",
      [userId]
    );
    expect(before.rows[0]!.count).toBe("1");

    // The promise the privacy policy makes. If the guard ever starts blocking
    // this, account deletion fails outright.
    await client.query("DELETE FROM public.users WHERE id = $1::uuid", [userId]);

    const after = await client.query<{ count: string }>(
      "SELECT count(*)::text AS count FROM public.user_terms_acceptances WHERE user_id = $1::uuid",
      [userId]
    );
    expect(after.rows[0]!.count).toBe("0");
  });
});
