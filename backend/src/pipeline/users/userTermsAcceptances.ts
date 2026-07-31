import type { Pool, PoolClient } from "pg";

type Queryable = Pick<Pool | PoolClient, "query">;

/**
 * How the acceptance was given. 'backfill' exists only for the rows migration
 * 201 reconstructed from the users columns, where the original context was
 * never recorded; nothing writes it at runtime.
 */
export type TermsAcceptanceContext = "registration" | "renewal";

export type RecordTermsAcceptanceInput = {
  userId: string;
  termsVersion: string;
  context: TermsAcceptanceContext;
};

/**
 * Appends one acceptance to a user's history.
 *
 * MUST run in the same transaction as the write that sets
 * users.accepted_terms_version. If the two can come apart, the users row can
 * end up claiming a version with no history behind it — which is the failure
 * this table exists to prevent.
 *
 * The caller is responsible for only passing the current terms version;
 * authService and apiServer both check that before reaching here.
 */
export async function recordTermsAcceptance(
  db: Queryable,
  input: RecordTermsAcceptanceInput
): Promise<void> {
  await db.query(
    `
      INSERT INTO public.user_terms_acceptances (user_id, terms_version, context)
      VALUES ($1::uuid, $2, $3)
    `,
    [input.userId, input.termsVersion, input.context]
  );
}
