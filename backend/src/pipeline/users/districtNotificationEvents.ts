import type { Pool, PoolClient } from "pg";

import { isUuid } from "../../utils/uuid.js";
import { US_LATEST_LOCAL_DATE_SQL } from "../../utils/usLocalDate.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type DistrictNotificationEventCreationResult = {
  createdCount: number;
};

export class DistrictNotificationEventsError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "DistrictNotificationEventsError";
  }
}

/**
 * Fans a batch of freshly inserted elections out to users who live in the
 * election's district and have email_new_election_alerts on. The caller (the
 * elections writer) passes only rows it just INSERTed — updates never
 * re-notify — and the SQL re-checks that the election is still future-dated.
 * Verified-email filtering happens at send time, not here, so a user who
 * verifies later still gets the alert.
 */
export async function createDistrictNewElectionNotificationEvents(
  db: Queryable,
  electionIds: readonly string[]
): Promise<DistrictNotificationEventCreationResult> {
  if (electionIds.length === 0) {
    return { createdCount: 0 };
  }
  const normalizedElectionIds = electionIds.map((electionId) => {
    const normalized = electionId.trim();
    if (!isUuid(normalized)) {
      throw new DistrictNotificationEventsError("Election IDs must be valid UUIDs");
    }
    return normalized;
  });

  const result = await db.query<{ id: string }>(
    `
      WITH source_elections AS (
        SELECT
          election.id AS election_id,
          election.district_id
        FROM public.elections AS election
        WHERE election.id = ANY($1::uuid[])
          AND election.election_date >= ${US_LATEST_LOCAL_DATE_SQL}
      ),
      eligible_users AS (
        SELECT
          user_district.user_id,
          source_elections.election_id
        FROM source_elections
        JOIN public.user_districts AS user_district
          ON user_district.district_id = source_elections.district_id
        JOIN public.users AS user_row
          ON user_row.id = user_district.user_id
        WHERE user_row.deleted_at IS NULL
          AND user_row.email_new_election_alerts = true
      )
      INSERT INTO public.user_district_notification_events (
        user_id,
        election_id
      )
      SELECT
        eligible_users.user_id,
        eligible_users.election_id
      FROM eligible_users
      ON CONFLICT DO NOTHING
      RETURNING id
    `,
    [normalizedElectionIds]
  );

  return { createdCount: result.rowCount ?? result.rows.length };
}
