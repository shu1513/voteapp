import type { Pool, PoolClient } from "pg";

import { isUuid } from "../../utils/uuid.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export const CANDIDATE_RECORD_UPDATE_EVENT_TYPE = "candidate_record_update";
export const CANDIDATE_FUTURE_ELECTION_EVENT_TYPE = "candidate_future_election";

export type CandidateFollowNotificationEventCreationResult = {
  createdCount: number;
};

export type CandidateFollowNotificationEventsErrorCode =
  | "invalid_candidate_record_id"
  | "invalid_candidate_id"
  | "invalid_election_id";

export class CandidateFollowNotificationEventsError extends Error {
  constructor(
    readonly code: CandidateFollowNotificationEventsErrorCode,
    message: string
  ) {
    super(message);
    this.name = "CandidateFollowNotificationEventsError";
  }
}

function normalizeUuid(value: string, code: CandidateFollowNotificationEventsErrorCode, label: string): string {
  const normalized = value.trim();
  if (!isUuid(normalized)) {
    throw new CandidateFollowNotificationEventsError(code, `${label} must be a valid UUID`);
  }
  return normalized;
}

export async function createCandidateRecordUpdateNotificationEvents(
  db: Queryable,
  candidateRecordId: string
): Promise<CandidateFollowNotificationEventCreationResult> {
  const normalizedCandidateRecordId = normalizeUuid(
    candidateRecordId,
    "invalid_candidate_record_id",
    "Candidate record ID"
  );

  const result = await db.query<{ id: string }>(
    `
      WITH source_record AS (
        SELECT
          record.id AS candidate_record_id,
          record.candidate_id
        FROM public.candidate_records AS record
        JOIN public.candidates AS candidate
          ON candidate.id = record.candidate_id
        WHERE record.id = $1::uuid
          AND candidate.deleted_at IS NULL
          AND candidate.merged_into_candidate_id IS NULL
      ),
      eligible_follows AS (
        SELECT
          follow.user_id,
          source_record.candidate_id,
          source_record.candidate_record_id
        FROM source_record
        JOIN public.user_candidate_follows AS follow
          ON follow.candidate_id = source_record.candidate_id
        JOIN public.users AS user_row
          ON user_row.id = follow.user_id
        WHERE follow.notify_updates = true
          AND user_row.deleted_at IS NULL
      )
      INSERT INTO public.user_candidate_follow_notification_events (
        user_id,
        candidate_id,
        event_type,
        candidate_record_id
      )
      SELECT
        eligible_follows.user_id,
        eligible_follows.candidate_id,
        $2,
        eligible_follows.candidate_record_id
      FROM eligible_follows
      ON CONFLICT DO NOTHING
      RETURNING id
    `,
    [normalizedCandidateRecordId, CANDIDATE_RECORD_UPDATE_EVENT_TYPE]
  );

  return { createdCount: result.rowCount ?? result.rows.length };
}

export async function createCandidateFutureElectionNotificationEvents(
  db: Queryable,
  input: { candidateId: string; electionId: string }
): Promise<CandidateFollowNotificationEventCreationResult> {
  const normalizedCandidateId = normalizeUuid(input.candidateId, "invalid_candidate_id", "Candidate ID");
  const normalizedElectionId = normalizeUuid(input.electionId, "invalid_election_id", "Election ID");

  const result = await db.query<{ id: string }>(
    `
      WITH source_event AS (
        SELECT
          candidate.id AS candidate_id,
          election.id AS election_id
        FROM public.candidates AS candidate
        JOIN public.candidate_elections AS candidate_election
          ON candidate_election.candidate_id = candidate.id
        JOIN public.elections AS election
          ON election.id = candidate_election.election_id
        WHERE candidate.id = $1::uuid
          AND election.id = $2::uuid
          AND candidate.deleted_at IS NULL
          AND candidate.merged_into_candidate_id IS NULL
          AND election.election_date >= CURRENT_DATE
      ),
      eligible_follows AS (
        SELECT
          follow.user_id,
          source_event.candidate_id,
          source_event.election_id
        FROM source_event
        JOIN public.user_candidate_follows AS follow
          ON follow.candidate_id = source_event.candidate_id
        JOIN public.users AS user_row
          ON user_row.id = follow.user_id
        WHERE follow.notify_elections = true
          AND user_row.deleted_at IS NULL
      )
      INSERT INTO public.user_candidate_follow_notification_events (
        user_id,
        candidate_id,
        event_type,
        election_id
      )
      SELECT
        eligible_follows.user_id,
        eligible_follows.candidate_id,
        $3,
        eligible_follows.election_id
      FROM eligible_follows
      ON CONFLICT DO NOTHING
      RETURNING id
    `,
    [normalizedCandidateId, normalizedElectionId, CANDIDATE_FUTURE_ELECTION_EVENT_TYPE]
  );

  return { createdCount: result.rowCount ?? result.rows.length };
}
