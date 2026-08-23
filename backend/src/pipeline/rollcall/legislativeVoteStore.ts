import type { Pool } from "pg";

import type { LegislativeVoteChamber, LegislativeVoteReviewStatus } from "./legislativeVotes.js";

type Queryable = Pick<Pool, "query">;

// The columns the fetcher owns on public.legislative_votes (migration 251).
// Judgment columns (yea_description, nay_description, labels_json,
// review_status, reviewed_at) belong to the AI pass and the reviewer and are
// never touched here.
export type LegislativeVoteSourceRow = {
  jurisdiction: string;
  chamber: LegislativeVoteChamber;
  session: string;
  rollNumber: number;
  // ISO date, YYYY-MM-DD.
  voteDate: string;
  measureId: string | null;
  exactQuestion: string;
  isFloorVote: boolean | null;
  result: string;
  yeas: number;
  nays: number;
  displayUrl: string;
  machineUrl: string;
  billUrl: string | null;
  sourceSha256: string;
  fetchedAt: Date;
  importerVersion: string;
};

export type LegislativeVoteUpsertOutcome =
  // No row for this roll key yet.
  | "inserted"
  // Row existed and at least one source field differs (the feed republished,
  // or the parser/classifier changed).
  | "updated"
  // Row existed with identical source fields; only fetched_at and
  // importer_version advanced.
  | "unchanged"
  // Row is approved and the source differs. Approval covers the row as the
  // reviewer saw it, so nothing is written (the DB trigger would reject it
  // anyway); the run report surfaces it for a human to re-review.
  | "approved_conflict";

export type LegislativeVoteUpsertResult = {
  outcome: LegislativeVoteUpsertOutcome;
  id: string;
  reviewStatus: LegislativeVoteReviewStatus;
  // True when an update changed what the judgment is ABOUT (the question or
  // the measure), so the stored sentences/labels were cleared and the row
  // went back to pending for a fresh AI pass and review.
  judgmentCleared: boolean;
};

type ExistingRow = {
  id: string;
  review_status: LegislativeVoteReviewStatus;
  vote_date: string;
  measure_id: string | null;
  exact_question: string;
  is_floor_vote: boolean | null;
  result: string;
  yeas: number;
  nays: number;
  display_url: string;
  machine_url: string;
  bill_url: string | null;
  source_sha256: string;
};

// The AI sentences and labels are written against the question and the
// measure. Tallies, result, date, URLs, and the file hash can all change
// (the Senate republishes with a modify_date; the House corrects totals)
// without changing what the vote was about, so they never discard a paid
// judgment or a reviewer's decision.
function judgmentInputsMatch(existing: ExistingRow, row: LegislativeVoteSourceRow): boolean {
  return existing.exact_question === row.exactQuestion && existing.measure_id === row.measureId;
}

function sourceFieldsMatch(existing: ExistingRow, row: LegislativeVoteSourceRow): boolean {
  return (
    existing.vote_date === row.voteDate &&
    existing.measure_id === row.measureId &&
    existing.exact_question === row.exactQuestion &&
    existing.is_floor_vote === row.isFloorVote &&
    existing.result === row.result &&
    existing.yeas === row.yeas &&
    existing.nays === row.nays &&
    existing.display_url === row.displayUrl &&
    existing.machine_url === row.machineUrl &&
    existing.bill_url === row.billUrl &&
    existing.source_sha256 === row.sourceSha256
  );
}

/**
 * Writes one fetched roll call under its (jurisdiction, chamber, session,
 * roll_number) key. Each call is its own statement pair — no transaction —
 * so one bad roll call never holds up the rest of a run.
 */
export async function upsertLegislativeVoteSource(
  db: Queryable,
  row: LegislativeVoteSourceRow
): Promise<LegislativeVoteUpsertResult> {
  const existing = await db.query<ExistingRow>(
    `SELECT id,
            review_status,
            vote_date::text AS vote_date,
            measure_id,
            exact_question,
            is_floor_vote,
            result,
            yeas,
            nays,
            display_url,
            machine_url,
            bill_url,
            source_sha256
       FROM legislative_votes
      WHERE jurisdiction = $1
        AND chamber = $2
        AND session = $3
        AND roll_number = $4`,
    [row.jurisdiction, row.chamber, row.session, row.rollNumber]
  );
  const current = existing.rows[0];

  if (!current) {
    const inserted = await db.query<{ id: string; review_status: LegislativeVoteReviewStatus }>(
      `INSERT INTO legislative_votes (
         jurisdiction, chamber, session, roll_number, vote_date, measure_id,
         exact_question, is_floor_vote, result, yeas, nays, display_url,
         machine_url, bill_url, source_sha256, fetched_at, importer_version
       ) VALUES (
         $1, $2, $3, $4, $5::date, $6,
         $7, $8, $9, $10, $11, $12,
         $13, $14, $15, $16::timestamptz, $17
       )
       RETURNING id, review_status`,
      [
        row.jurisdiction,
        row.chamber,
        row.session,
        row.rollNumber,
        row.voteDate,
        row.measureId,
        row.exactQuestion,
        row.isFloorVote,
        row.result,
        row.yeas,
        row.nays,
        row.displayUrl,
        row.machineUrl,
        row.billUrl,
        row.sourceSha256,
        row.fetchedAt.toISOString(),
        row.importerVersion,
      ]
    );
    const created = inserted.rows[0]!;
    return { outcome: "inserted", id: created.id, reviewStatus: created.review_status, judgmentCleared: false };
  }

  if (sourceFieldsMatch(current, row)) {
    await db.query(
      `UPDATE legislative_votes
          SET fetched_at = $2::timestamptz,
              importer_version = $3
        WHERE id = $1`,
      [current.id, row.fetchedAt.toISOString(), row.importerVersion]
    );
    return { outcome: "unchanged", id: current.id, reviewStatus: current.review_status, judgmentCleared: false };
  }

  if (current.review_status === "approved") {
    return {
      outcome: "approved_conflict",
      id: current.id,
      reviewStatus: current.review_status,
      judgmentCleared: false,
    };
  }

  // A different question or measure makes any stored judgment a judgment
  // of something else; a reviewer must not be able to approve it. Clearing
  // the sentences/labels and returning to pending keeps the
  // approved_fields and reviewed_at CHECKs satisfied.
  const judgmentCleared = !judgmentInputsMatch(current, row);
  const judgmentReset = judgmentCleared
    ? `,
            yea_description = NULL,
            nay_description = NULL,
            labels_json = NULL,
            review_status = 'pending',
            reviewed_at = NULL`
    : "";

  await db.query(
    `UPDATE legislative_votes
        SET vote_date = $2::date,
            measure_id = $3,
            exact_question = $4,
            is_floor_vote = $5,
            result = $6,
            yeas = $7,
            nays = $8,
            display_url = $9,
            machine_url = $10,
            bill_url = $11,
            source_sha256 = $12,
            fetched_at = $13::timestamptz,
            importer_version = $14${judgmentReset}
      WHERE id = $1`,
    [
      current.id,
      row.voteDate,
      row.measureId,
      row.exactQuestion,
      row.isFloorVote,
      row.result,
      row.yeas,
      row.nays,
      row.displayUrl,
      row.machineUrl,
      row.billUrl,
      row.sourceSha256,
      row.fetchedAt.toISOString(),
      row.importerVersion,
    ]
  );
  return {
    outcome: "updated",
    id: current.id,
    reviewStatus: judgmentCleared ? "pending" : current.review_status,
    judgmentCleared,
  };
}
