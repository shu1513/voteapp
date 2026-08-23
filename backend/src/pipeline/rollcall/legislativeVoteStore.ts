import type { Pool } from "pg";

import { parseFederalMeasure } from "./federalMeasures.js";
import type { LegislativeVoteChamber, LegislativeVoteReviewStatus } from "./legislativeVotes.js";

type Queryable = Pick<Pool, "query">;

// The columns the fetcher owns on public.legislative_votes (migration 251).
// Judgment columns (yea_description, nay_description, labels_json,
// review_status, reviewed_at) are written only by applyLegislativeVoteJudgment
// (rollcall:judge) and read by the importer (loadLegislativeVote).
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

// What the fan-out needs from one reviewed row.
export type LegislativeVoteForImport = {
  id: string;
  voteDate: string;
  measureId: string | null;
  exactQuestion: string;
  isFloorVote: boolean | null;
  yeas: number;
  nays: number;
  machineUrl: string;
  sourceSha256: string;
  yeaDescription: string | null;
  nayDescription: string | null;
  labelsJson: unknown;
  reviewStatus: LegislativeVoteReviewStatus;
};

export async function loadLegislativeVote(
  db: Queryable,
  key: { jurisdiction: string; chamber: LegislativeVoteChamber; session: string; rollNumber: number }
): Promise<LegislativeVoteForImport | null> {
  const result = await db.query<{
    id: string;
    vote_date: string;
    measure_id: string | null;
    exact_question: string;
    is_floor_vote: boolean | null;
    yeas: number;
    nays: number;
    machine_url: string;
    source_sha256: string;
    yea_description: string | null;
    nay_description: string | null;
    labels_json: unknown;
    review_status: LegislativeVoteReviewStatus;
  }>(
    `SELECT id,
            vote_date::text AS vote_date,
            measure_id,
            exact_question,
            is_floor_vote,
            yeas,
            nays,
            machine_url,
            source_sha256,
            yea_description,
            nay_description,
            labels_json,
            review_status
       FROM legislative_votes
      WHERE jurisdiction = $1
        AND chamber = $2
        AND session = $3
        AND roll_number = $4`,
    [key.jurisdiction, key.chamber, key.session, key.rollNumber]
  );
  const row = result.rows[0];
  if (!row) {
    return null;
  }
  return {
    id: row.id,
    voteDate: row.vote_date,
    measureId: row.measure_id,
    exactQuestion: row.exact_question,
    isFloorVote: row.is_floor_vote,
    yeas: row.yeas,
    nays: row.nays,
    machineUrl: row.machine_url,
    sourceSha256: row.source_sha256,
    yeaDescription: row.yea_description,
    nayDescription: row.nay_description,
    labelsJson: row.labels_json,
    reviewStatus: row.review_status,
  };
}

/**
 * Re-reads the row inside the fan-out's transaction, holding a share lock so
 * a reviewer's "back to pending" cannot commit underneath the write, and
 * checks that what is about to fan out is still the approved judgment: the
 * row was loaded seconds earlier, before the sentence validation's network
 * round trip, and a revoked or re-approved judgment must not replicate.
 */
export async function assertLegislativeVoteStillApproved(db: Queryable, vote: LegislativeVoteForImport): Promise<void> {
  const result = await db.query<{
    review_status: LegislativeVoteReviewStatus;
    source_sha256: string;
    yea_description: string | null;
    nay_description: string | null;
    labels_json: unknown;
  }>(
    `SELECT review_status, source_sha256, yea_description, nay_description, labels_json
       FROM legislative_votes
      WHERE id = $1
      FOR SHARE`,
    [vote.id]
  );
  const row = result.rows[0];
  if (!row) {
    throw new Error(`legislative_votes row ${vote.id} no longer exists`);
  }
  if (row.review_status !== "approved") {
    throw new Error(`legislative_votes row ${vote.id} is ${row.review_status} now; approval was withdrawn during the run`);
  }
  if (
    row.source_sha256 !== vote.sourceSha256 ||
    row.yea_description !== vote.yeaDescription ||
    row.nay_description !== vote.nayDescription ||
    JSON.stringify(row.labels_json) !== JSON.stringify(vote.labelsJson)
  ) {
    throw new Error(`legislative_votes row ${vote.id} was re-approved with different content during the run`);
  }
}

export type LegislativeVoteJudgment = {
  jurisdiction: string;
  chamber: LegislativeVoteChamber;
  session: string;
  rollNumber: number;
  // What the judgment was written about, so a roll number that is mistyped
  // onto another existing roll call cannot carry these sentences with it.
  // measureId compares as a parsed measure (`H R 1` and `H.R. 1` agree).
  measureId: string | null;
  voteDate: string;
  yeaDescription: string;
  nayDescription: string;
  // Already shape-checked (see parseRollCallLabels); stored as given.
  labels: readonly { slug: string; yea: "for" | "against" | null }[];
  reviewStatus: "pending" | "approved";
};

export type LegislativeVoteJudgmentOutcome =
  // Same sentences, labels, and status already on the row.
  | "unchanged"
  // Sentences/labels and/or status written.
  | "updated";

// jsonb stores object keys in its own order (shorter first), so stored
// labels and the file's labels are compared element by element, not as
// serialized strings.
function canonicalLabels(value: unknown): string {
  if (!Array.isArray(value)) {
    return "";
  }
  return JSON.stringify(
    value.map((element) => {
      const { slug, yea } = (element ?? {}) as { slug?: unknown; yea?: unknown };
      return [slug ?? null, yea ?? null];
    })
  );
}

function sameMeasure(stored: string | null, expected: string | null): boolean {
  const a = parseFederalMeasure(stored);
  const b = parseFederalMeasure(expected);
  if (a === null || b === null) {
    return a === b && (stored ?? "").trim().toUpperCase() === (expected ?? "").trim().toUpperCase();
  }
  return a.type === b.type && a.number === b.number;
}

/**
 * Writes one roll call's judgment (the two sentences, the labels, and the
 * review decision) onto its legislative_votes row. Call inside a
 * transaction: the row is locked, and a row that is already approved with a
 * different judgment is moved back to pending first, since the freeze
 * trigger (migration 251) lets an approved row change only through pending.
 * The importer then rewrites any records it already fanned out.
 *
 * Approved → pending is refused once records have been fanned out: the
 * importer only skips a pending row, it never withdraws records, so the
 * operator must either supply a corrected approved judgment (rewritten in
 * place on the next import) or retire the records first.
 */
export async function applyLegislativeVoteJudgment(
  db: Queryable,
  judgment: LegislativeVoteJudgment
): Promise<LegislativeVoteJudgmentOutcome> {
  const current = await db.query<{
    id: string;
    is_floor_vote: boolean | null;
    measure_id: string | null;
    vote_date: string;
    review_status: LegislativeVoteReviewStatus;
    yea_description: string | null;
    nay_description: string | null;
    labels_json: unknown;
  }>(
    `SELECT id, is_floor_vote, measure_id, vote_date::text AS vote_date, review_status, yea_description, nay_description, labels_json
       FROM legislative_votes
      WHERE jurisdiction = $1
        AND chamber = $2
        AND session = $3
        AND roll_number = $4
      FOR UPDATE`,
    [judgment.jurisdiction, judgment.chamber, judgment.session, judgment.rollNumber]
  );
  const row = current.rows[0];
  const name = `${judgment.chamber} ${judgment.session} roll ${judgment.rollNumber}`;
  if (!row) {
    throw new Error(`${name} is not in legislative_votes; run rollcall:fetch first`);
  }
  if (!sameMeasure(row.measure_id, judgment.measureId) || row.vote_date !== judgment.voteDate) {
    throw new Error(
      `${name} is ${row.measure_id ?? "no measure"} on ${row.vote_date}, but the judgment says ${judgment.measureId ?? "no measure"} on ${judgment.voteDate}`
    );
  }
  if (judgment.reviewStatus === "approved" && row.is_floor_vote !== true) {
    throw new Error(`${name} is not a kept floor vote (is_floor_vote = ${String(row.is_floor_vote)}); it cannot be approved`);
  }
  if (row.review_status === "approved" && judgment.reviewStatus === "pending") {
    const fannedOut = await db.query<{ n: string }>(
      `SELECT count(*)::text AS n
         FROM candidate_records
        WHERE origin = 'rollcall_import'
          AND starts_with(origin_run_id, $1)
          AND retired_at IS NULL`,
      [`rollcall:${judgment.jurisdiction}:${judgment.chamber}:${judgment.session}:${judgment.rollNumber}:`]
    );
    const n = Number(fannedOut.rows[0]?.n ?? "0");
    if (n > 0) {
      throw new Error(
        `${name} already fanned out ${n} live candidate records; setting it back to pending would not withdraw them. ` +
          "Supply a corrected approved judgment (the next rollcall:import rewrites them in place) or retire the records first."
      );
    }
  }
  const labelsJson = JSON.stringify(judgment.labels);
  const sameJudgment =
    row.yea_description === judgment.yeaDescription &&
    row.nay_description === judgment.nayDescription &&
    canonicalLabels(row.labels_json) === canonicalLabels(judgment.labels);
  if (sameJudgment && row.review_status === judgment.reviewStatus) {
    return "unchanged";
  }
  if (row.review_status === "approved" && !sameJudgment) {
    await db.query(
      `UPDATE legislative_votes
          SET review_status = 'pending',
              reviewed_at = NULL
        WHERE id = $1`,
      [row.id]
    );
  }
  await db.query(
    `UPDATE legislative_votes
        SET yea_description = $2,
            nay_description = $3,
            labels_json = $4::jsonb,
            review_status = $5,
            reviewed_at = CASE WHEN $5 = 'approved' THEN now() ELSE NULL END
      WHERE id = $1`,
    [row.id, judgment.yeaDescription, judgment.nayDescription, labelsJson, judgment.reviewStatus]
  );
  return "updated";
}
