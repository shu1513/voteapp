import type { Pool, PoolClient } from "pg";

import {
  upsertCandidateRecordAreaTags,
  validateCandidateRecordAreaLabels,
  type CandidateRecordAreaLabelInput,
  type CandidateRecordAreaStance,
} from "../candidates/candidateRecordAreaTagging.js";
import { recordIdentityTransition } from "../candidates/candidateRecordStore.js";
import type { FederalMeasure } from "./federalMeasures.js";
import { citesAnyRollCall, citesSameRollCall, descriptionMentionsMeasure, looksLikeVoteClaim } from "./rollCallRecordUrls.js";

// The fan-out step of the roll-call import
// (docs/plans/roll-call-vote-import.md §3): one approved legislative_votes
// row becomes one candidate_records row per matched member who voted. The
// pure parts (which side a vote lands on, the labels each side gets, what
// to do with a candidate's existing records) live here next to the writes
// that carry them out, so the importer script only sequences them.

export type RollCallVoteSide = "yea" | "nay";

const VOTE_SIDES: Record<string, RollCallVoteSide | null> = {
  yea: "yea",
  aye: "yea",
  nay: "nay",
  no: "nay",
  // No record: the member took no position.
  present: null,
  "not voting": null,
};

/**
 * Which sentence a member's vote earns. Anything outside the six values the
 * two feeds print on a floor vote (e.g. Guilty / Not Guilty) throws, so a
 * feed change fails the roll call instead of silently skipping members.
 */
export function memberVoteSide(vote: string): RollCallVoteSide | null {
  const normalized = vote.trim().toLowerCase().replace(/\s+/g, " ");
  const side = VOTE_SIDES[normalized];
  if (side === undefined) {
    throw new Error(`unknown vote value: ${vote}`);
  }
  return side;
}

// One element of legislative_votes.labels_json: the research area and
// which stance a YEA vote takes on it (null only for the non-stance areas).
export type RollCallLabel = { slug: string; yea: CandidateRecordAreaStance | null };

export type RollCallSideLabel = { researchAreaSlug: string; stance: CandidateRecordAreaStance | null };

function flip(stance: CandidateRecordAreaStance | null): CandidateRecordAreaStance | null {
  return stance === "for" ? "against" : stance === "against" ? "for" : null;
}

/** The tags a record on `side` gets: yea voters take the label as written, nay voters the opposite. */
export function labelsForSide(labels: readonly RollCallLabel[], side: RollCallVoteSide): RollCallSideLabel[] {
  return labels.map((label) => ({
    researchAreaSlug: label.slug,
    stance: side === "yea" ? label.yea : flip(label.yea),
  }));
}

/**
 * Reads and checks labels_json. The DB only guarantees a non-empty array on
 * an approved row (migration 251); element shape — the slug exists, the
 * stance value is for/against/null, non-stance areas carry null and stance
 * areas do not — is checked here with the same rule the manual writer uses,
 * once for each side, since a bad label would replicate across every record.
 */
export function parseRollCallLabels(labelsJson: unknown, allowedSlugs: ReadonlySet<string>): RollCallLabel[] {
  if (!Array.isArray(labelsJson) || labelsJson.length === 0) {
    throw new Error("labels_json is not a non-empty array");
  }
  const labels: RollCallLabel[] = [];
  const seen = new Set<string>();
  for (const [index, element] of labelsJson.entries()) {
    if (typeof element !== "object" || element === null || Array.isArray(element)) {
      throw new Error(`labels_json[${index}] is not an object`);
    }
    const { slug, yea } = element as { slug?: unknown; yea?: unknown };
    if (typeof slug !== "string" || slug.trim().length === 0) {
      throw new Error(`labels_json[${index}].slug is not a string`);
    }
    if (yea !== "for" && yea !== "against" && yea !== null && yea !== undefined) {
      throw new Error(`labels_json[${index}].yea must be "for", "against", or null`);
    }
    const normalizedSlug = slug.trim().toLowerCase();
    if (seen.has(normalizedSlug)) {
      throw new Error(`labels_json names ${normalizedSlug} twice`);
    }
    seen.add(normalizedSlug);
    labels.push({ slug: normalizedSlug, yea: yea ?? null });
  }
  for (const side of ["yea", "nay"] as const) {
    const validation = validateCandidateRecordAreaLabels(
      labelsForSide(labels, side).map((label) => ({ ...label, candidateRecordId: side })),
      allowedSlugs
    );
    if (!validation.ok) {
      throw new Error(
        `labels_json is invalid for ${side} voters: ${validation.failures.map((failure) => failure.reason).join("; ")}`
      );
    }
  }
  return labels;
}

// A candidate's existing candidate_records rows on the vote date.
export type ExistingCandidateRecord = {
  id: string;
  description: string;
  source_url: string;
  record_identity_key: string;
  retired_at: string | null;
};

export type CandidateRecordPlan =
  // No row for this vote yet.
  | { action: "insert" }
  // This run's exact content is already there (re-run).
  | { action: "unchanged"; recordId: string }
  // One live hand-written row cites the same roll call: rewrite it in place.
  | { action: "rewrite"; recordId: string; oldIdentityKey: string; oldSourceUrl: string; oldDescription: string }
  // Same as rewrite, but the run asked to leave old rows alone.
  | { action: "skip_existing"; recordId: string }
  // A retired row carries this claim or cites this roll call, and no live
  // row does: a human withdrew the attribution, so nothing is written.
  | { action: "retired"; recordId: string }
  // More than one live row is this vote (two hand-written rows, or a
  // previous run's row plus a later hand-written one); a human must merge.
  | { action: "ambiguous"; recordIds: string[] };

export type CandidateRecordDecision = {
  plan: CandidateRecordPlan;
  // Live same-day rows that may be this vote without citing the roll call:
  // they name the measure, or make a vote claim from a non-roll-call source
  // (a press release, a news story). Listed for a human, never touched.
  relatedRecordIds: string[];
};

/**
 * Decides what the fan-out does for one candidate given their records on
 * the vote date. Duplicate = a row whose URL is this roll call (any
 * spelling, including the Clerk's MemberVotes page); exactly one live
 * duplicate is rewritten, anything else stops.
 */
export function planCandidateRecord(input: {
  existing: readonly ExistingCandidateRecord[];
  identityKey: string;
  rollCallKey: string;
  measure: FederalMeasure | null;
  skipExisting: boolean;
}): CandidateRecordDecision {
  const live = input.existing.filter((record) => record.retired_at === null);
  const sameKey = live.filter((record) => record.record_identity_key === input.identityKey);
  const sameRollCall = live.filter(
    (record) => record.record_identity_key !== input.identityKey && citesSameRollCall(record.source_url, input.rollCallKey)
  );
  const related = live.filter(
    (record) =>
      !sameKey.includes(record) &&
      !sameRollCall.includes(record) &&
      // A row citing any roll call is a different, known vote (this roll
      // call's spellings are already in sameRollCall) — even when its text
      // names this measure, as an amendment or procedural vote on the same
      // bill does. Only uncited rows may be this vote told off a press
      // release.
      !citesAnyRollCall(record.source_url) &&
      ((input.measure !== null && descriptionMentionsMeasure(record.description, input.measure)) ||
        looksLikeVoteClaim(record.description))
  );
  const relatedRecordIds = related.map((record) => record.id);

  // A retired row for this vote blocks the fan-out only when no live row
  // carries it: retirement withdrew the claim. Beside a live row it is just
  // history (a retired duplicate copy), and the live row stays writable.
  if (sameKey.length + sameRollCall.length === 0) {
    const retired = input.existing.find(
      (record) =>
        record.retired_at !== null &&
        (record.record_identity_key === input.identityKey || citesSameRollCall(record.source_url, input.rollCallKey))
    );
    if (retired) {
      return { plan: { action: "retired", recordId: retired.id }, relatedRecordIds };
    }
  }
  // A previous run's row plus a hand-written row for the same vote (or two
  // hand-written rows) is a merge for a human, not a rewrite.
  if (sameKey.length + sameRollCall.length > 1) {
    return {
      plan: { action: "ambiguous", recordIds: [...sameKey, ...sameRollCall].map((record) => record.id) },
      relatedRecordIds,
    };
  }
  if (sameKey.length > 0) {
    return { plan: { action: "unchanged", recordId: sameKey[0]!.id }, relatedRecordIds };
  }
  const duplicate = sameRollCall[0];
  if (duplicate) {
    return {
      plan: input.skipExisting
        ? { action: "skip_existing", recordId: duplicate.id }
        : {
            action: "rewrite",
            recordId: duplicate.id,
            oldIdentityKey: duplicate.record_identity_key,
            oldSourceUrl: duplicate.source_url,
            oldDescription: duplicate.description,
          },
      relatedRecordIds,
    };
  }
  return { plan: { action: "insert" }, relatedRecordIds };
}

// Followers are told about new roll calls only; a backfill of old votes
// must not email them about years-old records (plan §3).
export const NOTIFY_WITHIN_DAYS = 30;

export function shouldNotifyForVoteDate(voteDate: string, todayIso: string): boolean {
  const cutoff = new Date(`${todayIso}T00:00:00Z`);
  cutoff.setUTCDate(cutoff.getUTCDate() - NOTIFY_WITHIN_DAYS);
  return voteDate >= cutoff.toISOString().slice(0, 10);
}

type Queryable = Pick<Pool | PoolClient, "query">;

/** Every candidate_records row (live or retired) of the given candidates on one date, grouped by candidate. */
export async function loadExistingRecordsForDate(
  db: Queryable,
  candidateIds: readonly string[],
  eventDate: string
): Promise<Map<string, ExistingCandidateRecord[]>> {
  const byCandidate = new Map<string, ExistingCandidateRecord[]>();
  if (candidateIds.length === 0) {
    return byCandidate;
  }
  const result = await db.query<ExistingCandidateRecord & { candidate_id: string }>(
    `
      SELECT candidate_id, id, description, source_url, record_identity_key, retired_at::text AS retired_at
      FROM public.candidate_records
      WHERE candidate_id = ANY($1::uuid[])
        AND event_date = $2::date
      ORDER BY created_at, id
    `,
    [candidateIds, eventDate]
  );
  for (const { candidate_id: candidateId, ...record } of result.rows) {
    const records = byCandidate.get(candidateId) ?? [];
    records.push(record);
    byCandidate.set(candidateId, records);
  }
  return byCandidate;
}

export type RollCallRecordContent = {
  candidateId: string;
  description: string;
  sourceUrl: string;
  eventDate: string;
  identityKey: string;
  originRunId: string;
};

export async function insertRollCallRecord(client: Queryable, record: RollCallRecordContent): Promise<string> {
  const result = await client.query<{ id: string }>(
    `
      INSERT INTO public.candidate_records
        (candidate_id, description, source_url, event_date, record_identity_key, origin, origin_run_id)
      VALUES ($1, $2, $3, $4::date, $5, 'rollcall_import', $6)
      ON CONFLICT (candidate_id, record_identity_key) DO NOTHING
      RETURNING id
    `,
    [record.candidateId, record.description, record.sourceUrl, record.eventDate, record.identityKey, record.originRunId]
  );
  const id = result.rows[0]?.id;
  if (!id) {
    // The plan saw no row with this key moments ago; another writer raced.
    throw new Error(`candidate ${record.candidateId} already holds record key ${record.identityKey}`);
  }
  return id;
}

/**
 * The in-place rewrite of a hand-written duplicate (plan §3): same row id,
 * so its tags' history and notification events stay attached, with the
 * identity transition that lets research:promote follow the re-key. Guarded
 * on the old key so a row edited since the plan was made is left alone.
 */
export async function rewriteRollCallRecord(
  client: Queryable,
  record: RollCallRecordContent & { recordId: string; oldIdentityKey: string }
): Promise<void> {
  const result = await client.query(
    `
      UPDATE public.candidate_records
      SET description = $3,
          source_url = $4,
          record_identity_key = $5,
          origin = 'rollcall_import',
          origin_run_id = $6,
          updated_at = now()
      WHERE id = $1
        AND record_identity_key = $2
        AND retired_at IS NULL
    `,
    [record.recordId, record.oldIdentityKey, record.description, record.sourceUrl, record.identityKey, record.originRunId]
  );
  if (result.rowCount !== 1) {
    throw new Error(`record ${record.recordId} changed under the rewrite (key ${record.oldIdentityKey})`);
  }
  await recordIdentityTransition(client, {
    candidateId: record.candidateId,
    oldIdentityKey: record.oldIdentityKey,
    newIdentityKey: record.identityKey,
    reason: "rollcall_normalization",
  });
}

/** Makes the record's area tags exactly the roll call's labels for that side. */
export async function syncRollCallRecordTags(
  client: Queryable,
  recordId: string,
  labels: readonly RollCallSideLabel[],
  researchAreaIdBySlug: ReadonlyMap<string, string>
): Promise<{ deleted: number }> {
  const inputs: CandidateRecordAreaLabelInput[] = labels.map((label) => ({ ...label, candidateRecordId: recordId }));
  const keep = inputs.map((label) => {
    const id = researchAreaIdBySlug.get(label.researchAreaSlug);
    if (!id) {
      throw new Error(`no research area id for slug ${label.researchAreaSlug}`);
    }
    return id;
  });
  const deleted = await client.query(
    `
      DELETE FROM public.candidate_record_area_tags
      WHERE candidate_record_id = $1
        AND NOT (research_area_id = ANY($2::uuid[]))
    `,
    [recordId, keep]
  );
  await upsertCandidateRecordAreaTags(client, inputs, researchAreaIdBySlug);
  return { deleted: deleted.rowCount ?? 0 };
}
