// Guarded merge of a duplicate candidate into its surviving row.
//
// Roster imports occasionally create two candidate rows for the same person
// (punctuation/spelling variants, a state roster and a federal roster each
// minting a shell). The schema has carried merge semantics since 001_init —
// candidates.merged_into_candidate_id plus chk_candidates_merged_implies_deleted
// — and every reader already filters merged rows (detail reader, ballot
// lookup, sitemap, follows, digests, finance auto-link), but no wrapper could
// WRITE a merge. This wrapper merges EXACTLY ONE explicitly identified
// duplicate into one explicitly identified survivor: rehome the duplicate's
// dependent rows onto the survivor, union its hard identifiers, then mark it
// merged (deleted_at + merged_into_candidate_id). Nothing is heuristic; both
// ids come from the operator.
//
// Per-table semantics:
// - candidate_elections (candidate_id): rehomed; when both candidates link
//   the same election the rows must agree on status/is_incumbent/running mate
//   (duplicate row is then deleted; disagreeing rows are a research question,
//   not a merge). Deleting a duplicate link is refused while any table still
//   references its id (fl_candidate_finance_outside_group_links cascades on
//   delete — checked dynamically against every FK onto candidate_elections);
// - candidate_elections (running_mate_candidate_id): rehomed; refused when
//   both candidates appear as running mates anywhere (partial-unique
//   collision risk) or when a ticket pairs the two candidates with each
//   other (a merge would create a self-ticket);
// - election_results.winners JSON embeds candidate_id and
//   candidate_election_id, invisible to FK scans: refused when any persisted
//   winner references the duplicate or a link row this merge would delete;
// - candidate_records: rehomed; rows whose record_identity_key already exists
//   on the survivor are deleted as duplicates (area tags and record-update
//   notification events cascade);
// - candidate_record_sweep_confirmations: the duplicate's confirmation is
//   deleted; the survivor's is also deleted when the merge rehomed records,
//   because its completeness claims describe the pre-merge record set;
// - user_candidate_follows / notification events: rehomed; rows that would
//   collide with the survivor's (user already follows both, or was already
//   notified for the same future election) are deleted;
// - every other table with a FK onto candidates (all state finance tables,
//   presidential tables, future additions — discovered from the catalog):
//   rehomed only when the survivor has no rows in the same column; both
//   sides present is refused rather than guessed at;
// - candidates already merged into the duplicate are repointed at the
//   survivor so chains collapse to one hop;
// - fec_ids / state_filing_ids are unioned onto the survivor (same
//   dedupe as pipeline profile-identity merging);
// - local-database guard, row locks (candidates locked first in id order so
//   two concurrent merges serialize instead of deadlocking), single
//   transaction, --dry-run.
import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import {
  mergeIdentifierLists,
  unionFormerWebsiteUrls,
} from "../pipeline/candidates/candidateProfileIdentity.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";
import {
  isManualCandidateFinanceTargetFkReference,
  listCandidateElectionLinkFkReferences,
} from "./moveManualCandidateElectionLink.js";

type QueryResultLike<T> = { rows: T[] };

export type MergeCandidatesClient = {
  query<T = unknown>(text: string, values?: unknown[]): Promise<QueryResultLike<T>>;
};

export type MergeCandidatesOptions = {
  candidateId: string;
  intoCandidateId: string;
  dryRun: boolean;
};

export type MergeCandidatesResult = {
  dryRun: boolean;
  mergedCandidateId: string;
  mergedCandidateName: string;
  survivorCandidateId: string;
  survivorCandidateName: string;
  links: { rehomed: number; duplicatesDeleted: number };
  mateLinks: { rehomed: number };
  records: {
    rehomed: number;
    duplicatesDeleted: number;
    areaTagsCopied: number;
    retirementsPropagated: number;
  };
  sweepConfirmations: { mergedDeleted: boolean; survivorDeleted: boolean };
  follows: { rehomed: number; duplicatesDeleted: number };
  /** user_election_choices on duplicate links; choices on rehomed links ride
   * the FK's ON UPDATE CASCADE and are not counted here. */
  choices: { repointedToSurvivor: number; duplicatesDeleted: number };
  notificationEvents: {
    rehomed: number;
    duplicatesDeleted: number;
    remappedToSurvivorRecords: number;
  };
  otherTables: { table: string; column: string; rowsRehomed: number }[];
  chainCollapsedCandidates: number;
  identifiers: { fecIdsAppended: number; stateFilingIdsAppended: number };
  profile: {
    fieldsFilled: string[];
    sourcesAppended: number;
    formerWebsiteUrlsAppended: number;
  };
};

type CandidateRow = {
  id: string;
  display_name: string | null;
  first_name: string;
  last_name: string;
  party: string;
  state: string;
  deleted_at: string | null;
  merged_into_candidate_id: string | null;
  fec_ids: unknown;
  state_filing_ids: unknown;
  summary: string | null;
  current_office: string | null;
  date_of_birth: string | null;
  twitter_handle: string | null;
  linkedin_url: string | null;
  official_website_url: string | null;
  former_website_urls: unknown;
  profile_sources: unknown;
};

type LinkRow = {
  id: string;
  candidate_id: string;
  election_id: string;
  is_incumbent: boolean;
  status: string;
  running_mate_candidate_id: string | null;
};

type RecordRow = {
  id: string;
  candidate_id: string;
  record_identity_key: string;
  retired_at: string | null;
  retired_reason: string | null;
};

type FollowRow = {
  id: string;
  candidate_id: string;
  user_id: string;
};

type EventRow = {
  id: string;
  candidate_id: string;
  user_id: string;
  event_type: string;
  election_id: string | null;
  candidate_record_id: string | null;
};

// Scalar profile fields copied from the duplicate ONLY where the survivor is
// blank — a populated survivor value always wins (the operator explicitly
// picked the survivor). display_name is deliberately excluded: the
// duplicate's name presentation is often exactly what made it a duplicate.
const PROFILE_FILL_FIELDS = [
  "summary",
  "current_office",
  "date_of_birth",
  "twitter_handle",
  "linkedin_url",
  "official_website_url",
] as const;
type ProfileFillField = (typeof PROFILE_FILL_FIELDS)[number];

function isBlank(value: string | null | undefined): boolean {
  return value == null || value.trim().length === 0;
}

// elections.sources-style jsonb array of URL strings; trimmed, exact dedupe
// (same semantics as the supersede wrapper's source handling).
function normalizeUrlList(raw: unknown): string[] {
  return Array.isArray(raw)
    ? raw
        .filter((value): value is string => typeof value === "string" && value.trim().length > 0)
        .map((value) => value.trim())
    : [];
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

// Tables whose candidate FKs get bespoke handling above; everything else
// found in the catalog goes through the generic rehome-or-refuse path.
const SPECIALLY_HANDLED_TABLES = new Set([
  "public.candidates",
  "public.candidate_elections",
  "public.candidate_records",
  "public.candidate_record_sweep_confirmations",
  "public.user_candidate_follows",
  "public.user_candidate_follow_notification_events",
  // Rename-audit rows are history about the row the rename happened to: the
  // old/new name columns describe THAT candidate row. They deliberately stay
  // on the merged tombstone — rehoming them would falsify the audit (its
  // old_* values describe the duplicate, not the survivor), and refusing the
  // merge over them (the generic both-sides rule) is pointless because the
  // table has no unique keys to collide. Merges never hard-delete
  // candidates, so the FK stays valid.
  "public.candidate_rename_audit",
]);

function usage(): string {
  return [
    "Merge one duplicate candidate into its surviving row.",
    "",
    "Usage:",
    "  npm run manual:candidates:merge -- --candidate-id uuid --into-candidate-id uuid --reason text [--dry-run]",
  ].join("\n");
}

function readFlag(name: string): string | null {
  const index = process.argv.indexOf(name);
  if (index < 0) return null;
  const value = process.argv[index + 1];
  if (!value || value.startsWith("--")) {
    throw new Error(`Missing value for ${name}.\n${usage()}`);
  }
  return value.trim();
}

function requireFlag(name: string): string {
  const value = readFlag(name);
  if (!value) throw new Error(`Missing ${name}.\n${usage()}`);
  return value;
}

function requireEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required for manual candidate merge`);
  return value;
}

function candidateName(row: CandidateRow): string {
  const display = row.display_name?.trim();
  const name = display && display.length > 0 ? display : `${row.first_name} ${row.last_name}`;
  return `${name} (${row.party}, ${row.state})`;
}

function parseIdentifierList(raw: unknown): string[] {
  return Array.isArray(raw)
    ? raw.filter((value): value is string => typeof value === "string" && value.trim().length > 0)
    : [];
}

/**
 * Every (table, column) pair with a foreign key onto the given target table,
 * straight from the catalog, so newly added referencing tables (a new state
 * finance table, a new notification table) are covered without touching this
 * wrapper. Identifiers feed dynamic SQL, so they come exclusively from
 * regclass / quote_ident — never from user input.
 */
export async function listFkReferences(
  client: MergeCandidatesClient,
  targetTable: "public.candidates" | "public.candidate_elections"
): Promise<{ table: string; column: string }[]> {
  // Schema-qualified explicitly: regclass::text drops the "public." prefix
  // for tables on the search_path, which would break the handled-table
  // exclusion matching below.
  const result = await client.query<{ table_name: string; column_name: string }>(
    `
      SELECT DISTINCT quote_ident(ns.nspname) || '.' || quote_ident(cl.relname) AS table_name,
             quote_ident(a.attname) AS column_name
      FROM pg_constraint c
      JOIN pg_class cl ON cl.oid = c.conrelid
      JOIN pg_namespace ns ON ns.oid = cl.relnamespace
      JOIN unnest(c.conkey) AS k(attnum) ON true
      JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
      WHERE c.contype = 'f'
        AND c.confrelid = '${targetTable}'::regclass
      ORDER BY 1, 2
    `
  );
  return result.rows.map((row) => ({ table: row.table_name, column: row.column_name }));
}

export async function runMergeCandidates(
  client: MergeCandidatesClient,
  options: MergeCandidatesOptions
): Promise<MergeCandidatesResult> {
  // PostgreSQL returns uuid columns lowercased; a valid uppercase input
  // would otherwise fail the row-matching below with a false "not found".
  const mergedId = options.candidateId.toLowerCase();
  const survivorId = options.intoCandidateId.toLowerCase();
  const { dryRun } = options;
  if (mergedId === survivorId) {
    throw new Error("--candidate-id and --into-candidate-id must differ");
  }
  const pair = [mergedId, survivorId];

  await client.query("BEGIN");
  try {
    // Locked in id order so two concurrent merges (in either direction)
    // serialize on the candidate rows instead of deadlocking; every later
    // step runs under these locks.
    const candidatesResult = await client.query<CandidateRow>(
      `
        SELECT id, display_name, first_name, last_name, party, state,
               deleted_at::text, merged_into_candidate_id, fec_ids, state_filing_ids,
               summary, current_office, date_of_birth::text, twitter_handle,
               linkedin_url, official_website_url, former_website_urls, profile_sources
        FROM public.candidates
        WHERE id = ANY($1::uuid[])
        ORDER BY id
        FOR UPDATE
      `,
      [pair]
    );
    const merged = candidatesResult.rows.find((row) => row.id === mergedId);
    const survivor = candidatesResult.rows.find((row) => row.id === survivorId);
    if (!merged) throw new Error(`Candidate not found: ${mergedId}`);
    if (!survivor) throw new Error(`Candidate not found: ${survivorId}`);
    if (merged.merged_into_candidate_id) {
      throw new Error(
        `Candidate ${mergedId} is already merged into ${merged.merged_into_candidate_id}`
      );
    }
    if (survivor.merged_into_candidate_id) {
      throw new Error(
        `Survivor ${survivorId} is itself merged into ${survivor.merged_into_candidate_id}; merge into that row instead`
      );
    }
    if (survivor.deleted_at) {
      throw new Error(`Survivor ${survivorId} is soft-deleted; a merge target must be live`);
    }

    // Every candidate_elections row either candidate touches — as the
    // candidate or as a running mate — locked in ONE query in id order, so
    // two concurrent merges reaching the same rows through different columns
    // still acquire locks in the same sequence and cannot deadlock.
    const linksResult = await client.query<LinkRow>(
      `
        SELECT id, candidate_id, election_id, is_incumbent, status, running_mate_candidate_id
        FROM public.candidate_elections
        WHERE candidate_id = ANY($1::uuid[]) OR running_mate_candidate_id = ANY($1::uuid[])
        ORDER BY id
        FOR UPDATE
      `,
      [pair]
    );
    const mergedLinks = linksResult.rows.filter((row) => row.candidate_id === mergedId);
    const survivorLinkByElection = new Map(
      linksResult.rows
        .filter((row) => row.candidate_id === survivorId)
        .map((row) => [row.election_id, row])
    );
    const mergedAsMate = linksResult.rows.filter(
      (row) => row.running_mate_candidate_id === mergedId
    );
    const survivorAsMate = linksResult.rows.filter(
      (row) => row.running_mate_candidate_id === survivorId
    );

    // A ticket pairing the two candidates with each other would become a
    // self-ticket (chk_candidate_elections_running_mate_not_self) — that is
    // an identity error in the data, not something a merge can resolve.
    const selfTicket =
      mergedAsMate.find((row) => row.candidate_id === survivorId) ??
      survivorAsMate.find((row) => row.candidate_id === mergedId);
    if (selfTicket) {
      throw new Error(
        `candidate_elections link ${selfTicket.id} pairs these two candidates as head and running mate ` +
          "of the same ticket; if they are the same person that link is wrong — resolve it first, then re-run."
      );
    }
    const presidentialPair = await client.query<{ id: string }>(
      `
        SELECT id FROM public.presidential_cycle_candidates
        WHERE (candidate_id = $1::uuid AND running_mate_candidate_id = $2::uuid)
           OR (candidate_id = $2::uuid AND running_mate_candidate_id = $1::uuid)
        LIMIT 1
      `,
      [mergedId, survivorId]
    );
    if (presidentialPair.rows[0]) {
      throw new Error(
        `presidential_cycle_candidates row ${presidentialPair.rows[0].id} pairs these two candidates ` +
          "as a ticket; resolve that row first, then re-run."
      );
    }

    // Running-mate collision, scoped to the election: the unique key at risk
    // is (election_id, running_mate_candidate_id), so both candidates riding
    // as mates on DIFFERENT elections is the normal duplicate shape (primary
    // shell recorded under one row, general under the other) and merges fine.
    // The same election carrying both is two mate rows for one person —
    // a research question. Checked before any write so a refusal implies an
    // untouched database.
    const survivorMateElectionIds = new Set(survivorAsMate.map((row) => row.election_id));
    const mateCollision = mergedAsMate.find((row) => survivorMateElectionIds.has(row.election_id));
    if (mateCollision) {
      throw new Error(
        `Election ${mateCollision.election_id} has links carrying both candidates as running mates; ` +
          "resolve the duplicate ticket first, then re-run."
      );
    }

    // Partition the duplicate's links: rehome where the survivor has no link
    // on the election, converge identical duplicates, refuse disagreements.
    const rehomeLinkIds: string[] = [];
    const duplicateLinkIds: string[] = [];
    for (const link of mergedLinks) {
      const survivorLink = survivorLinkByElection.get(link.election_id);
      if (!survivorLink) {
        rehomeLinkIds.push(link.id);
        continue;
      }
      const identical =
        survivorLink.is_incumbent === link.is_incumbent &&
        survivorLink.status === link.status &&
        survivorLink.running_mate_candidate_id === link.running_mate_candidate_id;
      if (!identical) {
        throw new Error(
          `Both candidates are linked to election ${link.election_id} and the links disagree ` +
            `(duplicate: status=${link.status}, is_incumbent=${link.is_incumbent}, running_mate=${link.running_mate_candidate_id ?? "null"}; ` +
            `survivor: status=${survivorLink.status}, is_incumbent=${survivorLink.is_incumbent}, running_mate=${survivorLink.running_mate_candidate_id ?? "null"}). ` +
            "Which row is right is a research question; resolve it, then re-run."
        );
      }
      duplicateLinkIds.push(link.id);
    }

    // Manual finance targets are keyed to (candidate_id, election_id), and
    // those same IDs live inside an immutable hashed filing payload. Neither
    // rehoming nor deleting a candidate_elections row may cascade just the
    // derived columns: that would make them disagree with the retained
    // source. Count affected rows explicitly so this known composite FK does
    // not globally disable merges when the count is zero.
    const manualFinanceTargets = await client.query<{ n: string }>(
      `
        SELECT count(*)::text AS n
        FROM public.manual_candidate_finance_filing_targets
        WHERE candidate_id = $1::uuid
          AND election_id = ANY($2::uuid[])
      `,
      [mergedId, mergedLinks.map((link) => link.election_id)]
    );
    const manualFinanceTargetCount = Number(manualFinanceTargets.rows[0]?.n ?? "0");
    if (manualFinanceTargetCount > 0) {
      throw new Error(
        `${manualFinanceTargetCount} manual candidate-finance filing target row(s) reference candidate ` +
          `${mergedId} on links this merge would rehome or delete; their candidate/election IDs also live ` +
          "inside immutable filing payloads. Resolve those filings explicitly (user decision), then re-run."
      );
    }

    // Persisted-results guard: election_results.winners stores candidate_id
    // and candidate_election_id inside JSON, invisible to the FK scans below.
    // A merge would leave a winner pointing at a merged candidate, or — for
    // links deleted as duplicates — at nothing at all.
    //
    // Known residual race: a concurrent result write that loaded its roster
    // before this merge commits can still persist winners referencing the
    // duplicate afterwards. No lock taken here fixes that — blocking the
    // writer's INSERT until the merge commits does not refresh the roster
    // snapshot it already matched against; only writer-side validation
    // could. Result writes are manual/scheduled and rare, so this stays a
    // documented gap rather than an ineffective table lock.
    const winnersResult = await client.query<{ id: string }>(
      `
        SELECT er.id FROM public.election_results er
        WHERE jsonb_typeof(er.winners) = 'array'
          AND EXISTS (
            SELECT 1 FROM jsonb_array_elements(er.winners) AS w
            WHERE w->>'candidate_id' = $1
               OR w->>'candidate_election_id' = ANY($2::text[])
          )
        LIMIT 1
      `,
      [mergedId, duplicateLinkIds]
    );
    if (winnersResult.rows[0]) {
      throw new Error(
        `election_results row ${winnersResult.rows[0].id} has persisted winners referencing candidate ` +
          `${mergedId} or a link this merge would delete; resolve the result rows first (user decision), then re-run.`
      );
    }

    // Choices on duplicate links, reconciled BEFORE those links are deleted —
    // the choice FK cascades on link delete, which would silently erase
    // users' picks. A user who picked both candidates in a duplicate election
    // keeps the survivor's pick (mirrors the follows rule below); picks
    // naming only the merged candidate are repointed to the survivor's
    // identical candidacy, which exists by definition of a duplicate link.
    // Choices on REHOMED links need no handling here: the FK's ON UPDATE
    // CASCADE carries them through the rehome UPDATE below, collision-free
    // because a survivor pick cannot exist for an election the survivor has
    // no link in (the same FK forbids it).
    const duplicateChoiceDeleteIds: string[] = [];
    const repointChoiceIds: string[] = [];
    if (duplicateLinkIds.length > 0) {
      const duplicateElectionIds = mergedLinks
        .filter((link) => survivorLinkByElection.has(link.election_id))
        .map((link) => link.election_id);
      const choicesResult = await client.query<{ id: string; survivor_has_pick: boolean }>(
        `
          SELECT
            merged_choice.id,
            EXISTS (
              SELECT 1 FROM public.user_election_choices AS survivor_choice
              WHERE survivor_choice.user_id = merged_choice.user_id
                AND survivor_choice.election_id = merged_choice.election_id
                AND survivor_choice.candidate_id = $2::uuid
            ) AS survivor_has_pick
          FROM public.user_election_choices AS merged_choice
          WHERE merged_choice.candidate_id = $1::uuid
            AND merged_choice.election_id = ANY($3::uuid[])
          ORDER BY merged_choice.id
          FOR UPDATE OF merged_choice
        `,
        [mergedId, survivorId, duplicateElectionIds]
      );
      for (const choice of choicesResult.rows) {
        if (choice.survivor_has_pick) {
          duplicateChoiceDeleteIds.push(choice.id);
        } else {
          repointChoiceIds.push(choice.id);
        }
      }
      if (!dryRun && duplicateChoiceDeleteIds.length > 0) {
        await client.query(`DELETE FROM public.user_election_choices WHERE id = ANY($1::uuid[])`, [
          duplicateChoiceDeleteIds,
        ]);
      }
      if (!dryRun && repointChoiceIds.length > 0) {
        await client.query(
          `UPDATE public.user_election_choices SET candidate_id = $2::uuid, updated_at = now() WHERE id = ANY($1::uuid[])`,
          [repointChoiceIds, survivorId]
        );
      }
    }

    // Deleting a duplicate link must not cascade dependents away
    // (fl_candidate_finance_outside_group_links references
    // candidate_elections.id with ON DELETE CASCADE). Checked dynamically so
    // future link-scoped tables block too. The per-column count is only
    // meaningful for a single-column FK onto id; any other shape is refused
    // outright instead of guessed at (same rule as the link-move script) —
    // except the choices FK, whose rows were reconciled just above.
    if (duplicateLinkIds.length > 0) {
      const linkReferences = (await listCandidateElectionLinkFkReferences(client)).filter(
        (ref) =>
          ref.table !== "public.candidate_elections" &&
          ref.constraintName !== "fk_user_election_choices_candidacy" &&
          !isManualCandidateFinanceTargetFkReference(ref)
      );
      const unsupported = [
        ...new Set(
          linkReferences
            .filter((ref) => ref.columnCount !== 1 || ref.referencedColumn !== "id")
            .map((ref) => `${ref.table}.${ref.constraintName}`)
        ),
      ];
      if (unsupported.length > 0) {
        throw new Error(
          `Foreign keys onto candidate_elections whose shape this guard cannot check ` +
            `(composite, or not referencing id): ${unsupported.join(", ")}. ` +
            "Refusing the duplicate-link delete; extend the guard before merging under such constraints."
        );
      }
      const blocking: string[] = [];
      const counted = new Set<string>();
      for (const { table, column } of linkReferences) {
        const key = `${table}.${column}`;
        if (counted.has(key)) continue;
        counted.add(key);
        const countResult = await client.query<{ n: string }>(
          `SELECT count(*)::text AS n FROM ${table} WHERE ${column} = ANY($1::uuid[])`,
          [duplicateLinkIds]
        );
        const n = Number(countResult.rows[0]?.n ?? "0");
        if (n > 0) blocking.push(`${key} (${n})`);
      }
      if (blocking.length > 0) {
        throw new Error(
          `Duplicate candidate_elections links still referenced and cannot be deleted: ${blocking.join(", ")}. ` +
            "Resolve those rows first (user decision), then re-run."
        );
      }
    }

    if (!dryRun && rehomeLinkIds.length > 0) {
      await client.query(
        `UPDATE public.candidate_elections SET candidate_id = $2::uuid, updated_at = now() WHERE id = ANY($1::uuid[])`,
        [rehomeLinkIds, survivorId]
      );
    }
    if (!dryRun && duplicateLinkIds.length > 0) {
      await client.query(`DELETE FROM public.candidate_elections WHERE id = ANY($1::uuid[])`, [
        duplicateLinkIds,
      ]);
    }

    // Running-mate rehome (collisions were refused before any write above).
    if (!dryRun && mergedAsMate.length > 0) {
      await client.query(
        `UPDATE public.candidate_elections SET running_mate_candidate_id = $2::uuid, updated_at = now() WHERE id = ANY($1::uuid[])`,
        [mergedAsMate.map((row) => row.id), survivorId]
      );
    }

    // Records: duplicates by record_identity_key keep the survivor's copy;
    // everything else is rehomed. Before a duplicate row is deleted, its
    // dependents are reconciled onto the survivor's copy instead of being
    // cascaded away (migration 069 set this precedent for tags when it
    // deduped identical keys): area tags the survivor's copy lacks are
    // copied over (survivor's stance wins on conflict), and record-update
    // notification events are remapped so "already notified" history and
    // pending digest events survive the delete.
    const recordsResult = await client.query<RecordRow>(
      `
        SELECT id, candidate_id, record_identity_key, retired_at::text AS retired_at, retired_reason
        FROM public.candidate_records
        WHERE candidate_id = ANY($1::uuid[])
        ORDER BY id
        FOR UPDATE
      `,
      [pair]
    );
    const survivorRecordIdByKey = new Map(
      recordsResult.rows
        .filter((row) => row.candidate_id === survivorId)
        .map((row) => [row.record_identity_key, row.id])
    );
    const rehomeRecordIds: string[] = [];
    const duplicateRecordPairs: { duplicateRecordId: string; survivorRecordId: string }[] = [];
    for (const record of recordsResult.rows) {
      if (record.candidate_id !== mergedId) continue;
      const survivorRecordId = survivorRecordIdByKey.get(record.record_identity_key);
      if (survivorRecordId) {
        duplicateRecordPairs.push({ duplicateRecordId: record.id, survivorRecordId });
      } else {
        rehomeRecordIds.push(record.id);
      }
    }
    const duplicateRecordIds = duplicateRecordPairs.map((pairRow) => pairRow.duplicateRecordId);
    const survivorRecordIdByDuplicate = new Map(
      duplicateRecordPairs.map((pairRow) => [pairRow.duplicateRecordId, pairRow.survivorRecordId])
    );

    // Mixed retirement states across an identical-key pair are conflicting
    // operator decisions about the SAME claim for the same person. Retirement
    // wins: deleting a retired duplicate while the survivor's copy stays
    // active would silently resurrect a withdrawn claim, and hiding a claim
    // pending re-review is the recoverable direction (un-hiding a wrongly
    // shown one is not — it may already have been served). The survivor's own
    // retirement, when it is the retired side, simply stands.
    const recordRowById = new Map(recordsResult.rows.map((row) => [row.id, row]));
    let retirementsPropagated = 0;
    for (const { duplicateRecordId, survivorRecordId } of duplicateRecordPairs) {
      const duplicate = recordRowById.get(duplicateRecordId);
      const survivor = recordRowById.get(survivorRecordId);
      if (!duplicate?.retired_at || survivor?.retired_at) {
        continue;
      }
      retirementsPropagated += 1;
      if (!dryRun) {
        await client.query(
          `
            UPDATE public.candidate_records
            SET retired_at = $2::timestamptz,
                retired_reason = $3,
                updated_at = now()
            WHERE id = $1::uuid
              AND retired_at IS NULL
          `,
          [survivorRecordId, duplicate.retired_at, duplicate.retired_reason]
        );
      }
    }

    let areaTagsCopied = 0;
    for (const { duplicateRecordId, survivorRecordId } of duplicateRecordPairs) {
      // Counted via a conflict-free pre-check rather than INSERT ... RETURNING
      // so dry runs report the same number a live run would.
      const missingTags = await client.query<{ id: string }>(
        `
          SELECT t.id
          FROM public.candidate_record_area_tags t
          WHERE t.candidate_record_id = $1::uuid
            AND NOT EXISTS (
              SELECT 1 FROM public.candidate_record_area_tags s
              WHERE s.candidate_record_id = $2::uuid
                AND s.research_area_id = t.research_area_id
            )
        `,
        [duplicateRecordId, survivorRecordId]
      );
      areaTagsCopied += missingTags.rows.length;
      if (!dryRun && missingTags.rows.length > 0) {
        await client.query(
          `
            INSERT INTO public.candidate_record_area_tags
              (candidate_record_id, research_area_id, stance, created_at, updated_at)
            SELECT $2::uuid, t.research_area_id, t.stance, t.created_at, t.updated_at
            FROM public.candidate_record_area_tags t
            WHERE t.candidate_record_id = $1::uuid
            ON CONFLICT (candidate_record_id, research_area_id) DO NOTHING
          `,
          [duplicateRecordId, survivorRecordId]
        );
      }
    }

    // Notification events are reconciled BEFORE the duplicate records are
    // deleted so nothing cascades away silently. Rehomed so "already
    // notified" history survives; an election-scoped event colliding with the
    // survivor's (uq_ucf_notification_events_election /
    // uq_ucf_notification_events_withdrawal — both per-type on
    // user/candidate/election) is deleted — the user was already notified
    // about that election for this person. Events on duplicate-deleted
    // records are remapped to the survivor's copy of the same content
    // (deleted when the user already has one there), so pending digest
    // events stay pending and nobody is re-notified later.
    const ELECTION_SCOPED_EVENT_TYPES = new Set([
      "candidate_future_election",
      "candidate_election_withdrawal",
    ]);
    const eventsResult = await client.query<EventRow>(
      `
        SELECT id, candidate_id, user_id, event_type, election_id, candidate_record_id
        FROM public.user_candidate_follow_notification_events
        WHERE candidate_id = ANY($1::uuid[])
        ORDER BY id
        FOR UPDATE
      `,
      [pair]
    );
    const survivorElectionEventKeys = new Set(
      eventsResult.rows
        .filter(
          (row) =>
            row.candidate_id === survivorId &&
            ELECTION_SCOPED_EVENT_TYPES.has(row.event_type) &&
            row.election_id !== null
        )
        .map((row) => `${row.user_id}:${row.event_type}:${row.election_id}`)
    );
    const survivorRecordEventKeys = new Set(
      eventsResult.rows
        .filter(
          (row) =>
            row.candidate_id === survivorId &&
            row.event_type === "candidate_record_update" &&
            row.candidate_record_id !== null
        )
        .map((row) => `${row.user_id}:${row.candidate_record_id}`)
    );
    const rehomeEventIds: string[] = [];
    const duplicateEventIds: string[] = [];
    const remapEvents: { eventId: string; survivorRecordId: string }[] = [];
    for (const event of eventsResult.rows) {
      if (event.candidate_id !== mergedId) continue;
      const remapTarget = event.candidate_record_id
        ? survivorRecordIdByDuplicate.get(event.candidate_record_id)
        : undefined;
      if (remapTarget) {
        if (survivorRecordEventKeys.has(`${event.user_id}:${remapTarget}`)) {
          duplicateEventIds.push(event.id);
        } else {
          remapEvents.push({ eventId: event.id, survivorRecordId: remapTarget });
        }
        continue;
      }
      const collides =
        ELECTION_SCOPED_EVENT_TYPES.has(event.event_type) &&
        event.election_id !== null &&
        survivorElectionEventKeys.has(`${event.user_id}:${event.event_type}:${event.election_id}`);
      if (collides) {
        duplicateEventIds.push(event.id);
      } else {
        rehomeEventIds.push(event.id);
      }
    }
    if (!dryRun && duplicateEventIds.length > 0) {
      await client.query(
        `DELETE FROM public.user_candidate_follow_notification_events WHERE id = ANY($1::uuid[])`,
        [duplicateEventIds]
      );
    }
    if (!dryRun) {
      for (const { eventId, survivorRecordId } of remapEvents) {
        await client.query(
          `UPDATE public.user_candidate_follow_notification_events SET candidate_id = $2::uuid, candidate_record_id = $3::uuid WHERE id = $1::uuid`,
          [eventId, survivorId, survivorRecordId]
        );
      }
    }
    if (!dryRun && rehomeEventIds.length > 0) {
      await client.query(
        `UPDATE public.user_candidate_follow_notification_events SET candidate_id = $2::uuid WHERE id = ANY($1::uuid[])`,
        [rehomeEventIds, survivorId]
      );
    }

    if (!dryRun && duplicateRecordIds.length > 0) {
      await client.query(`DELETE FROM public.candidate_records WHERE id = ANY($1::uuid[])`, [
        duplicateRecordIds,
      ]);
    }
    if (!dryRun && rehomeRecordIds.length > 0) {
      await client.query(
        `UPDATE public.candidate_records SET candidate_id = $2::uuid, updated_at = now() WHERE id = ANY($1::uuid[])`,
        [rehomeRecordIds, survivorId]
      );
    }

    // Sweep confirmations are keyed by candidate + context. Every duplicate
    // context row goes. The survivor's rows go too when records were rehomed: their
    // completeness claims (and its empty-claim evidence ledger) describe the
    // pre-merge record set, and a fresh sweep re-establishes them.
    const confirmationsResult = await client.query<{ candidate_id: string }>(
      `
        SELECT candidate_id FROM public.candidate_record_sweep_confirmations
        WHERE candidate_id = ANY($1::uuid[])
        ORDER BY candidate_id
        FOR UPDATE
      `,
      [pair]
    );
    const confirmationIds = new Set(confirmationsResult.rows.map((row) => row.candidate_id));
    const mergedConfirmationDeleted = confirmationIds.has(mergedId);
    const survivorConfirmationDeleted =
      confirmationIds.has(survivorId) && rehomeRecordIds.length > 0;
    const confirmationDeleteIds = [
      ...(mergedConfirmationDeleted ? [mergedId] : []),
      ...(survivorConfirmationDeleted ? [survivorId] : []),
    ];
    if (!dryRun && confirmationDeleteIds.length > 0) {
      await client.query(
        `DELETE FROM public.candidate_record_sweep_confirmations WHERE candidate_id = ANY($1::uuid[])`,
        [confirmationDeleteIds]
      );
    }

    // Follows: a user following both candidates keeps the survivor follow
    // (their notification preferences on the surviving page win).
    const followsResult = await client.query<FollowRow>(
      `
        SELECT id, candidate_id, user_id
        FROM public.user_candidate_follows
        WHERE candidate_id = ANY($1::uuid[])
        ORDER BY id
        FOR UPDATE
      `,
      [pair]
    );
    const survivorFollowUserIds = new Set(
      followsResult.rows
        .filter((row) => row.candidate_id === survivorId)
        .map((row) => row.user_id)
    );
    const rehomeFollowIds: string[] = [];
    const duplicateFollowIds: string[] = [];
    for (const follow of followsResult.rows) {
      if (follow.candidate_id !== mergedId) continue;
      if (survivorFollowUserIds.has(follow.user_id)) {
        duplicateFollowIds.push(follow.id);
      } else {
        rehomeFollowIds.push(follow.id);
      }
    }
    if (!dryRun && duplicateFollowIds.length > 0) {
      await client.query(`DELETE FROM public.user_candidate_follows WHERE id = ANY($1::uuid[])`, [
        duplicateFollowIds,
      ]);
    }
    if (!dryRun && rehomeFollowIds.length > 0) {
      await client.query(
        `UPDATE public.user_candidate_follows SET candidate_id = $2::uuid WHERE id = ANY($1::uuid[])`,
        [rehomeFollowIds, survivorId]
      );
    }

    // Everything else that references candidates — every state finance table,
    // the presidential tables, whatever ships next. Rehome when only the
    // duplicate has rows; refuse when both do (unique keys like
    // (candidate_id, election_id, committee_id) could collide, and which rows
    // win is a user decision).
    const candidateReferences = await listFkReferences(client, "public.candidates");
    const otherTables: MergeCandidatesResult["otherTables"] = [];
    for (const { table, column } of candidateReferences) {
      if (SPECIALLY_HANDLED_TABLES.has(table)) continue;
      const mergedCount = await client.query<{ n: string }>(
        `SELECT count(*)::text AS n FROM ${table} WHERE ${column} = $1::uuid`,
        [mergedId]
      );
      const n = Number(mergedCount.rows[0]?.n ?? "0");
      if (n === 0) continue;
      const survivorCount = await client.query<{ n: string }>(
        `SELECT count(*)::text AS n FROM ${table} WHERE ${column} = $1::uuid`,
        [survivorId]
      );
      const s = Number(survivorCount.rows[0]?.n ?? "0");
      if (s > 0) {
        throw new Error(
          `Both candidates have rows in ${table}.${column} (duplicate: ${n}, survivor: ${s}); ` +
            "merging them could collide on the table's unique keys. Resolve those rows first (user decision), then re-run."
        );
      }
      if (!dryRun) {
        await client.query(`UPDATE ${table} SET ${column} = $2::uuid WHERE ${column} = $1::uuid`, [
          mergedId,
          survivorId,
        ]);
      }
      otherTables.push({ table, column, rowsRehomed: n });
    }

    // Collapse merge chains: anything already merged into the duplicate now
    // points straight at the survivor.
    const chainResult = await client.query<{ id: string }>(
      `
        SELECT id FROM public.candidates
        WHERE merged_into_candidate_id = $1::uuid
        ORDER BY id
        FOR UPDATE
      `,
      [mergedId]
    );
    if (!dryRun && chainResult.rows.length > 0) {
      await client.query(
        `UPDATE public.candidates SET merged_into_candidate_id = $2::uuid, updated_at = now() WHERE merged_into_candidate_id = $1::uuid`,
        [mergedId, survivorId]
      );
    }

    // Union hard identifiers onto the survivor — the two rows usually carry
    // complementary ids (one shell has the FEC id, the other the state filing
    // id), and losing either would undo the identity work the merge encodes.
    // Appended counts are measured against the survivor's DEDUPED list — a
    // raw list already carrying a case/whitespace duplicate would otherwise
    // offset a genuinely new id and hide the append (same trap as the
    // supersede wrapper's source counting).
    const survivorFecIds = mergeIdentifierLists(parseIdentifierList(survivor.fec_ids), undefined) ?? [];
    const survivorFilingIds =
      mergeIdentifierLists(parseIdentifierList(survivor.state_filing_ids), undefined) ?? [];
    const mergedFecIds = mergeIdentifierLists(survivorFecIds, parseIdentifierList(merged.fec_ids)) ?? [];
    const mergedFilingIds =
      mergeIdentifierLists(survivorFilingIds, parseIdentifierList(merged.state_filing_ids)) ?? [];
    const fecIdsAppended = mergedFecIds.length - survivorFecIds.length;
    const stateFilingIdsAppended = mergedFilingIds.length - survivorFilingIds.length;
    // Profile fields: fill survivor blanks from the duplicate (a populated
    // survivor value always wins), and union profile_sources so provenance
    // survives. Without this, a website or summary living only on the
    // duplicate would silently vanish from the visible profile.
    const fieldsFilled: ProfileFillField[] = [];
    for (const field of PROFILE_FILL_FIELDS) {
      if (isBlank(survivor[field]) && !isBlank(merged[field])) {
        fieldsFilled.push(field);
      }
    }
    const survivorProfileSources = [...new Set(normalizeUrlList(survivor.profile_sources))];
    const mergedProfileSources = [
      ...new Set([...survivorProfileSources, ...normalizeUrlList(merged.profile_sources)]),
    ];
    const profileSourcesAppended = mergedProfileSources.length - survivorProfileSources.length;

    // The duplicate's websites (current + former) join the survivor's
    // former_website_urls archive so they keep matching this person in future
    // profile writes. The survivor's effective current website — its own, or
    // the duplicate's when the fill above copied it over — stays out of the
    // archive.
    const survivorEffectiveWebsite =
      !isBlank(survivor.official_website_url) || !fieldsFilled.includes("official_website_url")
        ? survivor.official_website_url
        : merged.official_website_url;
    const survivorFormerWebsites = normalizeUrlList(survivor.former_website_urls);
    // The appended count is measured against a baseline given the SAME
    // dedupe + current-site exclusion as the union below (same trap as the
    // identifier counts above): a stored archive already carrying a
    // normalized duplicate, or the survivor's own current site, would
    // otherwise offset a genuinely new URL and hide the append. The baseline
    // is a subset of the union, so the difference cannot go negative.
    const survivorFormerWebsitesBaseline = unionFormerWebsiteUrls({
      survivorCurrentWebsite: isBlank(survivorEffectiveWebsite) ? null : survivorEffectiveWebsite,
      survivorFormerWebsites,
      duplicateCurrentWebsite: null,
      duplicateFormerWebsites: [],
    });
    const mergedFormerWebsites = unionFormerWebsiteUrls({
      survivorCurrentWebsite: isBlank(survivorEffectiveWebsite) ? null : survivorEffectiveWebsite,
      survivorFormerWebsites,
      duplicateCurrentWebsite: isBlank(merged.official_website_url)
        ? null
        : merged.official_website_url,
      duplicateFormerWebsites: normalizeUrlList(merged.former_website_urls),
    });
    const formerWebsiteUrlsAppended =
      mergedFormerWebsites.length - survivorFormerWebsitesBaseline.length;

    // Column names come from the PROFILE_FILL_FIELDS constant above, never
    // from user input.
    const survivorSet: string[] = [];
    const survivorValues: unknown[] = [survivorId];
    const addAssignment = (column: string, cast: string, value: unknown) => {
      survivorValues.push(value);
      survivorSet.push(`${column} = $${survivorValues.length}${cast}`);
    };
    if (fecIdsAppended > 0) addAssignment("fec_ids", "::jsonb", JSON.stringify(mergedFecIds));
    if (stateFilingIdsAppended > 0) {
      addAssignment("state_filing_ids", "::jsonb", JSON.stringify(mergedFilingIds));
    }
    for (const field of fieldsFilled) {
      addAssignment(field, field === "date_of_birth" ? "::date" : "", merged[field]);
    }
    if (profileSourcesAppended > 0) {
      addAssignment("profile_sources", "::jsonb", JSON.stringify(mergedProfileSources));
    }
    if (JSON.stringify(mergedFormerWebsites) !== JSON.stringify(survivorFormerWebsites)) {
      addAssignment(
        "former_website_urls",
        "::jsonb",
        mergedFormerWebsites.length > 0 ? JSON.stringify(mergedFormerWebsites) : null
      );
    }
    if (!dryRun && survivorSet.length > 0) {
      await client.query(
        `UPDATE public.candidates SET ${survivorSet.join(", ")}, updated_at = now() WHERE id = $1::uuid`,
        survivorValues
      );
    }

    // Finally mark the duplicate merged. chk_candidates_merged_implies_deleted
    // requires deleted_at; an already-soft-deleted duplicate keeps its
    // original timestamp.
    if (!dryRun) {
      await client.query(
        `
          UPDATE public.candidates
          SET merged_into_candidate_id = $2::uuid,
              deleted_at = COALESCE(deleted_at, now()),
              updated_at = now()
          WHERE id = $1::uuid
        `,
        [mergedId, survivorId]
      );
    }

    if (dryRun) {
      await client.query("ROLLBACK");
    } else {
      await client.query("COMMIT");
    }

    return {
      dryRun,
      mergedCandidateId: mergedId,
      mergedCandidateName: candidateName(merged),
      survivorCandidateId: survivorId,
      survivorCandidateName: candidateName(survivor),
      links: { rehomed: rehomeLinkIds.length, duplicatesDeleted: duplicateLinkIds.length },
      mateLinks: { rehomed: mergedAsMate.length },
      records: {
        rehomed: rehomeRecordIds.length,
        duplicatesDeleted: duplicateRecordIds.length,
        areaTagsCopied,
        retirementsPropagated,
      },
      sweepConfirmations: {
        mergedDeleted: mergedConfirmationDeleted,
        survivorDeleted: survivorConfirmationDeleted,
      },
      follows: { rehomed: rehomeFollowIds.length, duplicatesDeleted: duplicateFollowIds.length },
      choices: {
        repointedToSurvivor: repointChoiceIds.length,
        duplicatesDeleted: duplicateChoiceDeleteIds.length,
      },
      notificationEvents: {
        rehomed: rehomeEventIds.length,
        duplicatesDeleted: duplicateEventIds.length,
        remappedToSurvivorRecords: remapEvents.length,
      },
      otherTables,
      chainCollapsedCandidates: chainResult.rows.length,
      identifiers: { fecIdsAppended, stateFilingIdsAppended },
      profile: {
        fieldsFilled: fieldsFilled,
        sourcesAppended: profileSourcesAppended,
        formerWebsiteUrlsAppended,
      },
    };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  }
}

async function main(): Promise<void> {
  assertKnownCliFlags("manual:candidates:merge", process.argv.slice(2), [
    { name: "--candidate-id", value: "space" },
    { name: "--into-candidate-id", value: "space" },
    { name: "--reason", value: "space" },
    { name: "--dry-run", value: "none" },
  ]);
  loadProjectEnv();

  const candidateId = requireFlag("--candidate-id");
  const intoCandidateId = requireFlag("--into-candidate-id");
  const reason = requireFlag("--reason");
  const dryRun = process.argv.includes("--dry-run");

  for (const [name, value] of [
    ["--candidate-id", candidateId],
    ["--into-candidate-id", intoCandidateId],
  ] as const) {
    if (!UUID_RE.test(value)) throw new Error(`Invalid ${name}: ${value}`);
  }
  if (reason.length < 20) {
    throw new Error("--reason must explain the merge in at least 20 characters");
  }

  const databaseUrl = requireEnv("DATABASE_URL");
  requireLocalDatabaseTarget(databaseUrl);
  const pool = new Pool({ connectionString: databaseUrl });
  const client = await pool.connect();
  try {
    const result = await runMergeCandidates(client, { candidateId, intoCandidateId, dryRun });
    console.log(JSON.stringify({ ...result, reason }, null, 2));
  } finally {
    client.release();
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  void main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
