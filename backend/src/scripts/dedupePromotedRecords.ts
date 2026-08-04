import { writeFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

import { Pool, type PoolClient } from "pg";

import { loadProjectEnv } from "../config/env.js";
import {
  DESCRIPTION_SIMILARITY_UPDATE_THRESHOLD,
  normalizeUrlForIdentity,
  scoreCandidateRecordDescriptionSimilarity,
} from "../pipeline/candidates/candidateRecordStore.js";
import {
  assertConfirmedTarget,
  assertPromotionEndpoints,
  confirmationTokenFor,
  describeEndpoint,
  loadProjection,
  RECORD_PROJECTION_SQL,
  recordKey,
  resolveIdentityTransitions,
  TRANSITION_PROJECTION_SQL,
  chunk,
  type PromotionClient,
  type RecordRow,
  type TransitionRow,
} from "./promoteResearchData.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";

// Removes the duplicate candidate_records rows that promotion left on the
// target before it learned to recognize re-keyed rows (promoteResearchData's
// planRecordRekeys). The failure it repairs: a sanctioned local description
// edit (the 2026-08-01 plain-language rewrite, 817 records) re-keys its row,
// and the 2026-08-02 promotion — matching on (candidate_id,
// record_identity_key) alone — inserted the edited row as a NEW record while
// the old-phrasing row sat unmatched on the target. Readers then saw the same
// fact twice, in two phrasings, with identical date and source.
//
// A target row is deleted only when ALL of the following hold, so a
// legitimately target-only row (e.g. one deleted locally on purpose) is never
// touched:
//   1. its (candidate_id, record_identity_key) exists nowhere in the local
//      database — no local row claims this content;
//   2. a sibling row on the SAME target with the same candidate, event date
//      and normalized source URL DOES exist locally — the fact is still
//      represented, by the row that superseded this one; and
//   3. the two descriptions score at or above the ingest writer's own
//      update-similarity threshold — the same rule that would have made the
//      writer update in place rather than insert.
//
// Deletion (not retirement) is deliberate: these rows are transport
// artifacts, not researched claims someone withdrew. Tags cascade (the
// keeper carries its own copies), but USER-generated references are rehomed
// onto the keeper before the delete: notification events (the already-told
// dedupe ledger — losing them would let a worker re-notify) and content
// reports (no FK; they would silently dangle). See the rehome statements
// below.

export type PromotedDuplicate = {
  staleRow: RecordRow;
  keeperRow: RecordRow;
  /** 1 for transition-matched pairs — the ledger is exact, not a score. */
  similarity: number;
  /**
   * 'transition': the identity ledger names this old key as superseded by the
   * keeper's key — exact provenance, catches rewrites of any magnitude.
   * 'similarity': same-slot heuristic for pre-ledger edits.
   */
  via: "transition" | "similarity";
};

export type PromotedRecordDedupePlan = {
  deletions: PromotedDuplicate[];
  /**
   * Target-only rows that matched no keeper: locally deleted rows, rows whose
   * successor changed date or URL, or genuine strays. Reported for the
   * operator, never deleted by this tool.
   */
  unmatchedOrphans: RecordRow[];
};

export function planPromotedRecordDedupe(input: {
  sourceRows: readonly RecordRow[];
  targetRows: readonly RecordRow[];
  normalizeUrl: (url: string) => string;
  similarityOf: (left: string, right: string) => number;
  threshold: number;
  /** Chain-resolved ledger from resolveIdentityTransitions; empty disables the exact pass. */
  transitions?: ReadonlyMap<string, string>;
}): PromotedRecordDedupePlan {
  const transitions = input.transitions ?? new Map<string, string>();
  const sourceKeys = new Set(input.sourceRows.map(recordKey));

  const bucketOf = (row: RecordRow): string =>
    [row.candidate_id, row.event_date, input.normalizeUrl(row.source_url)].join(" ");

  // Keepers are target rows the local database still stands behind. Only
  // they can justify deleting a sibling.
  const keeperByKey = new Map<string, RecordRow>();
  const keeperBuckets = new Map<string, RecordRow[]>();
  const orphans: RecordRow[] = [];
  for (const row of input.targetRows) {
    if (sourceKeys.has(recordKey(row))) {
      keeperByKey.set(recordKey(row), row);
      const bucket = bucketOf(row);
      keeperBuckets.set(bucket, [...(keeperBuckets.get(bucket) ?? []), row]);
    } else {
      orphans.push(row);
    }
  }

  const deletions: PromotedDuplicate[] = [];
  const unmatchedOrphans: RecordRow[] = [];
  for (const staleRow of orphans) {
    // Exact pass: the ledger names this old key's successor. Delete only when
    // that successor is ALSO on the target (otherwise the fact would vanish
    // from the target entirely — that state belongs to promotion's rekey, not
    // to cleanup).
    const finalKey = transitions.get(recordKey(staleRow));
    const ledgeredKeeper =
      finalKey === undefined
        ? undefined
        : keeperByKey.get(recordKey({ candidate_id: staleRow.candidate_id, record_identity_key: finalKey }));
    if (ledgeredKeeper !== undefined) {
      deletions.push({ staleRow, keeperRow: ledgeredKeeper, similarity: 1, via: "transition" });
      continue;
    }

    let best: { row: RecordRow; similarity: number } | null = null;
    for (const keeperRow of keeperBuckets.get(bucketOf(staleRow)) ?? []) {
      const similarity = input.similarityOf(staleRow.description, keeperRow.description);
      if (best === null || similarity > best.similarity) {
        best = { row: keeperRow, similarity };
      }
    }
    if (best !== null && best.similarity >= input.threshold) {
      deletions.push({ staleRow, keeperRow: best.row, similarity: best.similarity, via: "similarity" });
    } else {
      unmatchedOrphans.push(staleRow);
    }
  }

  return { deletions, unmatchedOrphans };
}

// Addressed by natural key — unique on the target, so each wire row deletes
// at most one record. Tags cascade by FK on purpose: the keeper carries its
// own tags, so the stale row's copies are redundant research data. Anything
// USER-generated that points at the stale row is rehomed first (below) —
// cascade must never eat notification history, and content reports have no
// FK and would silently dangle.
export const DELETE_DUPLICATES_SQL = `
  DELETE FROM public.candidate_records AS t
  USING jsonb_to_recordset($1::jsonb) AS s(candidate_id uuid, record_identity_key text, keeper_record_identity_key text)
  WHERE t.candidate_id = s.candidate_id
    AND t.record_identity_key = s.record_identity_key
`;

// Resolves each (stale, keeper) pair to target row ids at execution time —
// the wire carries natural keys, and ids differ per database.
const PAIRS_CTE = `
  pairs AS (
    SELECT stale.id AS stale_id, keeper.id AS keeper_id
    FROM jsonb_to_recordset($1::jsonb)
      AS s(candidate_id uuid, record_identity_key text, keeper_record_identity_key text)
    JOIN public.candidate_records AS stale
      ON stale.candidate_id = s.candidate_id
     AND stale.record_identity_key = s.record_identity_key
    JOIN public.candidate_records AS keeper
      ON keeper.candidate_id = s.candidate_id
     AND keeper.record_identity_key = s.keeper_record_identity_key
  )
`;

// Notification events are the "this follower was already told" ledger: the
// partial unique index (user_id, candidate_record_id) on record-update
// events is what stops a worker from notifying twice. Deleting the stale
// row would cascade its events away and let a later worker re-announce the
// same fact under the keeper's id, so events move to the keeper instead.
// The NOT EXISTS guard skips an event whose user already has one for the
// keeper — that leftover then cascades with the delete, which is correct:
// it is a duplicate of a notification the keeper's own event already records.
export const REHOME_NOTIFICATION_EVENTS_SQL = `
  WITH ${PAIRS_CTE}
  UPDATE public.user_candidate_follow_notification_events AS e
  SET candidate_record_id = p.keeper_id
  FROM pairs AS p
  WHERE e.candidate_record_id = p.stale_id
    AND NOT EXISTS (
      SELECT 1 FROM public.user_candidate_follow_notification_events AS k
      WHERE k.user_id = e.user_id
        AND k.candidate_record_id = p.keeper_id
        AND k.event_type = e.event_type
    )
`;

// content_reports.entity_id is a loose UUID with no FK — a report filed
// against the stale row (a duplicated record is exactly what a reader would
// report) would silently dangle after the delete. Point it at the keeper,
// which is the same fact.
export const REHOME_CONTENT_REPORTS_SQL = `
  WITH ${PAIRS_CTE}
  UPDATE public.content_reports AS r
  SET entity_id = p.keeper_id
  FROM pairs AS p
  WHERE r.entity_type = 'candidate_record'
    AND r.entity_id = p.stale_id
`;

const CASCADED_TAGS_SQL = `
  SELECT count(*)::int AS tags
  FROM public.candidate_record_area_tags AS tag
  JOIN public.candidate_records AS r ON r.id = tag.candidate_record_id
  JOIN jsonb_to_recordset($1::jsonb) AS s(candidate_id uuid, record_identity_key text)
    ON r.candidate_id = s.candidate_id AND r.record_identity_key = s.record_identity_key
`;

export type DedupeReport = {
  mode: "dry_run" | "apply";
  source: string;
  target: string;
  deletions: number;
  deletionsViaTransition: number;
  deletionsViaSimilarity: number;
  unmatchedOrphans: number;
  cascadedTags?: number;
  /** Notification events moved from stale rows onto their keepers (apply only). */
  rehomedNotificationEvents?: number;
  /** Content reports re-pointed from stale rows onto their keepers (apply only). */
  rehomedContentReports?: number;
  deleted?: number;
  byCandidate: Record<string, number>;
  samples: { candidateId: string; eventDate: string; keep: string; delete: string; similarity: number }[];
};

const SCRIPT_LABEL = "dedupe promoted records";

function usage(): string {
  return [
    "Usage:",
    "  npm run research:promote:dedupe                       # dry run, writes nothing",
    "  npm run research:promote:dedupe:apply -- --confirm-target <host>:<port>/<database>",
    "",
    "Endpoints (same contract as research:promote):",
    "  source  DATABASE_URL                     (must be local; read-only)",
    "  target  PROMOTION_TARGET_DATABASE_URL    (env only, never a flag)",
  ].join("\n");
}

function readFlagValue(argv: readonly string[], name: string): string | null {
  const index = argv.indexOf(name);
  if (index < 0) {
    return null;
  }
  const value = argv[index + 1];
  return value !== undefined && !value.startsWith("--") ? value : null;
}

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  assertKnownCliFlags(SCRIPT_LABEL, argv, [
    { name: "--apply", value: "none" },
    { name: "--confirm-target", value: "space" },
    { name: "--report-file", value: "space" },
  ]);

  loadProjectEnv();
  const apply = argv.includes("--apply");
  const endpoints = assertPromotionEndpoints({
    sourceUrl: process.env.DATABASE_URL ?? "",
    targetUrl: process.env.PROMOTION_TARGET_DATABASE_URL ?? "",
  });
  if (apply) {
    assertConfirmedTarget(endpoints.target, readFlagValue(argv, "--confirm-target") ?? "");
  }

  console.log(`source: ${describeEndpoint(endpoints.source)}`);
  console.log(`target: ${describeEndpoint(endpoints.target)}`);
  console.log(`mode:   ${apply ? "APPLY (deletes duplicate rows)" : "dry run (writes nothing)"}`);

  const sourcePool = new Pool({ connectionString: process.env.DATABASE_URL });
  const targetPool = new Pool({
    connectionString: process.env.PROMOTION_TARGET_DATABASE_URL,
    connectionTimeoutMillis: 30_000,
    statement_timeout: 300_000,
  });
  const source: PromotionClient = { query: (text, values) => sourcePool.query(text, values as unknown[]) };
  const target: PromotionClient = { query: (text, values) => targetPool.query(text, values as unknown[]) };

  try {
    const [sourceRows, targetRows] = await Promise.all([
      loadProjection<RecordRow>(source, RECORD_PROJECTION_SQL),
      loadProjection<RecordRow>(target, RECORD_PROJECTION_SQL),
    ]);
    const transitions = resolveIdentityTransitions(
      await loadProjection<TransitionRow>(source, TRANSITION_PROJECTION_SQL)
    );

    const plan = planPromotedRecordDedupe({
      sourceRows,
      targetRows,
      normalizeUrl: normalizeUrlForIdentity,
      similarityOf: scoreCandidateRecordDescriptionSimilarity,
      threshold: DESCRIPTION_SIMILARITY_UPDATE_THRESHOLD,
      transitions,
    });

    const byCandidate: Record<string, number> = {};
    for (const deletion of plan.deletions) {
      byCandidate[deletion.staleRow.candidate_id] = (byCandidate[deletion.staleRow.candidate_id] ?? 0) + 1;
    }

    const report: DedupeReport = {
      mode: apply ? "apply" : "dry_run",
      source: describeEndpoint(endpoints.source),
      target: describeEndpoint(endpoints.target),
      deletions: plan.deletions.length,
      deletionsViaTransition: plan.deletions.filter((deletion) => deletion.via === "transition").length,
      deletionsViaSimilarity: plan.deletions.filter((deletion) => deletion.via === "similarity").length,
      unmatchedOrphans: plan.unmatchedOrphans.length,
      byCandidate,
      samples: plan.deletions.slice(0, 10).map((deletion) => ({
        candidateId: deletion.staleRow.candidate_id,
        eventDate: deletion.staleRow.event_date,
        keep: deletion.keeperRow.description.slice(0, 90),
        delete: deletion.staleRow.description.slice(0, 90),
        similarity: Number(deletion.similarity.toFixed(3)),
      })),
    };

    const wireRows = plan.deletions.map((deletion) => ({
      candidate_id: deletion.staleRow.candidate_id,
      record_identity_key: deletion.staleRow.record_identity_key,
      keeper_record_identity_key: deletion.keeperRow.record_identity_key,
    }));

    if (apply && wireRows.length > 0) {
      const client: PoolClient = await targetPool.connect();
      try {
        await client.query("BEGIN");
        let cascadedTags = 0;
        let rehomedEvents = 0;
        let rehomedReports = 0;
        let deleted = 0;
        for (const batch of chunk(wireRows)) {
          const payload = JSON.stringify(batch);
          // User-generated references move to the keeper BEFORE the delete;
          // only redundant research data (tags) is allowed to cascade.
          const events = await client.query(REHOME_NOTIFICATION_EVENTS_SQL, [payload]);
          rehomedEvents += events.rowCount ?? 0;
          const reports = await client.query(REHOME_CONTENT_REPORTS_SQL, [payload]);
          rehomedReports += reports.rowCount ?? 0;
          const tagCount = await client.query(CASCADED_TAGS_SQL, [payload]);
          cascadedTags += (tagCount.rows as { tags: number }[])[0]?.tags ?? 0;
          const result = await client.query(DELETE_DUPLICATES_SQL, [payload]);
          deleted += result.rowCount ?? 0;
        }
        // Every planned deletion must land: the natural key is unique, so a
        // shortfall means the target changed since the plan; an overrun is
        // impossible. Commit nothing rather than commit a partial guess.
        if (deleted !== wireRows.length) {
          throw new Error(
            `Refusing to commit: planned ${wireRows.length} deletion(s) but the target matched ${deleted}. ` +
              "The target changed since the plan was computed; re-run."
          );
        }
        await client.query("COMMIT");
        report.deleted = deleted;
        report.cascadedTags = cascadedTags;
        report.rehomedNotificationEvents = rehomedEvents;
        report.rehomedContentReports = rehomedReports;
      } catch (error) {
        await client.query("ROLLBACK").catch((rollbackError: unknown) => {
          console.error(
            `ROLLBACK failed after the error below; the transaction's state on the target is unknown: ${
              rollbackError instanceof Error ? rollbackError.message : String(rollbackError)
            }`
          );
        });
        throw error;
      } finally {
        client.release();
      }
    } else if (apply) {
      report.deleted = 0;
      report.cascadedTags = 0;
      report.rehomedNotificationEvents = 0;
      report.rehomedContentReports = 0;
    }

    const reportFile = readFlagValue(argv, "--report-file");
    if (reportFile) {
      await writeFile(reportFile, `${JSON.stringify(report, null, 2)}\n`, "utf8");
    }
    console.log(JSON.stringify(report, null, 2));
    if (plan.unmatchedOrphans.length > 0) {
      console.log(
        `\n${plan.unmatchedOrphans.length} target-only row(s) matched no keeper and were left alone ` +
          "(locally deleted rows, re-dated successors, or strays). First few:"
      );
      for (const orphan of plan.unmatchedOrphans.slice(0, 5)) {
        console.log(`  candidate ${orphan.candidate_id} ${orphan.event_date}: ${orphan.description.slice(0, 80)}`);
      }
    }
    if (!apply) {
      console.log(
        "\nDry run only — nothing was deleted. Re-run with:\n" +
          `  npm run research:promote:dedupe:apply -- --confirm-target ${confirmationTokenFor(endpoints.target)}`
      );
    }
  } finally {
    await Promise.allSettled([sourcePool.end(), targetPool.end()]);
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint && import.meta.url === entrypoint) {
  main().catch((error: unknown) => {
    console.error(`${SCRIPT_LABEL} failed: ${error instanceof Error ? error.message : String(error)}`);
    console.error(usage());
    process.exit(1);
  });
}
