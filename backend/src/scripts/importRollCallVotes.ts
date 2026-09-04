import { createHash } from "node:crypto";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { Pool, type PoolClient } from "pg";

import { validateCandidateRecordDiscoveryPayload } from "../ai/enrichCandidateRecords.js";
import { loadProjectEnv } from "../config/env.js";
import { loadAllResearchAreas } from "../pipeline/candidates/candidateRecordAreaTagging.js";
import { buildCandidateRecordIdentityKey } from "../pipeline/candidates/candidateRecordStore.js";
import { loadCongressLegislators } from "../pipeline/rollcall/congressLegislators.js";
import { parseFederalMeasure } from "../pipeline/rollcall/federalMeasures.js";
import {
  loadCandidateFecIndex,
  resolveFederalMembers,
  type FederalMemberResolution,
  type FederalMemberResolutionOutcome,
} from "../pipeline/rollcall/federalMemberResolver.js";
import { parseFederalMemberVotes } from "../pipeline/rollcall/federalRollCallMembers.js";
import { parseFederalRollCallXml } from "../pipeline/rollcall/federalRollCallXml.js";
import {
  assertLegislativeVoteStillApproved,
  loadLegislativeVote,
  type LegislativeVoteForImport,
} from "../pipeline/rollcall/legislativeVoteStore.js";
import type { LegislativeVoteReviewStatus } from "../pipeline/rollcall/legislativeVotes.js";
import {
  insertRollCallRecord,
  labelsForSide,
  loadExistingRecordsForDate,
  memberVoteSide,
  parseRollCallLabels,
  planCandidateRecord,
  refreshRollCallRecord,
  rewriteRollCallRecord,
  shouldNotifyForVoteDate,
  syncRollCallRecordTags,
  type CandidateRecordPlan,
  type RollCallLabel,
  type RollCallVoteSide,
} from "../pipeline/rollcall/rollCallFanOut.js";
import { rollCallUrlKey } from "../pipeline/rollcall/rollCallRecordUrls.js";
import { createCandidateRecordUpdateNotificationEvents } from "../pipeline/users/candidateFollowNotificationEvents.js";
import { reportPath } from "../pipeline/rollcall/rollCallReportPaths.js";
import { usLatestLocalDateIso } from "../utils/usLocalDate.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";
import {
  assertMemberRowsComplete,
  DEFAULT_SCOPE_FROM,
  listRollCallEvidenceFiles,
  type RollCallEvidenceFile,
} from "./resolveRollCallMembers.js";
import { WALL_CLOCK_FORCE_EXIT_GRACE_MS, withWallClockTimeout } from "./wallClockTimeout.js";

// Fan-out writer of the roll-call import (docs/plans/roll-call-vote-import.md
// §3): for every roll-call XML under an evidence dir whose legislative_votes
// row a human APPROVED, resolve the members to candidates (exact FEC id,
// Nov-2026 scope) and write one candidate_records row per member who voted,
// with the roll call's sentences and labels. One transaction per roll call;
// a re-run writes nothing new. Local DB only (prod gets rows via
// research:promote). No AI. Examples:
//
//   npm run rollcall:import -- --evidence-dir evidence/rollcall/<run-id> --dry-run
//   npm run rollcall:import -- --evidence-dir evidence/rollcall/<run-id>
//   npm run rollcall:import -- --evidence-dir evidence/rollcall/<run-id> --skip-existing

export const ROLLCALL_IMPORT_IMPORTER_VERSION = "rollcall-import-v1";
const DEFAULT_LEGISLATORS_DIR = "evidence/rollcall/congress-legislators";
const JURISDICTION = "US";

export type RollCallImportOutcome =
  | "imported"
  | "dry_run"
  // The row is pending or rejected: the queue decides, not the importer.
  | "not_approved"
  // No legislative_votes row: rollcall:fetch has not stored this roll call.
  | "missing_row"
  // The XML on disk is not the file the reviewer approved.
  | "source_mismatch"
  | "error";

// Outcomes that mean the run did not do what was asked; the exit code stops
// automation from reading such a run as clean.
const FAILURE_OUTCOMES: ReadonlySet<RollCallImportOutcome> = new Set(["source_mismatch", "error"]);

// A plan action, plus the maintenance-only marker for an out-of-scope
// candidate whose plan came back `insert` (nothing is written; the report
// records why).
export type RollCallImportAction = CandidateRecordPlan["action"] | "out_of_scope_no_record";

export type RollCallImportCandidateRow = {
  candidateId: string;
  candidateName: string;
  memberId: string;
  memberName: string;
  state: string;
  vote: string;
  side: RollCallVoteSide;
  // Resolved to a known candidate who is no longer on a Nov-2026-or-later
  // election: their EXISTING records are still maintained (rewrite on a
  // corrected judgment, tag re-sync), but no new record is inserted.
  maintenanceOnly: boolean;
  action: RollCallImportAction;
  recordId: string | null;
  // The live rows an `ambiguous` candidate already has for this vote.
  ambiguousRecordIds: string[];
  // At least one follower event was created for an inserted recent vote.
  notified: boolean;
  relatedRecordIds: string[];
};

export type RollCallImportReportRow = RollCallEvidenceFile & {
  outcome: RollCallImportOutcome;
  legislativeVoteId: string | null;
  reviewStatus: LegislativeVoteReviewStatus | null;
  voteDate: string | null;
  measureId: string | null;
  question: string | null;
  originRunId: string | null;
  members: number;
  resolution: Partial<Record<FederalMemberResolutionOutcome, number>>;
  // Matched members who voted Present / Not Voting: no record.
  notVoting: number;
  actions: Partial<Record<RollCallImportAction, number>>;
  // Follower notification events created (one per eligible follow).
  notified: number;
  candidates: RollCallImportCandidateRow[];
  error: string | null;
};

/**
 * The sentences, checked once per roll call by the same validator every
 * other records writer uses (schema, quality gate, source policy, and that
 * the roll-call URL answers). Both must survive, and the stored URL must
 * still be this roll call — a redirect elsewhere would break the dedupe.
 * (Shared with the Ohio importer: rollCallUrlKey folds the Ohio actions
 * URL too, so the same check covers both feeds.)
 */
export async function validateSideTemplates(
  vote: LegislativeVoteForImport,
  timeoutMs: number
): Promise<Record<RollCallVoteSide, { description: string; sourceUrl: string; eventDate: string }>> {
  const sides: RollCallVoteSide[] = ["yea", "nay"];
  const sentences = { yea: (vote.yeaDescription ?? "").trim(), nay: (vote.nayDescription ?? "").trim() };
  // The validator folds case when it de-duplicates rows, so a pair that
  // differs only in case would come back as one record.
  if (sentences.yea.toLowerCase() === sentences.nay.toLowerCase()) {
    throw new Error("yea_description and nay_description are the same sentence");
  }
  const validated = await withWallClockTimeout(
    validateCandidateRecordDiscoveryPayload(
      {
        records: sides.map((side) => ({
          description: sentences[side],
          source_url: vote.machineUrl,
          // The reviewed official-date override (migration 257) is the
          // records' event_date; the raw source date stays on the row for
          // the evidence checks.
          event_date: vote.officialVoteDate ?? vote.voteDate,
        })),
      },
      timeoutMs
    ),
    "roll-call sentence validation",
    { forceExitAfterMs: WALL_CLOCK_FORCE_EXIT_GRACE_MS }
  );
  if (!validated.ok) {
    throw new Error(`sentences failed validation: ${validated.reason}`);
  }
  if (validated.droppedRecords.length > 0) {
    throw new Error(`sentences failed validation: ${validated.droppedRecords.map((dropped) => dropped.reason).join("; ")}`);
  }
  const expectedKey = rollCallUrlKey(vote.machineUrl)?.key;
  const templates: Partial<Record<RollCallVoteSide, { description: string; sourceUrl: string; eventDate: string }>> = {};
  for (const side of sides) {
    const record = validated.records.find((candidate) => candidate.description === sentences[side]);
    if (!record) {
      throw new Error(`validator did not return the ${side} sentence`);
    }
    if (!expectedKey || rollCallUrlKey(record.source_url)?.key !== expectedKey) {
      throw new Error(`validated source URL ${record.source_url} is not roll call ${vote.machineUrl}`);
    }
    templates[side] = { description: record.description, sourceUrl: record.source_url, eventDate: record.event_date };
  }
  return templates as Record<RollCallVoteSide, { description: string; sourceUrl: string; eventDate: string }>;
}

function count<K extends string>(counts: Partial<Record<K, number>>, key: K): void {
  counts[key] = (counts[key] ?? 0) + 1;
}

export type RollCallVoter = Pick<
  RollCallImportCandidateRow,
  "candidateId" | "candidateName" | "memberId" | "memberName" | "state" | "vote" | "side" | "maintenanceOnly"
>;

/**
 * The members who took a position and resolve to exactly one candidate, one
 * per candidate. Counts every resolution outcome into `resolutionCounts` for
 * the report. `matched` voters get full treatment; an `out_of_scope`
 * resolution with a single unambiguous candidate becomes a MAINTENANCE
 * voter — a corrected judgment must still rewrite the records that were fanned
 * out while the member was in scope (a withdrawn candidate's profile keeps
 * showing them), but no new record is inserted for them. A candidate reached
 * from two member rows of one roll call is a data defect (two ids sharing an
 * FEC id), so it fails the roll call rather than writing twice.
 */
export function collectVoters(
  resolutions: readonly FederalMemberResolution[],
  resolutionCounts: Partial<Record<FederalMemberResolutionOutcome, number>>
): { voters: RollCallVoter[]; notVoting: number } {
  const voters: RollCallVoter[] = [];
  const seen = new Set<string>();
  let notVoting = 0;
  for (const resolution of resolutions) {
    count(resolutionCounts, resolution.outcome);
    const candidate =
      resolution.outcome === "matched"
        ? resolution.candidate
        : resolution.outcome === "out_of_scope" && resolution.candidates.length === 1
          ? resolution.candidates[0]!
          : null;
    if (!candidate) {
      continue;
    }
    const side = memberVoteSide(resolution.member.vote);
    if (side === null) {
      if (resolution.outcome === "matched") {
        notVoting += 1;
      }
      continue;
    }
    const { candidateId, name: candidateName } = candidate;
    if (seen.has(candidateId)) {
      throw new Error(`candidate ${candidateId} matches more than one member of this roll call`);
    }
    seen.add(candidateId);
    voters.push({
      candidateId,
      candidateName,
      memberId: resolution.member.memberId,
      memberName: resolution.member.name,
      state: resolution.member.state,
      vote: resolution.member.vote,
      side,
      maintenanceOnly: resolution.outcome === "out_of_scope",
    });
  }
  return { voters, notVoting };
}

/**
 * Which report file a run writes. Once the dir holds import-report.json —
 * the committed ledger of the FIRST import — neither kind of later run may
 * eat it: a dry rerun (a post-import check) lands in
 * import-dry-run-rerun-report.json, and a real rerun (a maintenance
 * re-import propagating a corrected judgment) lands in
 * import-rerun-report.json. Before the import, repeated dry runs overwrite
 * the plan file; that is the reviewing loop.
 */
export function importReportFileName(evidenceDir: string, dryRun: boolean): string {
  const hasLedger = existsSync(resolve(evidenceDir, "import-report.json"));
  if (!dryRun) {
    return hasLedger ? "import-rerun-report.json" : "import-report.json";
  }
  return hasLedger ? "import-dry-run-rerun-report.json" : "import-dry-run-report.json";
}

function readValueFlag(argv: readonly string[], flagName: string): string | null {
  const index = argv.indexOf(flagName);
  if (index >= 0) {
    const value = argv[index + 1];
    if (value === undefined || value.startsWith("--")) {
      throw new Error(`${flagName} requires a value`);
    }
    return value;
  }
  const inline = argv.find((token) => token.startsWith(`${flagName}=`));
  return inline ? inline.slice(flagName.length + 1) : null;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  assertKnownCliFlags("rollcall:import", argv, [
    { name: "--evidence-dir", value: "both" },
    { name: "--legislators-sha", value: "both" },
    { name: "--legislators-dir", value: "both" },
    { name: "--scope-from", value: "both" },
    { name: "--dry-run", value: "none" },
    { name: "--skip-existing", value: "none" },
  ]);
  const evidenceDirRaw = readValueFlag(argv, "--evidence-dir");
  if (evidenceDirRaw === null) {
    throw new Error("--evidence-dir is required");
  }
  const evidenceDir = resolve(evidenceDirRaw);
  const legislatorsSha = readValueFlag(argv, "--legislators-sha") ?? undefined;
  const legislatorsDir = resolve(readValueFlag(argv, "--legislators-dir") ?? DEFAULT_LEGISLATORS_DIR);
  const scopeFrom = readValueFlag(argv, "--scope-from") ?? DEFAULT_SCOPE_FROM;
  const dryRun = argv.includes("--dry-run");
  const skipExisting = argv.includes("--skip-existing");

  const files = listRollCallEvidenceFiles(evidenceDir);
  if (files.length === 0) {
    throw new Error(`${evidenceDir} holds no <chamber>-<congress>-<session>-roll<N>.xml files`);
  }

  loadProjectEnv();
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required");
  }
  requireLocalDatabaseTarget(databaseUrl);
  const validationTimeoutMs = Number(process.env.AI_TIMEOUT_MS?.trim() || 90_000);

  const startedAt = new Date();
  const today = usLatestLocalDateIso(startedAt);
  const legislators = await loadCongressLegislators({ sha: legislatorsSha, cacheDir: legislatorsDir });
  const pool = new Pool({ connectionString: databaseUrl });
  const rows: RollCallImportReportRow[] = [];
  try {
    const candidatesByFec = await loadCandidateFecIndex(pool, scopeFrom);
    const researchAreas = await loadAllResearchAreas(pool);
    const researchAreaSlugs = new Set(researchAreas.map((area) => area.slug));
    const researchAreaIdBySlug = new Map(researchAreas.map((area) => [area.slug, area.id]));

    for (const evidence of files) {
      const row: RollCallImportReportRow = {
        ...evidence,
        outcome: "error",
        legislativeVoteId: null,
        reviewStatus: null,
        voteDate: null,
        measureId: null,
        question: null,
        originRunId: null,
        members: 0,
        resolution: {},
        notVoting: 0,
        actions: {},
        notified: 0,
        candidates: [],
        error: null,
      };
      rows.push(row);
      const session = `${evidence.congress}-${evidence.session}`;
      try {
        const vote = await loadLegislativeVote(pool, {
          jurisdiction: JURISDICTION,
          chamber: evidence.chamber,
          session,
          rollNumber: evidence.roll,
        });
        if (!vote) {
          row.outcome = "missing_row";
          continue;
        }
        row.legislativeVoteId = vote.id;
        row.reviewStatus = vote.reviewStatus;
        row.voteDate = vote.voteDate;
        row.measureId = vote.measureId;
        row.question = vote.exactQuestion;
        if (vote.reviewStatus !== "approved") {
          row.outcome = "not_approved";
          continue;
        }

        const xml = readFileSync(resolve(evidenceDir, evidence.file), "utf8");
        const sha256 = createHash("sha256").update(xml).digest("hex");
        if (sha256 !== vote.sourceSha256) {
          row.outcome = "source_mismatch";
          row.error = `XML sha256 ${sha256} is not the approved ${vote.sourceSha256}; re-fetch and re-review`;
          continue;
        }
        const parsed = parseFederalRollCallXml(evidence.chamber, xml);
        if (parsed.congress !== evidence.congress || parsed.session !== evidence.session || parsed.rollNumber !== evidence.roll) {
          throw new Error(`XML says congress ${parsed.congress} session ${parsed.session} roll ${parsed.rollNumber}`);
        }
        const members = parseFederalMemberVotes(evidence.chamber, xml);
        row.members = members.length;
        assertMemberRowsComplete(parsed, members.length);

        // Everything that could fail the roll call is settled before the
        // first write: sentences, labels, identities, and the plan per
        // candidate.
        const templates = await validateSideTemplates(vote, validationTimeoutMs);
        const labels: RollCallLabel[] = parseRollCallLabels(vote.labelsJson, researchAreaSlugs);
        const sideLabels = { yea: labelsForSide(labels, "yea"), nay: labelsForSide(labels, "nay") };
        const measure = parseFederalMeasure(vote.measureId);
        const rollCallKey = rollCallUrlKey(vote.machineUrl)!.key;
        const originRunPrefix = `rollcall:${JURISDICTION}:${evidence.chamber}:${session}:${evidence.roll}:`;
        const originRunId = `${originRunPrefix}${startedAt.toISOString()}`;
        row.originRunId = originRunId;

        const resolutions = resolveFederalMembers(members, vote.voteDate, legislators.index, candidatesByFec);
        const voters = collectVoters(resolutions, row.resolution);
        row.notVoting = voters.notVoting;
        // The effective and raw dates catch hand-written rows; the run-id
        // prefix catches this pipeline's own rows on any date, so a changed
        // or cleared override still rewrites them instead of duplicating.
        const existingByCandidate = await loadExistingRecordsForDate(
          pool,
          voters.voters.map((voter) => voter.candidateId),
          [...new Set([vote.officialVoteDate ?? vote.voteDate, vote.voteDate])],
          originRunPrefix
        );
        const work = voters.voters.map((voter) => {
          const template = templates[voter.side];
          const identityKey = buildCandidateRecordIdentityKey(template);
          const decision = planCandidateRecord({
            existing: existingByCandidate.get(voter.candidateId) ?? [],
            identityKey,
            description: template.description,
            sourceUrl: template.sourceUrl,
            rollCallKey,
            measure,
            skipExisting,
          });
          // A maintenance-only voter has no record to maintain when the
          // plan is `insert`: nothing is written, and the report says why.
          const action: RollCallImportAction =
            voter.maintenanceOnly && decision.plan.action === "insert" ? "out_of_scope_no_record" : decision.plan.action;
          const reportRow: RollCallImportCandidateRow = {
            ...voter,
            action,
            recordId: "recordId" in decision.plan ? decision.plan.recordId : null,
            ambiguousRecordIds: decision.plan.action === "ambiguous" ? decision.plan.recordIds : [],
            notified: false,
            relatedRecordIds: decision.relatedRecordIds,
          };
          count(row.actions, action);
          row.candidates.push(reportRow);
          return { voter, template, identityKey, plan: decision.plan, reportRow };
        });

        if (dryRun) {
          row.outcome = "dry_run";
          continue;
        }

        const notify = shouldNotifyForVoteDate(vote.officialVoteDate ?? vote.voteDate, today);
        const client: PoolClient = await pool.connect();
        try {
          await client.query("BEGIN");
          await assertLegislativeVoteStillApproved(client, vote);
          for (const item of work) {
            const content = { ...item.template, candidateId: item.voter.candidateId, identityKey: item.identityKey, originRunId };
            let recordId: string | null = null;
            if (item.plan.action === "insert" && !item.voter.maintenanceOnly) {
              recordId = await insertRollCallRecord(client, content);
              item.reportRow.recordId = recordId;
              if (notify) {
                const events = await createCandidateRecordUpdateNotificationEvents(client, recordId);
                item.reportRow.notified = events.createdCount > 0;
                row.notified += events.createdCount;
              }
            } else if (item.plan.action === "rewrite") {
              recordId = item.plan.recordId;
              await rewriteRollCallRecord(client, { ...content, recordId, oldIdentityKey: item.plan.oldIdentityKey });
            } else if (item.plan.action === "refresh") {
              recordId = item.plan.recordId;
              await refreshRollCallRecord(client, {
                ...content,
                recordId,
                oldDescription: item.plan.oldDescription,
                oldSourceUrl: item.plan.oldSourceUrl,
              });
            } else if (item.plan.action === "unchanged") {
              recordId = item.plan.recordId;
            }
            if (recordId !== null) {
              await syncRollCallRecordTags(client, recordId, sideLabels[item.voter.side], researchAreaIdBySlug);
            }
          }
          await client.query("COMMIT");
          row.outcome = "imported";
        } catch (error) {
          await client.query("ROLLBACK").catch(() => undefined);
          throw error;
        } finally {
          client.release();
        }
      } catch (error) {
        row.outcome = "error";
        row.error = errorMessage(error);
      }
    }
  } finally {
    await pool.end();
  }

  const outcomes: Partial<Record<RollCallImportOutcome, number>> = {};
  const actions: Partial<Record<RollCallImportAction, number>> = {};
  for (const row of rows) {
    count(outcomes, row.outcome);
    for (const [action, n] of Object.entries(row.actions)) {
      actions[action as RollCallImportAction] = (actions[action as RollCallImportAction] ?? 0) + n;
    }
  }
  const report = {
    importerVersion: ROLLCALL_IMPORT_IMPORTER_VERSION,
    dryRun,
    skipExisting,
    startedAt: startedAt.toISOString(),
    finishedAt: new Date().toISOString(),
    evidenceDir: reportPath(evidenceDir),
    scopeFrom,
    legislatorsSha: legislators.sha,
    files: files.length,
    outcomes,
    actions,
    notified: rows.reduce((sum, row) => sum + row.notified, 0),
    rolls: rows,
  };
  writeFileSync(resolve(evidenceDir, importReportFileName(evidenceDir, dryRun)), `${JSON.stringify(report, null, 2)}\n`);
  console.log(
    JSON.stringify(
      {
        ...report,
        rolls: rows.map((row) => ({
          ...row,
          candidates: row.candidates.length,
          related: row.candidates.filter((candidate) => candidate.relatedRecordIds.length > 0).length,
        })),
      },
      null,
      2
    )
  );
  if (rows.some((row) => FAILURE_OUTCOMES.has(row.outcome))) {
    process.exitCode = 1;
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("rollcall:import failed:", errorMessage(error));
    process.exitCode = 1;
  });
}
