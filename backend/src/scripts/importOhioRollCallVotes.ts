import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { Pool, type PoolClient } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { loadAllResearchAreas } from "../pipeline/candidates/candidateRecordAreaTagging.js";
import { buildCandidateRecordIdentityKey } from "../pipeline/candidates/candidateRecordStore.js";
import {
  loadOhioCrosswalkCandidates,
  parseOhioCrosswalkFile,
  parseOhioLegislators,
  resolveOhioMembers,
  type OhioMemberResolution,
  type OhioMemberResolutionOutcome,
} from "../pipeline/rollcall/ohioMemberResolver.js";
import {
  ohioActionChamber,
  ohioActionMemberVotes,
  ohioActionSha256,
  ohioActionVoteDate,
  ohioRollNumber,
  parseOhioVoteEvidence,
  OHIO_JURISDICTION,
  type OhioAction,
} from "../pipeline/rollcall/ohioRollCall.js";
import {
  assertLegislativeVoteStillApproved,
  loadLegislativeVote,
} from "../pipeline/rollcall/legislativeVoteStore.js";
import type { LegislativeVoteReviewStatus } from "../pipeline/rollcall/legislativeVotes.js";
import {
  insertRollCallRecord,
  labelsForSide,
  loadExistingRecordsForDate,
  parseRollCallLabels,
  planCandidateRecord,
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
import { readPositiveIntegerFlag } from "../utils/cliFlags.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";
import { importReportFileName, validateSideTemplates } from "./importRollCallVotes.js";
import { DEFAULT_SCOPE_FROM } from "./resolveRollCallMembers.js";
import { listOhioRollCallEvidenceFiles, type OhioRollCallEvidenceFile } from "./resolveOhioRollCallMembers.js";

// Fan-out writer for the Ohio pilot (plan §5 phase 3): for every Ohio vote
// evidence file whose legislative_votes row a human APPROVED, resolve the
// yea/nay lpids through the committed crosswalk and write one
// candidate_records row per matched member, with the roll call's sentences
// and labels. Same guarantees as the federal importer: one transaction per
// roll call, sha-pinned evidence, approval re-checked under lock inside the
// write, re-runs write nothing new. Local DB only, no AI.
//
//   npm run rollcall:oh:import -- --ga 136 --evidence-dir evidence/rollcall/ohio-136/batch-01 \
//     --crosswalk-file evidence/rollcall/ohio-136/crosswalk.json \
//     --roster-file evidence/rollcall/ohio-136/oh-legislators-136.json --dry-run

export const OHIO_ROLLCALL_IMPORT_IMPORTER_VERSION = "rollcall-oh-import-v1";

export type OhioRollCallImportOutcome =
  | "imported"
  | "dry_run"
  | "not_approved"
  | "missing_row"
  | "source_mismatch"
  | "error";

const FAILURE_OUTCOMES: ReadonlySet<OhioRollCallImportOutcome> = new Set(["source_mismatch", "error"]);

export type OhioRollCallImportCandidateRow = {
  candidateId: string;
  candidateName: string;
  lpid: string;
  memberName: string | null;
  side: RollCallVoteSide;
  action: CandidateRecordPlan["action"];
  recordId: string | null;
  ambiguousRecordIds: string[];
  notified: boolean;
  relatedRecordIds: string[];
};

export type OhioRollCallImportReportRow = OhioRollCallEvidenceFile & {
  outcome: OhioRollCallImportOutcome;
  legislativeVoteId: string | null;
  reviewStatus: LegislativeVoteReviewStatus | null;
  bill: string | null;
  voteDate: string | null;
  measureId: string | null;
  question: string | null;
  originRunId: string | null;
  members: number;
  resolution: Partial<Record<OhioMemberResolutionOutcome, number>>;
  actions: Partial<Record<CandidateRecordPlan["action"], number>>;
  notified: number;
  candidates: OhioRollCallImportCandidateRow[];
  error: string | null;
};

export type OhioRollCallVoter = {
  candidateId: string;
  candidateName: string;
  lpid: string;
  memberName: string | null;
  side: RollCallVoteSide;
};

/**
 * The matched members, one per candidate. Ohio's feed has no Present / Not
 * Voting rows — absent members are simply absent from both lists — so
 * every resolution carries a side. A candidate reached from two lpids of
 * one roll call is a data defect (a mis-mapped crosswalk), so it fails the
 * roll call rather than writing twice.
 */
export function collectOhioVoters(
  resolutions: readonly OhioMemberResolution[],
  resolutionCounts: Partial<Record<OhioMemberResolutionOutcome, number>>
): OhioRollCallVoter[] {
  const voters: OhioRollCallVoter[] = [];
  const seen = new Set<string>();
  for (const resolution of resolutions) {
    resolutionCounts[resolution.outcome] = (resolutionCounts[resolution.outcome] ?? 0) + 1;
    if (resolution.outcome !== "matched" || !resolution.candidate) {
      continue;
    }
    const { candidateId, name: candidateName } = resolution.candidate;
    if (seen.has(candidateId)) {
      throw new Error(`candidate ${candidateId} matches more than one member of this roll call`);
    }
    seen.add(candidateId);
    voters.push({
      candidateId,
      candidateName,
      lpid: resolution.lpid,
      memberName: resolution.legislator?.displayName ?? null,
      side: resolution.side,
    });
  }
  return voters;
}

function count<K extends string>(counts: Partial<Record<K, number>>, key: K): void {
  counts[key] = (counts[key] ?? 0) + 1;
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
  assertKnownCliFlags("rollcall:oh:import", argv, [
    { name: "--ga", value: "both" },
    { name: "--evidence-dir", value: "both" },
    { name: "--crosswalk-file", value: "both" },
    { name: "--roster-file", value: "both" },
    { name: "--scope-from", value: "both" },
    { name: "--dry-run", value: "none" },
    { name: "--skip-existing", value: "none" },
  ]);
  const generalAssembly = readPositiveIntegerFlag(argv, "--ga", 0);
  if (generalAssembly === 0) {
    throw new Error("--ga is required (e.g. --ga 136)");
  }
  const evidenceDirRaw = readValueFlag(argv, "--evidence-dir");
  if (evidenceDirRaw === null) {
    throw new Error("--evidence-dir is required");
  }
  const evidenceDir = resolve(evidenceDirRaw);
  const crosswalkFileRaw = readValueFlag(argv, "--crosswalk-file");
  if (crosswalkFileRaw === null) {
    throw new Error("--crosswalk-file is required (the committed lpid → candidate review artifact)");
  }
  const rosterFileRaw = readValueFlag(argv, "--roster-file");
  if (rosterFileRaw === null) {
    throw new Error("--roster-file is required (the committed roster snapshot; rollcall:oh:resolve writes it)");
  }
  const scopeFrom = readValueFlag(argv, "--scope-from") ?? DEFAULT_SCOPE_FROM;
  const dryRun = argv.includes("--dry-run");
  const skipExisting = argv.includes("--skip-existing");

  const files = listOhioRollCallEvidenceFiles(evidenceDir).filter((file) => file.generalAssembly === generalAssembly);
  if (files.length === 0) {
    throw new Error(`${evidenceDir} holds no oh-<chamber>-${generalAssembly}-roll<N>.json files`);
  }

  const crosswalk = parseOhioCrosswalkFile(JSON.parse(readFileSync(resolve(crosswalkFileRaw), "utf8")) as unknown);
  if (crosswalk.generalAssembly !== generalAssembly) {
    throw new Error(`crosswalk is for GA ${crosswalk.generalAssembly}, run is --ga ${generalAssembly}`);
  }
  const roster = parseOhioLegislators(JSON.parse(readFileSync(resolve(rosterFileRaw), "utf8")) as unknown);
  const rosterByLpid = new Map(roster.map((legislator) => [legislator.lpid, legislator]));

  loadProjectEnv();
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required");
  }
  requireLocalDatabaseTarget(databaseUrl);
  const validationTimeoutMs = Number(process.env.AI_TIMEOUT_MS?.trim() || 90_000);

  const startedAt = new Date();
  const today = usLatestLocalDateIso(startedAt);
  const session = String(generalAssembly);
  const pool = new Pool({ connectionString: databaseUrl });
  const rows: OhioRollCallImportReportRow[] = [];
  try {
    const candidatesById = await loadOhioCrosswalkCandidates(pool, crosswalk, scopeFrom);
    const researchAreas = await loadAllResearchAreas(pool);
    const researchAreaSlugs = new Set(researchAreas.map((area) => area.slug));
    const researchAreaIdBySlug = new Map(researchAreas.map((area) => [area.slug, area.id]));

    for (const evidence of files) {
      const row: OhioRollCallImportReportRow = {
        ...evidence,
        outcome: "error",
        legislativeVoteId: null,
        reviewStatus: null,
        bill: null,
        voteDate: null,
        measureId: null,
        question: null,
        originRunId: null,
        members: 0,
        resolution: {},
        actions: {},
        notified: 0,
        candidates: [],
        error: null,
      };
      rows.push(row);
      try {
        const vote = await loadLegislativeVote(pool, {
          jurisdiction: OHIO_JURISDICTION,
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

        const parsed = parseOhioVoteEvidence(
          JSON.parse(readFileSync(resolve(evidenceDir, evidence.file), "utf8")) as unknown,
          { chamber: evidence.chamber, generalAssembly, rollNumber: evidence.roll }
        );
        row.bill = parsed.bill;
        const sha256 = ohioActionSha256(parsed.action);
        if (sha256 !== vote.sourceSha256) {
          row.outcome = "source_mismatch";
          row.error = `action sha256 ${sha256} is not the approved ${vote.sourceSha256}; re-fetch and re-review`;
          continue;
        }
        const action = parsed.action as OhioAction;
        // The sha pins the bytes; these pin the derivations the reviewer saw.
        if (ohioActionChamber(action) !== evidence.chamber || ohioRollNumber(action) !== evidence.roll) {
          throw new Error("evidence action derives a different chamber or roll than its file name");
        }
        if (ohioActionVoteDate(action) !== vote.voteDate) {
          throw new Error(`evidence action is dated ${ohioActionVoteDate(action)}, the approved row ${vote.voteDate}`);
        }
        if (parsed.machineUrl !== vote.machineUrl) {
          throw new Error(`evidence machineUrl ${parsed.machineUrl} is not the approved ${vote.machineUrl}`);
        }
        const votes = ohioActionMemberVotes(action);
        row.members = votes.yeas.length + votes.nays.length;
        if (votes.yeas.length !== vote.yeas || votes.nays.length !== vote.nays) {
          throw new Error(
            `evidence lists ${votes.yeas.length}-${votes.nays.length} but the approved row says ${vote.yeas}-${vote.nays}`
          );
        }

        // Everything that could fail the roll call is settled before the
        // first write, exactly like the federal importer.
        const templates = await validateSideTemplates(vote, validationTimeoutMs);
        const labels: RollCallLabel[] = parseRollCallLabels(vote.labelsJson, researchAreaSlugs);
        const sideLabels = { yea: labelsForSide(labels, "yea"), nay: labelsForSide(labels, "nay") };
        const rollCallKey = rollCallUrlKey(vote.machineUrl)?.key;
        if (!rollCallKey) {
          throw new Error(`machine_url ${vote.machineUrl} is not a recognized roll-call URL`);
        }
        const originRunPrefix = `rollcall:${OHIO_JURISDICTION}:${evidence.chamber}:${session}:${evidence.roll}:`;
        const originRunId = `${originRunPrefix}${startedAt.toISOString()}`;
        row.originRunId = originRunId;

        const resolutions = resolveOhioMembers(votes, crosswalk, rosterByLpid, candidatesById);
        const voters = collectOhioVoters(resolutions, row.resolution);
        // The effective and raw dates catch hand-written rows; the run-id
        // prefix catches this pipeline's own rows on any date, so a changed
        // or cleared override still rewrites them instead of duplicating.
        const existingByCandidate = await loadExistingRecordsForDate(
          pool,
          voters.map((voter) => voter.candidateId),
          [...new Set([vote.officialVoteDate ?? vote.voteDate, vote.voteDate])],
          originRunPrefix
        );
        const work = voters.map((voter) => {
          const template = templates[voter.side];
          const identityKey = buildCandidateRecordIdentityKey(template);
          const decision = planCandidateRecord({
            existing: existingByCandidate.get(voter.candidateId) ?? [],
            identityKey,
            rollCallKey,
            // Ohio measures do not parse as federal ones; same-day rows that
            // name the bill without vote words are not detected, and the
            // vote-claim scan below still lists the rest. Report-only either
            // way.
            measure: null,
            skipExisting,
          });
          const reportRow: OhioRollCallImportCandidateRow = {
            candidateId: voter.candidateId,
            candidateName: voter.candidateName,
            lpid: voter.lpid,
            memberName: voter.memberName,
            side: voter.side,
            action: decision.plan.action,
            recordId: "recordId" in decision.plan ? decision.plan.recordId : null,
            ambiguousRecordIds: decision.plan.action === "ambiguous" ? decision.plan.recordIds : [],
            notified: false,
            relatedRecordIds: decision.relatedRecordIds,
          };
          count(row.actions, decision.plan.action);
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
            if (item.plan.action === "insert") {
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

  const outcomes: Partial<Record<OhioRollCallImportOutcome, number>> = {};
  const actions: Partial<Record<CandidateRecordPlan["action"], number>> = {};
  for (const row of rows) {
    count(outcomes, row.outcome);
    for (const [action, n] of Object.entries(row.actions)) {
      actions[action as CandidateRecordPlan["action"]] = (actions[action as CandidateRecordPlan["action"]] ?? 0) + n;
    }
  }
  const report = {
    importerVersion: OHIO_ROLLCALL_IMPORT_IMPORTER_VERSION,
    dryRun,
    skipExisting,
    startedAt: startedAt.toISOString(),
    finishedAt: new Date().toISOString(),
    generalAssembly,
    evidenceDir: reportPath(evidenceDir),
    scopeFrom,
    crosswalkFile: reportPath(crosswalkFileRaw),
    crosswalkEntries: crosswalk.byLpid.size,
    rosterFile: resolve(rosterFileRaw),
    rosterMembers: roster.length,
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
    console.error("rollcall:oh:import failed:", errorMessage(error));
    process.exitCode = 1;
  });
}
