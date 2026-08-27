import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { Pool, type PoolClient } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { loadAllResearchAreas } from "../pipeline/candidates/candidateRecordAreaTagging.js";
import { buildCandidateRecordIdentityKey } from "../pipeline/candidates/candidateRecordStore.js";
import {
  loadLegiscanCrosswalkCandidates,
  parseLegiscanCrosswalkFile,
  parseLegiscanPeopleSnapshot,
  resolveLegiscanMembers,
  type LegiscanMemberResolution,
  type LegiscanMemberResolutionOutcome,
} from "../pipeline/rollcall/legiscanMemberResolver.js";
import {
  legiscanMemberVotes,
  legiscanRollCallSha256,
  parseLegiscanRollCall,
  parseLegiscanVoteEvidence,
} from "../pipeline/rollcall/legiscanRollCall.js";
import { getLegiscanStateConfig } from "../pipeline/rollcall/legiscanStateConfigs.js";
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
import { usLatestLocalDateIso } from "../utils/usLocalDate.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";
import { importReportFileName, validateSideTemplates } from "./importRollCallVotes.js";
import { DEFAULT_SCOPE_FROM } from "./resolveRollCallMembers.js";
import { listLegiscanRollCallEvidenceFiles, type LegiscanRollCallEvidenceFile } from "./resolveLegiscanRollCallMembers.js";

// Fan-out writer for the LegiScan states (plan §5 phase 4): for every
// LegiScan vote evidence file whose legislative_votes row a human APPROVED,
// resolve the yea/nay people_ids through the committed crosswalk and write
// one candidate_records row per matched member, with the roll call's
// sentences and labels. Same guarantees as the federal and Ohio importers:
// one transaction per roll call, sha-pinned evidence, approval re-checked
// under lock inside the write, re-runs write nothing new. Runs off
// committed files only (evidence dir + crosswalk + people snapshot).
// Local DB only, no AI.
//
//   npm run rollcall:legiscan:import -- --state TX \
//     --evidence-dir evidence/rollcall/legiscan-tx-2172/batch-01 \
//     --crosswalk-file evidence/rollcall/legiscan-tx-2172/crosswalk.json \
//     --people-file evidence/rollcall/legiscan-tx-2172/legiscan-people-tx-2172.json --dry-run

export const LEGISCAN_ROLLCALL_IMPORT_IMPORTER_VERSION = "rollcall-legiscan-import-v1";

export type LegiscanRollCallImportOutcome =
  | "imported"
  | "dry_run"
  | "not_approved"
  | "missing_row"
  | "source_mismatch"
  | "error";

const FAILURE_OUTCOMES: ReadonlySet<LegiscanRollCallImportOutcome> = new Set(["source_mismatch", "error"]);

export type LegiscanRollCallImportCandidateRow = {
  candidateId: string;
  candidateName: string;
  peopleId: number;
  memberName: string | null;
  side: RollCallVoteSide;
  action: CandidateRecordPlan["action"];
  recordId: string | null;
  ambiguousRecordIds: string[];
  notified: boolean;
  relatedRecordIds: string[];
};

export type LegiscanRollCallImportReportRow = LegiscanRollCallEvidenceFile & {
  outcome: LegiscanRollCallImportOutcome;
  legislativeVoteId: string | null;
  reviewStatus: LegislativeVoteReviewStatus | null;
  bill: string | null;
  voteDate: string | null;
  officialVoteDate: string | null;
  measureId: string | null;
  question: string | null;
  originRunId: string | null;
  members: number;
  notVoting: number;
  absent: number;
  resolution: Partial<Record<LegiscanMemberResolutionOutcome, number>>;
  actions: Partial<Record<CandidateRecordPlan["action"], number>>;
  notified: number;
  candidates: LegiscanRollCallImportCandidateRow[];
  error: string | null;
};

export type LegiscanRollCallVoter = {
  candidateId: string;
  candidateName: string;
  peopleId: number;
  memberName: string | null;
  side: RollCallVoteSide;
};

/**
 * The matched members, one per candidate. NV / absent members were never
 * resolved (legiscanMemberVotes lists only yea and nay people_ids), so
 * every resolution carries a side. A candidate reached from two people_ids
 * of one roll call is a data defect (a mis-mapped crosswalk), so it fails
 * the roll call rather than writing twice.
 */
export function collectLegiscanVoters(
  resolutions: readonly LegiscanMemberResolution[],
  resolutionCounts: Partial<Record<LegiscanMemberResolutionOutcome, number>>
): LegiscanRollCallVoter[] {
  const voters: LegiscanRollCallVoter[] = [];
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
      peopleId: resolution.peopleId,
      memberName: resolution.person?.name ?? null,
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
  assertKnownCliFlags("rollcall:legiscan:import", argv, [
    { name: "--state", value: "both" },
    { name: "--evidence-dir", value: "both" },
    { name: "--crosswalk-file", value: "both" },
    { name: "--people-file", value: "both" },
    { name: "--scope-from", value: "both" },
    { name: "--dry-run", value: "none" },
    { name: "--skip-existing", value: "none" },
  ]);
  const stateRaw = readValueFlag(argv, "--state");
  if (stateRaw === null) {
    throw new Error("--state is required (e.g. --state TX)");
  }
  const config = getLegiscanStateConfig(stateRaw);
  const evidenceDirRaw = readValueFlag(argv, "--evidence-dir");
  if (evidenceDirRaw === null) {
    throw new Error("--evidence-dir is required");
  }
  const evidenceDir = resolve(evidenceDirRaw);
  const crosswalkFileRaw = readValueFlag(argv, "--crosswalk-file");
  if (crosswalkFileRaw === null) {
    throw new Error("--crosswalk-file is required (the committed people_id → candidate review artifact)");
  }
  const peopleFileRaw = readValueFlag(argv, "--people-file");
  if (peopleFileRaw === null) {
    throw new Error("--people-file is required (the committed people snapshot; rollcall:legiscan:resolve writes it)");
  }
  const scopeFrom = readValueFlag(argv, "--scope-from") ?? DEFAULT_SCOPE_FROM;
  const dryRun = argv.includes("--dry-run");
  const skipExisting = argv.includes("--skip-existing");

  const files = listLegiscanRollCallEvidenceFiles(evidenceDir, {
    jurisdiction: config.jurisdiction,
    sessionId: config.sessionId,
  });
  if (files.length === 0) {
    throw new Error(
      `${evidenceDir} holds no ls-${config.jurisdiction.toLowerCase()}-<chamber>-${config.sessionId}-roll<N>.json files`
    );
  }

  const crosswalk = parseLegiscanCrosswalkFile(
    JSON.parse(readFileSync(resolve(crosswalkFileRaw), "utf8")) as unknown,
    config.jurisdiction
  );
  const snapshot = parseLegiscanPeopleSnapshot(JSON.parse(readFileSync(resolve(peopleFileRaw), "utf8")) as unknown, {
    jurisdiction: config.jurisdiction,
    sessionId: config.sessionId,
  });

  loadProjectEnv();
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required");
  }
  requireLocalDatabaseTarget(databaseUrl);
  const validationTimeoutMs = Number(process.env.AI_TIMEOUT_MS?.trim() || 90_000);

  const startedAt = new Date();
  const today = usLatestLocalDateIso(startedAt);
  const session = String(config.sessionId);
  const pool = new Pool({ connectionString: databaseUrl });
  const rows: LegiscanRollCallImportReportRow[] = [];
  try {
    const candidatesById = await loadLegiscanCrosswalkCandidates(pool, crosswalk, scopeFrom);
    const researchAreas = await loadAllResearchAreas(pool);
    const researchAreaSlugs = new Set(researchAreas.map((area) => area.slug));
    const researchAreaIdBySlug = new Map(researchAreas.map((area) => [area.slug, area.id]));

    for (const evidence of files) {
      const row: LegiscanRollCallImportReportRow = {
        ...evidence,
        outcome: "error",
        legislativeVoteId: null,
        reviewStatus: null,
        bill: null,
        voteDate: null,
        officialVoteDate: null,
        measureId: null,
        question: null,
        originRunId: null,
        members: 0,
        notVoting: 0,
        absent: 0,
        resolution: {},
        actions: {},
        notified: 0,
        candidates: [],
        error: null,
      };
      rows.push(row);
      try {
        const vote = await loadLegislativeVote(pool, {
          jurisdiction: config.jurisdiction,
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
        row.officialVoteDate = vote.officialVoteDate;
        row.measureId = vote.measureId;
        row.question = vote.exactQuestion;
        if (vote.reviewStatus !== "approved") {
          row.outcome = "not_approved";
          continue;
        }

        const parsed = parseLegiscanVoteEvidence(
          JSON.parse(readFileSync(resolve(evidenceDir, evidence.file), "utf8")) as unknown,
          { jurisdiction: config.jurisdiction, chamber: evidence.chamber, sessionId: evidence.sessionId, rollNumber: evidence.roll }
        );
        row.bill = parsed.bill;
        const sha256 = legiscanRollCallSha256(parsed.rollCall);
        if (sha256 !== vote.sourceSha256) {
          row.outcome = "source_mismatch";
          row.error = `roll_call sha256 ${sha256} is not the approved ${vote.sourceSha256}; re-fetch and re-review`;
          continue;
        }
        const rollCall = parseLegiscanRollCall(parsed.rollCall as Record<string, unknown>);
        // The sha pins the bytes; these pin the derivations the reviewer saw.
        if (rollCall.chamber !== evidence.chamber || rollCall.rollCallId !== evidence.roll) {
          throw new Error("evidence roll_call derives a different chamber or roll than its file name");
        }
        if (rollCall.date !== vote.voteDate) {
          throw new Error(`evidence roll_call is dated ${rollCall.date}, the approved row ${vote.voteDate}`);
        }
        if (parsed.machineUrl !== vote.machineUrl) {
          throw new Error(`evidence machineUrl ${parsed.machineUrl} is not the approved ${vote.machineUrl}`);
        }
        const votes = legiscanMemberVotes(rollCall);
        row.members = votes.yeas.length + votes.nays.length;
        row.notVoting = votes.notVoting;
        row.absent = votes.absent;
        if (votes.yeas.length !== vote.yeas || votes.nays.length !== vote.nays) {
          throw new Error(
            `evidence lists ${votes.yeas.length}-${votes.nays.length} but the approved row says ${vote.yeas}-${vote.nays}`
          );
        }

        // Everything that could fail the roll call is settled before the
        // first write, exactly like the federal and Ohio importers.
        const templates = await validateSideTemplates(vote, validationTimeoutMs);
        const labels: RollCallLabel[] = parseRollCallLabels(vote.labelsJson, researchAreaSlugs);
        const sideLabels = { yea: labelsForSide(labels, "yea"), nay: labelsForSide(labels, "nay") };
        const rollCallKey = rollCallUrlKey(vote.machineUrl)?.key;
        if (!rollCallKey) {
          throw new Error(`machine_url ${vote.machineUrl} is not a recognized roll-call URL`);
        }
        const originRunId = `rollcall:${config.jurisdiction}:${evidence.chamber}:${session}:${evidence.roll}:${startedAt.toISOString()}`;
        row.originRunId = originRunId;

        const resolutions = resolveLegiscanMembers(votes, crosswalk, snapshot.byPeopleId, candidatesById);
        const voters = collectLegiscanVoters(resolutions, row.resolution);
        const existingByCandidate = await loadExistingRecordsForDate(
          pool,
          voters.map((voter) => voter.candidateId),
          vote.officialVoteDate ?? vote.voteDate
        );
        const work = voters.map((voter) => {
          const template = templates[voter.side];
          const identityKey = buildCandidateRecordIdentityKey(template);
          const decision = planCandidateRecord({
            existing: existingByCandidate.get(voter.candidateId) ?? [],
            identityKey,
            rollCallKey,
            // State measures do not parse as federal ones; same-day rows
            // that name the bill without vote words are not detected, and
            // the vote-claim scan below still lists the rest. Report-only
            // either way.
            measure: null,
            skipExisting,
          });
          const reportRow: LegiscanRollCallImportCandidateRow = {
            candidateId: voter.candidateId,
            candidateName: voter.candidateName,
            peopleId: voter.peopleId,
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

  const outcomes: Partial<Record<LegiscanRollCallImportOutcome, number>> = {};
  const actions: Partial<Record<CandidateRecordPlan["action"], number>> = {};
  for (const row of rows) {
    count(outcomes, row.outcome);
    for (const [action, n] of Object.entries(row.actions)) {
      actions[action as CandidateRecordPlan["action"]] = (actions[action as CandidateRecordPlan["action"]] ?? 0) + n;
    }
  }
  const report = {
    importerVersion: LEGISCAN_ROLLCALL_IMPORT_IMPORTER_VERSION,
    dryRun,
    skipExisting,
    startedAt: startedAt.toISOString(),
    finishedAt: new Date().toISOString(),
    jurisdiction: config.jurisdiction,
    sessionId: config.sessionId,
    evidenceDir,
    scopeFrom,
    crosswalkFile: resolve(crosswalkFileRaw),
    crosswalkEntries: crosswalk.byPeopleId.size,
    peopleFile: resolve(peopleFileRaw),
    peopleMembers: snapshot.byPeopleId.size,
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
    console.error("rollcall:legiscan:import failed:", errorMessage(error));
    process.exitCode = 1;
  });
}
