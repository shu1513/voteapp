import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { Pool, type PoolClient } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { loadAllResearchAreas } from "../pipeline/candidates/candidateRecordAreaTagging.js";
import { buildCandidateRecordIdentityKey } from "../pipeline/candidates/candidateRecordStore.js";
import {
  classifyHawaiiAction,
  hawaiiExpectedSeats,
  hawaiiHistorySha256,
  hawaiiRollNumber,
  hawaiiSittingMembers,
  parseHawaiiFloorVoteText,
  parseHawaiiHistoryRow,
  parseHawaiiSeatFile,
  parseHawaiiVoteEvidence,
  reconstructHawaiiMembers,
  HAWAII_JURISDICTION,
  type HawaiiMemberLists,
  type HawaiiSeatFile,
} from "../pipeline/rollcall/hawaiiRollCall.js";
import {
  loadLegiscanCrosswalkCandidates,
  parseLegiscanCrosswalkFile,
  parseLegiscanPeopleSnapshot,
  resolveLegiscanMembers,
} from "../pipeline/rollcall/legiscanMemberResolver.js";
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
  refreshRollCallRecord,
  rewriteRollCallRecord,
  shouldNotifyForVoteDate,
  syncRollCallRecordTags,
  type CandidateRecordPlan,
  type RollCallLabel,
} from "../pipeline/rollcall/rollCallFanOut.js";
import { rollCallUrlKey } from "../pipeline/rollcall/rollCallRecordUrls.js";
import { createCandidateRecordUpdateNotificationEvents } from "../pipeline/users/candidateFollowNotificationEvents.js";
import { reportPath } from "../pipeline/rollcall/rollCallReportPaths.js";
import { usLatestLocalDateIso } from "../utils/usLocalDate.js";
import { readPositiveIntegerFlag } from "../utils/cliFlags.js";
import {
  collectLegiscanVoters,
  type LegiscanRollCallImportCandidateRow,
  type LegiscanRollCallImportOutcome,
} from "./importLegiscanRollCallVotes.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";
import { importReportFileName, validateSideTemplates } from "./importRollCallVotes.js";
import { DEFAULT_SCOPE_FROM } from "./resolveRollCallMembers.js";
import { listHawaiiRollCallEvidenceFiles, type HawaiiRollCallEvidenceFile } from "./resolveHawaiiRollCallMembers.js";

// Fan-out writer for Hawaii: for every Hawaii vote evidence file whose
// legislative_votes row a human APPROVED, re-derive the member lists from
// the pinned history line (they must equal what the fetcher wrote), resolve
// the yea/nay people_ids through the committed crosswalk, and write one
// candidate_records row per matched member. Same guarantees as the other
// importers: one transaction per roll call, sha-pinned evidence, approval
// re-checked under lock inside the write, re-runs write nothing new. Local
// DB only, no AI.
//
//   npm run rollcall:hi:import -- --session 2175 --evidence-dir evidence/rollcall/hawaii-2175/batch-01 \
//     --crosswalk-file evidence/rollcall/hawaii-2175/crosswalk.json \
//     --people-file evidence/rollcall/hawaii-2175/hawaii-people-2175.json --dry-run

export const HAWAII_ROLLCALL_IMPORT_IMPORTER_VERSION = "rollcall-hi-import-v1";

const FAILURE_OUTCOMES: ReadonlySet<LegiscanRollCallImportOutcome> = new Set(["source_mismatch", "error"]);

export type HawaiiRollCallImportReportRow = HawaiiRollCallEvidenceFile & {
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
  excused: number;
  resolution: Partial<Record<string, number>>;
  actions: Partial<Record<CandidateRecordPlan["action"], number>>;
  notified: number;
  candidates: LegiscanRollCallImportCandidateRow[];
  error: string | null;
};

function sameLists(a: HawaiiMemberLists, b: HawaiiMemberLists): boolean {
  const same = (x: readonly number[], y: readonly number[]) => x.length === y.length && x.every((value, index) => value === y[index]);
  return same(a.yeas, b.yeas) && same(a.nays, b.nays) && same(a.excused, b.excused) && same(a.reservations, b.reservations);
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
  assertKnownCliFlags("rollcall:hi:import", argv, [
    { name: "--session", value: "both" },
    { name: "--evidence-dir", value: "both" },
    { name: "--crosswalk-file", value: "both" },
    { name: "--people-file", value: "both" },
    { name: "--seat-file", value: "both" },
    { name: "--scope-from", value: "both" },
    { name: "--dry-run", value: "none" },
    { name: "--skip-existing", value: "none" },
  ]);
  const sessionId = readPositiveIntegerFlag(argv, "--session", 0);
  if (sessionId === 0) {
    throw new Error("--session is required (e.g. --session 2175)");
  }
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
    throw new Error("--people-file is required (the committed people snapshot; rollcall:hi:resolve writes it)");
  }
  const seatFileRaw = readValueFlag(argv, "--seat-file");
  const scopeFrom = readValueFlag(argv, "--scope-from") ?? DEFAULT_SCOPE_FROM;
  const dryRun = argv.includes("--dry-run");
  const skipExisting = argv.includes("--skip-existing");

  const files = listHawaiiRollCallEvidenceFiles(evidenceDir, sessionId);
  if (files.length === 0) {
    throw new Error(`${evidenceDir} holds no hi-<chamber>-${sessionId}-roll<N>.json files`);
  }

  const crosswalk = parseLegiscanCrosswalkFile(JSON.parse(readFileSync(resolve(crosswalkFileRaw), "utf8")) as unknown, HAWAII_JURISDICTION);
  const snapshot = parseLegiscanPeopleSnapshot(JSON.parse(readFileSync(resolve(peopleFileRaw), "utf8")) as unknown, {
    jurisdiction: HAWAII_JURISDICTION,
    sessionId,
  });
  const people = [...snapshot.byPeopleId.values()];
  let seats: HawaiiSeatFile | null = null;
  if (seatFileRaw !== null) {
    seats = parseHawaiiSeatFile(JSON.parse(readFileSync(resolve(seatFileRaw), "utf8")) as unknown, sessionId);
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
  const session = String(sessionId);
  const pool = new Pool({ connectionString: databaseUrl });
  const rows: HawaiiRollCallImportReportRow[] = [];
  try {
    const candidatesById = await loadLegiscanCrosswalkCandidates(pool, crosswalk, scopeFrom);
    const researchAreas = await loadAllResearchAreas(pool);
    const researchAreaSlugs = new Set(researchAreas.map((area) => area.slug));
    const researchAreaIdBySlug = new Map(researchAreas.map((area) => [area.slug, area.id]));

    for (const evidence of files) {
      const row: HawaiiRollCallImportReportRow = {
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
        excused: 0,
        resolution: {},
        actions: {},
        notified: 0,
        candidates: [],
        error: null,
      };
      rows.push(row);
      try {
        const vote = await loadLegislativeVote(pool, {
          jurisdiction: HAWAII_JURISDICTION,
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

        const parsed = parseHawaiiVoteEvidence(
          JSON.parse(readFileSync(resolve(evidenceDir, evidence.file), "utf8")) as unknown,
          { chamber: evidence.chamber, sessionId: evidence.sessionId, rollNumber: evidence.roll }
        );
        row.bill = parsed.bill;
        const sha256 = hawaiiHistorySha256(parsed.history);
        if (sha256 !== vote.sourceSha256) {
          row.outcome = "source_mismatch";
          row.error = `history sha256 ${sha256} is not the approved ${vote.sourceSha256}; re-fetch and re-review`;
          continue;
        }
        // The sha pins the bytes; these pin the derivations the reviewer saw,
        // including the reconstructed member lists.
        const historyRow = parseHawaiiHistoryRow(parsed.history, parsed.historyIndex);
        if (historyRow.chamber !== evidence.chamber || hawaiiRollNumber(parsed.billId, parsed.historyIndex) !== evidence.roll) {
          throw new Error("evidence history row derives a different chamber or roll than its file name");
        }
        if (historyRow.date !== vote.voteDate) {
          throw new Error(`evidence history row is dated ${historyRow.date}, the approved row ${vote.voteDate}`);
        }
        if (parsed.machineUrl !== vote.machineUrl) {
          throw new Error(`evidence machineUrl ${parsed.machineUrl} is not the approved ${vote.machineUrl}`);
        }
        if (parsed.measureId !== vote.measureId) {
          throw new Error(`evidence measure ${parsed.measureId} is not the approved ${vote.measureId ?? "none"}`);
        }
        if (!classifyHawaiiAction(historyRow.action, "B").isFloorVote) {
          throw new Error("evidence history row is not a floor passage line");
        }
        const text = parseHawaiiFloorVoteText(historyRow.action, historyRow.chamber);
        const members = reconstructHawaiiMembers(
          text,
          historyRow.chamber,
          hawaiiSittingMembers(people, historyRow.chamber, historyRow.date, seats),
          hawaiiExpectedSeats(historyRow.chamber, historyRow.date, seats)
        );
        if (!sameLists(members, parsed.members)) {
          throw new Error("the member lists reconstructed today differ from the evidence file's; re-fetch and re-review");
        }
        row.members = members.yeas.length + members.nays.length;
        row.excused = members.excused.length;
        if (members.yeas.length !== vote.yeas || members.nays.length !== vote.nays) {
          throw new Error(`evidence gives ${members.yeas.length}-${members.nays.length} but the approved row says ${vote.yeas}-${vote.nays}`);
        }

        const templates = await validateSideTemplates(vote, validationTimeoutMs);
        const labels: RollCallLabel[] = parseRollCallLabels(vote.labelsJson, researchAreaSlugs);
        const sideLabels = { yea: labelsForSide(labels, "yea"), nay: labelsForSide(labels, "nay") };
        const rollCallKey = rollCallUrlKey(vote.machineUrl)?.key;
        if (!rollCallKey) {
          throw new Error(`machine_url ${vote.machineUrl} is not a recognized roll-call URL`);
        }
        const originRunPrefix = `rollcall:${HAWAII_JURISDICTION}:${evidence.chamber}:${session}:${evidence.roll}:`;
        const originRunId = `${originRunPrefix}${startedAt.toISOString()}`;
        row.originRunId = originRunId;

        const resolutions = resolveLegiscanMembers({ yeas: members.yeas, nays: members.nays }, crosswalk, snapshot.byPeopleId, candidatesById);
        const voters = collectLegiscanVoters(resolutions, row.resolution);
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
            description: template.description,
            sourceUrl: template.sourceUrl,
            rollCallKey,
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

  const outcomes: Partial<Record<LegiscanRollCallImportOutcome, number>> = {};
  const actions: Partial<Record<CandidateRecordPlan["action"], number>> = {};
  for (const row of rows) {
    count(outcomes, row.outcome);
    for (const [action, n] of Object.entries(row.actions)) {
      actions[action as CandidateRecordPlan["action"]] = (actions[action as CandidateRecordPlan["action"]] ?? 0) + n;
    }
  }
  const report = {
    importerVersion: HAWAII_ROLLCALL_IMPORT_IMPORTER_VERSION,
    dryRun,
    skipExisting,
    startedAt: startedAt.toISOString(),
    finishedAt: new Date().toISOString(),
    jurisdiction: HAWAII_JURISDICTION,
    sessionId,
    evidenceDir: reportPath(evidenceDir),
    scopeFrom,
    crosswalkFile: reportPath(crosswalkFileRaw),
    crosswalkEntries: crosswalk.byPeopleId.size,
    peopleFile: reportPath(peopleFileRaw),
    peopleMembers: snapshot.byPeopleId.size,
    seatFile: seatFileRaw === null ? null : reportPath(seatFileRaw),
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
    console.error("rollcall:hi:import failed:", errorMessage(error));
    process.exitCode = 1;
  });
}
