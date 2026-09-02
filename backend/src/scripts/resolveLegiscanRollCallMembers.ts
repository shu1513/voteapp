import { existsSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import {
  loadLegiscanCrosswalkCandidates,
  loadLegiscanStateLegCandidates,
  parseLegiscanCrosswalkFile,
  parseLegiscanPeopleSnapshot,
  proposeLegiscanCrosswalk,
  resolveLegiscanMembers,
  type LegiscanCrosswalk,
  type LegiscanMemberResolutionOutcome,
  type LegiscanPeopleSnapshot,
} from "../pipeline/rollcall/legiscanMemberResolver.js";
import {
  legiscanMemberVotes,
  parseLegiscanRollCall,
  parseLegiscanVoteEvidence,
  LEGISCAN_EVIDENCE_FILE_PATTERN,
} from "../pipeline/rollcall/legiscanRollCall.js";
import { getLegiscanStateConfig } from "../pipeline/rollcall/legiscanStateConfigs.js";
import type { LegislativeVoteChamber } from "../pipeline/rollcall/legislativeVotes.js";
import { reportPath } from "../pipeline/rollcall/rollCallReportPaths.js";
import { readLegiscanDataset } from "./fetchLegiscanRollCallVotes.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";
import { DEFAULT_SCOPE_FROM } from "./resolveRollCallMembers.js";

// Identity report for the LegiScan states (plan §2, phase-4 variant).
// Members are people_ids with no id shared with our candidates, so the
// identity layer is a committed crosswalk file that a human writes; this
// script does the reading work on both sides of that review:
//   - it always proposes people_id → candidate pairs by strict name
//     matching with seat corroboration against the state's Nov-2026
//     state-legislative pool (suggestions only);
//   - when --crosswalk-file is given, it validates the file (every
//     candidate id must exist) and resolves every evidence file's yea/nay
//     people_ids through it, so the pre-import counts are on record.
// The people snapshot comes from --people-file (committed) or is written
// into the evidence dir from --dataset-dir, so later runs need only
// committed files. Read-only on candidates: no writes, no AI.
//
//   npm run rollcall:legiscan:resolve -- --state TX --dataset-dir scratch/legiscan/TX \
//     --evidence-dir evidence/rollcall/legiscan-tx-2172/batch-01 \
//     --crosswalk-file evidence/rollcall/legiscan-tx-2172/crosswalk.json

export const LEGISCAN_ROLLCALL_RESOLVE_IMPORTER_VERSION = "rollcall-legiscan-resolve-v1";

export type LegiscanRollCallEvidenceFile = {
  file: string;
  state: string;
  chamber: LegislativeVoteChamber;
  sessionId: number;
  roll: number;
};

/** The LegiScan evidence files of a dir for one state + session, in chamber/roll (= time) order. */
export function listLegiscanRollCallEvidenceFiles(
  evidenceDir: string,
  expected: { jurisdiction: string; sessionId: number }
): LegiscanRollCallEvidenceFile[] {
  const files: LegiscanRollCallEvidenceFile[] = [];
  for (const file of readdirSync(evidenceDir)) {
    const match = LEGISCAN_EVIDENCE_FILE_PATTERN.exec(file);
    if (!match) {
      continue;
    }
    const entry: LegiscanRollCallEvidenceFile = {
      file,
      state: match[1]!.toUpperCase(),
      chamber: match[2] as LegislativeVoteChamber,
      sessionId: Number(match[3]),
      roll: Number(match[4]),
    };
    // A mixed directory must not run another state's or session's votes
    // through this state's people and crosswalk.
    if (entry.state !== expected.jurisdiction || entry.sessionId !== expected.sessionId) {
      continue;
    }
    files.push(entry);
  }
  return files.sort((a, b) => a.chamber.localeCompare(b.chamber) || a.roll - b.roll);
}

export function peopleSnapshotFileName(jurisdiction: string, sessionId: number): string {
  return `legiscan-people-${jurisdiction.toLowerCase()}-${sessionId}.json`;
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
  assertKnownCliFlags("rollcall:legiscan:resolve", argv, [
    { name: "--state", value: "both" },
    { name: "--dataset-dir", value: "both" },
    { name: "--people-file", value: "both" },
    { name: "--evidence-dir", value: "both" },
    { name: "--crosswalk-file", value: "both" },
    { name: "--scope-from", value: "both" },
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
  const datasetDirRaw = readValueFlag(argv, "--dataset-dir");
  const peopleFileRaw = readValueFlag(argv, "--people-file");
  if ((datasetDirRaw === null) === (peopleFileRaw === null)) {
    throw new Error("pass exactly one of --dataset-dir (fresh dataset) or --people-file (committed snapshot)");
  }
  const crosswalkFile = readValueFlag(argv, "--crosswalk-file");
  const scopeFrom = readValueFlag(argv, "--scope-from") ?? DEFAULT_SCOPE_FROM;

  loadProjectEnv();
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required to read the candidate pool");
  }

  const startedAt = new Date();

  // People: from the committed snapshot when given, else from the dataset
  // and written into the evidence dir so the run's snapshot is committable.
  let peopleSource: string;
  let snapshot: LegiscanPeopleSnapshot;
  if (peopleFileRaw !== null) {
    peopleSource = resolve(peopleFileRaw);
    snapshot = parseLegiscanPeopleSnapshot(JSON.parse(readFileSync(peopleSource, "utf8")) as unknown, {
      jurisdiction: config.jurisdiction,
      sessionId: config.sessionId,
    });
  } else {
    peopleSource = resolve(datasetDirRaw!);
    const dataset = readLegiscanDataset(peopleSource);
    if (dataset.fileErrors.length > 0) {
      throw new Error(
        `${peopleSource} has unreadable files: ${dataset.fileErrors.map((entry) => `${entry.file} (${entry.error})`).join("; ")}`
      );
    }
    // Person elements carry no state or session, so the dataset's bills are
    // the only proof this is the configured state's session.
    for (const bill of dataset.billsById.values()) {
      if (bill.state !== config.jurisdiction || bill.sessionId !== config.sessionId) {
        throw new Error(
          `${peopleSource} holds ${bill.state} session ${bill.sessionId} (bill ${bill.billNumber}); the run is ${config.jurisdiction} session ${config.sessionId}`
        );
      }
    }
    const raw = { jurisdiction: config.jurisdiction, sessionId: config.sessionId, people: dataset.people };
    snapshot = parseLegiscanPeopleSnapshot(raw, { jurisdiction: config.jurisdiction, sessionId: config.sessionId });
    const snapshotPath = resolve(evidenceDir, peopleSnapshotFileName(config.jurisdiction, config.sessionId));
    if (!existsSync(snapshotPath)) {
      writeFileSync(snapshotPath, `${JSON.stringify(raw, null, 2)}\n`);
    }
  }
  const people = [...snapshot.byPeopleId.values()];

  const pool = new Pool({ connectionString: databaseUrl });
  let candidatesPool;
  let crosswalk: LegiscanCrosswalk | null = null;
  let crosswalkCandidates = new Map<string, { candidateId: string; name: string; inScope: boolean }>();
  try {
    candidatesPool = await loadLegiscanStateLegCandidates(pool, config.jurisdiction, scopeFrom);
    if (crosswalkFile !== null) {
      crosswalk = parseLegiscanCrosswalkFile(
        JSON.parse(readFileSync(resolve(crosswalkFile), "utf8")) as unknown,
        config.jurisdiction
      );
      crosswalkCandidates = await loadLegiscanCrosswalkCandidates(pool, crosswalk, scopeFrom);
    }
  } finally {
    await pool.end();
  }

  const proposals = proposeLegiscanCrosswalk(people, candidatesPool);

  // people_ids in the crosswalk that the snapshot does not know: not fatal
  // (a member who resigned still appears on old roll calls), but each one
  // is listed for the reviewer.
  const crosswalkPeopleNotInSnapshot =
    crosswalk === null ? [] : [...crosswalk.byPeopleId.keys()].filter((peopleId) => !snapshot.byPeopleId.has(peopleId));

  const rolls: {
    file: string;
    chamber: LegislativeVoteChamber;
    roll: number;
    bill: string | null;
    members: number;
    notVoting: number;
    absent: number;
    counts: Partial<Record<LegiscanMemberResolutionOutcome, number>>;
    matched: { peopleId: number; side: "yea" | "nay"; candidateId: string; candidateName: string }[];
    noCrosswalk: number[];
    error: string | null;
  }[] = [];
  const files = listLegiscanRollCallEvidenceFiles(evidenceDir, {
    jurisdiction: config.jurisdiction,
    sessionId: config.sessionId,
  });
  if (crosswalk !== null) {
    for (const evidence of files) {
      const row: (typeof rolls)[number] = {
        file: evidence.file,
        chamber: evidence.chamber,
        roll: evidence.roll,
        bill: null,
        members: 0,
        notVoting: 0,
        absent: 0,
        counts: {},
        matched: [],
        noCrosswalk: [],
        error: null,
      };
      rolls.push(row);
      try {
        const parsed = parseLegiscanVoteEvidence(JSON.parse(readFileSync(resolve(evidenceDir, evidence.file), "utf8")) as unknown, {
          jurisdiction: config.jurisdiction,
          chamber: evidence.chamber,
          sessionId: evidence.sessionId,
          rollNumber: evidence.roll,
        });
        row.bill = parsed.bill;
        const votes = legiscanMemberVotes(parseLegiscanRollCall(parsed.rollCall as Record<string, unknown>));
        row.members = votes.yeas.length + votes.nays.length;
        row.notVoting = votes.notVoting;
        row.absent = votes.absent;
        const resolutions = resolveLegiscanMembers(votes, crosswalk, snapshot.byPeopleId, crosswalkCandidates);
        for (const resolution of resolutions) {
          row.counts[resolution.outcome] = (row.counts[resolution.outcome] ?? 0) + 1;
          if (resolution.outcome === "matched" && resolution.candidate) {
            row.matched.push({
              peopleId: resolution.peopleId,
              side: resolution.side,
              candidateId: resolution.candidate.candidateId,
              candidateName: resolution.candidate.name,
            });
          } else if (resolution.outcome === "no_crosswalk") {
            row.noCrosswalk.push(resolution.peopleId);
          }
        }
      } catch (error) {
        row.error = errorMessage(error);
      }
    }
  }

  const counts: Partial<Record<LegiscanMemberResolutionOutcome, number>> = {};
  for (const roll of rolls) {
    for (const [outcome, n] of Object.entries(roll.counts)) {
      counts[outcome as LegiscanMemberResolutionOutcome] = (counts[outcome as LegiscanMemberResolutionOutcome] ?? 0) + n;
    }
  }
  const report = {
    importerVersion: LEGISCAN_ROLLCALL_RESOLVE_IMPORTER_VERSION,
    startedAt: startedAt.toISOString(),
    finishedAt: new Date().toISOString(),
    jurisdiction: config.jurisdiction,
    sessionId: config.sessionId,
    evidenceDir: reportPath(evidenceDir),
    scopeFrom,
    peopleSource: reportPath(peopleSource),
    peopleMembers: people.length,
    candidatePool: candidatesPool.length,
    crosswalkFile: crosswalkFile === null ? null : reportPath(crosswalkFile),
    crosswalkEntries: crosswalk === null ? null : crosswalk.byPeopleId.size,
    crosswalkPeopleNotInSnapshot,
    proposals: proposals.proposals,
    unmatchedPeople: proposals.unmatchedPeople.map((person) => ({
      peopleId: person.peopleId,
      name: person.name,
      seat: `${person.chamber ?? "?"} ${person.district ?? "?"}`,
    })),
    unmatchedCandidates: proposals.unmatchedCandidates,
    files: files.length,
    fileErrors: rolls.filter((roll) => roll.error !== null).length,
    counts,
    rolls,
  };
  writeFileSync(resolve(evidenceDir, "resolve-report.json"), `${JSON.stringify(report, null, 2)}\n`);
  console.log(
    JSON.stringify(
      {
        ...report,
        proposals: report.proposals.length,
        unmatchedPeople: report.unmatchedPeople.length,
        unmatchedCandidates: report.unmatchedCandidates.length,
        rolls: rolls.map((roll) => ({ ...roll, matched: roll.matched.length })),
      },
      null,
      2
    )
  );
  if (report.fileErrors > 0) {
    process.exitCode = 1;
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("rollcall:legiscan:resolve failed:", errorMessage(error));
    process.exitCode = 1;
  });
}
