import { existsSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import {
  parseHawaiiVoteEvidence,
  readHawaiiDataset,
  HAWAII_EVIDENCE_FILE_PATTERN,
  HAWAII_JURISDICTION,
} from "../pipeline/rollcall/hawaiiRollCall.js";
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
import type { LegislativeVoteChamber } from "../pipeline/rollcall/legislativeVotes.js";
import { reportPath } from "../pipeline/rollcall/rollCallReportPaths.js";
import { readPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";
import { DEFAULT_SCOPE_FROM } from "./resolveRollCallMembers.js";

// Identity report for Hawaii. Members are LegiScan people_ids (the dataset's
// people file), so the identity layer is the SAME committed crosswalk file
// the LegiScan states use, keyed by people_id and pinned to jurisdiction
// "HI"; this script proposes entries by name matching against the Nov-2026
// Hawaii state-legislative pool and, given a crosswalk, resolves every
// evidence file's reconstructed yea/nay lists through it. Read-only on
// candidates, no AI.
//
//   npm run rollcall:hi:resolve -- --session 2175 --dataset-dir <dir> --evidence-dir <dir>
//   npm run rollcall:hi:resolve -- --session 2175 --people-file evidence/rollcall/hawaii-2175/hawaii-people-2175.json \
//     --evidence-dir evidence/rollcall/hawaii-2175/batch-01 --crosswalk-file evidence/rollcall/hawaii-2175/crosswalk.json

export const HAWAII_ROLLCALL_RESOLVE_IMPORTER_VERSION = "rollcall-hi-resolve-v1";

export type HawaiiRollCallEvidenceFile = {
  file: string;
  chamber: LegislativeVoteChamber;
  sessionId: number;
  roll: number;
};

/** The Hawaii evidence files of a dir for one session, in chamber/roll order. */
export function listHawaiiRollCallEvidenceFiles(evidenceDir: string, sessionId: number): HawaiiRollCallEvidenceFile[] {
  const files: HawaiiRollCallEvidenceFile[] = [];
  for (const file of readdirSync(evidenceDir)) {
    const match = HAWAII_EVIDENCE_FILE_PATTERN.exec(file);
    if (!match) {
      continue;
    }
    const entry: HawaiiRollCallEvidenceFile = {
      file,
      chamber: match[1] as LegislativeVoteChamber,
      sessionId: Number(match[2]),
      roll: Number(match[3]),
    };
    // A mixed directory must not run another session's votes through this
    // session's people and crosswalk.
    if (entry.sessionId !== sessionId) {
      continue;
    }
    files.push(entry);
  }
  return files.sort((a, b) => a.chamber.localeCompare(b.chamber) || a.roll - b.roll);
}

export function hawaiiPeopleSnapshotFileName(sessionId: number): string {
  return `hawaii-people-${sessionId}.json`;
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
  assertKnownCliFlags("rollcall:hi:resolve", argv, [
    { name: "--session", value: "both" },
    { name: "--dataset-dir", value: "both" },
    { name: "--people-file", value: "both" },
    { name: "--evidence-dir", value: "both" },
    { name: "--crosswalk-file", value: "both" },
    { name: "--scope-from", value: "both" },
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
  const expected = { jurisdiction: HAWAII_JURISDICTION, sessionId };

  let peopleSource: string;
  let snapshot: LegiscanPeopleSnapshot;
  if (peopleFileRaw !== null) {
    peopleSource = resolve(peopleFileRaw);
    snapshot = parseLegiscanPeopleSnapshot(JSON.parse(readFileSync(peopleSource, "utf8")) as unknown, expected);
  } else {
    peopleSource = resolve(datasetDirRaw!);
    const dataset = readHawaiiDataset(peopleSource);
    if (dataset.fileErrors.length > 0) {
      throw new Error(
        `${peopleSource} has unreadable files: ${dataset.fileErrors.map((entry) => `${entry.file} (${entry.error})`).join("; ")}`
      );
    }
    for (const { summary } of dataset.billsById.values()) {
      if (summary.state !== HAWAII_JURISDICTION || summary.sessionId !== sessionId) {
        throw new Error(
          `${peopleSource} holds ${summary.state} session ${summary.sessionId} (bill ${summary.billNumber}); the run is session ${sessionId}`
        );
      }
    }
    const raw = { jurisdiction: HAWAII_JURISDICTION, sessionId, people: dataset.people };
    snapshot = parseLegiscanPeopleSnapshot(raw, expected);
    const snapshotPath = resolve(evidenceDir, hawaiiPeopleSnapshotFileName(sessionId));
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
    candidatesPool = await loadLegiscanStateLegCandidates(pool, HAWAII_JURISDICTION, scopeFrom);
    if (crosswalkFile !== null) {
      crosswalk = parseLegiscanCrosswalkFile(JSON.parse(readFileSync(resolve(crosswalkFile), "utf8")) as unknown, HAWAII_JURISDICTION);
      crosswalkCandidates = await loadLegiscanCrosswalkCandidates(pool, crosswalk, scopeFrom);
    }
  } finally {
    await pool.end();
  }

  const proposals = proposeLegiscanCrosswalk(people, candidatesPool);
  const crosswalkPeopleNotInSnapshot =
    crosswalk === null ? [] : [...crosswalk.byPeopleId.keys()].filter((peopleId) => !snapshot.byPeopleId.has(peopleId));

  const rolls: {
    file: string;
    chamber: LegislativeVoteChamber;
    roll: number;
    bill: string | null;
    members: number;
    excused: number;
    counts: Partial<Record<LegiscanMemberResolutionOutcome, number>>;
    matched: { peopleId: number; side: "yea" | "nay"; candidateId: string; candidateName: string }[];
    noCrosswalk: number[];
    error: string | null;
  }[] = [];
  const files = listHawaiiRollCallEvidenceFiles(evidenceDir, sessionId);
  if (crosswalk !== null) {
    for (const evidence of files) {
      const row: (typeof rolls)[number] = {
        file: evidence.file,
        chamber: evidence.chamber,
        roll: evidence.roll,
        bill: null,
        members: 0,
        excused: 0,
        counts: {},
        matched: [],
        noCrosswalk: [],
        error: null,
      };
      rolls.push(row);
      try {
        const parsed = parseHawaiiVoteEvidence(JSON.parse(readFileSync(resolve(evidenceDir, evidence.file), "utf8")) as unknown, {
          chamber: evidence.chamber,
          sessionId: evidence.sessionId,
          rollNumber: evidence.roll,
        });
        row.bill = parsed.bill;
        row.members = parsed.members.yeas.length + parsed.members.nays.length;
        row.excused = parsed.members.excused.length;
        const resolutions = resolveLegiscanMembers(
          { yeas: parsed.members.yeas, nays: parsed.members.nays },
          crosswalk,
          snapshot.byPeopleId,
          crosswalkCandidates
        );
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
    importerVersion: HAWAII_ROLLCALL_RESOLVE_IMPORTER_VERSION,
    startedAt: startedAt.toISOString(),
    finishedAt: new Date().toISOString(),
    jurisdiction: HAWAII_JURISDICTION,
    sessionId,
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
    console.error("rollcall:hi:resolve failed:", errorMessage(error));
    process.exitCode = 1;
  });
}
