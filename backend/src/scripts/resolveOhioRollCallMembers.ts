import { existsSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import {
  loadOhioCrosswalkCandidates,
  loadOhioStateLegCandidates,
  parseOhioCrosswalkFile,
  parseOhioLegislators,
  proposeOhioCrosswalk,
  resolveOhioMembers,
  type OhioCrosswalk,
  type OhioLegislator,
  type OhioMemberResolutionOutcome,
} from "../pipeline/rollcall/ohioMemberResolver.js";
import {
  ohioActionMemberVotes,
  ohioLegislatorsUrl,
  parseOhioVoteEvidence,
  type OhioAction,
} from "../pipeline/rollcall/ohioRollCall.js";
import type { LegislativeVoteChamber } from "../pipeline/rollcall/legislativeVotes.js";
import { reportPath } from "../pipeline/rollcall/rollCallReportPaths.js";
import { readPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";
import { DEFAULT_SCOPE_FROM } from "./resolveRollCallMembers.js";

// Identity report for the Ohio pilot (plan §2, state variant). Ohio members
// are lpids with no id shared with our candidates, so the identity layer is
// a committed crosswalk file that a human writes; this script does the
// reading work on both sides of that review:
//   - it always proposes lpid → candidate pairs by strict name matching
//     against the Nov-2026 Ohio state-legislative pool (suggestions only);
//   - when --crosswalk-file is given, it validates the file (every
//     candidate id must exist; lpids should be roster members) and
//     resolves every evidence file's yea/nay lpids through it, so the
//     pre-import counts are on record.
// Read-only: no candidate writes, no AI.
//
//   npm run rollcall:oh:resolve -- --ga 136 --evidence-dir evidence/rollcall/ohio-136/batch-01 \
//     --crosswalk-file evidence/rollcall/ohio-136/crosswalk.json

export const OHIO_ROLLCALL_RESOLVE_IMPORTER_VERSION = "rollcall-oh-resolve-v1";

export type OhioRollCallEvidenceFile = {
  file: string;
  chamber: LegislativeVoteChamber;
  generalAssembly: number;
  roll: number;
};

const EVIDENCE_FILE_PATTERN = /^oh-(house|senate)-(\d+)-roll(\d+)\.json$/;

/** The Ohio evidence files of a dir, in chamber/roll (= time) order. */
export function listOhioRollCallEvidenceFiles(evidenceDir: string): OhioRollCallEvidenceFile[] {
  const files: OhioRollCallEvidenceFile[] = [];
  for (const file of readdirSync(evidenceDir)) {
    const match = EVIDENCE_FILE_PATTERN.exec(file);
    if (!match) {
      continue;
    }
    files.push({
      file,
      chamber: match[1] as LegislativeVoteChamber,
      generalAssembly: Number(match[2]),
      roll: Number(match[3]),
    });
  }
  return files.sort((a, b) => a.chamber.localeCompare(b.chamber) || a.generalAssembly - b.generalAssembly || a.roll - b.roll);
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
  assertKnownCliFlags("rollcall:oh:resolve", argv, [
    { name: "--ga", value: "both" },
    { name: "--evidence-dir", value: "both" },
    { name: "--crosswalk-file", value: "both" },
    { name: "--roster-file", value: "both" },
    { name: "--scope-from", value: "both" },
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
  const crosswalkFile = readValueFlag(argv, "--crosswalk-file");
  const rosterFile = readValueFlag(argv, "--roster-file");
  const scopeFrom = readValueFlag(argv, "--scope-from") ?? DEFAULT_SCOPE_FROM;

  loadProjectEnv();
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required to read the candidate pool");
  }

  const startedAt = new Date();

  // Roster: from the committed snapshot when given, else fetched live and
  // written into the evidence dir so the run's snapshot is committable.
  let rosterRaw: unknown;
  let rosterSource: string;
  if (rosterFile !== null) {
    rosterSource = resolve(rosterFile);
    rosterRaw = JSON.parse(readFileSync(rosterSource, "utf8")) as unknown;
  } else {
    rosterSource = ohioLegislatorsUrl(generalAssembly);
    // Same 30s abort every other Ohio GET uses; a stalled roster request
    // must not hang the run.
    const response = await fetch(rosterSource, {
      headers: { accept: "application/json" },
      signal: AbortSignal.timeout(30_000),
    });
    if (!response.ok) {
      throw new Error(`${rosterSource} answered ${response.status}`);
    }
    const body = await response.text();
    rosterRaw = JSON.parse(body) as unknown;
    const snapshot = resolve(evidenceDir, `oh-legislators-${generalAssembly}.json`);
    if (!existsSync(snapshot)) {
      writeFileSync(snapshot, body.endsWith("\n") ? body : `${body}\n`);
    }
  }
  const roster = parseOhioLegislators(rosterRaw);
  const rosterByLpid = new Map(roster.map((legislator) => [legislator.lpid, legislator]));

  const pool = new Pool({ connectionString: databaseUrl });
  let candidatesPool;
  let crosswalk: OhioCrosswalk | null = null;
  let crosswalkCandidates = new Map<string, { candidateId: string; name: string; inScope: boolean }>();
  try {
    candidatesPool = await loadOhioStateLegCandidates(pool, scopeFrom);
    if (crosswalkFile !== null) {
      crosswalk = parseOhioCrosswalkFile(JSON.parse(readFileSync(resolve(crosswalkFile), "utf8")) as unknown);
      if (crosswalk.generalAssembly !== generalAssembly) {
        throw new Error(`crosswalk is for GA ${crosswalk.generalAssembly}, run is --ga ${generalAssembly}`);
      }
      crosswalkCandidates = await loadOhioCrosswalkCandidates(pool, crosswalk, scopeFrom);
    }
  } finally {
    await pool.end();
  }

  const proposals = proposeOhioCrosswalk(roster, candidatesPool);

  // lpids in the crosswalk that the roster does not know: not fatal (the
  // roster is current membership; a member who resigned still appears in
  // old journals), but each one is listed for the reviewer.
  const crosswalkLpidsNotInRoster =
    crosswalk === null ? [] : [...crosswalk.byLpid.keys()].filter((lpid) => !rosterByLpid.has(lpid));

  const rolls: {
    file: string;
    chamber: LegislativeVoteChamber;
    roll: number;
    bill: string | null;
    members: number;
    counts: Partial<Record<OhioMemberResolutionOutcome, number>>;
    matched: { lpid: string; side: "yea" | "nay"; candidateId: string; candidateName: string }[];
    noCrosswalk: string[];
    error: string | null;
  }[] = [];
  // Same filter the importer applies: a mixed-GA directory must not run
  // another assembly's votes through this GA's roster and crosswalk.
  const files = listOhioRollCallEvidenceFiles(evidenceDir).filter((file) => file.generalAssembly === generalAssembly);
  if (crosswalk !== null) {
    for (const evidence of files) {
      const row: (typeof rolls)[number] = {
        file: evidence.file,
        chamber: evidence.chamber,
        roll: evidence.roll,
        bill: null,
        members: 0,
        counts: {},
        matched: [],
        noCrosswalk: [],
        error: null,
      };
      rolls.push(row);
      try {
        const parsed = parseOhioVoteEvidence(
          JSON.parse(readFileSync(resolve(evidenceDir, evidence.file), "utf8")) as unknown,
          { chamber: evidence.chamber, generalAssembly: evidence.generalAssembly, rollNumber: evidence.roll }
        );
        row.bill = parsed.bill;
        const votes = ohioActionMemberVotes(parsed.action as OhioAction);
        row.members = votes.yeas.length + votes.nays.length;
        const resolutions = resolveOhioMembers(votes, crosswalk, rosterByLpid, crosswalkCandidates);
        for (const resolution of resolutions) {
          row.counts[resolution.outcome] = (row.counts[resolution.outcome] ?? 0) + 1;
          if (resolution.outcome === "matched" && resolution.candidate) {
            row.matched.push({
              lpid: resolution.lpid,
              side: resolution.side,
              candidateId: resolution.candidate.candidateId,
              candidateName: resolution.candidate.name,
            });
          } else if (resolution.outcome === "no_crosswalk") {
            row.noCrosswalk.push(resolution.lpid);
          }
        }
      } catch (error) {
        row.error = errorMessage(error);
      }
    }
  }

  const counts: Partial<Record<OhioMemberResolutionOutcome, number>> = {};
  for (const roll of rolls) {
    for (const [outcome, n] of Object.entries(roll.counts)) {
      counts[outcome as OhioMemberResolutionOutcome] = (counts[outcome as OhioMemberResolutionOutcome] ?? 0) + n;
    }
  }
  const report = {
    importerVersion: OHIO_ROLLCALL_RESOLVE_IMPORTER_VERSION,
    startedAt: startedAt.toISOString(),
    finishedAt: new Date().toISOString(),
    generalAssembly,
    evidenceDir: reportPath(evidenceDir),
    scopeFrom,
    // A file path is reported like every other input path; the default
    // roster URL is a URL and is written as-is.
    rosterSource: rosterFile === null ? rosterSource : reportPath(rosterFile),
    rosterMembers: roster.length,
    candidatePool: candidatesPool.length,
    crosswalkFile: crosswalkFile === null ? null : reportPath(crosswalkFile),
    crosswalkEntries: crosswalk === null ? null : crosswalk.byLpid.size,
    crosswalkLpidsNotInRoster,
    proposals: proposals.proposals,
    unmatchedRoster: proposals.unmatchedRoster.map((legislator) => ({
      lpid: legislator.lpid,
      name: legislator.displayName,
      seat: `${legislator.chamber} ${legislator.district}`,
      active: legislator.active,
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
        unmatchedRoster: report.unmatchedRoster.length,
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
    console.error("rollcall:oh:resolve failed:", errorMessage(error));
    process.exitCode = 1;
  });
}
