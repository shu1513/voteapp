import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { loadCongressLegislators } from "../pipeline/rollcall/congressLegislators.js";
import {
  loadCandidateFecIndex,
  resolveFederalMembers,
  type CandidateFecMatch,
  type FederalMemberResolution,
  type FederalMemberResolutionOutcome,
} from "../pipeline/rollcall/federalMemberResolver.js";
import { parseFederalMemberVotes } from "../pipeline/rollcall/federalRollCallMembers.js";
import { parseFederalRollCallXml, type ParsedFederalRollCall } from "../pipeline/rollcall/federalRollCallXml.js";
import type { LegislativeVoteChamber } from "../pipeline/rollcall/legislativeVotes.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";

// Dry-run identity report for the roll-call import
// (docs/plans/roll-call-vote-import.md §2): for every roll-call XML a
// rollcall:fetch run left under its evidence dir, resolve each member row to
// a candidate by exact FEC id and write resolve-report.json next to the XML.
// Read-only: no candidate writes, no AI, one SELECT for the FEC index.
// Example:
//
//   npm run rollcall:resolve -- --evidence-dir evidence/rollcall/rollcall-fetch-house-119-1-20260822T000000Z

export const ROLLCALL_RESOLVE_IMPORTER_VERSION = "rollcall-resolve-v1";
// Only candidates on a Nov-2026-or-later election are in scope (plan §2).
export const DEFAULT_SCOPE_FROM = "2026-11-01";
const DEFAULT_LEGISLATORS_DIR = "evidence/rollcall/congress-legislators";
const EVIDENCE_FILE_PATTERN = /^(house|senate)-(\d+)-(\d+)-roll(\d+)\.xml$/;

export type RollCallEvidenceFile = {
  file: string;
  chamber: LegislativeVoteChamber;
  congress: number;
  session: number;
  roll: number;
};

/** The roll-call XML files of an evidence dir, in chamber/congress/session/roll order. */
export function listRollCallEvidenceFiles(evidenceDir: string): RollCallEvidenceFile[] {
  const files: RollCallEvidenceFile[] = [];
  for (const file of readdirSync(evidenceDir)) {
    const match = EVIDENCE_FILE_PATTERN.exec(file);
    if (!match) {
      continue;
    }
    files.push({
      file,
      chamber: match[1] as LegislativeVoteChamber,
      congress: Number(match[2]),
      session: Number(match[3]),
      roll: Number(match[4]),
    });
  }
  return files.sort(
    (a, b) =>
      a.chamber.localeCompare(b.chamber) || a.congress - b.congress || a.session - b.session || a.roll - b.roll
  );
}

type MatchedRow = {
  memberId: string;
  name: string;
  state: string;
  vote: string;
  candidateId: string;
  candidateName: string;
};

export type RollCallResolveReportRow = RollCallEvidenceFile & {
  voteDate: string | null;
  question: string | null;
  members: number;
  counts: Partial<Record<FederalMemberResolutionOutcome, number>>;
  matched: MatchedRow[];
  error: string | null;
};

// One line per distinct (chamber, member id, outcome) so a human reads each
// unresolved person once, not once per roll call.
export type UnmatchedMember = {
  chamber: LegislativeVoteChamber;
  memberId: string;
  name: string;
  state: string;
  outcome: Exclude<FederalMemberResolutionOutcome, "matched">;
  legislator: string | null;
  fecIds: string[];
  candidates: CandidateFecMatch[];
  detail: string;
  rolls: number;
};

export function summarizeUnmatched(resolutions: readonly FederalMemberResolution[]): UnmatchedMember[] {
  const byKey = new Map<string, UnmatchedMember>();
  for (const resolution of resolutions) {
    if (resolution.outcome === "matched") {
      continue;
    }
    const key = `${resolution.member.chamber}:${resolution.member.memberId}:${resolution.outcome}`;
    const existing = byKey.get(key);
    if (existing) {
      existing.rolls += 1;
      continue;
    }
    byKey.set(key, {
      chamber: resolution.member.chamber,
      memberId: resolution.member.memberId,
      name: resolution.member.name,
      state: resolution.member.state,
      outcome: resolution.outcome,
      legislator: resolution.legislator?.name ?? null,
      fecIds: resolution.legislator?.fecIds ?? [],
      candidates: resolution.candidates,
      detail: resolution.detail,
      rolls: 1,
    });
  }
  return [...byKey.values()].sort(
    (a, b) => a.outcome.localeCompare(b.outcome) || a.chamber.localeCompare(b.chamber) || a.name.localeCompare(b.name)
  );
}

/**
 * Both feeds put the metadata first, so a body cut off after the header —
 * or part-way through the member list — still parses. Every recorded roll
 * call lists every member, and the tally can never exceed the rows that
 * produced it, so either condition means the evidence file is incomplete.
 */
export function assertMemberRowsComplete(parsed: Pick<ParsedFederalRollCall, "yeas" | "nays">, members: number): void {
  if (members === 0) {
    throw new Error("XML has no member rows; the evidence file is incomplete");
  }
  if (parsed.yeas + parsed.nays > members) {
    throw new Error(
      `XML tallies ${parsed.yeas + parsed.nays} yea/nay votes but lists only ${members} member rows; the evidence file is incomplete`
    );
  }
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
  assertKnownCliFlags("rollcall:resolve", argv, [
    { name: "--evidence-dir", value: "both" },
    { name: "--legislators-sha", value: "both" },
    { name: "--legislators-dir", value: "both" },
    { name: "--scope-from", value: "both" },
  ]);

  const evidenceDirRaw = readValueFlag(argv, "--evidence-dir");
  if (evidenceDirRaw === null) {
    throw new Error("--evidence-dir is required");
  }
  const evidenceDir = resolve(evidenceDirRaw);
  const legislatorsSha = readValueFlag(argv, "--legislators-sha") ?? undefined;
  const legislatorsDir = resolve(readValueFlag(argv, "--legislators-dir") ?? DEFAULT_LEGISLATORS_DIR);
  const scopeFrom = readValueFlag(argv, "--scope-from") ?? DEFAULT_SCOPE_FROM;

  const files = listRollCallEvidenceFiles(evidenceDir);
  if (files.length === 0) {
    throw new Error(`${evidenceDir} holds no <chamber>-<congress>-<session>-roll<N>.xml files`);
  }

  loadProjectEnv();
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required to read candidates.fec_ids");
  }

  const startedAt = new Date();
  const legislators = await loadCongressLegislators({ sha: legislatorsSha, cacheDir: legislatorsDir });
  const pool = new Pool({ connectionString: databaseUrl });
  let candidatesByFec;
  try {
    candidatesByFec = await loadCandidateFecIndex(pool, scopeFrom);
  } finally {
    await pool.end();
  }
  const candidateIds = new Set<string>();
  const inScopeCandidateIds = new Set<string>();
  for (const matches of candidatesByFec.values()) {
    for (const match of matches) {
      candidateIds.add(match.candidateId);
      if (match.inScope) {
        inScopeCandidateIds.add(match.candidateId);
      }
    }
  }

  const rows: RollCallResolveReportRow[] = [];
  const allResolutions: FederalMemberResolution[] = [];
  const counts: Partial<Record<FederalMemberResolutionOutcome, number>> = {};
  for (const evidence of files) {
    const row: RollCallResolveReportRow = {
      ...evidence,
      voteDate: null,
      question: null,
      members: 0,
      counts: {},
      matched: [],
      error: null,
    };
    rows.push(row);
    try {
      const xml = readFileSync(resolve(evidenceDir, evidence.file), "utf8");
      const parsed = parseFederalRollCallXml(evidence.chamber, xml);
      if (parsed.congress !== evidence.congress || parsed.session !== evidence.session || parsed.rollNumber !== evidence.roll) {
        throw new Error(`XML says congress ${parsed.congress} session ${parsed.session} roll ${parsed.rollNumber}`);
      }
      row.voteDate = parsed.voteDate;
      row.question = parsed.question;
      const members = parseFederalMemberVotes(evidence.chamber, xml);
      row.members = members.length;
      assertMemberRowsComplete(parsed, members.length);
      const resolutions = resolveFederalMembers(members, parsed.voteDate, legislators.index, candidatesByFec);
      allResolutions.push(...resolutions);
      for (const resolution of resolutions) {
        row.counts[resolution.outcome] = (row.counts[resolution.outcome] ?? 0) + 1;
        counts[resolution.outcome] = (counts[resolution.outcome] ?? 0) + 1;
        if (resolution.outcome === "matched" && resolution.candidate) {
          row.matched.push({
            memberId: resolution.member.memberId,
            name: resolution.member.name,
            state: resolution.member.state,
            vote: resolution.member.vote,
            candidateId: resolution.candidate.candidateId,
            candidateName: resolution.candidate.name,
          });
        }
      }
    } catch (error) {
      row.error = errorMessage(error);
    }
  }

  const report = {
    importerVersion: ROLLCALL_RESOLVE_IMPORTER_VERSION,
    startedAt: startedAt.toISOString(),
    finishedAt: new Date().toISOString(),
    evidenceDir,
    scopeFrom,
    legislators: {
      sha: legislators.sha,
      people: legislators.index.count,
      files: legislators.files.map((file) => ({
        file: file.file,
        sha256: file.sha256,
        count: file.count,
        fromCache: file.fromCache,
      })),
    },
    candidatesWithFecIds: candidateIds.size,
    inScopeCandidates: inScopeCandidateIds.size,
    files: files.length,
    fileErrors: rows.filter((row) => row.error !== null).length,
    counts,
    rolls: rows,
    unmatched: summarizeUnmatched(allResolutions),
  };
  writeFileSync(resolve(evidenceDir, "resolve-report.json"), `${JSON.stringify(report, null, 2)}\n`);
  console.log(
    JSON.stringify(
      {
        ...report,
        rolls: rows.map((row) => ({ ...row, matched: row.matched.length })),
        unmatched: report.unmatched.length,
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
    console.error("rollcall:resolve failed:", errorMessage(error));
    process.exitCode = 1;
  });
}
