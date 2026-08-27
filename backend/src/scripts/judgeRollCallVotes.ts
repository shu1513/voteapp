import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";
import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { loadAllResearchAreas } from "../pipeline/candidates/candidateRecordAreaTagging.js";
import {
  applyLegislativeVoteJudgment,
  type LegislativeVoteJudgment,
  type LegislativeVoteJudgmentOutcome,
} from "../pipeline/rollcall/legislativeVoteStore.js";
import { LEGISCAN_STATE_JURISDICTIONS } from "../pipeline/rollcall/legiscanStateConfigs.js";
import { LEGISLATIVE_VOTE_CHAMBERS, type LegislativeVoteChamber } from "../pipeline/rollcall/legislativeVotes.js";
import { parseRollCallLabels } from "../pipeline/rollcall/rollCallFanOut.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";

// Applies the operator's judgments to the roll-call review queue
// (docs/plans/roll-call-vote-import.md §1 steps 4-5): for each roll call in
// a judgments file, the yea sentence, the nay sentence, the labels, and the
// review decision are written onto its legislative_votes row. The file is the
// review artifact — it is kept under backend/evidence/rollcall/<run-id>/ and
// committed — so nothing here is invented: a judgment is either in the file
// or not applied. The whole file is checked before the first write and
// applied in one transaction; re-applying it writes nothing. No AI.
//
//   npm run rollcall:judge -- --judgments-file evidence/rollcall/<run-id>/judgments.json --dry-run
//   npm run rollcall:judge -- --judgments-file evidence/rollcall/<run-id>/judgments.json
//
// judgments.json (a federal entry names congress + session; a state entry
// names jurisdiction + the source's session key instead, and its roll is
// the stored surrogate roll number — for Ohio, the vote's occurred
// timestamp in epoch seconds, printed by the fetch report). An entry may
// carry an optional official_vote_date when the source stamped the
// legislative day instead of the calendar day (an overnight sine-die vote):
// vote_date stays what the source file says, and the records fan out on the
// official date. Cite the official record that dates it in the run's notes:
//   {
//     "judgments": [
//       {
//         "chamber": "house", "congress": 119, "session": 1, "roll": 145,
//         "measure_id": "H R 1", "vote_date": "2025-05-22",
//         "review_status": "approved",
//         "yea_description": "Voted to pass H.R. 1, ... It passed the House 215-214.",
//         "nay_description": "Voted against passing H.R. 1, ... It passed the House 215-214.",
//         "labels": [{ "slug": "personal_income_tax_reduction", "yea": "for" }, { "slug": "general" }]
//       },
//       {
//         "jurisdiction": "OH", "chamber": "house", "session": "136",
//         "roll": 1744207254, "measure_id": "HB 96", "vote_date": "2025-04-09",
//         "review_status": "approved",
//         "yea_description": "...", "nay_description": "...",
//         "labels": [{ "slug": "government_spending_reduction", "yea": "against" }]
//       }
//     ]
//   }

const FEDERAL_JURISDICTION = "US";
// State entries name their jurisdiction explicitly; only sources with a
// fetcher are accepted, so a typo cannot write a judgment nothing imports:
// the Ohio pilot's own feed, plus every state registered in the LegiScan
// config registry (whose session id is the entry's session key).
const STATE_JURISDICTIONS = new Set(["OH", ...LEGISCAN_STATE_JURISDICTIONS]);
const REVIEW_STATUSES = ["pending", "approved"] as const;

function fail(index: number, message: string): never {
  throw new Error(`judgments[${index}]: ${message}`);
}

function readPositiveInteger(index: number, value: unknown, field: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1) {
    fail(index, `${field} must be a positive integer`);
  }
  return value;
}

function readSentence(index: number, value: unknown, field: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    fail(index, `${field} must be a non-empty string`);
  }
  return value.trim();
}

/**
 * Reads and checks the whole file: shape, chamber/session values, the
 * measure and date the judgment is about (checked against the row at write
 * time), both sentences present and different (the records validator folds
 * case when it de-duplicates, so a case-only difference counts as the same
 * sentence), labels by the importer's own rule, and each roll call named
 * once.
 */
export function parseJudgmentsFile(raw: unknown, allowedSlugs: ReadonlySet<string>): LegislativeVoteJudgment[] {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    throw new Error("judgments file must be an object with a judgments array");
  }
  const { judgments } = raw as { judgments?: unknown };
  if (!Array.isArray(judgments) || judgments.length === 0) {
    throw new Error("judgments must be a non-empty array");
  }
  const entries: LegislativeVoteJudgment[] = [];
  const seen = new Set<string>();
  for (const [index, element] of judgments.entries()) {
    if (typeof element !== "object" || element === null || Array.isArray(element)) {
      fail(index, "must be an object");
    }
    const entry = element as Record<string, unknown>;
    const chamber = entry.chamber;
    if (typeof chamber !== "string" || !(LEGISLATIVE_VOTE_CHAMBERS as readonly string[]).includes(chamber)) {
      fail(index, `chamber must be one of ${LEGISLATIVE_VOTE_CHAMBERS.join(", ")}`);
    }
    let jurisdiction: string;
    let sessionKey: string;
    if (entry.jurisdiction === undefined || entry.jurisdiction === FEDERAL_JURISDICTION) {
      jurisdiction = FEDERAL_JURISDICTION;
      const congress = readPositiveInteger(index, entry.congress, "congress");
      const session = readPositiveInteger(index, entry.session, "session");
      if (session !== 1 && session !== 2) {
        fail(index, "session must be 1 or 2");
      }
      sessionKey = `${congress}-${session}`;
    } else {
      if (typeof entry.jurisdiction !== "string" || !STATE_JURISDICTIONS.has(entry.jurisdiction)) {
        fail(index, `jurisdiction must be omitted (federal) or one of ${[...STATE_JURISDICTIONS].join(", ")}`);
      }
      jurisdiction = entry.jurisdiction;
      if (entry.congress !== undefined) {
        fail(index, "a state entry names session, not congress");
      }
      if (typeof entry.session !== "string" || entry.session.trim().length === 0) {
        fail(index, "a state entry's session must be the source's session key (e.g. \"136\")");
      }
      sessionKey = entry.session.trim();
    }
    const roll = readPositiveInteger(index, entry.roll, "roll");
    const measureId = entry.measure_id;
    if (measureId !== null && (typeof measureId !== "string" || measureId.trim().length === 0)) {
      fail(index, "measure_id must be a non-empty string, or null for a vote with no measure");
    }
    const voteDate = entry.vote_date;
    if (typeof voteDate !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(voteDate)) {
      fail(index, "vote_date must be an ISO date (YYYY-MM-DD)");
    }
    const officialVoteDate = entry.official_vote_date ?? null;
    if (officialVoteDate !== null && (typeof officialVoteDate !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(officialVoteDate))) {
      fail(index, "official_vote_date must be an ISO date (YYYY-MM-DD), or omitted");
    }
    if (officialVoteDate === voteDate) {
      fail(index, "official_vote_date equals vote_date; omit it unless the source stamped a different day");
    }
    const reviewStatus = entry.review_status;
    if (typeof reviewStatus !== "string" || !(REVIEW_STATUSES as readonly string[]).includes(reviewStatus)) {
      fail(index, `review_status must be one of ${REVIEW_STATUSES.join(", ")}`);
    }
    const yeaDescription = readSentence(index, entry.yea_description, "yea_description");
    const nayDescription = readSentence(index, entry.nay_description, "nay_description");
    if (yeaDescription.toLowerCase() === nayDescription.toLowerCase()) {
      fail(index, "yea_description and nay_description are the same sentence");
    }
    let labels;
    try {
      labels = parseRollCallLabels(entry.labels, allowedSlugs);
    } catch (error) {
      fail(index, (error instanceof Error ? error.message : String(error)).replace(/^labels_json/, "labels"));
    }
    const key = `${jurisdiction}:${chamber}:${sessionKey}:${roll}`;
    if (seen.has(key)) {
      fail(index, `${key} appears more than once`);
    }
    seen.add(key);
    entries.push({
      jurisdiction,
      chamber: chamber as LegislativeVoteChamber,
      session: sessionKey,
      rollNumber: roll,
      measureId: measureId === null ? null : measureId.trim(),
      voteDate,
      officialVoteDate,
      yeaDescription,
      nayDescription,
      labels,
      reviewStatus: reviewStatus as "pending" | "approved",
    });
  }
  return entries;
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
  assertKnownCliFlags("rollcall:judge", argv, [
    { name: "--judgments-file", value: "both" },
    { name: "--dry-run", value: "none" },
  ]);
  const judgmentsFile = readValueFlag(argv, "--judgments-file");
  if (judgmentsFile === null) {
    throw new Error("--judgments-file is required");
  }
  const dryRun = argv.includes("--dry-run");
  const raw = JSON.parse(readFileSync(judgmentsFile, "utf8")) as unknown;

  loadProjectEnv();
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required");
  }
  requireLocalDatabaseTarget(databaseUrl);

  const pool = new Pool({ connectionString: databaseUrl });
  const rows: {
    chamber: LegislativeVoteChamber;
    session: string;
    roll: number;
    reviewStatus: string;
    outcome: LegislativeVoteJudgmentOutcome | "dry_run";
  }[] = [];
  try {
    const slugs = new Set((await loadAllResearchAreas(pool)).map((area) => area.slug));
    const entries = parseJudgmentsFile(raw, slugs);
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      for (const entry of entries) {
        const outcome = await applyLegislativeVoteJudgment(client, entry);
        rows.push({
          chamber: entry.chamber,
          session: entry.session,
          roll: entry.rollNumber,
          reviewStatus: entry.reviewStatus,
          outcome: dryRun && outcome !== "unchanged" ? "dry_run" : outcome,
        });
      }
      // A dry run still runs every statement, so the CHECKs and the freeze
      // trigger vet the file too; it just never commits.
      await client.query(dryRun ? "ROLLBACK" : "COMMIT");
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  } finally {
    await pool.end();
  }

  const counts: Record<string, number> = {};
  for (const row of rows) {
    counts[row.outcome] = (counts[row.outcome] ?? 0) + 1;
  }
  console.log(JSON.stringify({ judgmentsFile, dryRun, judgments: rows.length, counts, rows }, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("rollcall:judge failed:", errorMessage(error));
    process.exitCode = 1;
  });
}
