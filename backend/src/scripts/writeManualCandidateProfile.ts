import { readFile } from "node:fs/promises";
import { Pool } from "pg";
import { createClient } from "redis";

import { resolveIncludePartyForCandidateContest } from "../ai/candidatePartisanship.js";
import { loadProjectEnv } from "../config/env.js";
import { parseCandidateProfilePayload } from "../contracts/candidateProfilePayloadContract.js";
import { enqueueCandidateRecordDrafts } from "../pipeline/candidates/candidateRecordDraftEmitter.js";
import {
  findOrCreateCandidateFromProfile,
  hasAtLeastOneHardIdentifier,
} from "../pipeline/candidates/candidateProfileIdentity.js";
import { upsertCandidateElection } from "../pipeline/candidates/candidateProfileLinks.js";
import { createCandidateFutureElectionNotificationEvents } from "../pipeline/users/candidateFollowNotificationEvents.js";

type ElectionContextRow = {
  election_id: string;
  state: string;
  district_type: string;
  official_ballot_title: string;
  is_partisan: boolean | null;
  race_type: string;
};

function usage(): string {
  return [
    "Usage:",
    "  npm run manual:candidate-profile:write -- --election-id uuid --file profile.json [--run-id id] [--is-incumbent true|false] [--emit-record-draft] [--allow-no-hard-identifier] [--dry-run]",
    "",
    "Payload must match CandidateProfilePayload. Live runs find/create a candidate and link it to the election.",
  ].join("\n");
}

function readFlag(name: string): string | null {
  const index = process.argv.indexOf(name);
  if (index >= 0) {
    const value = process.argv[index + 1];
    if (!value || value.startsWith("--") || value.trim().length === 0) {
      throw new Error(`Missing value for ${name}.\n${usage()}`);
    }
    return value;
  }
  const prefix = `${name}=`;
  const match = process.argv.find((token) => token.startsWith(prefix));
  if (!match) {
    return null;
  }
  const value = match.slice(prefix.length);
  if (value.trim().length === 0) {
    throw new Error(`Missing value for ${name}.\n${usage()}`);
  }
  return value;
}

function hasFlag(name: string): boolean {
  return process.argv.includes(name);
}

function readBooleanFlag(name: string): boolean | undefined {
  const value = readFlag(name);
  if (value === null) {
    return undefined;
  }
  if (value === "true") {
    return true;
  }
  if (value === "false") {
    return false;
  }
  throw new Error(`${name} must be true or false.\n${usage()}`);
}

async function readJsonFile(path: string): Promise<unknown> {
  const raw = await readFile(path, "utf8");
  return JSON.parse(raw) as unknown;
}

async function loadElectionContext(pool: Pool, electionId: string): Promise<ElectionContextRow | null> {
  const result = await pool.query<ElectionContextRow>(
    `
      SELECT
        e.id::text AS election_id,
        d.state,
        d.district_type,
        e.official_ballot_title,
        e.is_partisan,
        e.race_type
      FROM public.elections AS e
      JOIN public.districts AS d
        ON d.id = e.district_id
      WHERE e.id::text = $1
      LIMIT 1
    `,
    [electionId]
  );
  return result.rows[0] ?? null;
}

async function main(): Promise<void> {
  loadProjectEnv();

  const file = readFlag("--file");
  const electionId = readFlag("--election-id");
  if (!file || !electionId) {
    throw new Error(`Missing --file or --election-id.\n${usage()}`);
  }

  const rawPayload = await readJsonFile(file);
  const parsed = parseCandidateProfilePayload(rawPayload);
  if (!parsed.ok) {
    throw new Error(`Candidate profile payload failed validation: ${parsed.reason}`);
  }

  const profile = parsed.payload;
  if (!hasFlag("--allow-no-hard-identifier") && !hasAtLeastOneHardIdentifier(profile)) {
    throw new Error(
      "Candidate profile has no hard identifier. Add official_website_url, FEC/state filing ID, DOB, Twitter, LinkedIn, or pass --allow-no-hard-identifier deliberately."
    );
  }

  const dryRun = hasFlag("--dry-run");
  const emitRecordDraft = hasFlag("--emit-record-draft");
  const runId = readFlag("--run-id") ?? `manual_candidate_profile_${new Date().toISOString()}`;
  const isIncumbent = readBooleanFlag("--is-incumbent");
  const manualKey = `manual:candidate-profile:${electionId}:${profile.display_name.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "")}`;

  if (dryRun) {
    console.log(
      JSON.stringify(
        {
          dryRun: true,
          manualKey,
          runId,
          electionId,
          displayName: profile.display_name,
          hasHardIdentifier: hasAtLeastOneHardIdentifier(profile),
          emitRecordDraft,
        },
        null,
        2
      )
    );
    return;
  }

  const pool = new Pool({
    connectionString: process.env.DATABASE_URL ?? "postgresql://localhost:5432/voteapp",
  });
  const redis = emitRecordDraft
    ? createClient({ url: process.env.REDIS_URL ?? "redis://localhost:6379" })
    : null;

  try {
    const election = await loadElectionContext(pool, electionId);
    if (!election) {
      throw new Error(`Election not found for election_id=${electionId}`);
    }
    if (election.race_type !== "office") {
      throw new Error(`Candidate profile write requires an office election; election_id=${electionId} has race_type=${election.race_type}`);
    }

    const client = await pool.connect();
    let candidateId: string;
    let matchedExisting: boolean;
    let candidateElectionCreated = false;
    try {
      await client.query("BEGIN");
      const candidateResult = await findOrCreateCandidateFromProfile({
        client,
        profile,
        state: election.state,
        rosterParty: profile.party,
        includeParty: resolveIncludePartyForCandidateContest({
          districtType: election.district_type,
          state: election.state,
          officialBallotTitle: election.official_ballot_title,
          electionIsPartisan: election.is_partisan,
        }),
      });
      candidateId = candidateResult.candidateId;
      matchedExisting = candidateResult.matchedExisting;

      const linkResult = await upsertCandidateElection({
        client,
        candidateId,
        electionId,
        isIncumbent,
      });
      candidateElectionCreated = linkResult.created;
      if (linkResult.created) {
        await createCandidateFutureElectionNotificationEvents(client, {
          candidateId,
          electionId,
        });
      }
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }

    let recordDraft: { emittedCount: number; skippedCount: number } | null = null;
    if (redis) {
      await redis.connect();
      recordDraft = await enqueueCandidateRecordDrafts(redis, [{ candidateId, electionId, runId }]);
    }

    console.log(
      JSON.stringify(
        {
          manualKey,
          runId,
          electionId,
          candidateId,
          matchedExisting,
          candidateElectionCreated,
          recordDraft,
        },
        null,
        2
      )
    );
  } finally {
    if (redis) {
      await redis.quit().catch(() => undefined);
    }
    await pool.end();
  }
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error("manual candidate profile write failed:", message);
  process.exitCode = 1;
});
