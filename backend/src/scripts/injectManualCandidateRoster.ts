import { readFile } from "node:fs/promises";
import { Pool } from "pg";
import { createClient } from "redis";

import { loadProjectEnv } from "../config/env.js";
import {
  STAGING_CANDIDATE_ROSTER_DRAFT_STREAM,
  STAGING_ITEM_TYPE_CANDIDATE_ROSTER,
} from "../config/electionsPipeline.js";
import { resolveCandidateResearchMode } from "../ai/candidateResearchMode.js";
import { parseCandidateRosterPayload } from "../contracts/candidateRosterPayloadContract.js";

type ElectionPreflightRow = {
  id: string;
  official_ballot_title: string;
  district_type: string;
  race_type: string;
};

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

function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

function usage(): string {
  return [
    "Usage:",
    "  npm run manual:candidate-roster:inject -- --election-id uuid --file roster.json [--run-id id] [--dry-run]",
    "",
    "Payload may be either { candidates: [...] } or { election_id, candidates: [...] }.",
  ].join("\n");
}

async function readJsonFile(path: string): Promise<unknown> {
  const raw = await readFile(path, "utf8");
  return JSON.parse(raw) as unknown;
}

function payloadElectionId(payload: unknown): string | null {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return null;
  }
  const value = (payload as Record<string, unknown>).election_id;
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : null;
}

async function loadElectionPreflight(pool: Pool, electionId: string): Promise<ElectionPreflightRow | null> {
  const result = await pool.query<ElectionPreflightRow>(
    `
      SELECT e.id::text AS id,
             e.official_ballot_title,
             d.district_type,
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

function requireEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required for manual candidate roster injection`);
  }
  return value;
}

async function main(): Promise<void> {
  loadProjectEnv();

  const file = readFlag("--file");
  if (!file) {
    throw new Error(`Missing --file.\n${usage()}`);
  }

  const rawPayload = await readJsonFile(file);
  const cliElectionId = readFlag("--election-id");
  const payloadId = payloadElectionId(rawPayload);
  if (cliElectionId && payloadId && cliElectionId !== payloadId) {
    throw new Error(
      `--election-id (${cliElectionId}) does not match payload.election_id (${payloadId}).`
    );
  }
  const electionId = cliElectionId ?? payloadId;
  if (!electionId) {
    throw new Error(`Missing --election-id and payload.election_id.\n${usage()}`);
  }

  const dryRun = hasFlag("--dry-run");
  const pool = new Pool({ connectionString: requireEnv("DATABASE_URL") });
  let redis: ReturnType<typeof createClient> | null = null;

  try {
    const election = await loadElectionPreflight(pool, electionId);
    if (!election) {
      throw new Error(`Election not found for election_id=${electionId}`);
    }
    if (election.race_type !== "office") {
      throw new Error(
        `Candidate roster injection requires an office election; election_id=${electionId} has race_type=${election.race_type}`
      );
    }

    const researchMode = resolveCandidateResearchMode({
      districtType: election.district_type,
      officialBallotTitle: election.official_ballot_title,
    });
    const includeFecIds = researchMode !== "state_level";
    const parsed = parseCandidateRosterPayload(rawPayload, {
      allowFecIds: includeFecIds,
      requireFecIds: includeFecIds,
    });
    if (!parsed.ok) {
      throw new Error(`Candidate roster payload failed validation: ${parsed.reason}`);
    }

    const ingestKey = `candidate_roster:${electionId}`;
    const runId = readFlag("--run-id") ?? `manual_candidate_roster_${new Date().toISOString()}`;
    const stagedPayload = {
      election_id: electionId,
      candidates: parsed.payload.candidates,
    };

    if (dryRun) {
      console.log(
        JSON.stringify(
          {
            dryRun: true,
            ingestKey,
            runId,
            electionId,
            researchMode,
            requiresFecIds: includeFecIds,
            candidateCount: parsed.payload.candidates.length,
          },
          null,
          2
        )
      );
      return;
    }

    redis = createClient({ url: requireEnv("REDIS_URL") });

    await redis.connect();

    await pool.query(
      `
        INSERT INTO staging_items (
          ingest_key,
          item_type,
          payload,
          status,
          reason,
          run_id,
          model,
          schema_version,
          prompt_version,
          validated_at,
          written_at,
          failure_debug,
          ai_raw_debug
        )
        VALUES ($1, $2, $3::jsonb, 'validated', NULL, $4, $5, NULL, NULL, now(), NULL, NULL, $6::jsonb)
        ON CONFLICT (ingest_key) DO UPDATE SET
          item_type = EXCLUDED.item_type,
          payload = EXCLUDED.payload,
          status = 'validated',
          reason = NULL,
          run_id = EXCLUDED.run_id,
          model = EXCLUDED.model,
          schema_version = NULL,
          prompt_version = NULL,
          validated_at = now(),
          written_at = NULL,
          failure_debug = NULL,
          ai_raw_debug = EXCLUDED.ai_raw_debug,
          updated_at = now()
      `,
      [
        ingestKey,
        STAGING_ITEM_TYPE_CANDIDATE_ROSTER,
        JSON.stringify(stagedPayload),
        runId,
        "manual-research:codex",
        JSON.stringify({ manual_research: true }),
      ]
    );

    let redisMessageId: string;
    try {
      redisMessageId = await redis.xAdd(STAGING_CANDIDATE_ROSTER_DRAFT_STREAM, "*", {
        election_id: electionId,
        item_type: STAGING_ITEM_TYPE_CANDIDATE_ROSTER,
        run_id: runId,
      });
    } catch (error) {
      await pool.query(
        `
          UPDATE staging_items
          SET status = 'failed',
              reason = $2,
              updated_at = now()
          WHERE ingest_key = $1
            AND item_type = $3
        `,
        [
          ingestKey,
          `manual candidate roster redis publish failed: ${toReason(error)}`,
          STAGING_ITEM_TYPE_CANDIDATE_ROSTER,
        ]
      );
      throw error;
    }

    console.log(
      JSON.stringify(
        {
          ingestKey,
          runId,
          redisMessageId,
          electionId,
          officialBallotTitle: election.official_ballot_title,
          researchMode,
          requiresFecIds: includeFecIds,
          candidateCount: parsed.payload.candidates.length,
          next: ["npm run candidates:roster:enrich -- --once"],
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
  console.error("manual candidate roster inject failed:", message);
  process.exitCode = 1;
});
