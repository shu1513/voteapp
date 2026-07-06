import { readFile } from "node:fs/promises";
import { Pool } from "pg";

import { validateBallotMeasureAiPayload } from "../ai/enrichBallotMeasure.js";
import { loadProjectEnv } from "../config/env.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";
import {
  loadAllowedBallotMeasureResearchAreas,
  upsertBallotMeasureResearchAreaTags,
} from "../pipeline/ballotMeasures/ballotMeasureResearchAreaTags.js";

type BallotMeasureElectionRow = {
  id: string;
  district_id: string;
  official_ballot_title: string;
  race_type: string;
};

function usage(): string {
  return [
    "Usage:",
    "  npm run manual:ballot-measure:write -- --election-id uuid --file payload.json [--dry-run]",
    "",
    "Payload must match the ballot-measure AI payload shape.",
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

async function readJsonFile(path: string): Promise<unknown> {
  const raw = await readFile(path, "utf8");
  return JSON.parse(raw) as unknown;
}

function requireEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required for manual ballot measure write`);
  }
  return value;
}

function readPositiveIntegerEnv(name: string, fallback: number): number {
  const raw = process.env[name]?.trim() || String(fallback);
  if (!/^[1-9]\d*$/.test(raw)) {
    throw new Error(`Invalid ${name}: ${raw}. Expected a positive integer.`);
  }
  const parsed = Number(raw);
  return parsed;
}

async function loadElection(pool: Pool, electionId: string): Promise<BallotMeasureElectionRow | null> {
  const result = await pool.query<BallotMeasureElectionRow>(
    `
      SELECT
        id::text AS id,
        district_id::text AS district_id,
        official_ballot_title,
        race_type
      FROM public.elections
      WHERE id::text = $1
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
  const dryRun = hasFlag("--dry-run");
  const manualKey = `manual:ballot-measure:${electionId}`;

  const databaseUrl = requireEnv("DATABASE_URL");
  requireLocalDatabaseTarget(databaseUrl);
  const pool = new Pool({ connectionString: databaseUrl });

  try {
    const [election, allowedAreas] = await Promise.all([
      loadElection(pool, electionId),
      loadAllowedBallotMeasureResearchAreas(pool),
    ]);
    if (!election) {
      throw new Error(`Election not found for election_id=${electionId}`);
    }
    if (election.race_type !== "ballot_measure") {
      throw new Error(`Ballot-measure write requires race_type=ballot_measure; election_id=${electionId} has race_type=${election.race_type}`);
    }

    const validated = await validateBallotMeasureAiPayload(
      rawPayload,
      readPositiveIntegerEnv("AI_TIMEOUT_MS", 90000),
      new Set(allowedAreas.map((area) => area.slug))
    );
    if (!validated.ok) {
      throw new Error(`Ballot-measure payload failed validation: ${validated.reason}`);
    }

    if (dryRun) {
      console.log(
        JSON.stringify(
          {
            dryRun: true,
            manualKey,
            electionId,
            officialBallotTitle: election.official_ballot_title,
            validation: {
              payloadShape: "valid",
              officialMeasureUrlReachable: true,
              sourceUrlsReachable: true,
              researchAreaTagsAllowed: true,
            },
            officialMeasureUrl: validated.officialMeasureUrl,
            officialMeasureUrlVerification: validated.officialMeasureUrlVerification,
            sourceCount: validated.sources.length,
            sources: validated.sources,
            researchAreaTagCount: validated.researchAreaTags.length,
            researchAreaTags: validated.researchAreaTags,
          },
          null,
          2
        )
      );
      return;
    }

    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const measureResult = await client.query<{ id: string }>(
        `
          INSERT INTO public.ballot_measures (
            district_id,
            election_id,
            official_ballot_title,
            summary,
            what_yes_means,
            what_no_means,
            result,
            source_url,
            official_measure_url,
            last_researched,
            research_area_tags_researched_at
          )
          VALUES ($1, $2, $3, $4, $5, $6, NULL, $7::jsonb, $8, now(), now())
          ON CONFLICT (election_id)
          DO UPDATE SET
            official_ballot_title = EXCLUDED.official_ballot_title,
            summary = EXCLUDED.summary,
            what_yes_means = EXCLUDED.what_yes_means,
            what_no_means = EXCLUDED.what_no_means,
            source_url = EXCLUDED.source_url,
            official_measure_url = EXCLUDED.official_measure_url,
            last_researched = now(),
            research_area_tags_researched_at = now(),
            updated_at = now()
          RETURNING id
        `,
        [
          election.district_id,
          election.id,
          election.official_ballot_title,
          validated.summary,
          validated.whatYesMeans,
          validated.whatNoMeans,
          JSON.stringify(validated.sources),
          validated.officialMeasureUrl,
        ]
      );
      const ballotMeasureId = measureResult.rows[0]?.id;
      if (!ballotMeasureId) {
        throw new Error(`Ballot measure upsert returned no id for election_id=${election.id}`);
      }
      const tagResult = await upsertBallotMeasureResearchAreaTags(
        client,
        ballotMeasureId,
        validated.researchAreaTags,
        new Map(allowedAreas.map((area) => [area.slug, area.id]))
      );
      await client.query("COMMIT");

      console.log(
        JSON.stringify(
          {
            manualKey,
            electionId,
            ballotMeasureId,
            tagsProcessed: tagResult.processed,
          },
          null,
          2
        )
      );
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error("manual ballot measure write failed:", message);
  process.exitCode = 1;
});
