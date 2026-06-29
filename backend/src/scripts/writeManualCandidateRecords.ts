import { readFile } from "node:fs/promises";
import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { parseCandidateRecordAreaLabelPayload } from "../contracts/candidateRecordAreaLabelPayloadContract.js";
import { parseCandidateRecordDiscoveryPayload } from "../contracts/candidateRecordDiscoveryPayloadContract.js";
import {
  loadAllowedResearchAreasForOfficeId,
  upsertCandidateRecordAreaTags,
  validateCandidateRecordAreaLabels,
  type CandidateRecordAreaLabelInput,
} from "../pipeline/candidates/candidateRecordAreaTagging.js";
import { loadCandidateElectionOfficeContext } from "../pipeline/candidates/candidateRecordOfficeContext.js";
import {
  buildCandidateRecordIdentityKey,
  upsertCandidateRecords,
} from "../pipeline/candidates/candidateRecordStore.js";
import { createCandidateRecordUpdateNotificationEvents } from "../pipeline/users/candidateFollowNotificationEvents.js";

function usage(): string {
  return [
    "Usage:",
    "  npm run manual:candidate-records:write -- --candidate-id uuid --election-id uuid --records-file records.json --labels-file labels.json [--dry-run]",
    "",
    "records.json must match CandidateRecordDiscoveryPayload. labels.json must match CandidateRecordAreaLabelPayload.",
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

async function main(): Promise<void> {
  loadProjectEnv();

  const candidateId = readFlag("--candidate-id");
  const electionId = readFlag("--election-id");
  const recordsFile = readFlag("--records-file");
  const labelsFile = readFlag("--labels-file");
  if (!candidateId || !electionId || !recordsFile || !labelsFile) {
    throw new Error(`Missing required flag.\n${usage()}`);
  }

  const rawRecords = await readJsonFile(recordsFile);
  const parsedRecords = parseCandidateRecordDiscoveryPayload(rawRecords);
  if (!parsedRecords.ok) {
    throw new Error(`Candidate records payload failed validation: ${parsedRecords.reason}`);
  }

  const dryRun = hasFlag("--dry-run");
  const manualKey = `manual:candidate-records:${electionId}:${candidateId}`;

  const pool = new Pool({
    connectionString: process.env.DATABASE_URL ?? "postgresql://localhost:5432/voteapp",
  });

  try {
    const context = await loadCandidateElectionOfficeContext(pool, candidateId, electionId);
    if (!context) {
      throw new Error(`Candidate/election link not found for candidate_id=${candidateId} election_id=${electionId}`);
    }
    if (!context.officeId) {
      throw new Error(`Election has no office_id for candidate-record labeling; election_id=${electionId}`);
    }

    const allowedAreas = await loadAllowedResearchAreasForOfficeId(pool, context.officeId);
    if (allowedAreas.length === 0) {
      throw new Error(`No allowed research areas for office_id=${context.officeId}`);
    }
    const allowedSlugs = new Set(allowedAreas.map((area) => area.slug));
    const rawLabels = await readJsonFile(labelsFile);
    const parsedLabels = parseCandidateRecordAreaLabelPayload(rawLabels, {
      allowedResearchAreaSlugs: allowedSlugs,
      recordCount: parsedRecords.payload.records.length,
      requireLabelForEveryRecord: true,
    });
    if (!parsedLabels.ok) {
      throw new Error(`Candidate record labels payload failed validation: ${parsedLabels.reason}`);
    }

    if (dryRun) {
      console.log(
        JSON.stringify(
          {
            dryRun: true,
            manualKey,
            candidateId,
            electionId,
            candidateDisplayName: context.candidateDisplayName,
            recordCount: parsedRecords.payload.records.length,
            labelCount: parsedLabels.payload.labels.length,
            allowedResearchAreaSlugs: [...allowedSlugs].sort(),
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
      const upsert = await upsertCandidateRecords(
        client,
        parsedRecords.payload.records.map((record) => ({
          candidateId,
          description: record.description,
          sourceUrl: record.source_url,
          eventDate: record.event_date,
        }))
      );

      const labelsForValidation: CandidateRecordAreaLabelInput[] = parsedLabels.payload.labels.map((label) => {
        const record = parsedRecords.payload.records[label.record_index];
        if (!record) {
          throw new Error(`record_index out of range in labels: ${label.record_index}`);
        }
        const identityKey = buildCandidateRecordIdentityKey({
          description: record.description,
          sourceUrl: record.source_url,
          eventDate: record.event_date,
        });
        const candidateRecordId = upsert.recordIdsByIdentityKey.get(identityKey);
        if (!candidateRecordId) {
          throw new Error(`Missing persisted candidate_record id for record_index=${label.record_index}`);
        }
        return {
          candidateRecordId,
          researchAreaSlug: label.research_area_slug,
          stance: label.stance ?? null,
        };
      });

      const validation = validateCandidateRecordAreaLabels(labelsForValidation, allowedSlugs);
      if (!validation.ok) {
        const reason = validation.failures.map((failure) => failure.reason).join("; ");
        throw new Error(`Candidate record label validation failed: ${reason}`);
      }

      const tagResult = await upsertCandidateRecordAreaTags(
        client,
        validation.normalized,
        new Map(allowedAreas.map((area) => [area.slug, area.id]))
      );
      for (const insertedRecordId of upsert.insertedRecordIds) {
        await createCandidateRecordUpdateNotificationEvents(client, insertedRecordId);
      }
      await client.query("COMMIT");

      console.log(
        JSON.stringify(
          {
            manualKey,
            candidateId,
            electionId,
            inserted: upsert.inserted,
            updated: upsert.updated,
            processed: upsert.processed,
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
  console.error("manual candidate records write failed:", message);
  process.exitCode = 1;
});
