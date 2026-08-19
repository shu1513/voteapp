import { appendFile, readFile } from "node:fs/promises";
import { Pool } from "pg";

import { AI_CALLS_BLOCKED_REASON, isAiApiCallAllowed } from "../ai/aiCallGuard.js";
import { FRONTIER_AI_CANDIDATES, type AiCandidate } from "../ai/aiCandidates.js";
import {
  buildCandidateRecordAreasConfigFromEnv,
  enrichCandidateRecordAreas,
} from "../ai/enrichCandidateRecordAreas.js";
import { getPipelineEnv } from "../config/env.js";
import {
  loadAllResearchAreas,
  loadAllowedResearchAreasForOfficeId,
} from "../pipeline/candidates/candidateRecordAreaTagging.js";
import { loadCandidateElectionOfficeContext } from "../pipeline/candidates/candidateRecordOfficeContext.js";
import { isNonStanceResearchAreaSlug } from "../pipeline/candidates/candidateRecordResearchAreaPolicy.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";

// Additive stance backfill for existing candidate records.
//
// Records labeled before the stance rule was written down carry only their
// primary area (a school-funding vote is public_education_quality:for and
// nothing else), so a voter who cares about spending sees a blank. This
// re-runs the area labeler over live records and INSERTS labels the record
// is missing. It never changes or deletes an existing tag: an AI disagreement
// with a stored stance is reported as a conflict, not applied.
//
// Costs AI credits in BOTH modes — --dry-run skips only the INSERT.

const RECORDS_PER_AI_CALL = 20;

type CandidatePair = { candidateId: string; electionId: string };

type LiveRecord = { id: string; description: string; sourceUrl: string; eventDate: string };

type ProposedLabel = {
  record_id: string;
  research_area_slug: string;
  stance: "for" | "against";
  description: string;
};

type ConflictLabel = ProposedLabel & { existing_stance: "for" | "against" | null };

function usage(): string {
  return [
    "Usage:",
    "  AI_API_CALLS_ALLOWED=true npm run ai:candidate-records:relabel -- --election-date YYYY-MM-DD --out-file relabel.jsonl [--candidate-id uuid] [--limit N] [--provider claude|openai|gemini] [--concurrency N] [--dry-run]",
    "",
    "Appends one JSON line per candidate to --out-file (proposed labels, conflicts, inserted tag ids).",
    "Re-running with the same --out-file skips candidates already written there with status ok (ai_failed rows are retried), so a killed run resumes.",
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
  return null;
}

function hasFlag(name: string): boolean {
  return process.argv.includes(name);
}

function selectAiCandidates(provider: string | null): readonly AiCandidate[] {
  if (!provider) {
    return FRONTIER_AI_CANDIDATES;
  }
  const selected = FRONTIER_AI_CANDIDATES.filter((candidate) => candidate.provider === provider);
  if (selected.length === 0) {
    throw new Error(`Unknown --provider '${provider}'; expected one of ${FRONTIER_AI_CANDIDATES.map((c) => c.provider).join("|")}`);
  }
  return selected;
}

async function listCandidatePairs(
  pool: Pool,
  input: { electionDate: string; candidateId: string | null }
): Promise<CandidatePair[]> {
  // One office context per candidate: the allowlist is per office, and the
  // two Nov-2026 candidates running for two offices at once get the first
  // election by id (deterministic, logged by the caller as a known limit).
  const result = await pool.query<{ candidate_id: string; election_id: string }>(
    `
      SELECT ce.candidate_id::text AS candidate_id, MIN(ce.election_id::text) AS election_id
      FROM public.candidate_elections ce
      JOIN public.elections e ON e.id = ce.election_id
      JOIN public.candidates c ON c.id = ce.candidate_id
      WHERE e.election_date = $1::date
        AND e.race_type = 'office'
        AND c.deleted_at IS NULL
        AND COALESCE(ce.status, '') <> 'withdrawn'
        AND EXISTS (
          SELECT 1 FROM public.candidate_records r
          WHERE r.candidate_id = ce.candidate_id AND r.retired_at IS NULL
        )
        AND ($2::uuid IS NULL OR ce.candidate_id = $2::uuid)
      GROUP BY ce.candidate_id
      ORDER BY ce.candidate_id
    `,
    [input.electionDate, input.candidateId]
  );
  return result.rows.map((row) => ({ candidateId: row.candidate_id, electionId: row.election_id }));
}

async function loadLiveRecords(pool: Pool, candidateId: string): Promise<LiveRecord[]> {
  const result = await pool.query<{ id: string; description: string; source_url: string; event_date: string }>(
    `
      SELECT id::text, description, source_url, event_date::text
      FROM public.candidate_records
      WHERE candidate_id = $1::uuid AND retired_at IS NULL
      ORDER BY event_date ASC, id ASC
    `,
    [candidateId]
  );
  return result.rows.map((row) => ({
    id: row.id,
    description: row.description,
    sourceUrl: row.source_url,
    eventDate: row.event_date,
  }));
}

async function loadExistingTags(
  pool: Pool,
  candidateId: string
): Promise<Map<string, Map<string, "for" | "against" | null>>> {
  const result = await pool.query<{ record_id: string; slug: string; stance: "for" | "against" | null }>(
    `
      SELECT t.candidate_record_id::text AS record_id, ra.slug, t.stance
      FROM public.candidate_record_area_tags t
      JOIN public.candidate_records r ON r.id = t.candidate_record_id
      JOIN public.research_areas ra ON ra.id = t.research_area_id
      WHERE r.candidate_id = $1::uuid AND r.retired_at IS NULL
    `,
    [candidateId]
  );
  const byRecord = new Map<string, Map<string, "for" | "against" | null>>();
  for (const row of result.rows) {
    const tags = byRecord.get(row.record_id) ?? new Map<string, "for" | "against" | null>();
    tags.set(row.slug, row.stance);
    byRecord.set(row.record_id, tags);
  }
  return byRecord;
}

async function loadDoneCandidateIds(outFile: string): Promise<Set<string>> {
  let text: string;
  try {
    text = await readFile(outFile, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return new Set();
    }
    throw error;
  }
  const done = new Set<string>();
  for (const line of text.split("\n")) {
    if (!line.trim()) {
      continue;
    }
    const row = JSON.parse(line) as { candidate_id?: string; status?: string };
    if (row.candidate_id && row.status === "ok") {
      done.add(row.candidate_id);
    }
  }
  return done;
}

async function main(): Promise<void> {
  assertKnownCliFlags("ai:candidate-records:relabel", process.argv.slice(2), [
    { name: "--election-date", value: "space" },
    { name: "--out-file", value: "space" },
    { name: "--candidate-id", value: "space" },
    { name: "--limit", value: "space" },
    { name: "--provider", value: "space" },
    { name: "--concurrency", value: "space" },
    { name: "--dry-run", value: "none" },
  ]);

  const electionDate = readFlag("--election-date");
  const outFile = readFlag("--out-file");
  const candidateId = readFlag("--candidate-id");
  const rawLimit = readFlag("--limit");
  const provider = readFlag("--provider");
  const rawConcurrency = readFlag("--concurrency");
  const dryRun = hasFlag("--dry-run");
  if (!electionDate || !outFile) {
    throw new Error(`Missing required flag.\n${usage()}`);
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(electionDate)) {
    throw new Error(`--election-date must be YYYY-MM-DD, got '${electionDate}'`);
  }
  const limit = rawLimit === null ? null : Number.parseInt(rawLimit, 10);
  if (limit !== null && (!Number.isInteger(limit) || limit <= 0)) {
    throw new Error(`--limit must be a positive integer, got '${rawLimit}'`);
  }
  const concurrency = rawConcurrency === null ? 1 : Number.parseInt(rawConcurrency, 10);
  if (!Number.isInteger(concurrency) || concurrency <= 0 || concurrency > 16) {
    throw new Error(`--concurrency must be an integer 1-16, got '${rawConcurrency}'`);
  }
  if (!isAiApiCallAllowed()) {
    // Fail before touching the database: the labeler is an AI call in both
    // modes, and the provider client would only reject after the setup work.
    throw new Error(AI_CALLS_BLOCKED_REASON);
  }
  const aiCandidates = selectAiCandidates(provider);

  const env = getPipelineEnv();
  if (!dryRun) {
    requireLocalDatabaseTarget(env.DATABASE_URL);
  }
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const areasConfig = buildCandidateRecordAreasConfigFromEnv();

  const totals = {
    candidates: 0,
    records: 0,
    ai_calls: 0,
    ai_failures: 0,
    proposed: 0,
    inserted: 0,
    conflicts: 0,
    skipped_no_context: 0,
  };

  try {
    const doneIds = await loadDoneCandidateIds(outFile);
    const allPairs = await listCandidatePairs(pool, { electionDate, candidateId });
    // --limit counts NEW candidates, so "--limit 10" on a resumed file always
    // advances by ten instead of re-selecting the ten already done.
    const pairs = allPairs.filter((pair) => !doneIds.has(pair.candidateId)).slice(0, limit ?? undefined);
    console.log(
      `relabel: ${pairs.length} candidate(s) for election_date=${electionDate} (${allPairs.length - pairs.length} already done in out-file)${dryRun ? " (dry-run: no writes)" : ""} provider=${provider ?? "fallback-chain"} concurrency=${concurrency}`
    );

    const processCandidate = async (pair: CandidatePair): Promise<void> => {
      totals.candidates += 1;
      const context = await loadCandidateElectionOfficeContext(pool, pair.candidateId, pair.electionId);
      if (!context) {
        totals.skipped_no_context += 1;
        await appendFile(
          outFile,
          `${JSON.stringify({ candidate_id: pair.candidateId, election_id: pair.electionId, status: "skipped_no_context" })}\n`
        );
        return;
      }
      const allowedAreas = context.officeId
        ? await loadAllowedResearchAreasForOfficeId(pool, context.officeId)
        : await loadAllResearchAreas(pool);
      const researchAreaIdBySlug = new Map(allowedAreas.map((area) => [area.slug, area.id]));
      const allowedSlugs = [...new Set(allowedAreas.map((area) => area.slug))];
      const records = await loadLiveRecords(pool, pair.candidateId);
      const existingTags = await loadExistingTags(pool, pair.candidateId);
      totals.records += records.length;

      const proposed: ProposedLabel[] = [];
      const conflicts: ConflictLabel[] = [];
      let aiFailure: string | null = null;
      let providerUsed: string | null = null;
      let modelUsed: string | null = null;

      for (let offset = 0; offset < records.length; offset += RECORDS_PER_AI_CALL) {
        const batch = records.slice(offset, offset + RECORDS_PER_AI_CALL);
        totals.ai_calls += 1;
        const result = await enrichCandidateRecordAreas(
          {
            candidateDisplayName: context.candidateDisplayName,
            districtName: context.districtName,
            districtType: context.districtType,
            state: context.state,
            electionDate: context.electionDate,
            officialBallotTitle: context.officialBallotTitle,
            electionStage: context.electionStage,
            senateClass: context.senateClass,
            termEndYear: context.termEndYear,
            allowedResearchAreaSlugs: allowedSlugs,
            allowedResearchAreaGoals: allowedAreas.map((area) => ({
              slug: area.slug,
              description: area.description ?? null,
            })),
            records: batch.map((record) => ({
              description: record.description,
              sourceUrl: record.sourceUrl,
              eventDate: record.eventDate,
            })),
          },
          areasConfig,
          aiCandidates
        );
        if (!result.ok) {
          aiFailure = `${result.errorCode}: ${result.reason}`;
          totals.ai_failures += 1;
          console.warn(`relabel: AI labeling failed candidate_id=${pair.candidateId} batch_offset=${offset}: ${aiFailure}`);
          break;
        }
        providerUsed = result.provider;
        modelUsed = result.model;
        for (const label of result.labels) {
          // Backfill is about stance completeness: neutral areas add nothing
          // to a record that already carries a label, so they never insert.
          if (isNonStanceResearchAreaSlug(label.research_area_slug) || !label.stance) {
            continue;
          }
          const record = batch[label.record_index];
          if (!record) {
            continue;
          }
          const existing = existingTags.get(record.id);
          const entry: ProposedLabel = {
            record_id: record.id,
            research_area_slug: label.research_area_slug,
            stance: label.stance,
            description: record.description.slice(0, 200),
          };
          if (existing?.has(label.research_area_slug)) {
            const existingStance = existing.get(label.research_area_slug) ?? null;
            if (existingStance !== label.stance) {
              conflicts.push({ ...entry, existing_stance: existingStance });
            }
            continue;
          }
          proposed.push(entry);
        }
      }

      totals.proposed += proposed.length;
      totals.conflicts += conflicts.length;

      const insertedTagIds: string[] = [];
      if (!dryRun && proposed.length > 0) {
        const client = await pool.connect();
        try {
          await client.query("BEGIN");
          for (const label of proposed) {
            const researchAreaId = researchAreaIdBySlug.get(label.research_area_slug);
            if (!researchAreaId) {
              throw new Error(`missing research area id for slug '${label.research_area_slug}'`);
            }
            const inserted = await client.query<{ id: string }>(
              `
                INSERT INTO public.candidate_record_area_tags (candidate_record_id, research_area_id, stance)
                VALUES ($1::uuid, $2::uuid, $3)
                ON CONFLICT (candidate_record_id, research_area_id) DO NOTHING
                RETURNING id::text
              `,
              [label.record_id, researchAreaId, label.stance]
            );
            const id = inserted.rows[0]?.id;
            if (id) {
              insertedTagIds.push(id);
            }
          }
          await client.query("COMMIT");
        } catch (error) {
          await client.query("ROLLBACK");
          throw error;
        } finally {
          client.release();
        }
        totals.inserted += insertedTagIds.length;
      }

      await appendFile(
        outFile,
        `${JSON.stringify({
          candidate_id: pair.candidateId,
          election_id: pair.electionId,
          office_id: context.officeId,
          candidate_display_name: context.candidateDisplayName,
          status: aiFailure ? "ai_failed" : "ok",
          ai_failure: aiFailure,
          provider: providerUsed,
          model: modelUsed,
          dry_run: dryRun,
          record_count: records.length,
          proposed,
          conflicts,
          inserted_tag_ids: insertedTagIds,
        })}\n`
      );
      console.log(
        `relabel: candidate_id=${pair.candidateId} records=${records.length} proposed=${proposed.length} conflicts=${conflicts.length} inserted=${insertedTagIds.length}${aiFailure ? " AI_FAILED" : ""}`
      );
    };

    // Small worker pool: one AI call per candidate dominates wall-clock, and
    // the ~3.8k Nov-2026 candidates would take most of a day serially.
    // Each worker owns whole candidates, so JSONL lines never interleave.
    let nextIndex = 0;
    const workers = Array.from({ length: Math.min(concurrency, pairs.length) }, async () => {
      while (nextIndex < pairs.length) {
        const pair = pairs[nextIndex];
        nextIndex += 1;
        if (pair) {
          await processCandidate(pair);
        }
      }
    });
    await Promise.all(workers);
  } finally {
    await pool.end();
  }

  console.log(JSON.stringify({ type: "candidate_record_area_relabel_summary", dry_run: dryRun, ...totals }));
}

main().catch((error) => {
  console.error("candidate record area relabel failed:", error);
  process.exit(1);
});
