import { appendFile, readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { Pool } from "pg";

import { AI_CALLS_BLOCKED_REASON, isAiApiCallAllowed } from "../ai/aiCallGuard.js";
import { FRONTIER_AI_CANDIDATES, type AiCandidate } from "../ai/aiCandidates.js";
import {
  buildCandidateRecordAreasConfigFromEnv,
  enrichCandidateRecordAreas,
} from "../ai/enrichCandidateRecordAreas.js";
import { getPipelineEnv } from "../config/env.js";
import {
  type AllowedResearchArea,
  loadAllResearchAreas,
  loadAllowedResearchAreasForOfficeId,
} from "../pipeline/candidates/candidateRecordAreaTagging.js";
import {
  type CandidateElectionOfficeContext,
  loadCandidateElectionOfficeContext,
} from "../pipeline/candidates/candidateRecordOfficeContext.js";
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
// Two label sources: the AI labeler (default; costs AI credits in BOTH
// modes — --dry-run skips only the INSERT) or --labels-file, a reviewed
// payload for ONE --candidate-id written by a human or an agent that read
// the record sources. File mode makes no AI call and needs no AI flag.

const RECORDS_PER_AI_CALL = 20;

type CandidatePair = { candidateId: string; electionIds: string[] };

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
    "  npm run ai:candidate-records:relabel -- --election-date YYYY-MM-DD --candidate-id uuid --labels-file labels.json --out-file relabel.jsonl [--dry-run]",
    "",
    '--labels-file: {"labels": [{"record_id": "uuid", "research_area_slug": "slug", "stance": "for|against"}]} for the ONE --candidate-id; stanced areas only, validated against the office allowlist; no AI call. An empty labels array marks the candidate done.',
    "",
    "Appends one JSON line per candidate to --out-file (proposed labels, conflicts, inserted tag ids).",
    "Re-running with the same --out-file skips candidates already written there with status ok (ai_failed rows are retried), so a killed run resumes. Dry-run rows only count as done for another dry-run; a live run over a dry-run file re-labels and inserts.",
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
  // One row per candidate carrying EVERY same-date office election: tags
  // live on the record, not on an election, so a candidate running for two
  // offices at once is labeled against the union of both allowlists in one
  // AI pass instead of one office being silently dropped. Joint-ticket
  // running mates count too — the office-context loader matches either
  // column, so this selection has to as well.
  const result = await pool.query<{ candidate_id: string; election_ids: string[] }>(
    `
      WITH ticket AS (
        SELECT ce.election_id,
               ce.status,
               unnest(ARRAY[ce.candidate_id, ce.running_mate_candidate_id]) AS candidate_id
        FROM public.candidate_elections ce
      )
      SELECT t.candidate_id::text AS candidate_id,
             array_agg(DISTINCT t.election_id::text ORDER BY t.election_id::text) AS election_ids
      FROM ticket t
      JOIN public.elections e ON e.id = t.election_id
      JOIN public.candidates c ON c.id = t.candidate_id
      WHERE e.election_date = $1::date
        AND e.race_type = 'office'
        AND c.deleted_at IS NULL
        AND COALESCE(t.status, '') <> 'withdrawn'
        AND EXISTS (
          SELECT 1 FROM public.candidate_records r
          WHERE r.candidate_id = t.candidate_id AND r.retired_at IS NULL
        )
        AND ($2::uuid IS NULL OR t.candidate_id = $2::uuid)
      GROUP BY t.candidate_id
      ORDER BY t.candidate_id
    `,
    [input.electionDate, input.candidateId]
  );
  return result.rows.map((row) => ({ candidateId: row.candidate_id, electionIds: row.election_ids }));
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

export type FileLabel = { record_id: string; research_area_slug: string; stance: "for" | "against" };

// Validate a --labels-file payload against the candidate's live records and
// office allowlist. Every problem is collected before failing so one dry-run
// surfaces them all (same reason the contract parser does).
export function resolveFileLabels(
  payload: unknown,
  liveRecordIds: ReadonlySet<string>,
  allowedSlugs: ReadonlySet<string>
): { ok: true; labels: FileLabel[] } | { ok: false; reason: string } {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return { ok: false, reason: "labels-file payload must be an object" };
  }
  const rows = (payload as { labels?: unknown }).labels;
  if (!Array.isArray(rows)) {
    return { ok: false, reason: "labels-file payload.labels must be an array" };
  }
  const labels: FileLabel[] = [];
  const problems: string[] = [];
  const seen = new Set<string>();
  let sawAllowlistRejection = false;
  for (const [index, row] of rows.entries()) {
    if (typeof row !== "object" || row === null || Array.isArray(row)) {
      problems.push(`labels[${index}]: row must be an object`);
      continue;
    }
    const input = row as Record<string, unknown>;
    const recordId = typeof input.record_id === "string" ? input.record_id.trim() : "";
    const slug = typeof input.research_area_slug === "string" ? input.research_area_slug.trim().toLowerCase() : "";
    const stance = input.stance;
    if (!liveRecordIds.has(recordId)) {
      problems.push(`labels[${index}]: record_id '${recordId}' is not a live record of this candidate`);
      continue;
    }
    if (!allowedSlugs.has(slug)) {
      problems.push(`labels[${index}]: research_area_slug '${slug}' is not in the allowed research areas for this office`);
      sawAllowlistRejection = true;
      continue;
    }
    if (isNonStanceResearchAreaSlug(slug)) {
      problems.push(`labels[${index}]: '${slug}' is a non-stance area; the backfill adds stanced areas only`);
      continue;
    }
    if (stance !== "for" && stance !== "against") {
      problems.push(`labels[${index}]: stance must be 'for' or 'against', got ${JSON.stringify(stance)}`);
      continue;
    }
    const key = `${recordId}::${slug}`;
    if (seen.has(key)) {
      problems.push(`labels[${index}]: duplicate (record_id, research_area_slug) pair`);
      continue;
    }
    seen.add(key);
    labels.push({ record_id: recordId, research_area_slug: slug, stance });
  }
  if (problems.length > 0) {
    const hint = sawAllowlistRejection
      ? ` (allowed research areas for this office: ${[...allowedSlugs].sort().join(", ")})`
      : "";
    return { ok: false, reason: `labels-file contains invalid row: ${problems.join("; ")}${hint}` };
  }
  return { ok: true, labels };
}

// A candidate is "done" when a LIVE row exists for it, or — in a dry-run —
// when a dry-run row exists. A dry-run row must never make a later live run
// skip the candidate: the reviewed file is the natural --out-file for the
// real run, and reusing it would silently insert nothing.
export async function loadDoneCandidateIds(outFile: string, options: { dryRun: boolean }): Promise<Set<string>> {
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
  const lines = text.split("\n").filter((line) => line.trim().length > 0);
  for (const [index, line] of lines.entries()) {
    let row: { candidate_id?: string; status?: string; dry_run?: boolean };
    try {
      row = JSON.parse(line) as typeof row;
    } catch (error) {
      // Only the LAST line can be a half-written row from a killed run; it
      // is simply not done. A malformed line anywhere else is a corrupt
      // ledger and must stop the run.
      if (index === lines.length - 1) {
        console.warn(`relabel: ignoring truncated final line in ${outFile}`);
        continue;
      }
      throw error;
    }
    if (row.candidate_id && row.status === "ok" && (row.dry_run !== true || options.dryRun)) {
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
    { name: "--labels-file", value: "space" },
    { name: "--dry-run", value: "none" },
  ]);

  const electionDate = readFlag("--election-date");
  const outFile = readFlag("--out-file");
  const candidateId = readFlag("--candidate-id");
  const rawLimit = readFlag("--limit");
  const provider = readFlag("--provider");
  const rawConcurrency = readFlag("--concurrency");
  const labelsFile = readFlag("--labels-file");
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
  if (labelsFile && !candidateId) {
    throw new Error(`--labels-file requires --candidate-id (one reviewed payload per candidate).\n${usage()}`);
  }
  if (labelsFile && (provider || rawConcurrency)) {
    throw new Error("--provider and --concurrency have no effect with --labels-file; drop them.");
  }
  if (!labelsFile && !isAiApiCallAllowed()) {
    // Fail before touching the database: the labeler is an AI call in both
    // modes, and the provider client would only reject after the setup work.
    throw new Error(AI_CALLS_BLOCKED_REASON);
  }
  const aiCandidates = selectAiCandidates(provider);
  const filePayload: unknown = labelsFile ? JSON.parse(await readFile(labelsFile, "utf8")) : null;

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
    const doneIds = await loadDoneCandidateIds(outFile, { dryRun });
    const allPairs = await listCandidatePairs(pool, { electionDate, candidateId });
    // --limit counts NEW candidates, so "--limit 10" on a resumed file always
    // advances by ten instead of re-selecting the ten already done.
    const pendingPairs = allPairs.filter((pair) => !doneIds.has(pair.candidateId));
    const pairs = pendingPairs.slice(0, limit ?? undefined);
    console.log(
      `relabel: ${pairs.length} of ${pendingPairs.length} pending candidate(s) for election_date=${electionDate} (${allPairs.length - pendingPairs.length} already done in out-file)${dryRun ? " (dry-run: no writes)" : ""} source=${labelsFile ? "labels-file" : `ai:${provider ?? "fallback-chain"}`} concurrency=${concurrency}`
    );

    // One writer for the JSONL ledger: workers finish out of order, and a
    // serialized chain keeps every line whole regardless of filesystem
    // append semantics.
    let appendChain: Promise<void> = Promise.resolve();
    const appendLine = (row: Record<string, unknown>): Promise<void> => {
      appendChain = appendChain.then(() => appendFile(outFile, `${JSON.stringify(row)}\n`));
      return appendChain;
    };

    const processCandidate = async (pair: CandidatePair): Promise<void> => {
      totals.candidates += 1;
      const contexts: CandidateElectionOfficeContext[] = [];
      for (const electionId of pair.electionIds) {
        const loaded = await loadCandidateElectionOfficeContext(pool, pair.candidateId, electionId);
        if (loaded) {
          contexts.push(loaded);
        }
      }
      const context = contexts[0];
      if (!context) {
        totals.skipped_no_context += 1;
        await appendLine({ candidate_id: pair.candidateId, election_ids: pair.electionIds, status: "skipped_no_context" });
        return;
      }
      // Prompt context comes from the first election; the allowlist is the
      // union over every same-date office (deduped by slug).
      const allowedAreasBySlug = new Map<string, AllowedResearchArea>();
      for (const officeContext of contexts) {
        const areas = officeContext.officeId
          ? await loadAllowedResearchAreasForOfficeId(pool, officeContext.officeId)
          : await loadAllResearchAreas(pool);
        for (const area of areas) {
          if (!allowedAreasBySlug.has(area.slug)) {
            allowedAreasBySlug.set(area.slug, area);
          }
        }
      }
      const allowedAreas = [...allowedAreasBySlug.values()];
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

      // Additive merge, shared by both label sources: a missing (record,
      // area) pair is proposed; a present pair with a different stance is a
      // conflict for review; a matching pair is a no-op.
      const considerLabel = (record: LiveRecord, slug: string, stance: "for" | "against"): void => {
        const existing = existingTags.get(record.id);
        const entry: ProposedLabel = {
          record_id: record.id,
          research_area_slug: slug,
          stance,
          description: record.description.slice(0, 200),
        };
        if (existing?.has(slug)) {
          const existingStance = existing.get(slug) ?? null;
          if (existingStance !== stance) {
            conflicts.push({ ...entry, existing_stance: existingStance });
          }
          return;
        }
        proposed.push(entry);
      };

      if (labelsFile) {
        const resolved = resolveFileLabels(filePayload, new Set(records.map((record) => record.id)), new Set(allowedSlugs));
        if (!resolved.ok) {
          // Nothing is written and no ledger row is appended: fix the file
          // and rerun.
          throw new Error(resolved.reason);
        }
        const recordById = new Map(records.map((record) => [record.id, record]));
        for (const label of resolved.labels) {
          const record = recordById.get(label.record_id);
          if (record) {
            considerLabel(record, label.research_area_slug, label.stance);
          }
        }
        providerUsed = "labels-file";
      }

      for (let offset = 0; !labelsFile && offset < records.length; offset += RECORDS_PER_AI_CALL) {
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
          if (record) {
            considerLabel(record, label.research_area_slug, label.stance);
          }
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

      await appendLine({
        candidate_id: pair.candidateId,
        election_ids: pair.electionIds,
        office_ids: contexts.map((officeContext) => officeContext.officeId),
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
      });
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

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("candidate record area relabel failed:", error);
    process.exit(1);
  });
}
