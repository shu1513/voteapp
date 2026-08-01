import type { Pool } from "pg";

import type {
  PlainLanguageAiConfig,
  PlainLanguageRewriteResult,
  PlainLanguageVerifyResult,
} from "../../ai/rewritePlainLanguage.js";
import type { AiProvider } from "../../ai/types.js";
import type {
  PlainLanguageRewriteKind,
  PlainLanguageRewritePromptInput,
} from "../../ai/providers/plainLanguageRewritePrompt.js";
import type { PlainLanguageRewriteVerifyPromptInput } from "../../ai/providers/plainLanguageRewriteVerifyPrompt.js";
import { buildCandidateRecordIdentityKey } from "../candidates/candidateRecordStore.js";

export type PlainLanguageBackfillTarget = {
  targetTable: "candidates" | "ballot_measures" | "candidate_records";
  targetId: string;
  targetColumn: "summary" | "what_yes_means" | "what_no_means" | "description";
  kind: PlainLanguageRewriteKind;
  originalText: string;
  contestContext?: {
    officialBallotTitle: string;
    districtName: string;
    electionDate: string;
  };
  /** candidate_records only: inputs for recomputing record_identity_key. */
  recordIdentity?: {
    sourceUrl: string;
    eventDate: string;
  };
};

export type PlainLanguageBackfillDeps = {
  rewrite: (
    input: PlainLanguageRewritePromptInput,
    config: PlainLanguageAiConfig
  ) => Promise<PlainLanguageRewriteResult>;
  /**
   * rewriterProvider is the provider that produced the rewrite; the verify
   * implementation must exclude it so the judge is never the writer's own
   * model family (fail closed when no other provider is configured).
   */
  verify: (
    input: PlainLanguageRewriteVerifyPromptInput,
    config: PlainLanguageAiConfig,
    rewriterProvider: AiProvider
  ) => Promise<PlainLanguageVerifyResult>;
  aiConfig: PlainLanguageAiConfig;
  dryRun: boolean;
  limit?: number;
  filter?: PlainLanguageBackfillFilter;
  /**
   * Set only when `rewrite` returns operator-authored text rather than a model
   * call. Skips the independent-verifier step, which has no meaning when a
   * human wrote the replacement. Never set this for a model-driven run: the
   * verifier is what catches a dropped fact or a flipped stance.
   */
  manualAttestation?: boolean;
  log?: (line: string) => void;
};

export type PlainLanguageBackfillSummary = {
  processed: number;
  applied: number;
  flagged: number;
  /** Rows whose text changed under us mid-run: nothing written, resume retries. */
  staleSkipped: number;
  remaining: number;
  dryRun: boolean;
};

// A high flag rate means the rewrite prompt needs tuning; grinding on would
// bury every row in the manual queue. Only enforced once the sample is big
// enough to mean something.
const FLAG_RATE_HALT_THRESHOLD = 0.05;
const FLAG_RATE_MINIMUM_SAMPLE = 40;

// Candidate summaries have no ratio floor: the prompt tells the model to
// strip contest/horse-race clauses, and a contest-only summary legitimately
// shrinks to "Jane Doe is a lawyer." — the emptiness check plus the verifier
// own content loss there. Other kinds must keep every claim, so a large
// shrink means content was dropped.
const LENGTH_LOWER_BOUND: Record<PlainLanguageRewriteKind, number> = {
  candidate_summary: 0,
  measure_summary: 0.5,
  measure_what_yes_means: 0.5,
  measure_what_no_means: 0.5,
  record_description: 0.5,
};
// Ratio cap catches runaway embellishment; the flat allowance keeps short
// originals from failing just because the rewrite defines a term or two
// in-line ("subpoena — a court order to appear"), which adds absolute words.
const LENGTH_UPPER_BOUND = 1.7;
const LENGTH_UPPER_ALLOWANCE_CHARS = 120;

function extractUrls(text: string): Set<string> {
  return new Set((text.match(/https?:\/\/\S+/gi) ?? []).map((url) => url.replace(/[).,;]+$/, "")));
}

function extractNumberTokens(text: string): Set<string> {
  // Digit-bearing tokens, normalized: "11,250" and "11250" compare equal, and
  // trailing periods are not part of the number. Plural on purpose: source
  // texts truncated with a bare ellipsis ("Secs. 15-13-104,....") produced the
  // token "104..." under the single-period strip, so a clean rewrite's "104"
  // read as invented (live flag, batch 2 of the operator rewrite run).
  return new Set(
    (text.match(/\d[\d,.]*/g) ?? []).map((token) => token.replace(/,/g, "").replace(/\.+$/, ""))
  );
}

// An ISO date in the original ("2024-04-07") licenses its unpadded month and
// day: the natural-language rewrite "April 7, 2024" tokenizes to "7", which
// the padded original ("07") does not contain. Hit twice live; each workaround
// cost a day of date precision ("in April 2024").
function numberTokensLicensedByIsoDates(text: string): Set<string> {
  const licensed = new Set<string>();
  for (const match of text.matchAll(/\b(\d{4})-(\d{2})-(\d{2})\b/g)) {
    licensed.add(String(Number(match[2])));
    licensed.add(String(Number(match[3])));
  }
  return licensed;
}

// A plain-language rewrite legitimately turns number WORDS into digits
// ("over a century" -> "over 100 years", "twenty-five years" -> "25 years",
// "two million" -> "2 million" or "2,000,000"); the invented-number check
// must not flag those. Number phrases are composed before licensing so that
// a phrase only licenses ITS value: "two million" licenses 2 and 2000000 but
// never a bare 1000000, and "half a million" licenses 500000 but never 50 —
// a rewrite that changes the quantity still fails the check.
const NUMBER_UNIT_VALUES: Record<string, number> = {
  one: 1, two: 2, three: 3, four: 4, five: 5, six: 6, seven: 7, eight: 8, nine: 9,
  ten: 10, eleven: 11, twelve: 12, thirteen: 13, fourteen: 14, fifteen: 15,
  sixteen: 16, seventeen: 17, eighteen: 18, nineteen: 19,
  twenty: 20, thirty: 30, forty: 40, fifty: 50, sixty: 60, seventy: 70, eighty: 80, ninety: 90,
  dozen: 12,
};
const NUMBER_SCALE_VALUES: Record<string, number> = {
  hundred: 100, thousand: 1_000, million: 1_000_000, billion: 1_000_000_000, trillion: 1_000_000_000_000,
};
const NUMBER_STANDALONE_VALUES: Record<string, string[]> = {
  century: ["100"], centuries: ["100"], decade: ["10"], decades: ["10"],
  first: ["1", "1st"], second: ["2", "2nd"], third: ["3", "3rd"],
};

function numberTokensLicensedByWords(text: string): Set<string> {
  const licensed = new Set<string>();
  const words = text.toLowerCase().match(/[a-z]+/g) ?? [];

  for (let index = 0; index < words.length; index += 1) {
    const word = words[index];

    for (const value of NUMBER_STANDALONE_VALUES[word] ?? []) {
      licensed.add(value);
    }

    // "half (a/of) <scale>" licenses the computed value; a bare "half"
    // licenses only 0.5, never 50.
    if (word === "half") {
      let next = index + 1;
      while (words[next] === "a" || words[next] === "of" || words[next] === "an") {
        next += 1;
      }
      const scale = NUMBER_SCALE_VALUES[words[next] ?? ""];
      licensed.add("0.5");
      if (scale) {
        licensed.add(String(0.5 * scale));
        index = next;
      }
      continue;
    }

    // "a/an <scale>" reads as one of that scale: "a million" -> 1, 1000000.
    if ((word === "a" || word === "an") && NUMBER_SCALE_VALUES[words[index + 1] ?? ""]) {
      licensed.add("1");
      licensed.add(String(NUMBER_SCALE_VALUES[words[index + 1]]));
      index += 1;
      continue;
    }

    let value = NUMBER_UNIT_VALUES[word];
    if (value === undefined) {
      continue;
    }
    // Compose "twenty five" (also reached by hyphenated "twenty-five").
    const follower = NUMBER_UNIT_VALUES[words[index + 1] ?? ""];
    if (value >= 20 && value % 10 === 0 && follower !== undefined && follower < 10) {
      value += follower;
      index += 1;
    }
    // The count is always licensed ("two million" -> "2 million" keeps "2").
    licensed.add(String(value));
    // Multiply through scale words: "two hundred thousand" -> 200000. The
    // scale value itself is deliberately NOT licensed on its own.
    let scaled = value;
    let sawScale = false;
    while (NUMBER_SCALE_VALUES[words[index + 1] ?? ""] !== undefined) {
      scaled *= NUMBER_SCALE_VALUES[words[index + 1]];
      sawScale = true;
      index += 1;
    }
    if (sawScale) {
      licensed.add(String(scaled));
    }
  }

  return licensed;
}

/**
 * Cheap mechanical pre-filter (plan-content-wording.md Phase 2 layer 1).
 * Catches obvious breakage before spending a verification call; everything
 * subtle (flipped stance, lost negation, changed name) is the verifier's job.
 * Returns null when the rewrite passes, otherwise the flag reason.
 */
export function mechanicalCheckFailure(
  kind: PlainLanguageRewriteKind,
  originalText: string,
  rewrittenText: string
): string | null {
  if (rewrittenText.trim().length === 0) {
    return "rewrite is empty";
  }

  const ratio = rewrittenText.length / originalText.length;
  if (ratio < LENGTH_LOWER_BOUND[kind]) {
    return `rewrite too short (${Math.round(ratio * 100)}% of original)`;
  }
  if (rewrittenText.length > originalText.length * LENGTH_UPPER_BOUND + LENGTH_UPPER_ALLOWANCE_CHARS) {
    return `rewrite too long (${Math.round(ratio * 100)}% of original)`;
  }

  const originalUrls = extractUrls(originalText);
  for (const url of extractUrls(rewrittenText)) {
    if (!originalUrls.has(url)) {
      return `rewrite introduced a URL not in the original: ${url}`;
    }
  }

  // Candidate summaries may legitimately DROP numbers (vote percentages go
  // with the horse-race clauses), so only invented numbers are checked; the
  // verifier owns dropped-content judgment for every kind.
  const originalNumbers = extractNumberTokens(originalText);
  const licensedByWords = numberTokensLicensedByWords(originalText);
  const licensedByDates = numberTokensLicensedByIsoDates(originalText);
  for (const token of extractNumberTokens(rewrittenText)) {
    if (!originalNumbers.has(token) && !licensedByWords.has(token) && !licensedByDates.has(token)) {
      return `rewrite introduced a number not in the original: ${token}`;
    }
  }

  return null;
}

// Apply is a single statement (UPDATE in a CTE + audit INSERT driven by
// RETURNING), so a crash can never update the column without its audit row —
// the audit table is the resume marker, and a missing row would re-rewrite
// already-rewritten text. The UPDATE also requires the column to still equal
// the text the rewrite was based on: the run takes hours and research workers
// refresh rows concurrently, so a stale apply would silently overwrite newer
// research. Zero rows back means stale — nothing written, resume retries with
// fresh text. Identifiers are compile-time constants keyed by the target
// enum, never interpolated from data.
function buildApplySql(targetTable: string, targetColumn: string): string {
  const identityKeySet =
    targetTable === "candidate_records" ? ", record_identity_key = $8" : "";
  return `
    WITH updated AS (
      UPDATE public.${targetTable}
      SET ${targetColumn} = $4, updated_at = now()${identityKeySet}
      WHERE id = $2 AND ${targetColumn} IS NOT DISTINCT FROM $5
      RETURNING id
    )
    INSERT INTO public.plain_language_rewrites
      (target_table, target_id, target_column, status, original_text, rewritten_text, flag_reason, provider, model)
    SELECT $1, $2, $3, 'applied', $5, $4, NULL, $6, $7 FROM updated
  `;
}

// Flag inserts carry the same staleness guard: a flag permanently blocks
// auto-retry, which is only right if it judged the text the row still holds.
function buildFlaggedSql(targetTable: string, targetColumn: string): string {
  return `
    INSERT INTO public.plain_language_rewrites
      (target_table, target_id, target_column, status, original_text, rewritten_text, flag_reason, provider, model)
    SELECT $1, $2, $3, 'flagged', $4, $5, $6, $7, $8
    WHERE EXISTS (
      SELECT 1 FROM public.${targetTable} WHERE id = $2 AND ${targetColumn} IS NOT DISTINCT FROM $4
    )
  `;
}

/**
 * Narrows what the backfill picks up. Without it the target list is always
 * candidates -> ballot measures -> candidate records in that order, and
 * `--limit` slices from the front, so a records-only run had to process every
 * candidate summary first. `candidateIds` also drops ballot measures, which
 * belong to no candidate.
 */
export type PlainLanguageBackfillFilter = {
  onlyTable?: PlainLanguageBackfillTarget["targetTable"];
  candidateIds?: readonly string[];
  /**
   * Restrict candidate records to these row ids. An operator-authored run
   * derives this from its rewrites file, so targets the file does not cover
   * are never attempted — otherwise the first uncovered row aborts the batch.
   */
  recordIds?: readonly string[];
};

/**
 * Loads every row the backfill still has to process. The audit table is the
 * resume marker: any existing row (applied or flagged) excludes the target,
 * so flagged rows are never auto-retried.
 */
export async function loadPlainLanguageBackfillTargets(
  pool: Pool,
  filter: PlainLanguageBackfillFilter = {}
): Promise<PlainLanguageBackfillTarget[]> {
  const targets: PlainLanguageBackfillTarget[] = [];
  // Passed as a nullable array so one query text serves both the filtered and
  // unfiltered call; an empty list is a real filter that matches nothing.
  const candidateIds = filter.candidateIds ? [...filter.candidateIds] : null;
  const recordIds = filter.recordIds ? [...filter.recordIds] : null;
  const wants = (table: PlainLanguageBackfillTarget["targetTable"]): boolean =>
    filter.onlyTable === undefined || filter.onlyTable === table;

  if (wants("candidates")) {
    const candidateRows = await pool.query<{
      id: string;
      summary: string;
      official_ballot_title: string | null;
      district_name: string | null;
      election_date: string | null;
    }>(
      `
        SELECT c.id, c.summary, e.official_ballot_title, d.name AS district_name, e.election_date::text
        FROM public.candidates c
        LEFT JOIN LATERAL (
          SELECT e.official_ballot_title, e.election_date, e.district_id
          FROM public.candidate_elections ce
          JOIN public.elections e ON e.id = ce.election_id
          WHERE ce.candidate_id = c.id
          ORDER BY e.election_date DESC
          LIMIT 1
        ) e ON true
        LEFT JOIN public.districts d ON d.id = e.district_id
        WHERE c.summary IS NOT NULL AND c.summary <> '' AND c.deleted_at IS NULL
          AND ($1::uuid[] IS NULL OR c.id = ANY($1))
          AND NOT EXISTS (
            SELECT 1 FROM public.plain_language_rewrites r
            WHERE r.target_table = 'candidates' AND r.target_id = c.id AND r.target_column = 'summary'
          )
        ORDER BY c.id
      `,
      [candidateIds]
    );
    for (const row of candidateRows.rows) {
      targets.push({
        targetTable: "candidates",
        targetId: row.id,
        targetColumn: "summary",
        kind: "candidate_summary",
        originalText: row.summary,
        ...(row.official_ballot_title && row.district_name && row.election_date
          ? {
              contestContext: {
                officialBallotTitle: row.official_ballot_title,
                districtName: row.district_name,
                electionDate: row.election_date,
              },
            }
          : {}),
      });
    }
  }

  // Ballot measures belong to no candidate, so a candidate-scoped run skips them.
  if (wants("ballot_measures") && candidateIds === null) {
    const measureRows = await pool.query<{
      id: string;
      summary: string | null;
      what_yes_means: string;
      what_no_means: string;
    }>(
      `
        SELECT id, summary, what_yes_means, what_no_means
        FROM public.ballot_measures
        ORDER BY id
      `
    );
    const measureColumns = [
      { column: "summary" as const, kind: "measure_summary" as const },
      { column: "what_yes_means" as const, kind: "measure_what_yes_means" as const },
      { column: "what_no_means" as const, kind: "measure_what_no_means" as const },
    ];
    const processedMeasureTargets = await pool.query<{ target_id: string; target_column: string }>(
      `SELECT target_id, target_column FROM public.plain_language_rewrites WHERE target_table = 'ballot_measures'`
    );
    const processedMeasureKeys = new Set(
      processedMeasureTargets.rows.map((row) => `${row.target_id}:${row.target_column}`)
    );
    for (const row of measureRows.rows) {
      for (const { column, kind } of measureColumns) {
        const text = row[column];
        if (!text || text.trim().length === 0 || processedMeasureKeys.has(`${row.id}:${column}`)) {
          continue;
        }
        targets.push({ targetTable: "ballot_measures", targetId: row.id, targetColumn: column, kind, originalText: text });
      }
    }
  }

  if (wants("candidate_records")) {
    const recordRows = await pool.query<{
      id: string;
      description: string;
      source_url: string;
      event_date: string;
    }>(
      `
        SELECT cr.id, cr.description, cr.source_url, cr.event_date::text
        FROM public.candidate_records cr
        WHERE cr.description <> '' AND cr.retired_at IS NULL
          AND ($1::uuid[] IS NULL OR cr.candidate_id = ANY($1))
          AND ($2::uuid[] IS NULL OR cr.id = ANY($2))
          AND NOT EXISTS (
          SELECT 1 FROM public.plain_language_rewrites r
          WHERE r.target_table = 'candidate_records' AND r.target_id = cr.id AND r.target_column = 'description'
        )
        ORDER BY cr.id
      `,
      [candidateIds, recordIds]
    );
    for (const row of recordRows.rows) {
      targets.push({
        targetTable: "candidate_records",
        targetId: row.id,
        targetColumn: "description",
        kind: "record_description",
        originalText: row.description,
        recordIdentity: { sourceUrl: row.source_url, eventDate: row.event_date },
      });
    }
  }

  return targets;
}

export async function runPlainLanguageBackfill(
  pool: Pool,
  deps: PlainLanguageBackfillDeps
): Promise<PlainLanguageBackfillSummary> {
  const log = deps.log ?? console.log;
  const allTargets = await loadPlainLanguageBackfillTargets(pool, deps.filter ?? {});
  const targets = deps.limit !== undefined ? allTargets.slice(0, deps.limit) : allTargets;

  let processed = 0;
  let applied = 0;
  let flagged = 0;
  let staleSkipped = 0;

  // The halt gate judges the WHOLE backfill, not one invocation: small
  // --limit batches and resumes would otherwise reset the counters and let a
  // bad prompt grind every row into the manual queue. Dry runs write no audit
  // rows and judge only themselves.
  let gateProcessed = 0;
  let gateFlagged = 0;
  if (!deps.dryRun) {
    const auditCounts = await pool.query<{ status: string; count: string }>(
      `SELECT status, count(*)::text AS count FROM public.plain_language_rewrites GROUP BY status`
    );
    for (const row of auditCounts.rows) {
      gateProcessed += Number(row.count);
      if (row.status === "flagged") {
        gateFlagged += Number(row.count);
      }
    }
  }

  for (const target of targets) {
    const rewriteResult = await deps.rewrite(
      {
        kind: target.kind,
        text: target.originalText,
        ...(target.contestContext ? { contestContext: target.contestContext } : {}),
      },
      deps.aiConfig
    );
    if (!rewriteResult.ok) {
      // Provider infrastructure failure, not a bad rewrite: abort without an
      // audit row so the resume run retries this target.
      throw new Error(
        `rewrite call failed for ${target.targetTable}/${target.targetId}/${target.targetColumn}: ${rewriteResult.reason}`
      );
    }

    let flagReason = mechanicalCheckFailure(target.kind, target.originalText, rewriteResult.rewrittenText);
    let verifierMeta = "";
    // Operator-authored rewrites skip the model verifier because there is no
    // second model to be independent OF — the human who wrote the replacement
    // is the attestation, and the audit row records provider/model as manual
    // so a later reader can tell these apart from machine rewrites. Every
    // mechanical check above still applies.
    if (flagReason === null) {
      if (deps.manualAttestation === true) {
        verifierMeta = " verifier=operator-attested";
      } else if (rewriteResult.provider === "manual") {
        // Operator text reached a run that never declared manualAttestation:
        // refuse rather than silently skip the verifier or pretend a model
        // wrote it. Mismatched wiring, not a bad rewrite, so no audit row.
        throw new Error(
          `manual rewrite supplied for ${target.targetTable}/${target.targetId}/${target.targetColumn} without manualAttestation`
        );
      } else {
        const verifyResult = await deps.verify(
          { kind: target.kind, originalText: target.originalText, rewrittenText: rewriteResult.rewrittenText },
          deps.aiConfig,
          rewriteResult.provider
        );
        if (!verifyResult.ok) {
          throw new Error(
            `verify call failed for ${target.targetTable}/${target.targetId}/${target.targetColumn}: ${verifyResult.reason}`
          );
        }
        verifierMeta = ` verifier=${verifyResult.provider}/${verifyResult.model}`;
        if (verifyResult.verdict === "mismatch") {
          flagReason = `verifier mismatch: ${verifyResult.reason}`;
        }
      }
    }

    processed += 1;
    const label = `${target.targetTable}/${target.targetId}/${target.targetColumn}`;

    let outcome: "applied" | "flagged" | "stale";
    if (deps.dryRun) {
      log(`--- ${label} (${flagReason === null ? "would apply" : `would flag: ${flagReason}`})`);
      log(`  before: ${target.originalText}`);
      log(`  after:  ${rewriteResult.rewrittenText}`);
      outcome = flagReason === null ? "applied" : "flagged";
    } else if (flagReason === null) {
      const params: unknown[] = [
        target.targetTable,
        target.targetId,
        target.targetColumn,
        rewriteResult.rewrittenText,
        target.originalText,
        rewriteResult.provider,
        rewriteResult.model,
      ];
      if (target.targetTable === "candidate_records") {
        if (!target.recordIdentity) {
          throw new Error(`missing record identity inputs for ${label}`);
        }
        // The identity key hashes (url, date, description); the research
        // refresh dedupe relies on the stored key matching the stored text,
        // and every other description-changing path recomputes it.
        params.push(
          buildCandidateRecordIdentityKey({
            description: rewriteResult.rewrittenText,
            sourceUrl: target.recordIdentity.sourceUrl,
            eventDate: target.recordIdentity.eventDate,
          })
        );
      }
      const result = await pool.query(buildApplySql(target.targetTable, target.targetColumn), params);
      if ((result.rowCount ?? 0) > 0) {
        log(`applied ${label} rewriter=${rewriteResult.provider}/${rewriteResult.model}${verifierMeta}`);
        outcome = "applied";
      } else {
        log(`stale ${label}: text changed mid-run, nothing written; resume retries`);
        outcome = "stale";
      }
    } else {
      const result = await pool.query(buildFlaggedSql(target.targetTable, target.targetColumn), [
        target.targetTable,
        target.targetId,
        target.targetColumn,
        target.originalText,
        rewriteResult.rewrittenText,
        flagReason,
        rewriteResult.provider,
        rewriteResult.model,
      ]);
      if ((result.rowCount ?? 0) > 0) {
        log(`FLAGGED ${label}: ${flagReason}`);
        outcome = "flagged";
      } else {
        log(`stale ${label}: text changed mid-run, flag not recorded; resume retries`);
        outcome = "stale";
      }
    }

    if (outcome === "applied") {
      applied += 1;
    } else if (outcome === "flagged") {
      flagged += 1;
    } else {
      staleSkipped += 1;
    }
    if (outcome !== "stale") {
      gateProcessed += 1;
      if (outcome === "flagged") {
        gateFlagged += 1;
      }
    }

    if (gateProcessed >= FLAG_RATE_MINIMUM_SAMPLE && gateFlagged / gateProcessed > FLAG_RATE_HALT_THRESHOLD) {
      throw new Error(
        `halting: flag rate ${gateFlagged}/${gateProcessed} across the backfill exceeds ${FLAG_RATE_HALT_THRESHOLD * 100}% — tune the rewrite prompt before re-running`
      );
    }
  }

  return {
    processed,
    applied,
    flagged,
    staleSkipped,
    remaining: allTargets.length - targets.length,
    dryRun: deps.dryRun,
  };
}
