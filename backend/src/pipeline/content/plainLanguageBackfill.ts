import type { Pool } from "pg";

import type {
  PlainLanguageAiConfig,
  PlainLanguageRewriteResult,
  PlainLanguageVerifyResult,
} from "../../ai/rewritePlainLanguage.js";
import type {
  PlainLanguageRewriteKind,
  PlainLanguageRewritePromptInput,
} from "../../ai/providers/plainLanguageRewritePrompt.js";
import type { PlainLanguageRewriteVerifyPromptInput } from "../../ai/providers/plainLanguageRewriteVerifyPrompt.js";

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
};

export type PlainLanguageBackfillDeps = {
  rewrite: (
    input: PlainLanguageRewritePromptInput,
    config: PlainLanguageAiConfig
  ) => Promise<PlainLanguageRewriteResult>;
  verify: (
    input: PlainLanguageRewriteVerifyPromptInput,
    config: PlainLanguageAiConfig
  ) => Promise<PlainLanguageVerifyResult>;
  aiConfig: PlainLanguageAiConfig;
  dryRun: boolean;
  limit?: number;
  log?: (line: string) => void;
};

export type PlainLanguageBackfillSummary = {
  processed: number;
  applied: number;
  flagged: number;
  remaining: number;
  dryRun: boolean;
};

// A high flag rate means the rewrite prompt needs tuning; grinding on would
// bury every row in the manual queue. Only enforced once the sample is big
// enough to mean something.
const FLAG_RATE_HALT_THRESHOLD = 0.05;
const FLAG_RATE_MINIMUM_SAMPLE = 40;

// The rewrite may only shrink candidate summaries by stripping contest/horse-
// race clauses, which can legitimately remove half the text. Other kinds must
// keep every claim, so a large shrink there means content was dropped.
const LENGTH_LOWER_BOUND: Record<PlainLanguageRewriteKind, number> = {
  candidate_summary: 0.25,
  measure_summary: 0.5,
  measure_what_yes_means: 0.5,
  measure_what_no_means: 0.5,
  record_description: 0.5,
};
const LENGTH_UPPER_BOUND = 1.7;

function extractUrls(text: string): Set<string> {
  return new Set((text.match(/https?:\/\/\S+/gi) ?? []).map((url) => url.replace(/[).,;]+$/, "")));
}

function extractNumberTokens(text: string): Set<string> {
  // Digit-bearing tokens, normalized: "11,250" and "11250" compare equal, a
  // trailing sentence period is not part of the number.
  return new Set(
    (text.match(/\d[\d,.]*/g) ?? []).map((token) => token.replace(/,/g, "").replace(/\.$/, ""))
  );
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
  if (ratio > LENGTH_UPPER_BOUND) {
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
  for (const token of extractNumberTokens(rewrittenText)) {
    if (!originalNumbers.has(token)) {
      return `rewrite introduced a number not in the original: ${token}`;
    }
  }

  return null;
}

// Apply is a single statement (UPDATE in a CTE + audit INSERT), so a crash
// can never update the column without its audit row — the audit table is the
// resume marker, and a missing row would re-rewrite already-rewritten text.
// Identifiers are compile-time constants keyed by the target enum, never
// interpolated from data.
function buildApplySql(targetTable: string, targetColumn: string): string {
  return `
    WITH updated AS (
      UPDATE public.${targetTable} SET ${targetColumn} = $4, updated_at = now() WHERE id = $2
    )
    INSERT INTO public.plain_language_rewrites
      (target_table, target_id, target_column, status, original_text, rewritten_text, flag_reason, provider, model)
    VALUES ($1, $2, $3, 'applied', $5, $4, NULL, $6, $7)
  `;
}

const INSERT_FLAGGED_SQL = `
  INSERT INTO public.plain_language_rewrites
    (target_table, target_id, target_column, status, original_text, rewritten_text, flag_reason, provider, model)
  VALUES ($1, $2, $3, 'flagged', $4, $5, $6, $7, $8)
`;

/**
 * Loads every row the backfill still has to process. The audit table is the
 * resume marker: any existing row (applied or flagged) excludes the target,
 * so flagged rows are never auto-retried.
 */
export async function loadPlainLanguageBackfillTargets(pool: Pool): Promise<PlainLanguageBackfillTarget[]> {
  const targets: PlainLanguageBackfillTarget[] = [];

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
        AND NOT EXISTS (
          SELECT 1 FROM public.plain_language_rewrites r
          WHERE r.target_table = 'candidates' AND r.target_id = c.id AND r.target_column = 'summary'
        )
      ORDER BY c.id
    `
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

  const recordRows = await pool.query<{ id: string; description: string }>(
    `
      SELECT cr.id, cr.description
      FROM public.candidate_records cr
      WHERE NOT EXISTS (
        SELECT 1 FROM public.plain_language_rewrites r
        WHERE r.target_table = 'candidate_records' AND r.target_id = cr.id AND r.target_column = 'description'
      )
      ORDER BY cr.id
    `
  );
  for (const row of recordRows.rows) {
    targets.push({
      targetTable: "candidate_records",
      targetId: row.id,
      targetColumn: "description",
      kind: "record_description",
      originalText: row.description,
    });
  }

  return targets;
}

export async function runPlainLanguageBackfill(
  pool: Pool,
  deps: PlainLanguageBackfillDeps
): Promise<PlainLanguageBackfillSummary> {
  const log = deps.log ?? console.log;
  const allTargets = await loadPlainLanguageBackfillTargets(pool);
  const targets = deps.limit !== undefined ? allTargets.slice(0, deps.limit) : allTargets;

  let processed = 0;
  let applied = 0;
  let flagged = 0;

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
    if (flagReason === null) {
      const verifyResult = await deps.verify(
        { kind: target.kind, originalText: target.originalText, rewrittenText: rewriteResult.rewrittenText },
        deps.aiConfig
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

    processed += 1;
    const label = `${target.targetTable}/${target.targetId}/${target.targetColumn}`;

    if (deps.dryRun) {
      log(`--- ${label} (${flagReason === null ? "would apply" : `would flag: ${flagReason}`})`);
      log(`  before: ${target.originalText}`);
      log(`  after:  ${rewriteResult.rewrittenText}`);
    } else if (flagReason === null) {
      await pool.query(buildApplySql(target.targetTable, target.targetColumn), [
        target.targetTable,
        target.targetId,
        target.targetColumn,
        rewriteResult.rewrittenText,
        target.originalText,
        rewriteResult.provider,
        rewriteResult.model,
      ]);
      log(`applied ${label} rewriter=${rewriteResult.provider}/${rewriteResult.model}${verifierMeta}`);
    } else {
      await pool.query(INSERT_FLAGGED_SQL, [
        target.targetTable,
        target.targetId,
        target.targetColumn,
        target.originalText,
        rewriteResult.rewrittenText,
        flagReason,
        rewriteResult.provider,
        rewriteResult.model,
      ]);
      log(`FLAGGED ${label}: ${flagReason}`);
    }

    if (flagReason === null) {
      applied += 1;
    } else {
      flagged += 1;
    }

    if (processed >= FLAG_RATE_MINIMUM_SAMPLE && flagged / processed > FLAG_RATE_HALT_THRESHOLD) {
      throw new Error(
        `halting: flag rate ${flagged}/${processed} exceeds ${FLAG_RATE_HALT_THRESHOLD * 100}% — tune the rewrite prompt before re-running`
      );
    }
  }

  return {
    processed,
    applied,
    flagged,
    remaining: allTargets.length - targets.length,
    dryRun: deps.dryRun,
  };
}
