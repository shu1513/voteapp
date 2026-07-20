import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import {
  FINANCE_INDUSTRY_SLUGS,
  normalizeFinanceLabel,
  type FinanceIndustrySlug,
  type FinanceLabelClassification,
} from "../pipeline/finance/financeLabelClassifier.js";
import {
  financeClassificationKey,
  upsertFinanceLabelClassification,
} from "../pipeline/finance/financeIndustryClassificationService.js";
import { readPositiveIntegerFlag } from "../utils/cliFlags.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";
import {
  buildManualResearchRepairReport,
  writeManualResearchRepairReport,
  type ManualResearchRepairGap,
} from "./manualResearchRepairReport.js";

// Manual (no AI provider) research workflow for employer/donor → industry
// links (finance_label_classifications). Finance syncs persist a
// classification_source = 'unknown' row for every employer/donor label the
// rule classifier could not place; those rows are the work queue. A manual
// row is permanent: the sync-time precedence never re-sends a label whose
// classification_source is not 'unknown' to AI, and the ballot-lookup read
// path joins this table live, so a new link takes effect without a resync.
//
// Subcommands:
//   due    List labels the rule classifier could not place — the work queue.
//   write  Validate a researched payload and upsert the links with
//          classification_source = 'manual'.

type Subcommand = "due" | "write";

function usage(): string {
  return [
    "Usage:",
    "  npm run manual:finance-industry-labels:due -- [--limit 500]",
    "  npm run manual:finance-industry-labels:write -- --file labels.json [--repair-report-file file] [--dry-run]",
    "",
    "The write payload shape:",
    '  { "labels": [ { "label_type": "employer" | "donor", "raw_label", "industry_slug": "<slug>" | null, "confidence": "high" | "medium" | "low" } ] }',
    "",
    "industry_slug must be one of the fixed slugs the due list prints, or",
    "null for labels that map to no industry (government bodies, individuals,",
    "generic descriptions). Copy raw_label and label_type from the due list.",
  ].join("\n");
}

function readFlag(argv: readonly string[], name: string): string | null {
  const index = argv.indexOf(name);
  if (index >= 0) {
    const value = argv[index + 1];
    if (!value || value.startsWith("--") || value.trim().length === 0) {
      throw new Error(`Missing value for ${name}.\n${usage()}`);
    }
    return value.trim();
  }
  const inlinePrefix = `${name}=`;
  const inline = argv.find((token) => token.startsWith(inlinePrefix));
  if (inline) {
    const value = inline.slice(inlinePrefix.length).trim();
    if (value.length === 0) {
      throw new Error(`Missing value for ${name}.\n${usage()}`);
    }
    return value;
  }
  return null;
}

function requireEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required for manual finance industry labels`);
  }
  return value;
}

async function runDue(pool: Pool, argv: readonly string[]): Promise<void> {
  const limit = readPositiveIntegerFlag(argv, "--limit", 500);
  // Only 'unknown' rows are due: rule/ai/manual rows with a null slug are a
  // deliberate "no industry" verdict, not unfinished work.
  const result = await pool.query<{ label_type: string; raw_label: string; normalized_label: string }>(
    `
      SELECT label_type, raw_label, normalized_label
      FROM public.finance_label_classifications
      WHERE classification_source = 'unknown'
      ORDER BY label_type, normalized_label
    `
  );
  console.log(
    JSON.stringify(
      {
        unclassified_label_count: result.rows.length,
        industry_slugs: FINANCE_INDUSTRY_SLUGS,
        labels: result.rows.slice(0, limit),
      },
      null,
      2
    )
  );
}

export type IndustryLabelPayloadRow = {
  label_type: "employer" | "donor";
  raw_label: string;
  normalized_label: string;
  industry_slug: FinanceIndustrySlug | null;
  confidence: "high" | "medium" | "low";
};

/**
 * Validates the write payload. Throws with every problem listed at once so a
 * multi-row payload is fixed in one pass, mirroring the other manual writers.
 */
export function parseIndustryLabelPayload(raw: unknown): IndustryLabelPayloadRow[] {
  if (typeof raw !== "object" || raw === null || !Array.isArray((raw as { labels?: unknown }).labels)) {
    throw new Error(`Payload must be an object with a "labels" array.\n${usage()}`);
  }
  const labels = (raw as { labels: unknown[] }).labels;
  if (labels.length === 0) {
    throw new Error("Payload has an empty labels array — nothing to write.");
  }
  const errors: string[] = [];
  const seen = new Set<string>();
  const rows: IndustryLabelPayloadRow[] = [];
  const knownSlugs = new Set<string>(FINANCE_INDUSTRY_SLUGS);
  labels.forEach((entry, index) => {
    const at = `labels[${index}]`;
    if (typeof entry !== "object" || entry === null) {
      errors.push(`${at}: must be an object`);
      return;
    }
    const row = entry as Record<string, unknown>;
    const labelType = row.label_type === "employer" || row.label_type === "donor" ? row.label_type : null;
    const rawLabel = typeof row.raw_label === "string" ? row.raw_label.trim() : "";
    const confidence =
      row.confidence === "high" || row.confidence === "medium" || row.confidence === "low"
        ? row.confidence
        : null;

    if (!labelType) {
      errors.push(`${at}: label_type must be "employer" or "donor"`);
    }
    if (rawLabel.length === 0) {
      errors.push(`${at}: raw_label is required`);
    }
    if (!confidence) {
      errors.push(`${at}: confidence must be "high", "medium", or "low"`);
    }
    // The slug key must be present even when null: an absent key is far more
    // likely a forgotten field than a researched "no industry" verdict.
    if (!("industry_slug" in row)) {
      errors.push(`${at}: industry_slug is required (use null for labels with no industry)`);
    } else if (row.industry_slug !== null && (typeof row.industry_slug !== "string" || !knownSlugs.has(row.industry_slug))) {
      errors.push(
        `${at}: industry_slug must be null or one of: ${FINANCE_INDUSTRY_SLUGS.join(", ")}`
      );
    }
    if (!labelType || rawLabel.length === 0 || !confidence || errors.some((error) => error.startsWith(at))) {
      return;
    }

    const normalizedLabel = normalizeFinanceLabel(rawLabel, labelType);
    if (normalizedLabel.length === 0) {
      errors.push(`${at}: raw_label normalizes to an empty label`);
      return;
    }
    const key = financeClassificationKey(labelType, normalizedLabel);
    if (seen.has(key)) {
      errors.push(`${at}: duplicate (label_type, normalized label) in payload: ${labelType} ${normalizedLabel}`);
      return;
    }
    seen.add(key);
    rows.push({
      label_type: labelType,
      raw_label: rawLabel,
      normalized_label: normalizedLabel,
      industry_slug: (row.industry_slug ?? null) as FinanceIndustrySlug | null,
      confidence,
    });
  });
  if (errors.length > 0) {
    throw new Error(`Invalid industry-label payload:\n- ${errors.join("\n- ")}`);
  }
  return rows;
}

/**
 * One deep-validation problem, structured so the CLI can both print it and
 * emit a machine repair report a later session resumes from.
 */
export type IndustryLabelValidationIssue = {
  index: number;
  kind: "missing_label";
  reason: string;
};

/**
 * Every payload row must match a label some finance sync already persisted:
 * a mistyped raw_label would otherwise create a classification row no
 * breakdown ever joins, with no error anywhere. Existing rows of any source
 * stay writable — correcting a rule/ai/manual link is the same command.
 */
export function checkRowsAgainstKnownLabels(
  rows: readonly IndustryLabelPayloadRow[],
  knownLabels: ReadonlySet<string>
): IndustryLabelValidationIssue[] {
  const issues: IndustryLabelValidationIssue[] = [];
  rows.forEach((row, index) => {
    const key = financeClassificationKey(row.label_type, row.normalized_label);
    if (!knownLabels.has(key)) {
      issues.push({
        index,
        kind: "missing_label",
        reason: `(${row.label_type}, "${row.raw_label}") normalizes to "${row.normalized_label}", which has no classification row — copy label_type and raw_label from the due list`,
      });
    }
  });
  return issues;
}

// One repair gap per issue: the report is what a later session resumes
// from, so each gap carries a focused instruction matching the issue kind.
function industryLabelIssueToRepairGap(
  issue: IndustryLabelValidationIssue,
  row: IndustryLabelPayloadRow | undefined
): ManualResearchRepairGap {
  const label = row ? `${row.label_type}.${row.normalized_label}` : `labels_${issue.index}`;
  return {
    id: `finance_industry_label.${label}.${issue.kind}`,
    stage: "finance_industry_labels",
    objectType: "finance_industry_label",
    outcome: "needs_repair",
    reason: issue.reason,
    focusedResearchPass:
      "Regenerate this row from a fresh manual:finance-industry-labels:due run — copy label_type and raw_label verbatim — then rerun the industry-label writer.",
    labelIndex: issue.index,
    failureKind: "label_validation",
  };
}

async function runWrite(pool: Pool, argv: readonly string[]): Promise<void> {
  const file = readFlag(argv, "--file");
  if (!file) {
    throw new Error(`--file is required.\n${usage()}`);
  }
  const dryRun = argv.includes("--dry-run");
  const repairReportFile = readFlag(argv, "--repair-report-file");
  const rows = parseIndustryLabelPayload(JSON.parse(await readFile(file, "utf8")) as unknown);

  const existing = await pool.query<{ label_type: string; normalized_label: string }>(
    `
      SELECT c.label_type, c.normalized_label
      FROM public.finance_label_classifications AS c
      JOIN unnest($1::text[], $2::text[]) AS wanted(label_type, normalized_label)
        ON wanted.label_type = c.label_type
       AND wanted.normalized_label = c.normalized_label
    `,
    [rows.map((row) => row.label_type), rows.map((row) => row.normalized_label)]
  );
  const knownLabels = new Set(
    existing.rows.map((row) => financeClassificationKey(row.label_type as "employer" | "donor", row.normalized_label))
  );
  const issues = checkRowsAgainstKnownLabels(rows, knownLabels);
  if (issues.length > 0) {
    // Same machine repair report the profile/records writers emit, so a
    // later session can resume the fix without this run's terminal output.
    await writeManualResearchRepairReport(
      repairReportFile,
      buildManualResearchRepairReport({
        command: "manual:finance-industry-labels:write",
        manualKey: "manual:finance-industry-labels:payload",
        target: { file },
        gaps: issues.map((issue) => industryLabelIssueToRepairGap(issue, rows[issue.index])),
      })
    );
    throw new Error(
      `Industry-label validation failed:\n- ${issues
        .map((issue) => `labels[${issue.index}]: ${issue.reason}`)
        .join("\n- ")}`
    );
  }

  if (dryRun) {
    console.log(JSON.stringify({ dry_run: true, valid_rows: rows.length, rows }, null, 2));
    return;
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    for (const row of rows) {
      const classification: FinanceLabelClassification = {
        rawLabel: row.raw_label,
        labelType: row.label_type,
        normalizedLabel: row.normalized_label,
        industrySlug: row.industry_slug,
        confidence: row.confidence,
        classificationSource: "manual",
        matchedRule: null,
      };
      await upsertFinanceLabelClassification({ db: client, classification });
    }
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
  console.log(JSON.stringify({ written: rows.length }, null, 2));
}

async function main(): Promise<void> {
  const [command, ...rest] = process.argv.slice(2);
  if (command !== "due" && command !== "write") {
    throw new Error(`Unknown subcommand: ${command ?? "(none)"}.\n${usage()}`);
  }
  const subcommand: Subcommand = command;

  const flagSpecs = {
    due: [{ name: "--limit", value: "both" as const }],
    write: [
      { name: "--file", value: "both" as const },
      { name: "--repair-report-file", value: "both" as const },
      { name: "--dry-run", value: "none" as const },
    ],
  }[subcommand];
  assertKnownCliFlags(`manual:finance-industry-labels:${subcommand}`, rest, flagSpecs);

  loadProjectEnv();
  const databaseUrl = requireEnv("DATABASE_URL");
  if (subcommand === "write") {
    requireLocalDatabaseTarget(databaseUrl);
  }

  const pool = new Pool({ connectionString: databaseUrl });
  try {
    if (subcommand === "due") {
      await runDue(pool, rest);
    } else {
      await runWrite(pool, rest);
    }
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("manual finance industry labels failed:", message);
    process.exitCode = 1;
  });
}
