import type { Pool, PoolClient } from "pg";

import {
  type FinanceClassificationConfidence,
  type FinanceClassificationSource,
  type FinanceIndustrySlug,
  type FinanceLabelClassification,
  type FinanceLabelType,
  normalizeFinanceLabel,
} from "./financeLabelClassifier.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type FinanceIndustryClassificationCandidate = {
  rawLabel: string;
  labelType: Extract<FinanceLabelType, "employer" | "donor">;
  normalizedLabel: string;
  amount: number;
};

export type FinanceIndustryClassifier = (input: {
  labels: readonly FinanceIndustryClassificationCandidate[];
}) => Promise<FinanceLabelClassification[]>;

export type FinanceIndustryClassifiableBreakdown = {
  categoryType: string;
  categoryName: string;
  amount: number;
  contributorCount?: number | null;
  sourceUrl?: string | null;
};

export type FinanceIndustryClassifiableOutsideBreakdown = FinanceIndustryClassifiableBreakdown & {
  committeeId: string;
  supportOppose: "support" | "oppose";
};

export type FinanceIndustryDirectBreakdown = Omit<
  FinanceIndustryClassifiableBreakdown,
  "categoryType" | "contributorCount" | "sourceUrl"
> & {
  categoryType: "industry";
  contributorCount: number | null;
  sourceUrl: string | null;
};

export type FinanceIndustryOutsideBreakdown = Omit<
  FinanceIndustryClassifiableOutsideBreakdown,
  "categoryType" | "contributorCount" | "sourceUrl"
> & {
  categoryType: "industry";
  contributorCount: number | null;
  sourceUrl: string | null;
};

type FinanceClassificationRow = {
  raw_label: string;
  label_type: FinanceLabelType;
  normalized_label: string;
  industry_slug: FinanceIndustrySlug | null;
  confidence: FinanceClassificationConfidence;
  classification_source: FinanceClassificationSource;
};

export function financeClassificationKey(labelType: FinanceLabelType, normalizedLabel: string): string {
  return `${labelType}\u0000${normalizedLabel}`;
}

function shouldReplaceClassification(
  existing: FinanceLabelClassification | undefined,
  next: FinanceLabelClassification
): boolean {
  if (!existing) {
    return true;
  }
  // Manual rows are researched verdicts (including deliberate null slugs) and
  // outrank every automated source; only another manual row replaces one.
  if (existing.classificationSource === "manual") {
    return next.classificationSource === "manual";
  }
  if (next.classificationSource === "manual") {
    return true;
  }
  if (!existing.industrySlug && next.industrySlug) {
    return true;
  }
  return existing.classificationSource === "unknown" && next.classificationSource !== "unknown";
}

export function mergeFinanceLabelClassification(
  classifications: Map<string, FinanceLabelClassification>,
  classification: FinanceLabelClassification
): void {
  const key = financeClassificationKey(classification.labelType, classification.normalizedLabel);
  if (shouldReplaceClassification(classifications.get(key), classification)) {
    classifications.set(key, classification);
  }
}

function shouldSendClassificationToAi(classification: FinanceLabelClassification | undefined): boolean {
  if (!classification) {
    return true;
  }
  if (classification.industrySlug) {
    return false;
  }
  return classification.classificationSource === "unknown";
}

function collectClassificationCandidates(input: {
  directBreakdowns: Iterable<FinanceIndustryClassifiableBreakdown>;
  outsideBreakdowns: Iterable<FinanceIndustryClassifiableOutsideBreakdown>;
  shouldInclude: (existing: FinanceLabelClassification | undefined) => boolean;
  classifications: Map<string, FinanceLabelClassification>;
  minAmount: number;
}): FinanceIndustryClassificationCandidate[] {
  const candidates = new Map<string, FinanceIndustryClassificationCandidate>();

  const addCandidate = (breakdown: { categoryType: string; categoryName: string; amount: number }): void => {
    if (breakdown.categoryType !== "employer" && breakdown.categoryType !== "donor") {
      return;
    }
    const labelType = breakdown.categoryType;
    const normalizedLabel = normalizeFinanceLabel(breakdown.categoryName, labelType);
    if (!normalizedLabel) {
      return;
    }
    const key = financeClassificationKey(labelType, normalizedLabel);
    const classification = input.classifications.get(key);
    if (!input.shouldInclude(classification)) {
      return;
    }
    const existing = candidates.get(key);
    if (existing) {
      existing.amount += breakdown.amount;
      return;
    }
    candidates.set(key, {
      rawLabel: breakdown.categoryName,
      labelType,
      normalizedLabel,
      amount: breakdown.amount,
    });
  };

  for (const breakdown of input.directBreakdowns) {
    addCandidate(breakdown);
  }
  for (const breakdown of input.outsideBreakdowns) {
    addCandidate(breakdown);
  }

  return [...candidates.values()].filter((candidate) => candidate.amount >= input.minAmount);
}

function mapClassificationRow(row: FinanceClassificationRow): FinanceLabelClassification {
  return {
    rawLabel: row.raw_label,
    labelType: row.label_type,
    normalizedLabel: row.normalized_label,
    industrySlug: row.industry_slug,
    confidence: row.confidence,
    classificationSource: row.classification_source,
    matchedRule: null,
  };
}

async function loadCachedFinanceLabelClassifications(
  db: Queryable,
  labels: readonly FinanceIndustryClassificationCandidate[]
): Promise<FinanceLabelClassification[]> {
  if (labels.length === 0) {
    return [];
  }

  const valuesSql = labels
    .map((_label, index) => `($${index * 2 + 1}::text, $${index * 2 + 2}::text)`)
    .join(", ");
  const params = labels.flatMap((label) => [label.labelType, label.normalizedLabel]);
  const result = await db.query<FinanceClassificationRow>(
    `
      WITH requested(label_type, normalized_label) AS (
        VALUES ${valuesSql}
      )
      SELECT
        classification.raw_label,
        classification.label_type,
        classification.normalized_label,
        classification.industry_slug,
        classification.confidence,
        classification.classification_source
      FROM public.finance_label_classifications AS classification
      JOIN requested
        ON requested.label_type = classification.label_type
       AND requested.normalized_label = classification.normalized_label
    `,
    params
  );

  // Tolerate malformed rows instead of merging classifications with
  // undefined keys — the merge map would otherwise poison the persist step.
  return result.rows.filter((row) => row.label_type && row.normalized_label).map(mapClassificationRow);
}

export async function resolveFinanceIndustryClassifications(input: {
  db: Queryable;
  directBreakdowns: Iterable<FinanceIndustryClassifiableBreakdown>;
  outsideBreakdowns: Iterable<FinanceIndustryClassifiableOutsideBreakdown>;
  classifications: Map<string, FinanceLabelClassification>;
  classifier: FinanceIndustryClassifier | undefined;
  minAmount: number;
  dryRun: boolean;
}): Promise<void> {
  if (input.dryRun) {
    return;
  }
  const directBreakdowns = [...input.directBreakdowns];
  const outsideBreakdowns = [...input.outsideBreakdowns];

  // Cached rows are loaded for EVERY employer/donor label, independent of the
  // AI amount threshold and of whether the rule classifier already resolved
  // the label: a manual row must win over a fresh rule/unknown result even
  // when the label would never be sent to AI, otherwise the subsequent
  // persist would write the weaker in-memory result over the manual verdict.
  const lookupCandidates = collectClassificationCandidates({
    directBreakdowns,
    outsideBreakdowns,
    shouldInclude: () => true,
    classifications: input.classifications,
    minAmount: 0,
  });
  if (lookupCandidates.length === 0) {
    return;
  }

  for (const classification of await loadCachedFinanceLabelClassifications(input.db, lookupCandidates)) {
    mergeFinanceLabelClassification(input.classifications, classification);
  }

  const remainingCandidates = collectClassificationCandidates({
    directBreakdowns,
    outsideBreakdowns,
    shouldInclude: shouldSendClassificationToAi,
    classifications: input.classifications,
    minAmount: input.minAmount,
  });
  if (remainingCandidates.length === 0 || !input.classifier) {
    return;
  }

  try {
    const aiClassifications = await input.classifier({ labels: remainingCandidates });
    for (const classification of aiClassifications) {
      mergeFinanceLabelClassification(input.classifications, classification);
    }
  } catch {
    // Industry labels are enrichment-only. Keep finance syncs successful if AI classification fails.
  }
}

/**
 * Upserts one classification row. The conflict guard mirrors
 * shouldReplaceClassification at the database level: a manual row is only
 * ever replaced by another manual row, and an automated write must improve
 * the stored row (resolve an 'unknown', or fill a null slug) — so a sync
 * whose in-memory state is weaker than the stored row cannot degrade it.
 */
export async function upsertFinanceLabelClassification(input: {
  db: Queryable;
  classification: FinanceLabelClassification;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.finance_label_classifications (
        raw_label,
        label_type,
        normalized_label,
        industry_slug,
        confidence,
        classification_source
      )
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (label_type, normalized_label)
      DO UPDATE SET
        raw_label = EXCLUDED.raw_label,
        industry_slug = EXCLUDED.industry_slug,
        confidence = EXCLUDED.confidence,
        classification_source = EXCLUDED.classification_source
      WHERE EXCLUDED.classification_source = 'manual'
         OR (
           finance_label_classifications.classification_source <> 'manual'
           AND (
             finance_label_classifications.classification_source = 'unknown'
             OR (
               finance_label_classifications.industry_slug IS NULL
               AND EXCLUDED.industry_slug IS NOT NULL
             )
           )
         )
    `,
    [
      input.classification.rawLabel,
      input.classification.labelType,
      input.classification.normalizedLabel,
      input.classification.industrySlug,
      input.classification.confidence,
      input.classification.classificationSource,
    ]
  );
}

export function buildFinanceIndustryBreakdownsFromClassifications(input: {
  directBreakdowns: Iterable<FinanceIndustryClassifiableBreakdown>;
  outsideBreakdowns: Iterable<FinanceIndustryClassifiableOutsideBreakdown>;
  classifications: Map<string, FinanceLabelClassification>;
}): {
  directIndustryBreakdowns: FinanceIndustryDirectBreakdown[];
  outsideIndustryBreakdowns: FinanceIndustryOutsideBreakdown[];
} {
  const directIndustryBreakdowns: FinanceIndustryDirectBreakdown[] = [];
  const outsideIndustryBreakdowns: FinanceIndustryOutsideBreakdown[] = [];

  for (const breakdown of input.directBreakdowns) {
    if (breakdown.categoryType !== "employer" && breakdown.categoryType !== "donor") {
      continue;
    }
    const labelType = breakdown.categoryType;
    const classification = input.classifications.get(
      financeClassificationKey(labelType, normalizeFinanceLabel(breakdown.categoryName, labelType))
    );
    if (!classification?.industrySlug) {
      continue;
    }
    directIndustryBreakdowns.push({
      categoryType: "industry",
      categoryName: classification.industrySlug,
      amount: breakdown.amount,
      contributorCount: breakdown.contributorCount ?? null,
      sourceUrl: breakdown.sourceUrl ?? null,
    });
  }

  for (const breakdown of input.outsideBreakdowns) {
    if (breakdown.categoryType !== "employer" && breakdown.categoryType !== "donor") {
      continue;
    }
    const labelType = breakdown.categoryType;
    const classification = input.classifications.get(
      financeClassificationKey(labelType, normalizeFinanceLabel(breakdown.categoryName, labelType))
    );
    if (!classification?.industrySlug) {
      continue;
    }
    outsideIndustryBreakdowns.push({
      committeeId: breakdown.committeeId,
      supportOppose: breakdown.supportOppose,
      categoryType: "industry",
      categoryName: classification.industrySlug,
      amount: breakdown.amount,
      contributorCount: breakdown.contributorCount ?? null,
      sourceUrl: breakdown.sourceUrl ?? null,
    });
  }

  return { directIndustryBreakdowns, outsideIndustryBreakdowns };
}
