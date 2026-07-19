import type { Pool, PoolClient } from "pg";

import { captureError } from "../../observability/sentry.js";
import { describeError } from "../../observability/scrubText.js";
import type { BallotLookupFinanceSummary } from "./ballotLookupFinanceShared.js";

type Queryable = Pick<Pool | PoolClient, "query">;

type FinanceCommitteeLabelRow = {
  source: string;
  committee_id: string;
  label: string;
};

// (source, committee_id) — committee ids are only unique within one
// disclosing agency's namespace.
function committeeLabelKey(source: string, committeeId: string): string {
  return `${source} ${committeeId}`;
}

/**
 * Attaches manually researched committee labels (finance_committee_labels)
 * to the outside-spending group rows of already-built finance summaries.
 * Mutates the summaries in place.
 *
 * Runs as the LAST query of each ballot-lookup entry point and is fault
 * isolated: labels enrich the payload, so a missing table (migration not
 * yet applied) or any query failure degrades to unlabeled groups instead of
 * failing the lookup. Issues no query when the summaries carry no groups.
 */
export async function applyFinanceCommitteeLabels(
  db: Queryable,
  summaries: Iterable<BallotLookupFinanceSummary | null | undefined>
): Promise<void> {
  const summaryList = [...summaries].filter(
    (summary): summary is BallotLookupFinanceSummary => summary != null
  );
  const sources: string[] = [];
  const committeeIds: string[] = [];
  const seen = new Set<string>();
  for (const summary of summaryList) {
    for (const group of [
      ...summary.outside_spending.top_supporting_groups,
      ...summary.outside_spending.top_opposing_groups,
    ]) {
      const key = committeeLabelKey(summary.source, group.committee_id);
      if (!seen.has(key)) {
        seen.add(key);
        sources.push(summary.source);
        committeeIds.push(group.committee_id);
      }
    }
  }
  if (sources.length === 0) {
    return;
  }

  let labelByKey: Map<string, string>;
  try {
    const result = await db.query<FinanceCommitteeLabelRow>(
      `
        SELECT l.source, l.committee_id, l.label
        FROM public.finance_committee_labels AS l
        JOIN unnest($1::text[], $2::text[]) AS wanted(source, committee_id)
          ON wanted.source = l.source
         AND wanted.committee_id = l.committee_id
      `,
      [sources, committeeIds]
    );
    labelByKey = new Map(
      result.rows.map((row) => [committeeLabelKey(row.source, row.committee_id), row.label])
    );
  } catch (error) {
    console.warn("finance committee labels failed; continuing with unlabeled groups:", {
      reason: describeError(error),
    });
    captureError(error, { finance_loader: "applyFinanceCommitteeLabels" });
    return;
  }

  for (const summary of summaryList) {
    for (const group of [
      ...summary.outside_spending.top_supporting_groups,
      ...summary.outside_spending.top_opposing_groups,
    ]) {
      const label = labelByKey.get(committeeLabelKey(summary.source, group.committee_id));
      if (label !== undefined) {
        group.label = label;
      }
    }
  }
}
