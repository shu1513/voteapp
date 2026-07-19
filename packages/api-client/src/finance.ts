import type { FinanceSummary } from "./types";

// Shared by the web and mobile FinanceSummaryCard so "is there anything to
// render" stays one definition across platforms.

/**
 * Whether a summary has anything worth rendering. null money values mean
 * "not reported" and hide; an explicit 0 is a real disclosed amount and
 * counts as content. Employers and direct-donor industries are no longer
 * rendered by the cards, so they no longer count as content on their own.
 */
export function hasFinanceContent(summary: FinanceSummary | null | undefined): summary is FinanceSummary {
  if (!summary) {
    return false;
  }
  const direct = summary.direct_campaign;
  return (
    direct.total_raised !== null ||
    direct.total_spent !== null ||
    direct.cash_on_hand !== null ||
    direct.debts_owed !== null ||
    direct.public_funds_received != null ||
    direct.top_occupations.length > 0 ||
    (direct.contribution_size_buckets?.length ?? 0) > 0 ||
    hasOutsideFinanceContent(summary)
  );
}

/**
 * First source URL across every breakdown row, for the card's footer link.
 * Rows share one disclosure portal per source, so any row's URL serves.
 */
export function firstFinanceSourceUrl(summary: FinanceSummary): string | null {
  const rows: { source_url: string | null }[] = [
    ...summary.direct_campaign.top_occupations,
    ...(summary.direct_campaign.top_employers ?? []),
    ...summary.direct_campaign.top_industries,
    ...(summary.direct_campaign.contribution_size_buckets ?? []),
    ...summary.outside_spending.top_supporting_groups,
    ...summary.outside_spending.top_opposing_groups,
    ...summary.outside_spending.top_supporting_industries,
    ...summary.outside_spending.top_opposing_industries,
  ];
  for (const row of rows) {
    if (row.source_url) {
      return row.source_url;
    }
  }
  return null;
}

export function hasOutsideFinanceContent(summary: FinanceSummary): boolean {
  const outside = summary.outside_spending;
  return (
    outside.support_total !== null ||
    outside.oppose_total !== null ||
    outside.top_supporting_groups.length > 0 ||
    outside.top_opposing_groups.length > 0 ||
    outside.top_supporting_industries.length > 0 ||
    outside.top_opposing_industries.length > 0
  );
}
