import type { FinanceSummary } from "./types";

// Shared by the web and mobile FinanceSummaryCard so "is there anything to
// render" stays one definition across platforms.

/**
 * Whether a summary has anything worth rendering. null money values mean
 * "not reported" and hide; an explicit 0 is a real disclosed amount and
 * counts as content.
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
    direct.top_occupations.length > 0 ||
    direct.top_industries.length > 0 ||
    hasOutsideFinanceContent(summary)
  );
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
