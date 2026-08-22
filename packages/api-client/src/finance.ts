import type { FinanceSummary } from "./types";

const FINANCE_SOURCE_HOME_URLS: Partial<Record<FinanceSummary["source"], string>> = {
  MISSOURI_MEC: "https://www.mec.mo.gov/MEC/Campaign_Finance/",
  NEW_HAMPSHIRE_CFS: "https://cfs.sos.nh.gov/",
};

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
    (direct.loans_received ?? 0) > 0 ||
    direct.top_occupations.length > 0 ||
    (direct.contribution_size_buckets?.length ?? 0) > 0 ||
    hasOutsideFinanceContent(summary) ||
    hasMemberCommunications(summary)
  );
}

/**
 * Whether spending is higher than every funding source the card can show
 * (money raised this cycle, plus any public funds and reported loans). True
 * means the campaign may also be using money the visible stats don't count —
 * prior-cycle carryover, unreported loans, transfers, or other receipts
 * ("Raised" is direct contributions only for most state sources) — which
 * reads as impossible ("spent more than raised") without an explanation. The
 * cards show a one-line note when this is true; the note must stay non-causal
 * because this check cannot tell which of those sources filled the gap.
 */
export function spendingExceedsCycleFunds(summary: FinanceSummary): boolean {
  const direct = summary.direct_campaign;
  if (direct.total_raised === null || direct.total_spent === null) {
    return false;
  }
  return (
    direct.total_spent >
    direct.total_raised + (direct.public_funds_received ?? 0) + (direct.loans_received ?? 0)
  );
}

/**
 * First source URL across every finance evidence row, for the card's footer
 * link. Rows share one disclosure portal per source, so any row's URL serves.
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
    ...(summary.outside_spending.unallocated_candidate_edges ?? []),
  ];
  for (const row of rows) {
    if (row.source_url) {
      return row.source_url;
    }
  }
  return FINANCE_SOURCE_HOME_URLS[summary.source] ?? null;
}

/**
 * Most direct coverage notes qualify donor breakdowns and should stay hidden
 * when no breakdown is shown. Missouri's note also qualifies its itemized
 * transaction totals, so a real zero-row snapshot must show the note beside
 * the disclosed $0 figures.
 */
export function shouldShowDirectCoverageNote(summary: FinanceSummary): boolean {
  const direct = summary.direct_campaign;
  if (!direct.direct_coverage_note) {
    return false;
  }
  if (direct.top_occupations.length > 0 || (direct.contribution_size_buckets?.length ?? 0) > 0) {
    return true;
  }
  return (
    summary.source === "MISSOURI_MEC" &&
    (direct.total_raised !== null ||
      direct.total_spent !== null ||
      direct.cash_on_hand !== null ||
      direct.debts_owed !== null ||
      direct.public_funds_received != null ||
      (direct.loans_received ?? 0) > 0)
  );
}

/**
 * Whether one outside-spending direction (support or opposition) has
 * anything worth a box. A disclosed $0 total with no groups and no
 * industries hides: several syncs write totals for every linked candidate,
 * so "$0 opposing this candidate" reads as noise, not information.
 */
export function hasOutsideDirectionContent(
  total: number | null | undefined,
  groups: readonly unknown[],
  industries: readonly unknown[]
): boolean {
  return (total ?? 0) > 0 || groups.length > 0 || industries.length > 0;
}

export function hasOutsideFinanceContent(summary: FinanceSummary): boolean {
  const outside = summary.outside_spending;
  return (
    (outside.unallocated_candidate_edges?.length ?? 0) > 0 ||
    hasOutsideDirectionContent(
      outside.support_total,
      outside.top_supporting_groups,
      outside.top_supporting_industries
    ) ||
    hasOutsideDirectionContent(
      outside.oppose_total,
      outside.top_opposing_groups,
      outside.top_opposing_industries
    )
  );
}

/**
 * Whether the cards should render a member-communications block: spending
 * by organizations to their own members about this candidate (disclosed
 * separately from independent expenditures — today only by LA Ethics).
 * Unlike the other money fields, a disclosed $0 hides: the sync writes 0
 * for every linked candidate, so 0 means "none reported", not a disclosure
 * worth a row.
 */
export function hasMemberCommunications(summary: FinanceSummary): boolean {
  const outside = summary.outside_spending;
  return (outside.membership_support_total ?? 0) > 0 || (outside.membership_oppose_total ?? 0) > 0;
}
