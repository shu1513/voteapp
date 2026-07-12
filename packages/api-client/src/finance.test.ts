import { describe, expect, it } from "vitest";

import { firstFinanceSourceUrl, hasFinanceContent } from "./finance";
import type { FinanceSummary } from "./types";

function emptySummary(): FinanceSummary {
  return {
    source: "NEW_YORK_CITY_CFB",
    cycle: 2025,
    last_synced_at: "2025-01-01T00:00:00Z",
    direct_campaign: {
      total_raised: null,
      total_spent: null,
      cash_on_hand: null,
      debts_owed: null,
      top_occupations: [],
      top_industries: [],
    },
    outside_spending: {
      support_total: null,
      oppose_total: null,
      top_supporting_groups: [],
      top_opposing_groups: [],
      top_supporting_industries: [],
      top_opposing_industries: [],
    },
  };
}

describe("NYC finance shared fields", () => {
  it("treats explicit zero public funds as content", () => {
    const summary = emptySummary();
    summary.direct_campaign.public_funds_received = 0;
    expect(hasFinanceContent(summary)).toBe(true);
  });

  it("finds source URLs in employer and size breakdowns", () => {
    const summary = emptySummary();
    summary.direct_campaign.top_employers = [
      { category_name: "NYC DOE", amount: 1, contributor_count: 1, source_url: "https://www.nyccfb.info/" },
    ];
    expect(hasFinanceContent(summary)).toBe(true);
    expect(firstFinanceSourceUrl(summary)).toBe("https://www.nyccfb.info/");
  });
});
