import { describe, expect, it } from "vitest";

import {
  firstFinanceSourceUrl,
  hasFinanceContent,
  hasMemberCommunications,
  hasOutsideFinanceContent,
} from "./finance";
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

  it("finds source URLs in employer breakdowns even though they no longer count as content", () => {
    const summary = emptySummary();
    summary.direct_campaign.top_employers = [
      { category_name: "NYC DOE", amount: 1, contributor_count: 1, source_url: "https://www.nyccfb.info/" },
    ];
    // Employers aren't rendered anymore, so alone they don't make the card show…
    expect(hasFinanceContent(summary)).toBe(false);
    // …but their rows still serve as a provenance-link fallback.
    expect(firstFinanceSourceUrl(summary)).toBe("https://www.nyccfb.info/");
  });
});

describe("member communications (LA)", () => {
  it("counts as card content on its own, but not as outside-groups content", () => {
    const summary = emptySummary();
    summary.outside_spending.membership_support_total = 203457;
    expect(hasMemberCommunications(summary)).toBe(true);
    expect(hasFinanceContent(summary)).toBe(true);
    // Member communications are legally distinct from independent
    // expenditures; they render as their own section, so they must not
    // trigger the outside-groups section.
    expect(hasOutsideFinanceContent(summary)).toBe(false);
  });

  it("hides on 0 — the LA sync writes 0 for every linked candidate", () => {
    const summary = emptySummary();
    summary.outside_spending.membership_support_total = 0;
    summary.outside_spending.membership_oppose_total = 0;
    expect(hasMemberCommunications(summary)).toBe(false);
    expect(hasFinanceContent(summary)).toBe(false);
  });

  it("an oppose-only disclosure also counts", () => {
    const summary = emptySummary();
    summary.outside_spending.membership_oppose_total = 1200;
    expect(hasMemberCommunications(summary)).toBe(true);
  });
});
