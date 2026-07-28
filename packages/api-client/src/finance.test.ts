import { describe, expect, it } from "vitest";

import {
  firstFinanceSourceUrl,
  hasFinanceContent,
  hasMemberCommunications,
  hasOutsideFinanceContent,
  spendingExceedsCycleFunds,
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

  it("treats positive loans as content, but not zero or absent loans", () => {
    const loansOnly = emptySummary();
    loansOnly.direct_campaign.loans_received = 30_752_614;
    expect(hasFinanceContent(loansOnly)).toBe(true);

    const zeroLoans = emptySummary();
    zeroLoans.direct_campaign.loans_received = 0;
    expect(hasFinanceContent(zeroLoans)).toBe(false);

    expect(hasFinanceContent(emptySummary())).toBe(false);
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

describe("outside spending direction gating", () => {
  it("a disclosed $0 total with nothing behind it is not content", () => {
    const summary = emptySummary();
    summary.outside_spending.support_total = 0;
    summary.outside_spending.oppose_total = 0;
    expect(hasOutsideFinanceContent(summary)).toBe(false);
    expect(hasFinanceContent(summary)).toBe(false);
  });

  it("a positive total, a group, or an industry each count", () => {
    const withTotal = emptySummary();
    withTotal.outside_spending.oppose_total = 1;
    expect(hasOutsideFinanceContent(withTotal)).toBe(true);

    const withGroup = emptySummary();
    withGroup.outside_spending.support_total = 0;
    withGroup.outside_spending.top_supporting_groups = [
      { committee_id: "C1", committee_name: "Growth PAC", support_oppose: "support", amount: 5, source_url: null },
    ];
    expect(hasOutsideFinanceContent(withGroup)).toBe(true);

    const withIndustry = emptySummary();
    withIndustry.outside_spending.oppose_total = 0;
    withIndustry.outside_spending.top_opposing_industries = [
      { category_name: "technology", amount: 5, contributor_count: null, source_url: null },
    ];
    expect(hasOutsideFinanceContent(withIndustry)).toBe(true);
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

describe("spendingExceedsCycleFunds", () => {
  it("is false when raised or spent is unreported", () => {
    const summary = emptySummary();
    summary.direct_campaign.total_spent = 100;
    expect(spendingExceedsCycleFunds(summary)).toBe(false);
  });

  it("is true only when spending tops raised plus public funds", () => {
    const summary = emptySummary();
    summary.direct_campaign.total_raised = 100;
    summary.direct_campaign.total_spent = 250;
    expect(spendingExceedsCycleFunds(summary)).toBe(true);
    // Public matching money explains the gap, so no note.
    summary.direct_campaign.public_funds_received = 200;
    expect(spendingExceedsCycleFunds(summary)).toBe(false);
  });

  it("is false when spending fits within money raised this cycle", () => {
    const summary = emptySummary();
    summary.direct_campaign.total_raised = 100;
    summary.direct_campaign.total_spent = 100;
    expect(spendingExceedsCycleFunds(summary)).toBe(false);
  });

  it("counts reported loans as visible funding for the gap note", () => {
    // The self-funder shape: tiny donations, huge spending, huge loans. The
    // Loans stat on the card already explains the gap, so no note.
    const summary = emptySummary();
    summary.direct_campaign.total_raised = 28_700;
    summary.direct_campaign.total_spent = 20_000_000;
    summary.direct_campaign.loans_received = 30_752_614;
    expect(spendingExceedsCycleFunds(summary)).toBe(false);
    // Spending beyond even the visible loans is unexplained again.
    summary.direct_campaign.total_spent = 31_000_000;
    expect(spendingExceedsCycleFunds(summary)).toBe(true);
  });
});
