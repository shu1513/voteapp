import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import { FinanceSummaryCard, hasFinanceContent } from "./FinanceSummaryCard";
import { financeSummary, emptyFinanceSummary } from "../test/fixtures";

describe("hasFinanceContent", () => {
  it("is false for null and for a summary with only null money and empty lists", () => {
    expect(hasFinanceContent(null)).toBe(false);
    expect(hasFinanceContent(emptyFinanceSummary())).toBe(false);
  });

  it("treats an explicit $0 as content — 0 is a disclosure, null is absence", () => {
    const summary = emptyFinanceSummary();
    summary.direct_campaign.total_raised = 0;
    expect(hasFinanceContent(summary)).toBe(true);
  });

  it("is true when only a breakdown list is populated", () => {
    const summary = emptyFinanceSummary();
    summary.outside_spending.top_supporting_industries = [
      { category_name: "technology", amount: 100, contributor_count: null, source_url: null },
    ];
    expect(hasFinanceContent(summary)).toBe(true);
  });
});

describe("FinanceSummaryCard", () => {
  it("renders money, occupations, industries, outside spending, and the source line", () => {
    render(<FinanceSummaryCard summary={financeSummary()} />);

    expect(screen.getByText("Raised")).toBeInTheDocument();
    expect(screen.getByText("$120,000")).toBeInTheDocument();
    // Occupations stay distinct from industries.
    expect(screen.getByText("Top disclosed occupations of direct donors")).toBeInTheDocument();
    expect(screen.getByText("Retired")).toBeInTheDocument();
    expect(screen.getByText("Industries represented among direct contributions")).toBeInTheDocument();
    // Industry slugs render through the display map, never raw.
    expect(screen.getByText("Oil, gas, and energy")).toBeInTheDocument();
    expect(screen.queryByText("oil_gas_energy")).not.toBeInTheDocument();
    // Support and opposition stay distinct.
    expect(screen.getByText("Reported support: $50,000")).toBeInTheDocument();
    expect(screen.getByText("Growth PAC")).toBeInTheDocument();
    expect(screen.getByText("Reported opposition: $20,000")).toBeInTheDocument();
    expect(screen.getByText("Stop Them PAC")).toBeInTheDocument();
    // Source enum renders as a display label with the provenance link.
    expect(screen.getByText(/Source: FEC · 2026 cycle · synced/)).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "fec.gov" })).toHaveAttribute(
      "href",
      "https://www.fec.gov/data/candidate/H0AK00001/"
    );
  });

  it("omits the opposition column when that direction has no disclosures", () => {
    const summary = financeSummary();
    summary.outside_spending.oppose_total = null;
    summary.outside_spending.top_opposing_groups = [];
    summary.outside_spending.top_opposing_industries = [];
    render(<FinanceSummaryCard summary={summary} />);

    expect(screen.getByText(/Reported support/)).toBeInTheDocument();
    expect(screen.queryByText(/Reported opposition/)).not.toBeInTheDocument();
  });

  it("renders an explicit $0 raised instead of hiding it", () => {
    const summary = emptyFinanceSummary();
    summary.direct_campaign.total_raised = 0;
    render(<FinanceSummaryCard summary={summary} />);

    expect(screen.getByText("Raised")).toBeInTheDocument();
    expect(screen.getByText("$0")).toBeInTheDocument();
    expect(screen.queryByText("Spent")).not.toBeInTheDocument();
  });
});
