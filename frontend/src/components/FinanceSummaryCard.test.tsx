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

  it("no longer counts employers or direct-donor industries — the card doesn't render them", () => {
    const summary = emptyFinanceSummary();
    summary.direct_campaign.top_employers = [
      { category_name: "NYC DOE", amount: 1, contributor_count: 1, source_url: null },
    ];
    summary.direct_campaign.top_industries = [
      { category_name: "technology", amount: 1, contributor_count: 1, source_url: null },
    ];
    expect(hasFinanceContent(summary)).toBe(false);
  });
});

describe("FinanceSummaryCard", () => {
  it("renders money, occupations, outside spending, the updated date, and the source line", () => {
    const summary = financeSummary();
    summary.direct_campaign.public_funds_received = null;
    render(<FinanceSummaryCard summary={summary} />);

    // Freshness is surfaced up top, not buried in the footer.
    expect(screen.getByText("Data last updated July 1, 2026")).toBeInTheDocument();

    const raisedLabel = screen.getByText("Raised");
    expect(raisedLabel).toBeInTheDocument();
    expect(raisedLabel.closest("dl")).toHaveClass("sm:grid-cols-4");
    expect(raisedLabel.closest("dl")).not.toHaveClass("sm:grid-cols-5");
    expect(screen.queryByText("Public funds")).not.toBeInTheDocument();
    expect(screen.getByText("$120,000")).toBeInTheDocument();
    expect(screen.getByText("Top disclosed occupations of direct donors")).toBeInTheDocument();
    expect(screen.getByText("Retired")).toBeInTheDocument();
    // Employers and direct-donor industries are deliberately not rendered.
    expect(screen.queryByText("Top disclosed employers of direct donors")).not.toBeInTheDocument();
    expect(screen.queryByText("Industries represented among direct contributions")).not.toBeInTheDocument();
    expect(screen.queryByText("Oil, gas, and energy")).not.toBeInTheDocument();
    // Support and opposition stay distinct, with a plain-language intro.
    expect(
      screen.getByText(/Outside spending is money spent on this race by independent groups/)
    ).toBeInTheDocument();
    expect(screen.getByText("Reported support: $50,000")).toBeInTheDocument();
    expect(screen.getByText("Growth PAC")).toBeInTheDocument();
    expect(screen.getByText("Reported opposition: $20,000")).toBeInTheDocument();
    expect(screen.getByText("Stop Them PAC")).toBeInTheDocument();
    // Backing evidence names the organizations behind a supporting industry.
    expect(screen.getByText("Technology")).toBeInTheDocument();
    expect(screen.getByText("Money from Google and Anthropic, given to Growth PAC.")).toBeInTheDocument();
    // Industry money flows into the groups, not necessarily onto this race —
    // the heading and note must not present it as candidate-specific spend.
    expect(screen.getByText("Industries funding groups reporting support")).toBeInTheDocument();
    expect(screen.getByText("Industries funding groups reporting opposition")).toBeInTheDocument();
    expect(
      screen.getByText("Industry amounts are contributions to these groups, not amounts necessarily spent on this candidate.")
    ).toBeInTheDocument();
    // Source enum renders as a display label with the provenance link; the
    // sync date lives at the top of the card now.
    expect(screen.getByText(/Source: FEC · 2026 cycle/)).toBeInTheDocument();
    expect(screen.queryByText(/synced/)).not.toBeInTheDocument();
    expect(screen.getByRole("link", { name: "fec.gov" })).toHaveAttribute(
      "href",
      "https://www.fec.gov/data/candidate/H0AK00001/"
    );
  });

  it("does not paint outside spending with the ballot-measure yes/no palette", () => {
    const summary = financeSummary();
    const { container } = render(<FinanceSummaryCard summary={summary} />);
    expect(container.querySelector(".bg-green-50")).toBeNull();
    expect(container.querySelector(".bg-red-50")).toBeNull();
  });

  it("collapses occupations past the first four behind a Show more disclosure", () => {
    const summary = emptyFinanceSummary();
    summary.direct_campaign.top_occupations = [
      { category_name: "Retired", amount: 60000, contributor_count: null, source_url: null },
      { category_name: "Attorney", amount: 50000, contributor_count: null, source_url: null },
      { category_name: "Physician", amount: 40000, contributor_count: null, source_url: null },
      { category_name: "Engineer", amount: 30000, contributor_count: null, source_url: null },
      { category_name: "Teacher", amount: 20000, contributor_count: null, source_url: null },
      { category_name: "Homemaker", amount: 10000, contributor_count: null, source_url: null },
    ];
    render(<FinanceSummaryCard summary={summary} />);

    const disclosure = screen.getByText("Show 2 more");
    expect(disclosure).toBeInTheDocument();
    // The overflow rows live inside the (closed) disclosure, not the top list.
    expect(screen.getByText("Teacher").closest("details")).not.toBeNull();
    expect(screen.getByText("Retired").closest("details")).toBeNull();
  });

  it("orders size buckets largest-first regardless of payload order", () => {
    const summary = emptyFinanceSummary();
    summary.direct_campaign.contribution_size_buckets = [
      { category_name: "$1-$99", amount: 10_000, contributor_count: null, source_url: null },
      { category_name: "$5,000+", amount: 40_000, contributor_count: null, source_url: null },
      { category_name: "$1,000-$4,999", amount: 30_000, contributor_count: null, source_url: null },
      { category_name: "$500-$999", amount: 20_000, contributor_count: null, source_url: null },
    ];
    render(<FinanceSummaryCard summary={summary} />);

    expect(screen.getByText("Direct contributions by size")).toBeInTheDocument();
    const labels = screen
      .getAllByRole("listitem")
      .map((item) => item.textContent ?? "")
      .filter((text) => text.includes("$"));
    expect(labels[0]).toContain("$5,000+");
    expect(labels[1]).toContain("$1,000-$4,999");
    expect(labels[2]).toContain("$500-$999");
    expect(labels[3]).toContain("$1-$99");
  });

  it("omits the opposition section when that direction has no disclosures", () => {
    const summary = financeSummary();
    summary.outside_spending.oppose_total = null;
    summary.outside_spending.top_opposing_groups = [];
    summary.outside_spending.top_opposing_industries = [];
    render(<FinanceSummaryCard summary={summary} />);

    expect(screen.getByText(/Reported support/)).toBeInTheDocument();
    expect(screen.queryByText(/Reported opposition/)).not.toBeInTheDocument();
  });

  it("falls back to the plain supporting-industries list without backing evidence", () => {
    const summary = financeSummary();
    delete summary.backing_summary;
    render(<FinanceSummaryCard summary={summary} />);

    expect(screen.getByText("Technology")).toBeInTheDocument();
    expect(screen.queryByText(/Money from/)).not.toBeInTheDocument();
  });

  it("renders an explicit $0 raised instead of hiding it", () => {
    const summary = emptyFinanceSummary();
    summary.direct_campaign.total_raised = 0;
    render(<FinanceSummaryCard summary={summary} />);

    expect(screen.getByText("Raised")).toBeInTheDocument();
    expect(screen.getByText("$0")).toBeInTheDocument();
    expect(screen.queryByText("Spent")).not.toBeInTheDocument();
  });

  it("renders NYC public funds and size buckets", () => {
    const summary = emptyFinanceSummary();
    summary.source = "NEW_YORK_CITY_CFB";
    summary.direct_campaign.public_funds_received = 250_000;
    summary.direct_campaign.contribution_size_buckets = [
      { category_name: "$1-$99", amount: 10_000, contributor_count: 200, source_url: null },
    ];
    render(<FinanceSummaryCard summary={summary} />);
    const publicFundsLabel = screen.getByText("Public funds");
    expect(publicFundsLabel).toBeInTheDocument();
    expect(publicFundsLabel.closest("dl")).toHaveClass("sm:grid-cols-5");
    expect(screen.getByText("$250,000")).toBeInTheDocument();
    expect(screen.getByText("Direct contributions by size")).toBeInTheDocument();
    expect(screen.getByText(/Source: NYC Campaign Finance Board/)).toBeInTheDocument();
  });
});
