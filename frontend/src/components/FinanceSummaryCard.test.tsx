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
    // Support and opposition stay distinct, with a plain-language intro. The
    // independence claim attaches to the spending, not the groups — PACs can
    // also coordinate or give directly; the expenditure shown here doesn't.
    expect(screen.getByText("Spending by outside groups")).toBeInTheDocument();
    expect(
      screen.getByText(/Money spent on this race by outside groups/)
    ).toBeInTheDocument();
    expect(
      screen.getByText(/This spending is not coordinated with the candidate's campaign/)
    ).toBeInTheDocument();
    expect(
      screen.getByText("Outside money spent supporting this candidate: $50,000")
    ).toBeInTheDocument();
    expect(screen.getByText("Growth PAC")).toBeInTheDocument();
    expect(
      screen.getByText("Outside money spent opposing this candidate: $20,000")
    ).toBeInTheDocument();
    expect(screen.getByText("Stop Them PAC")).toBeInTheDocument();
    // Backing evidence names the organizations behind a supporting industry.
    // The fixture rows are employer-type: individuals who reported those
    // employers gave — the sentence must not read as the companies donating.
    expect(screen.getByText("Technology")).toBeInTheDocument();
    expect(
      screen.getByText("Money from contributors employed by Google and Anthropic, given to Growth PAC.")
    ).toBeInTheDocument();
    // Industry money flows into the groups, not necessarily onto this race —
    // the heading and note must not present it as candidate-specific spend.
    expect(screen.getByText("Industries funding these supporting groups")).toBeInTheDocument();
    expect(screen.getByText("Industries funding these opposing groups")).toBeInTheDocument();
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

  it("explains prior-cycle money only when spending tops raised plus public funds", () => {
    const summary = financeSummary();
    summary.direct_campaign.total_raised = 40000;
    summary.direct_campaign.total_spent = 90000;
    const { rerender } = render(<FinanceSummaryCard summary={summary} />);
    expect(
      screen.getByText(/money not counted in Raised/)
    ).toBeInTheDocument();

    // Public matching money accounts for the gap — no note.
    const funded = financeSummary();
    funded.direct_campaign.total_raised = 40000;
    funded.direct_campaign.total_spent = 90000;
    funded.direct_campaign.public_funds_received = 60000;
    rerender(<FinanceSummaryCard summary={funded} />);
    expect(
      screen.queryByText(/money not counted in Raised/)
    ).not.toBeInTheDocument();

    // Normal case (fixture raises more than it spends) — no note.
    rerender(<FinanceSummaryCard summary={financeSummary()} />);
    expect(
      screen.queryByText(/money not counted in Raised/)
    ).not.toBeInTheDocument();
  });

  it("shows the outside coverage note only when the source discloses a gap", () => {
    // No note by default: most sources have no known systematic gap, and an
    // unconditional caveat would understate data that is in fact complete.
    const { rerender } = render(<FinanceSummaryCard summary={financeSummary()} />);
    expect(screen.queryByText(/not included yet/)).not.toBeInTheDocument();

    const summary = financeSummary();
    summary.outside_spending.outside_coverage_note =
      "Covers outside spending reported by committees registered with the Ohio Secretary of State. " +
      "Groups that spend without registering file separately and are not included yet.";
    rerender(<FinanceSummaryCard summary={summary} />);
    const note = screen.getByText(/Groups that spend without registering/);
    expect(note).toBeInTheDocument();
    // It must sit with the totals, not down in the source footnote — a
    // reader who sees the dollar figure has to see the caveat.
    const outsideHeading = screen.getByText("Spending by outside groups");
    const supportTotal = screen.getByText(/Outside money spent supporting this candidate/);
    expect(
      outsideHeading.compareDocumentPosition(note) & Node.DOCUMENT_POSITION_FOLLOWING
    ).toBeTruthy();
    expect(note.compareDocumentPosition(supportTotal) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();

    // The note qualifies SHOWN figures. A summary whose only outside field
    // is the note renders no outside section and no note: with nothing
    // shown the card asserts nothing about outside money, so there is
    // nothing to caveat (same rule that hides a $0 direction). Deliberate —
    // pinned so a future edit doesn't silently change it in either
    // direction.
    const noteOnly = emptyFinanceSummary();
    noteOnly.direct_campaign.total_raised = 100;
    noteOnly.outside_spending.outside_coverage_note = summary.outside_spending.outside_coverage_note;
    rerender(<FinanceSummaryCard summary={noteOnly} />);
    expect(screen.queryByText("Spending by outside groups")).not.toBeInTheDocument();
    expect(screen.queryByText(/Groups that spend without registering/)).not.toBeInTheDocument();
  });

  it("shows the direct coverage note only with breakdowns to qualify", () => {
    // No note by default: most sources itemize everything the totals count.
    const { rerender } = render(<FinanceSummaryCard summary={financeSummary()} />);
    expect(screen.queryByText(/not shown in the breakdowns/)).not.toBeInTheDocument();

    const summary = financeSummary();
    summary.direct_campaign.direct_coverage_note =
      "Donor breakdowns reflect itemized contributions reported to Georgia's current filing system " +
      "(July 2025 onward). Official totals are cumulative and can include earlier or non-itemized money " +
      "not shown in the breakdowns.";
    rerender(<FinanceSummaryCard summary={summary} />);
    expect(screen.getByText(/not shown in the breakdowns/)).toBeInTheDocument();

    // The note qualifies the BREAKDOWNS. A summary with totals but no
    // occupation/size rows shows no note: under totals alone the card
    // asserts nothing about itemization.
    const noteOnly = emptyFinanceSummary();
    noteOnly.direct_campaign.total_raised = 100;
    noteOnly.direct_campaign.direct_coverage_note = summary.direct_campaign.direct_coverage_note;
    rerender(<FinanceSummaryCard summary={noteOnly} />);
    expect(screen.queryByText(/not shown in the breakdowns/)).not.toBeInTheDocument();
  });

  it("color-codes outside support green and opposition red", () => {
    const summary = financeSummary();
    const { container } = render(<FinanceSummaryCard summary={summary} />);
    const supportBox = screen
      .getByText("Outside money spent supporting this candidate: $50,000")
      .closest("div");
    const opposeBox = screen
      .getByText("Outside money spent opposing this candidate: $20,000")
      .closest("div");
    expect(supportBox).toHaveClass("bg-green-50");
    expect(opposeBox).toHaveClass("bg-red-50");
    // Exactly one box per direction — the tint marks direction, nothing else.
    expect(container.querySelectorAll(".bg-green-50")).toHaveLength(1);
    expect(container.querySelectorAll(".bg-red-50")).toHaveLength(1);
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

    expect(screen.getByText(/Outside money spent supporting/)).toBeInTheDocument();
    expect(screen.queryByText(/Outside money spent opposing/)).not.toBeInTheDocument();
  });

  it("hides a direction whose disclosed total is $0 with nothing behind it", () => {
    const summary = financeSummary();
    summary.outside_spending.oppose_total = 0;
    summary.outside_spending.top_opposing_groups = [];
    summary.outside_spending.top_opposing_industries = [];
    render(<FinanceSummaryCard summary={summary} />);

    // "$0 opposing this candidate" is noise, not information — the box hides.
    expect(screen.getByText(/Outside money spent supporting/)).toBeInTheDocument();
    expect(screen.queryByText(/Outside money spent opposing/)).not.toBeInTheDocument();
  });

  it("keeps a $0-total direction visible when it has groups, but drops the $0 from the heading", () => {
    const summary = financeSummary();
    summary.outside_spending.oppose_total = 0;
    render(<FinanceSummaryCard summary={summary} />);

    // The fixture's opposing group still discloses spending, so the box stays;
    // a "$0" amount on the heading would contradict the rows beneath it.
    const heading = screen.getByText(/Outside money spent opposing this candidate/);
    expect(heading).toBeInTheDocument();
    expect(heading.textContent).not.toContain("$0");
    expect(screen.getByText("Stop Them PAC")).toBeInTheDocument();
  });

  it("shows a researched committee label with its source links, and nothing when unlabeled", () => {
    const summary = financeSummary();
    summary.outside_spending.top_supporting_groups = [
      {
        ...summary.outside_spending.top_supporting_groups[0],
        label: "Super PAC funded primarily by real-estate developers",
        label_source_urls: ["https://www.opensecrets.org/pacs/lookup"],
      },
    ];
    render(<FinanceSummaryCard summary={summary} />);

    expect(screen.getByText("Growth PAC")).toBeInTheDocument();
    // The label is a factual claim, so its evidence renders beside it.
    const labelLine = screen.getByText(/Super PAC funded primarily by real-estate developers/);
    expect(labelLine).toBeInTheDocument();
    const sourceLink = screen.getByRole("link", { name: "opensecrets.org" });
    expect(sourceLink).toHaveAttribute("href", "https://www.opensecrets.org/pacs/lookup");
    // The unlabeled opposing group renders its name with no description
    // paragraph and no evidence links.
    const unlabeledItem = screen.getByText("Stop Them PAC").closest("li");
    expect(unlabeledItem).not.toBeNull();
    expect(unlabeledItem?.querySelector("p")).toBeNull();
    expect(unlabeledItem?.querySelector("a")).toBeNull();
  });

  it("keeps donor money direct and pairs each organization with its own committee", () => {
    const summary = financeSummary();
    summary.backing_summary = {
      top_outside_supporting_industries: [
        {
          category_name: "technology",
          amount: 35000,
          contributor_count: null,
          source_url: null,
          supporting_organizations: [
            {
              organization_name: "Google",
              organization_type: "donor",
              amount: 20000,
              contributor_count: null,
              committee_id: "pac-1",
              committee_name: "Growth PAC",
              source_url: null,
            },
            {
              organization_name: "Anthropic",
              organization_type: "employer",
              amount: 15000,
              contributor_count: 2,
              committee_id: "pac-3",
              committee_name: "Future PAC",
              source_url: null,
            },
          ],
        },
      ],
    };
    render(<FinanceSummaryCard summary={summary} />);

    // Two committees, two lines — no flattening Google onto Future PAC, and
    // the employer-type row never reads as Anthropic itself donating.
    expect(screen.getByText("Money from Google, given to Growth PAC.")).toBeInTheDocument();
    expect(
      screen.getByText("Money from contributors employed by Anthropic, given to Future PAC.")
    ).toBeInTheDocument();
  });

  it("falls back to the plain supporting-industries list without backing evidence", () => {
    const summary = financeSummary();
    delete summary.backing_summary;
    render(<FinanceSummaryCard summary={summary} />);

    expect(screen.getByText("Technology")).toBeInTheDocument();
    expect(screen.queryByText(/Money from/)).not.toBeInTheDocument();
  });

  it("renders member communications as its own section, hiding the $0 side", () => {
    const summary = emptyFinanceSummary();
    summary.outside_spending.membership_support_total = 203457;
    summary.outside_spending.membership_oppose_total = 0;
    render(<FinanceSummaryCard summary={summary} />);

    expect(screen.getByText("Member communications")).toBeInTheDocument();
    expect(
      screen.getByText(/spending to talk to their own members about this candidate/)
    ).toBeInTheDocument();
    expect(screen.getByText("Spent supporting this candidate: $203,457")).toBeInTheDocument();
    // The $0 oppose side hides (the LA sync writes 0 for every candidate),
    // and member money must not appear under the outside-groups section —
    // it is legally distinct from independent expenditures.
    expect(screen.queryByText(/Spent opposing this candidate/)).not.toBeInTheDocument();
    expect(screen.queryByText("Spending by outside groups")).not.toBeInTheDocument();
  });

  it("renders no member-communications section when nothing was reported", () => {
    const summary = financeSummary();
    render(<FinanceSummaryCard summary={summary} />);
    expect(screen.queryByText("Member communications")).not.toBeInTheDocument();
  });

  it("renders an explicit $0 raised instead of hiding it", () => {
    const summary = emptyFinanceSummary();
    summary.direct_campaign.total_raised = 0;
    render(<FinanceSummaryCard summary={summary} />);

    expect(screen.getByText("Raised")).toBeInTheDocument();
    expect(screen.getByText("$0")).toBeInTheDocument();
    expect(screen.queryByText("Spent")).not.toBeInTheDocument();
  });

  it("renders a Loans stat with its explainer only when loans are positive", () => {
    // Self-funder: tiny donations, huge loans (the Perry Johnson shape).
    const summary = emptyFinanceSummary();
    summary.source = "MICHIGAN_MITN";
    summary.direct_campaign.total_raised = 28_699.83;
    summary.direct_campaign.loans_received = 30_752_614;
    render(<FinanceSummaryCard summary={summary} />);
    const loansLabel = screen.getByText("Loans");
    expect(loansLabel).toBeInTheDocument();
    expect(loansLabel.closest("dl")).toHaveClass("sm:grid-cols-5");
    expect(screen.getByText("$30,752,614")).toBeInTheDocument();
    expect(screen.getByText(/borrowed money the campaign reported receiving/)).toBeInTheDocument();
  });

  it("hides the Loans stat for zero and for sources that do not report loans", () => {
    // Known zero (source covers loans, candidate has none): hidden, no $0 noise.
    const zeroLoans = financeSummary();
    zeroLoans.direct_campaign.loans_received = 0;
    const { rerender } = render(<FinanceSummaryCard summary={zeroLoans} />);
    expect(screen.queryByText("Loans")).not.toBeInTheDocument();
    expect(screen.queryByText(/borrowed money/)).not.toBeInTheDocument();

    // Field absent (source does not report loans): hidden.
    rerender(<FinanceSummaryCard summary={financeSummary()} />);
    expect(screen.queryByText("Loans")).not.toBeInTheDocument();
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
