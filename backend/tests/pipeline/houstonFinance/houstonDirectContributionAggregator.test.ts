import { describe, expect, it } from "vitest";
import { aggregateHoustonDirectContributions } from "../../../src/pipeline/houstonFinance/houstonDirectContributionAggregator.js";
import type { HoustonFinanceParsedReport } from "../../../src/pipeline/houstonFinance/houstonFinanceTypes.js";

function report(total: number | null, contributions: HoustonFinanceParsedReport["contributions"]): HoustonFinanceParsedReport {
  return { index: { sourceSystem: "ethics_efile", reportId: "1", filerId: "f", filerName: "Jane Doe", filerType: "COH", reportType: "SEMIJUL", receivedDate: "2026-07-01", filedAt: "2026-07-01", periodStart: "2026-01-01", periodEnd: "2026-06-30", officeDescription: "MAYOR", campaignYear: null, pdfUrl: "https://example.test/report.pdf" }, candidateName: "Jane Doe", electionDate: "2027-11-02", officeSought: { officeName: "Mayor", seat: "Houston" }, periodStart: "2026-01-01", periodEnd: "2026-06-30", directContributionTotal: total, contributions };
}

describe("Houston direct contribution aggregation", () => {
  it("uses cover totals while deriving occupation and size rows from itemization", () => {
    const result = aggregateHoustonDirectContributions({ reports: [report(1500, [
      { contributionDate: "2026-01-01", contributorName: "A", amount: 500, occupation: "Attorney", sourceUrl: "https://example.test" },
      { contributionDate: "2026-01-02", contributorName: "B", amount: 250, occupation: "attorney", sourceUrl: "https://example.test" },
      { contributionDate: "2026-01-03", contributorName: "Company", amount: 500, occupation: null, sourceUrl: "https://example.test" },
    ])] });
    expect(result.directContributionTotal).toBe(1500);
    expect(result.directBreakdowns).toContainEqual(expect.objectContaining({ categoryType: "occupation", categoryName: "ATTORNEY", amount: 750, contributorCount: 2 }));
  });

  it("falls back per report to itemized totals when cover total is absent", () => {
    expect(aggregateHoustonDirectContributions({ reports: [report(null, [
      { contributionDate: "2026-01-01", contributorName: "A", amount: 100, occupation: "Teacher", sourceUrl: "x" },
    ])] }).directContributionTotal).toBe(100);
  });

  it("uses five contribution-size buckets so the public loader can return all of them", () => {
    const amounts = [50, 250, 500, 1_000, 5_000];
    const result = aggregateHoustonDirectContributions({ reports: [report(null, amounts.map((amount, index) => ({
      contributionDate: "2026-01-01",
      contributorName: `Donor ${index}`,
      amount,
      occupation: null,
      sourceUrl: "x",
    })))] });

    expect(result.directBreakdowns.filter((row) => row.categoryType === "contribution_size")).toHaveLength(5);
  });
});
