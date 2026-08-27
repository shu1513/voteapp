import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

import { parseNevadaContributionCsv } from "../../../src/pipeline/nevadaFinance/nevadaAuroraCsv.js";
import { aggregateNevadaDirectContributions } from "../../../src/pipeline/nevadaFinance/nevadaDirectContributionAggregator.js";

const FIXTURES = new URL("../../fixtures/nevadaFinance/", import.meta.url);

describe("aggregateNevadaDirectContributions", () => {
  it("aggregates the Hansen Q2 fixture into exact size buckets", async () => {
    const rows = parseNevadaContributionCsv(
      await readFile(new URL("contributions-hansen-q2-2026.csv", FIXTURES), "utf8")
    );
    const result = aggregateNevadaDirectContributions({
      filerKey: "ALEXIS M HANSEN",
      periodStart: "2026-04-01",
      periodEnd: "2026-06-30",
      contributionRows: rows,
      sourceUrl: "https://example.test/hansen",
    });
    expect(result.directContributionTotalCents).toBe(18_000_00);
    expect(result.directContributionRowCount).toBe(11);
    expect(result.legalDefenseFundRowCount).toBe(0);
    expect(result.outOfWindowRowCount).toBe(0);

    const buckets = new Map(
      result.directBreakdowns
        .filter((breakdown) => breakdown.categoryType === "contribution_size")
        .map((breakdown) => [breakdown.categoryName, breakdown.amount])
    );
    expect(buckets.get("$1,000-$4,999")).toBe(13_000);
    expect(buckets.get("$5,000+")).toBe(5_000);
    expect([...buckets.values()].reduce((sum, amount) => sum + amount, 0)).toBe(18_000);

    // Industries are classifier-derived organization labels; each amount must
    // stay within the total, and industries sort before size buckets.
    const industries = result.directBreakdowns.filter(
      (breakdown) => breakdown.categoryType === "industry"
    );
    for (const industry of industries) {
      expect(industry.amount).toBeGreaterThan(0);
      expect(industry.amount).toBeLessThanOrEqual(18_000);
    }
    const types = result.directBreakdowns.map((breakdown) => breakdown.categoryType);
    expect(types.indexOf("contribution_size")).toBeGreaterThanOrEqual(
      types.lastIndexOf("industry") >= 0 ? types.lastIndexOf("industry") : 0
    );
  });

  it("filters by filer key, window, and Legal Defense Fund reports", () => {
    const base = {
      contributorName: "Someone",
      transactionType: "Monetary Contribution" as const,
      reportName: "2026 CE Report 2",
      isLegalDefenseFund: false,
    };
    const rows = [
      { ...base, date: "2026-05-01", amountCents: 10_00, filerName: "A B", filerKey: "A B" },
      { ...base, date: "2026-05-01", amountCents: 20_00, filerName: "Other", filerKey: "OTHER" },
      { ...base, date: "2027-01-01", amountCents: 40_00, filerName: "A B", filerKey: "A B" },
      {
        ...base,
        date: "2026-05-02",
        amountCents: 80_00,
        filerName: "A B",
        filerKey: "A B",
        reportName: "2026 CE Report 2 (Legal Defense Fund)",
        isLegalDefenseFund: true,
      },
    ];
    const result = aggregateNevadaDirectContributions({
      filerKey: "A B",
      periodStart: "2026-01-01",
      periodEnd: "2026-12-31",
      contributionRows: rows,
    });
    expect(result.directContributionTotalCents).toBe(10_00);
    expect(result.legalDefenseFundRowCount).toBe(1);
    expect(result.outOfWindowRowCount).toBe(1);
  });
});
