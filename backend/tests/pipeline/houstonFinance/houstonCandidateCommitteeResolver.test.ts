import { describe, expect, it } from "vitest";
import { resolveHoustonCandidateCommittee } from "../../../src/pipeline/houstonFinance/houstonCandidateCommitteeResolver.js";
import type { HoustonFinanceParsedReport } from "../../../src/pipeline/houstonFinance/houstonFinanceTypes.js";

function report(source: "ethics_efile" | "legacy_webforms", filerId: string): HoustonFinanceParsedReport {
  return { index: { sourceSystem: source, reportId: "1", filerId, filerName: "John Whitmire", filerType: "COH", reportType: "COH", receivedDate: "2023-01-01", filedAt: "2023-01-01", periodStart: "2023-01-01", periodEnd: "2023-06-30", officeDescription: "MAYOR", campaignYear: 2023, pdfUrl: source === "ethics_efile" ? "https://example.test/a.pdf" : null }, candidateName: "John Whitmire", electionDate: "2023-11-07", officeSought: { officeName: "Mayor", seat: "Houston" }, periodStart: "2023-01-01", periodEnd: "2023-06-30", directContributionTotal: 10, contributions: [] };
}

describe("Houston candidate committee resolver", () => {
  it("uses stable internal legacy identity when legacy report IDs vary", () => {
    const result = resolveHoustonCandidateCommittee({ candidateName: "Whitmire, John", electionYear: 2023, reports: [report("legacy_webforms", "100"), report("legacy_webforms", "200")] });
    expect(result).toMatchObject({ status: "matched", committeeId: "legacy:JOHN WHITMIRE:2023" });
  });
  it("rejects multiple current filer IDs", () => {
    expect(resolveHoustonCandidateCommittee({ candidateName: "John Whitmire", electionYear: 2023, reports: [report("ethics_efile", "1"), report("ethics_efile", "2")] }).status).toBe("ambiguous");
  });
  it("does not resolve a report for a different Houston office", () => {
    expect(resolveHoustonCandidateCommittee({
      candidateName: "John Whitmire",
      electionYear: 2023,
      officeTarget: { officeName: "Municipal Controller", seat: "Houston" },
      reports: [report("legacy_webforms", "1")],
    }).status).toBe("not_found");
  });
});
