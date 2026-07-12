import { describe, expect, it } from "vitest";
import { selectEffectiveHoustonCandidateReports } from "../../../src/pipeline/houstonFinance/houstonCampaignFinancePdfParser.js";
import type { HoustonFinanceParsedReport, HoustonFinanceSourceSystem } from "../../../src/pipeline/houstonFinance/houstonFinanceTypes.js";

function report(sourceSystem: HoustonFinanceSourceSystem, reportType: string, filedAt: string, reportId: string): HoustonFinanceParsedReport {
  return { index: { sourceSystem, reportId, filerId: "f", filerName: "Jane Doe", filerType: "COH", reportType, receivedDate: "2026-01-01", filedAt, periodStart: "2026-01-01", periodEnd: "2026-06-30", officeDescription: "MAYOR", campaignYear: 2027, pdfUrl: null }, candidateName: "Jane Doe", electionDate: "2027-11-02", officeSought: "Mayor", periodStart: "2026-01-01", periodEnd: "2026-06-30", directContributionTotal: 10, contributions: [] };
}

describe("Houston effective report selection", () => {
  it("uses corrections over originals", () => expect(selectEffectiveHoustonCandidateReports([report("legacy_webforms", "COH", "7/1/2026", "1"), report("legacy_webforms", "CORCOH", "7/2/2026", "2")])[0]?.index.reportId).toBe("2"));
  it("uses current eFile over overlapping legacy periods", () => expect(selectEffectiveHoustonCandidateReports([report("legacy_webforms", "COH", "7/2/2026", "1"), report("ethics_efile", "SEMIJUL", "2026/07/01", "2")])[0]?.index.reportId).toBe("2"));
});
