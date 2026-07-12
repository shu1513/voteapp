import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  parseHoustonCandidateFinancePdf,
  selectEffectiveHoustonCandidateReports,
} from "../../../src/pipeline/houstonFinance/houstonCampaignFinancePdfParser.js";
import type { HoustonFinanceParsedReport, HoustonFinanceSourceSystem } from "../../../src/pipeline/houstonFinance/houstonFinanceTypes.js";

const pdfFixture = vi.hoisted(() => ({ office: "MAYOR" }));

vi.mock("pdfjs-dist/legacy/build/pdf.mjs", () => ({
  getDocument: () => ({
    promise: Promise.resolve({
      numPages: 1,
      getPage: async () => ({
        getTextContent: async () => ({
          items: [
            "13 C/OH NAME Jane Doe 14 Filer ID",
            "10 ELECTION ELECTION DATE",
            "11/02/2027",
            "12 OFFICE SOUGHT",
            pdfFixture.office,
            "01/01/2026 THROUGH 06/30/2026",
            "2 TOTAL POLITICAL CONTRIBUTIONS $0.00",
          ].map((str, index) => ({ str, transform: [1, 0, 0, 1, 0, 700 - index * 20] })),
        }),
      }),
    }),
    destroy: async () => undefined,
  }),
}));

function report(sourceSystem: HoustonFinanceSourceSystem, reportType: string, filedAt: string, reportId: string): HoustonFinanceParsedReport {
  return { index: { sourceSystem, reportId, filerId: "f", filerName: "Jane Doe", filerType: "COH", reportType, receivedDate: "2026-01-01", filedAt, periodStart: "2026-01-01", periodEnd: "2026-06-30", officeDescription: "MAYOR", campaignYear: 2027, pdfUrl: null }, candidateName: "Jane Doe", electionDate: "2027-11-02", officeSought: { officeName: "Mayor", seat: "Houston" }, periodStart: "2026-01-01", periodEnd: "2026-06-30", directContributionTotal: 10, contributions: [] };
}

beforeEach(() => { pdfFixture.office = "MAYOR"; });

describe("Houston effective report selection", () => {
  it("uses corrections over originals", () => expect(selectEffectiveHoustonCandidateReports([report("legacy_webforms", "COH", "7/1/2026", "1"), report("legacy_webforms", "CORCOH", "7/2/2026", "2")])[0]?.index.reportId).toBe("2"));
  it("uses current eFile over overlapping legacy periods", () => expect(selectEffectiveHoustonCandidateReports([report("legacy_webforms", "COH", "7/2/2026", "1"), report("ethics_efile", "SEMIJUL", "2026/07/01", "2")])[0]?.index.reportId).toBe("2"));
});

describe("Houston PDF parsing", () => {
  it("accepts a legitimate zero-dollar cover-sheet total", async () => {
    const parsed = await parseHoustonCandidateFinancePdf({
      data: new Uint8Array([1]),
      index: report("ethics_efile", "SEMIJUL", "2026-07-01", "1").index,
    });

    expect(parsed.directContributionTotal).toBe(0);
  });

  it.each([
    ["Controller", { officeName: "City Controller", seat: "Houston" }],
    ["City Council - District C", { officeName: "City Council Member", seat: "District C" }],
    ["City Council Member, District, At Large 2", { officeName: "City Council Member", seat: "At-Large 2" }],
  ])("parses supported office target %s", async (office, expected) => {
    pdfFixture.office = office;
    const parsed = await parseHoustonCandidateFinancePdf({
      data: new Uint8Array([1]),
      index: report("ethics_efile", "SEMIJUL", "2026-07-01", "1").index,
    });
    expect(parsed.officeSought).toEqual(expected);
  });
});
