import { beforeEach, describe, expect, it, vi } from "vitest";

import type { HoustonFinanceParsedReport, HoustonFinanceReportIndexRecord } from "../../../src/pipeline/houstonFinance/houstonFinanceTypes.js";

const mocks = vi.hoisted(() => ({
  downloadEfile: vi.fn(),
  searchLegacy: vi.fn(),
  parsePdf: vi.fn(),
}));

vi.mock("../../../src/pipeline/houstonFinance/houstonEthicsEfileClient.js", () => ({
  downloadHoustonEthicsEfileReportPdf: mocks.downloadEfile,
  listHoustonEthicsEfileReports: vi.fn(),
}));

vi.mock("../../../src/pipeline/houstonFinance/houstonLegacyCampaignFinanceClient.js", () => ({
  downloadHoustonLegacyReportPdf: vi.fn(),
  searchHoustonLegacyCandidateReports: mocks.searchLegacy,
}));

vi.mock("../../../src/pipeline/houstonFinance/houstonCampaignFinancePdfCache.js", () => ({
  DEFAULT_HOUSTON_FINANCE_PDF_CACHE_DIR: "/tmp/houston-finance-test",
  readCachedHoustonFinancePdf: vi.fn().mockResolvedValue(null),
  validateHoustonFinancePdf: vi.fn(),
  cacheHoustonFinancePdf: vi.fn(),
}));

vi.mock("../../../src/pipeline/houstonFinance/houstonCampaignFinancePdfParser.js", () => ({
  parseHoustonCandidateFinancePdf: mocks.parsePdf,
}));

import { loadHoustonCandidateFinanceReports } from "../../../src/pipeline/houstonFinance/houstonCampaignFinanceReportSource.js";

function index(reportId: string): HoustonFinanceReportIndexRecord {
  return {
    sourceSystem: "ethics_efile",
    reportId,
    filerId: "filer-1",
    filerName: "Jane Doe",
    filerType: "COH",
    reportType: "SEMIJUL",
    receivedDate: "2026-07-01",
    filedAt: "2026-07-01",
    periodStart: "2026-01-01",
    periodEnd: "2026-06-30",
    officeDescription: "MAYOR",
    campaignYear: null,
    pdfUrl: `https://example.test/${reportId}.pdf`,
  };
}

function parsed(reportIndex: HoustonFinanceReportIndexRecord): HoustonFinanceParsedReport {
  return {
    index: reportIndex,
    candidateName: "Jane Doe",
    electionDate: "2027-11-02",
    officeSought: "Mayor",
    periodStart: "2026-01-01",
    periodEnd: "2026-06-30",
    directContributionTotal: 10,
    contributions: [],
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  mocks.downloadEfile.mockResolvedValue(new Uint8Array([1]));
  mocks.searchLegacy.mockResolvedValue({ reports: [] });
});

describe("Houston candidate finance report source", () => {
  it("keeps valid reports when another report is malformed", async () => {
    const reports = [index("1"), index("2")];
    mocks.parsePdf
      .mockRejectedValueOnce(new Error("not a Mayor filing"))
      .mockResolvedValueOnce(parsed(reports[1]!));

    await expect(loadHoustonCandidateFinanceReports({
      candidateName: "Jane Doe",
      firstName: "Jane",
      lastName: "Doe",
      electionYear: 2027,
      efileReports: reports,
    })).resolves.toEqual([parsed(reports[1]!)]);
  });

  it("fails the direct source when every discovered report fails", async () => {
    mocks.parsePdf.mockRejectedValue(new Error("malformed report"));

    await expect(loadHoustonCandidateFinanceReports({
      candidateName: "Jane Doe",
      firstName: "Jane",
      lastName: "Doe",
      electionYear: 2027,
      efileReports: [index("1")],
    })).rejects.toThrow("All 1 Houston candidate finance reports failed");
  });
});
