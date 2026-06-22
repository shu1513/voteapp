import { describe, expect, it } from "vitest";

import {
  parseProbeOklahomaGuardianIeReportsScriptArgs,
  toProbeOklahomaGuardianIeReportsScriptOutput,
} from "../../src/scripts/probeOklahomaGuardianIeReports.js";

describe("probeOklahomaGuardianIeReports script", () => {
  it("parses required and optional flags", () => {
    expect(
      parseProbeOklahomaGuardianIeReportsScriptArgs([
        "--candidate-name",
        "Kevin Stitt",
        "--year=2022",
        "--max-reports",
        "3",
      ])
    ).toEqual({
      candidateName: "Kevin Stitt",
      year: 2022,
      maxReports: 3,
    });
  });

  it("rejects missing or malformed flags", () => {
    expect(() => parseProbeOklahomaGuardianIeReportsScriptArgs(["--year=2022"])).toThrow(
      "--candidate-name is required"
    );
    expect(() =>
      parseProbeOklahomaGuardianIeReportsScriptArgs(["--candidate-name=Kevin Stitt"])
    ).toThrow("--year must be an election year");
    expect(() =>
      parseProbeOklahomaGuardianIeReportsScriptArgs(["--candidate-name=Kevin Stitt", "--year=2022abc"])
    ).toThrow("Invalid --year value");
    expect(() =>
      parseProbeOklahomaGuardianIeReportsScriptArgs([
        "--candidate-name=Kevin Stitt",
        "--year=2022",
        "--max-reports=0",
      ])
    ).toThrow("Invalid --max-reports value");
  });

  it("formats probe output without exposing raw PDF text", () => {
    const output = toProbeOklahomaGuardianIeReportsScriptOutput({
      startedAt: new Date("2026-01-02T03:04:05.000Z"),
      options: {
        candidateName: "Kevin Stitt",
        year: 2022,
        maxReports: 2,
      },
      result: {
        search: {
          candidateName: "Kevin Stitt",
          dateFrom: "01/01/2022",
          dateThrough: "12/31/2022",
          expenditureType: "independent_expenditure",
          rows: [
            {
              filerName: "THE OKLAHOMA PROJECT",
              reportDescription: "Independent Expenditure",
              periodBegin: "01/01/2022",
              periodEnd: "12/31/2022",
              filedDate: "03/01/2022",
              viewReportPostbackTarget: "ctl00$MainContent$GridView1$ctl02$lnkView",
            },
          ],
          sourceUrl: "https://guardian.ok.gov/PublicSite/SearchPages/Search.aspx",
        },
        reportsExamined: 1,
        usableReports: [
          {
            rowIndex: 0,
            sourceRow: {
              filerName: "THE OKLAHOMA PROJECT",
              reportDescription: "Independent Expenditure",
              periodBegin: "01/01/2022",
              periodEnd: "12/31/2022",
              filedDate: "03/01/2022",
              viewReportPostbackTarget: "ctl00$MainContent$GridView1$ctl02$lnkView",
            },
            spenderName: "THE OKLAHOMA PROJECT",
            candidateName: "Kevin Stitt",
            officeName: "Governor",
            supportOppose: "support",
            amount: 1234.56,
            reportingPeriodBegin: "01/01/2022",
            reportingPeriodEnd: "12/31/2022",
            reportDescription: "Independent expenditure",
            amended: false,
            sourceUrl: "https://guardian.ok.gov/PublicSite/report.pdf",
            pdfByteLength: 12345,
          },
        ],
        skippedReports: [],
      },
    });

    expect(output).toMatchObject({
      type: "oklahoma_guardian_ie_report_probe",
      started_at: "2026-01-02T03:04:05.000Z",
      candidate_name: "Kevin Stitt",
      election_year: 2022,
      max_reports: 2,
      search: {
        source_url: "https://guardian.ok.gov/PublicSite/SearchPages/Search.aspx",
        date_from: "01/01/2022",
        date_through: "12/31/2022",
        result_count: 1,
      },
      reports_examined: 1,
      usable_reports: [
        {
          spender_name: "THE OKLAHOMA PROJECT",
          support_oppose: "support",
          amount: 1234.56,
          pdf_byte_length: 12345,
        },
      ],
      skipped_reports: [],
    });
  });
});
