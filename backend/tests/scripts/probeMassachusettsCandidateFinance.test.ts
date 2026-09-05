import { afterEach, describe, expect, it, vi } from "vitest";

import {
  parseProbeMassachusettsCandidateFinanceArgs,
  runProbeMassachusettsCandidateFinance,
} from "../../src/scripts/probeMassachusettsCandidateFinance.js";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("probeMassachusettsCandidateFinance script", () => {
  it("parses live probe options and returns a safe empty result for unmatched candidates", async () => {
    const args = parseProbeMassachusettsCandidateFinanceArgs([
      "--candidate-name=Maura Healey",
      "--year",
      "2022",
      "--office=Governor",
      "--scope=statewide",
      "--limit=3",
      "--contribution-limit=1000",
      "--iepac-report-limit=7",
      "--min-industry-amount=25000",
      "--timeout-ms=5000",
    ]);

    expect(args).toEqual({
      candidateName: "Maura Healey",
      electionYear: 2022,
      officeScope: "statewide",
      officeName: "Governor",
      district: null,
      limit: 3,
      contributionItemLimit: 1000,
      iepacReportLimit: 7,
      minIndustryAmount: 25000,
      timeoutMs: 5000,
    });

    const client = {
      resolveCandidateCommittee: vi.fn(async () => ({
        status: "unmatched" as const,
        reason: "no_candidate_committee_match" as const,
        candidateNameNormalized: "MAURA HEALEY",
        officeNameNormalized: "STATEWIDE GOVERNOR",
      })),
    };

    const output = await runProbeMassachusettsCandidateFinance({
      args,
      client,
      now: new Date("2026-06-21T12:00:00.000Z"),
    });

    expect(output).toEqual({
      type: "massachusetts_candidate_finance_live_probe",
      ts: "2026-06-21T12:00:00.000Z",
      args,
      ok: false,
      resolution: {
        status: "unmatched",
        reason: "no_candidate_committee_match",
        candidateNameNormalized: "MAURA HEALEY",
        officeNameNormalized: "STATEWIDE GOVERNOR",
      },
      direct_campaign: {
        top_occupations: [],
        contribution_size_buckets: [],
      },
      outside_spending: {
        top_supporting_groups: [],
        top_opposing_groups: [],
        top_supporting_industries: [],
        top_opposing_industries: [],
        iepac_report_count: 0,
        iepac_report_detail_count: 0,
      },
    });
    expect(client.resolveCandidateCommittee).toHaveBeenCalledWith(
      expect.objectContaining({ candidateName: "Maura Healey", officeName: "Governor", electionYear: 2022 }),
      { timeoutMs: 5000 }
    );
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() =>
      parseProbeMassachusettsCandidateFinanceArgs([
        "--candidate-name=Maura Healey",
        "--year=2022",
        "--office=Governor",
        "--limit=9007199254740993",
      ])
    ).toThrow("Invalid --limit value: 9007199254740993");
  });

  it("builds a no-write probe summary with direct occupations and outside industry backtrace", async () => {
    const args = parseProbeMassachusettsCandidateFinanceArgs([
      "--candidate-name=Maura Healey",
      "--year=2022",
      "--office=Governor",
      "--limit=5",
      "--min-industry-amount=25000",
    ]);
    const client = {
      resolveCandidateCommittee: vi.fn(async () => ({
        status: "matched" as const,
        candidateCpfId: "15710",
        filerName: "Healey, Maura T.",
        committeeName: "Healey Committee",
        officeSought: "Statewide, Governor",
        confidence: "exact" as const,
        source: "ocpf_api" as const,
        sourceUrl: "https://api.ocpf.us/filers/listings/A?searchPhrase=Maura%20Healey",
        matchedFilerRowCount: 1,
      })),
      getContributionItems: vi.fn(async () => [
        {
          itemId: "1",
          reportId: 812510,
          cpfId: "15710",
          filerName: "Healey, Maura T.",
          contributorName: "Donor, Jane",
          contributorType: "Individual",
          occupation: "Attorney",
          employer: "Law Firm",
          recordTypeDescription: "Individual",
          amount: 250,
          date: "10/01/2022",
          sourceUrl: "https://www.ocpf.us/item/1",
        },
        {
          itemId: "2",
          reportId: 812511,
          cpfId: "15710",
          filerName: "Healey, Maura T.",
          contributorName: "Donor, John",
          contributorType: "Individual",
          occupation: "Attorney",
          employer: "Law Firm",
          recordTypeDescription: "Individual",
          amount: 500,
          date: "10/02/2022",
          sourceUrl: "https://www.ocpf.us/item/2",
        },
        {
          itemId: "3",
          reportId: 812512,
          cpfId: "15710",
          filerName: "Healey, Maura T.",
          contributorName: "PAC Donor",
          contributorType: "Committee",
          occupation: "",
          employer: "",
          recordTypeDescription: "Committee",
          amount: 1_000,
          date: "10/03/2022",
          sourceUrl: "https://www.ocpf.us/item/3",
        },
      ]),
      getIepacReportSummaries: vi.fn(async () => [
        {
          reportId: 858575,
          cpfId: "81068",
          committeeName: "Local 103 International Brotherhood of Electrical Workers Independent Expenditure PAC",
          reportYear: 2022,
          reportType: "IEPAC Report",
          reportingPeriod: "2022 Pre-election",
          candidateListing: "Maura T. Healey",
          candidateSpendingBreakdown: "Maura T. Healey (Supported) $32,420.00<br>",
          receiptsTotal: 32_420,
          expendituresTotal: 32_420,
          sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
        },
        {
          reportId: 999999,
          cpfId: "99999",
          committeeName: "Other IEPAC",
          reportYear: 2022,
          reportType: "IEPAC Report",
          reportingPeriod: "2022 Pre-election",
          candidateListing: "Other Candidate",
          candidateSpendingBreakdown: "Other Candidate (Supported) $50.00<br>",
          receiptsTotal: 50,
          expendituresTotal: 50,
          sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=999999",
        },
      ]),
      getReportDetail: vi.fn(async () => ({
        reportId: 858575,
        cpfId: "81068",
        committeeName: "Local 103 International Brotherhood of Electrical Workers Independent Expenditure PAC",
        reportYear: 2022,
        reportType: "IEPAC Report",
        reportingPeriod: "2022 Pre-election",
        candidateListing: "Maura T. Healey",
        candidateSpendingBreakdown: "Maura T. Healey (Supported) $32,420.00<br>",
        receiptsTotal: 32_420,
        expendituresTotal: 32_420,
        sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
        receipts: [
          {
            contributorName: "IBEW 103",
            contributorType: "Union/Association",
            recordTypeDescription: "Union/Association",
            amount: 32_420,
            date: "11/08/2022",
            sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
          },
        ],
        expenditures: [
          {
            affectedCandidateName: "Maura T. Healey",
            relatedCpfId: "15710",
            isSupported: true,
            recordTypeDescription: "Independent Expenditure",
            ieInfo: "support Maura T. Healey",
            amount: 32_420,
            date: "11/08/2022",
            sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
          },
        ],
      })),
    };

    const output = await runProbeMassachusettsCandidateFinance({
      args,
      client,
      now: new Date("2026-06-21T12:00:00.000Z"),
    });

    expect(output).toMatchObject({
      type: "massachusetts_candidate_finance_live_probe",
      ts: "2026-06-21T12:00:00.000Z",
      ok: true,
      resolution: { status: "matched", candidateCpfId: "15710" },
      direct_campaign: {
        top_occupations: [
          {
            category_name: "Attorney",
            amount: 750,
            contributor_count: 2,
          },
        ],
        contribution_size_buckets: [
          {
            category_name: "$500-$999",
            amount: 500,
            contributor_count: 1,
          },
          {
            category_name: "$250-$499",
            amount: 250,
            contributor_count: 1,
          },
        ],
      },
      outside_spending: {
        top_supporting_groups: [
          {
            iepac_cpf_id: "81068",
            iepac_name: "Local 103 International Brotherhood of Electrical Workers Independent Expenditure PAC",
            support_oppose: "support",
            amount: 32420,
          },
        ],
        top_supporting_industries: [
          {
            category_name: "labor_unions",
            industry_slug: "labor_unions",
            support_oppose: "support",
            amount: 32420,
            contributor_count: 1,
            evidence: [
              {
                organization_name: "IBEW 103",
                amount: 32420,
                contributor_count: 1,
                iepac_cpf_id: "81068",
                iepac_name: "Local 103 International Brotherhood of Electrical Workers Independent Expenditure PAC",
              },
            ],
          },
        ],
        iepac_report_count: 2,
        iepac_report_detail_count: 1,
      },
    });
    expect(client.getReportDetail).toHaveBeenCalledTimes(1);
    expect(client.getReportDetail).toHaveBeenCalledWith({ reportId: 858575 }, { timeoutMs: 30000 });
  });

  it("continues probing when one OCPF IE PAC report detail fails", async () => {
    const args = parseProbeMassachusettsCandidateFinanceArgs([
      "--candidate-name=Maura Healey",
      "--year=2022",
      "--office=Governor",
      "--iepac-report-limit=2",
    ]);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const client = {
      resolveCandidateCommittee: vi.fn(async () => ({
        status: "matched" as const,
        candidateCpfId: "15710",
        filerName: "Healey, Maura T.",
        committeeName: "Healey Committee",
        officeSought: "Statewide, Governor",
        confidence: "exact" as const,
        source: "ocpf_api" as const,
        sourceUrl: "https://api.ocpf.us/filers/listings/A?searchPhrase=Maura%20Healey",
        matchedFilerRowCount: 1,
      })),
      getContributionItems: vi.fn(async () => []),
      getIepacReportSummaries: vi.fn(async () => [
        {
          reportId: 858575,
          cpfId: "81068",
          committeeName: "Local 103 International Brotherhood of Electrical Workers Independent Expenditure PAC",
          reportYear: 2022,
          reportType: "IEPAC Report",
          reportingPeriod: "2022 Pre-election",
          candidateListing: "Maura T. Healey",
          candidateSpendingBreakdown: "Maura T. Healey (Supported) $32,420.00<br>",
          receiptsTotal: 32_420,
          expendituresTotal: 32_420,
          sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
        },
        {
          reportId: 858576,
          cpfId: "81069",
          committeeName: "Other Maura IEPAC",
          reportYear: 2022,
          reportType: "IEPAC Report",
          reportingPeriod: "2022 Pre-election",
          candidateListing: "Maura T. Healey",
          candidateSpendingBreakdown: "Maura T. Healey (Opposed) $12,000.00<br>",
          receiptsTotal: 12_000,
          expendituresTotal: 12_000,
          sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858576",
        },
      ]),
      getReportDetail: vi.fn(async ({ reportId }: { reportId: number }) => {
        if (reportId === 858576) {
          throw new Error("temporary OCPF report failure");
        }
        return {
          reportId: 858575,
          cpfId: "81068",
          committeeName: "Local 103 International Brotherhood of Electrical Workers Independent Expenditure PAC",
          reportYear: 2022,
          reportType: "IEPAC Report",
          reportingPeriod: "2022 Pre-election",
          candidateListing: "Maura T. Healey",
          candidateSpendingBreakdown: "Maura T. Healey (Supported) $32,420.00<br>",
          receiptsTotal: 32_420,
          expendituresTotal: 32_420,
          sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
          receipts: [],
          expenditures: [
            {
              affectedCandidateName: "Maura T. Healey",
              relatedCpfId: "15710",
              isSupported: true,
              recordTypeDescription: "Independent Expenditure",
              ieInfo: "support Maura T. Healey",
              amount: 32_420,
              date: "11/08/2022",
              sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
            },
          ],
        };
      }),
    };

    const output = await runProbeMassachusettsCandidateFinance({
      args,
      client,
      now: new Date("2026-06-21T12:00:00.000Z"),
    });

    expect(output).toMatchObject({
      ok: true,
      outside_spending: {
        iepac_report_count: 2,
        iepac_report_detail_count: 1,
        top_supporting_groups: [expect.objectContaining({ amount: 32420 })],
      },
    });
    expect(client.getReportDetail).toHaveBeenCalledTimes(2);
    expect(warn).toHaveBeenCalledWith(
      "Massachusetts finance live probe skipped OCPF report detail reportId=858576: temporary OCPF report failure"
    );
  });
});
