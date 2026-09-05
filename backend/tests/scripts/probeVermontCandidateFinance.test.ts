import { afterEach, describe, expect, it, vi } from "vitest";

import {
  parseProbeVermontCandidateFinanceArgs,
  runProbeVermontCandidateFinance,
  sumCsvAmountColumnFromText,
} from "../../src/scripts/probeVermontCandidateFinance.js";
import type { VermontContributionRow, VermontExpenditureRow } from "../../src/pipeline/vermontFinance/vermontCampaignFinanceClient.js";

afterEach(() => {
  vi.restoreAllMocks();
});

function contributionRow(overrides: Partial<VermontContributionRow>): VermontContributionRow {
  return {
    transactionId: 1,
    transactionVersionId: null,
    guid: "contribution-guid",
    filerRegistrationGuid: "candidate-guid",
    filerName: "Jane Candidate",
    transactionAmount: 100,
    transactionDate: "2026-01-01",
    sourceName: "Donor, Test",
    sourceFirstName: null,
    sourceLastName: null,
    sourceMiddleName: null,
    transactionSource: "Individual",
    transactionSourceTypeCode: "TIND",
    transactionSubTypeCode: null,
    transactionSubTypeDescription: null,
    filerTypeCode: "CAN",
    filerTypeDescription: "Candidate",
    electionYear: 2026,
    electionCycle: null,
    electionId: 2026,
    officeId: 1,
    officeType: null,
    entityId: 444,
    reportName: "2026 Report",
    candidateFirstName: "Jane",
    candidateLastName: "Candidate",
    candidateMiddleName: null,
    occupation: null,
    employer: null,
    filingYear: 2026,
    addressLine1: "1 Main St",
    addressLine2: null,
    city: "Montpelier",
    stateCode: "VT",
    zipCode: "05602",
    ...overrides,
  };
}

function expenditureRow(overrides: Partial<VermontExpenditureRow>): VermontExpenditureRow {
  return {
    transactionId: 2,
    transactionVersionId: null,
    guid: "expenditure-guid",
    filerRegistrationGuid: "pac-guid",
    filerName: "Workers PAC",
    transactionAmount: 1_000,
    transactionDate: "2026-02-01",
    transactionCategoryCode: "CONTR",
    transactionCategoryDescription: "Contribution",
    expenditurePurpose: "Contribution to candidate",
    description: "Contribution supporting Jane Candidate",
    isStanceSupport: null,
    payeeType: "Candidate",
    sourceName: "Jane Candidate",
    transactionSource: null,
    filerTypeCode: "PAC",
    filerTypeDescription: "PAC",
    electionYear: 2026,
    electionCycle: null,
    electionId: 2026,
    officeId: 1,
    officeType: null,
    entityId: 444,
    reportName: "2026 Report",
    candidateMentioned: "Jane Candidate",
    candidateFirstName: "Jane",
    candidateLastName: "Candidate",
    candidateMiddleName: null,
    sourceAddressLine1: null,
    sourceAddressLine2: null,
    sourceCity: null,
    sourceState: null,
    sourceZipCode: null,
    ...overrides,
  };
}

describe("probeVermontCandidateFinance script", () => {
  it("parses live probe options and returns a safe empty result for unmatched candidates", async () => {
    const args = parseProbeVermontCandidateFinanceArgs([
      "--candidate-name=Jane Candidate",
      "--year",
      "2026",
      "--office=Governor",
      "--scope=statewide",
      "--limit=3",
      "--page-size=25",
      "--max-pages=4",
      "--outside-group-max-pages=2",
      "--min-industry-amount=5000",
      "--timeout-ms=5000",
      "--direct-csv=/tmp/direct.csv",
      "--outside-support-csv=/tmp/outside.csv",
      "--csv-amount-column=Transaction Amount",
      "--csv-tolerance=0.05",
    ]);

    expect(args).toEqual({
      candidateName: "Jane Candidate",
      electionYear: 2026,
      officeScope: "statewide",
      officeName: "Governor",
      district: null,
      limit: 3,
      pageSize: 25,
      maxPages: 4,
      outsideGroupMaxPages: 2,
      minIndustryAmount: 5000,
      timeoutMs: 5000,
      directCsvPath: "/tmp/direct.csv",
      outsideSupportCsvPath: "/tmp/outside.csv",
      csvAmountColumn: "Transaction Amount",
      csvTolerance: 0.05,
    });

    const client = {
      resolveCandidateCommittee: vi.fn(async () => ({
        status: "unmatched" as const,
        reason: "no_candidate_committee_match" as const,
        candidateNameNormalized: "JANE CANDIDATE",
        officeNameNormalized: "GOVERNOR",
      })),
    };

    const output = await runProbeVermontCandidateFinance({
      args,
      client,
      now: new Date("2026-06-25T12:00:00.000Z"),
    });

    expect(output).toEqual({
      type: "vermont_candidate_finance_live_probe",
      ts: "2026-06-25T12:00:00.000Z",
      args,
      ok: false,
      resolution: {
        status: "unmatched",
        reason: "no_candidate_committee_match",
        candidateNameNormalized: "JANE CANDIDATE",
        officeNameNormalized: "GOVERNOR",
      },
      validation: {
        csv_comparisons: [],
        csv_comparison_ok: null,
      },
      rows_loaded: {
        candidate_contributions: 0,
        expenditure_rows: 0,
        outside_group_contributions: 0,
      },
      direct_campaign: {
        total_receipts: 0,
        direct_contribution_total: 0,
        top_occupations: [],
        contributor_source_types: [],
        contribution_size_buckets: [],
      },
      outside_spending: {
        support_total: 0,
        oppose_total: 0,
        top_supporting_groups: [],
        top_opposing_groups: [],
        top_supporting_industries: [],
        top_opposing_industries: [],
      },
      counters: {
        direct_matched_rows: 0,
        direct_included_rows: 0,
        direct_skipped_rows: 0,
        outside_matched_rows: 0,
        outside_included_rows: 0,
        outside_skipped_rows: 0,
        outside_group_matched_rows: 0,
        outside_group_included_rows: 0,
        outside_group_skipped_rows: 0,
      },
    });
    expect(client.resolveCandidateCommittee).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateName: "Jane Candidate",
        officeName: "Governor",
        district: null,
        electionYear: 2026,
      }),
      { timeoutMs: 5000 }
    );
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() =>
      parseProbeVermontCandidateFinanceArgs([
        "--candidate-name=Jane Candidate",
        "--year=2026",
        "--office=Governor",
        "--limit=9007199254740993",
      ])
    ).toThrow("Invalid --limit value: 9007199254740993");
  });

  it("builds a no-write Vermont validation summary with CSV total comparisons", async () => {
    const args = parseProbeVermontCandidateFinanceArgs([
      "--candidate-name=Jane Candidate",
      "--year=2026",
      "--office=Governor",
      "--limit=5",
      "--page-size=2",
      "--max-pages=2",
      "--direct-csv=/exports/direct.csv",
      "--outside-support-csv=/exports/outside.csv",
      "--csv-amount-column=Transaction Amount",
    ]);
    const client = {
      resolveCandidateCommittee: vi.fn(async () => ({
        status: "matched" as const,
        filerRegistrationGuid: "candidate-guid",
        filerName: "Jane Candidate",
        candidateName: "Jane Candidate",
        officeId: 1,
        officeName: "Governor",
        officeDisplayName: "Governor",
        electionYear: 2026,
        electionId: 2026,
        entityId: 444,
        reportName: "2026 Report",
        confidence: "exact" as const,
        source: "vermont_public_transactions" as const,
        sourceUrl: "https://campaignfinance.vermont.gov/",
        matchedTransactionRowCount: 1,
      })),
      getContributionDetails: vi.fn(async () => ({
        items: [
          contributionRow({ guid: "c-1", transactionAmount: 100, transactionSource: "Individual" }),
          contributionRow({
            guid: "c-2",
            transactionId: 3,
            transactionAmount: 500,
            transactionSource: "Political Committee",
            transactionSourceTypeCode: "TPAC",
            sourceName: "Other PAC",
          }),
        ],
        totalItems: 2,
      })),
      getExpenditureDetails: vi.fn(async () => ({
        items: [expenditureRow({ guid: "e-1", transactionAmount: 1_000 })],
        totalItems: 1,
      })),
      fetchOutsideGroupContributions: vi.fn(async () => ({
        outsideGroupBreakdowns: [
          {
            filerRegistrationGuid: "pac-guid",
            supportOppose: "support" as const,
            categoryType: "donor" as const,
            categoryName: "IBEW Local 300",
            amount: 40_000,
            contributorCount: 1,
            sourceUrl: "https://campaignfinance.vermont.gov/",
          },
          {
            filerRegistrationGuid: "pac-guid",
            supportOppose: "support" as const,
            categoryType: "industry" as const,
            categoryName: "labor_unions",
            amount: 40_000,
            contributorCount: 1,
            sourceUrl: "https://campaignfinance.vermont.gov/",
          },
        ],
        fetchedContributionRowCount: 1,
        matchedContributionRowCount: 1,
        includedContributionRowCount: 1,
        skippedContributionRowCount: 0,
      })),
    };

    const output = await runProbeVermontCandidateFinance({
      args,
      client,
      now: new Date("2026-06-25T12:00:00.000Z"),
      readCsvFile: vi.fn(async (path) => {
        if (path === "/exports/direct.csv") {
          return '"Transaction Amount",Memo\n"$100.00","small gift"\n"$500.00","PAC gift"\n';
        }
        return "Transaction Amount\n1000.00\n";
      }),
    });

    expect(output).toMatchObject({
      type: "vermont_candidate_finance_live_probe",
      ts: "2026-06-25T12:00:00.000Z",
      ok: true,
      resolution: { status: "matched", filerRegistrationGuid: "candidate-guid", entityId: 444 },
      validation: {
        csv_comparison_ok: true,
        csv_comparisons: [
          {
            label: "direct_contributions",
            file_path: "/exports/direct.csv",
            csv_total: 600,
            api_total: 600,
            delta: 0,
            ok: true,
          },
          {
            label: "outside_support",
            file_path: "/exports/outside.csv",
            csv_total: 1000,
            api_total: 1000,
            delta: 0,
            ok: true,
          },
        ],
      },
      rows_loaded: {
        candidate_contributions: 2,
        expenditure_rows: 1,
        outside_group_contributions: 1,
      },
      direct_campaign: {
        total_receipts: 600,
        direct_contribution_total: 600,
        top_occupations: [],
        contributor_source_types: [
          {
            category_name: "Political Committee",
            amount: 500,
            contributor_count: 1,
          },
          {
            category_name: "Individual",
            amount: 100,
            contributor_count: 1,
          },
        ],
        contribution_size_buckets: [
          {
            category_name: "$500-$999",
            amount: 500,
            contributor_count: 1,
          },
          {
            category_name: "$100-$249",
            amount: 100,
            contributor_count: 1,
          },
        ],
      },
      outside_spending: {
        support_total: 1000,
        oppose_total: 0,
        top_supporting_groups: [
          {
            filer_registration_guid: "pac-guid",
            filer_name: "Workers PAC",
            support_oppose: "support",
            support_mechanism: "vt_pac_contribution_to_registrant",
            amount: 1000,
            expenditure_count: 1,
            entity_id: 444,
          },
        ],
        top_supporting_industries: [
          {
            category_name: "labor_unions",
            industry_slug: "labor_unions",
            support_oppose: "support",
            amount: 40000,
            contributor_count: 1,
            evidence: [
              {
                organization_name: "IBEW Local 300",
                amount: 40000,
                contributor_count: 1,
                filer_registration_guid: "pac-guid",
                filer_name: "Workers PAC",
              },
            ],
          },
        ],
      },
      counters: {
        direct_matched_rows: 2,
        direct_included_rows: 2,
        direct_skipped_rows: 0,
        outside_matched_rows: 1,
        outside_included_rows: 1,
        outside_skipped_rows: 0,
        outside_group_matched_rows: 1,
        outside_group_included_rows: 1,
        outside_group_skipped_rows: 0,
      },
    });
    expect(client.getContributionDetails).toHaveBeenCalledWith(
      expect.objectContaining({
        pageNumber: 1,
        pageSize: 2,
        filerRegistrationGuid: "candidate-guid",
        electionYear: 2026,
        transactionTypeCode: "TCON",
      }),
      { timeoutMs: 30000 }
    );
    expect(client.getExpenditureDetails).toHaveBeenCalledWith(
      expect.objectContaining({ pageNumber: 1, pageSize: 2, electionYear: 2026, transactionTypeCode: "TEXP" }),
      { timeoutMs: 30000 }
    );
    expect(client.fetchOutsideGroupContributions).toHaveBeenCalledWith(
      expect.objectContaining({
        electionYear: 2026,
        outsideGroups: [
          expect.objectContaining({
            filerRegistrationGuid: "pac-guid",
            supportMechanism: "vt_pac_contribution_to_registrant",
          }),
        ],
        pageSize: 2,
        maxPagesPerGroup: 10,
      }),
      { timeoutMs: 30000 }
    );
  });

  it("sums Vermont CSV export amount columns with quoted currency and negative refunds", () => {
    expect(
      sumCsvAmountColumnFromText({
        text: 'Name,Transaction Amount\n"Donor, One","$1,000.50"\nRefund,($25.25)\n',
        amountColumn: "transaction amount",
      })
    ).toBe(975.25);
  });
});
