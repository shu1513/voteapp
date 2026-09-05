import { describe, expect, it, vi } from "vitest";

import {
  parseProbeTennesseeCandidateFinanceArgs,
  runProbeTennesseeCandidateFinance,
} from "../../src/scripts/probeTennesseeCandidateFinance.js";

describe("probeTennesseeCandidateFinance script", () => {
  it("parses live probe options and returns a safe empty result for unmatched candidates", async () => {
    const args = parseProbeTennesseeCandidateFinanceArgs([
      "--candidate-name=Bill Lee",
      "--year",
      "2022",
      "--office=Governor",
      "--scope=statewide",
      "--limit=3",
      "--min-industry-amount=25000",
      "--timeout-ms=5000",
    ]);

    expect(args).toEqual({
      candidateName: "Bill Lee",
      electionYear: 2022,
      officeScope: "statewide",
      officeName: "Governor",
      district: null,
      limit: 3,
      minIndustryAmount: 25000,
      timeoutMs: 5000,
    });

    const client = {
      resolveCandidateCommittee: vi.fn(async () => ({
        status: "unmatched" as const,
        reason: "no_candidate_committee_match" as const,
        candidateNameNormalized: "BILL LEE",
        officeNameNormalized: "GOVERNOR",
      })),
    };

    const output = await runProbeTennesseeCandidateFinance({
      args,
      client,
      now: new Date("2026-06-21T12:00:00.000Z"),
    });

    expect(output).toEqual({
      type: "tennessee_candidate_finance_live_probe",
      ts: "2026-06-21T12:00:00.000Z",
      args,
      ok: false,
      resolution: {
        status: "unmatched",
        reason: "no_candidate_committee_match",
        candidateNameNormalized: "BILL LEE",
        officeNameNormalized: "GOVERNOR",
      },
      direct_campaign: {
        total_raised: null,
        top_occupations: [],
        contribution_size_buckets: [],
      },
      outside_spending: {
        support_total: null,
        oppose_total: null,
        top_supporting_groups: [],
        top_opposing_groups: [],
        top_supporting_industries: [],
        top_opposing_industries: [],
        matched_outside_contribution_row_count: 0,
        included_outside_contribution_row_count: 0,
        skipped_outside_contribution_row_count: 0,
      },
      source_row_counts: {
        contribution_rows: 0,
        expenditure_rows: 0,
        outside_group_contribution_rows: 0,
      },
    });
    expect(client.resolveCandidateCommittee).toHaveBeenCalledWith(
      expect.objectContaining({ candidateName: "Bill Lee", officeName: "Governor", electionYear: 2022 }),
      { timeoutMs: 5000 }
    );
  });

  it("builds a no-write probe summary with direct occupations and outside industry evidence", async () => {
    const args = parseProbeTennesseeCandidateFinanceArgs([
      "--candidate-name=Bill Lee",
      "--year=2022",
      "--office=Governor",
      "--limit=5",
      "--min-industry-amount=25000",
    ]);
    const client = {
      resolveCandidateCommittee: vi.fn(async () => ({
        status: "matched" as const,
        campCandidateId: "6496",
        ownerName: "LEE, BILL",
        candidateName: "LEE, BILL",
        officeSought: "Governor",
        district: null,
        confidence: "exact" as const,
        source: "tncamp_search" as const,
        sourceUrl: "https://apps.tn.gov/tncamp/public/cpsearch.htm",
        reportListUrl: "https://apps.tn.gov/tncamp/public/replist.htm?id=6496&owner=LEE,%20BILL",
        matchedRowCount: 1,
      })),
      loadContributionDataForCandidate: vi.fn(async () => ({
        sourceUrl: "https://apps.tn.gov/tncamp/public/ceresults.htm?direct=1",
        contributions: [
          {
            type: "Monetary",
            adjustment: "N",
            amount: 250,
            date: "02/18/2022",
            electionYear: 2022,
            reportName: "1st Quarter",
            recipientName: "LEE, BILL",
            contributorName: "DOE, JANE",
            contributorOccupation: "Attorney",
            contributorEmployer: "Law Firm",
          },
          {
            type: "Monetary",
            adjustment: "N",
            amount: 500,
            date: "03/18/2022",
            electionYear: 2022,
            reportName: "1st Quarter",
            recipientName: "LEE, BILL",
            contributorName: "SMITH, JOHN",
            contributorOccupation: "Attorney",
            contributorEmployer: "Firm",
          },
        ],
        expenditureSourceUrl: "https://apps.tn.gov/tncamp/public/ceresults.htm?expenditures=1",
        expenditures: [
          {
            type: "Independent",
            adjustment: "N",
            amount: 100000,
            date: "10/01/2022",
            electionYear: 2022,
            reportName: "Pre-General",
            candidatePacName: "RIGHT TENNESSEE",
            vendorName: "Media Vendor",
            purpose: "Mail",
            candidateFor: "LEE, BILL",
            supportOpposeCode: "S",
          },
        ],
        outsideContributionSourceUrl: "https://apps.tn.gov/tncamp/public/ceresults.htm?outside=1",
        outsideGroupContributionRecords: [
          {
            type: "Monetary",
            adjustment: "N",
            amount: 50000,
            date: "09/01/2022",
            electionYear: 2022,
            reportName: "Pre-General",
            recipientName: "RIGHT TENNESSEE",
            contributorName: "TENNESSEE BANK PAC",
            contributorOccupation: null,
            contributorEmployer: null,
          },
          {
            type: "Monetary",
            adjustment: "N",
            amount: 25000,
            date: "09/02/2022",
            electionYear: 2022,
            reportName: "Pre-General",
            recipientName: "RIGHT TENNESSEE",
            contributorName: "GREEN ENERGY LLC",
            contributorOccupation: null,
            contributorEmployer: null,
          },
          {
            type: "Monetary",
            adjustment: "N",
            amount: 1000,
            date: "09/03/2022",
            electionYear: 2022,
            reportName: "Pre-General",
            recipientName: "RIGHT TENNESSEE",
            contributorName: "PERSON, PAT",
            contributorOccupation: null,
            contributorEmployer: null,
          },
        ],
      })),
    };

    const output = await runProbeTennesseeCandidateFinance({
      args,
      client,
      now: new Date("2026-06-21T12:00:00.000Z"),
    });

    expect(output).toMatchObject({
      type: "tennessee_candidate_finance_live_probe",
      ok: true,
      direct_campaign: {
        total_raised: 750,
        top_occupations: [
          {
            category_name: "Attorney",
            amount: 750,
            contributor_count: 2,
          },
        ],
      },
      outside_spending: {
        support_total: 100000,
        oppose_total: 0,
        top_supporting_groups: [
          {
            committee_key: "RIGHT TENNESSEE",
            committee_name: "RIGHT TENNESSEE",
            support_oppose: "support",
            amount: 100000,
          },
        ],
        top_supporting_industries: [
          {
            category_name: "finance_investment",
            industry_slug: "finance_investment",
            support_oppose: "support",
            amount: 50000,
            contributor_count: 1,
            evidence: [
              {
                organization_name: "TENNESSEE BANK PAC",
                amount: 50000,
                committee_key: "RIGHT TENNESSEE",
                committee_name: "RIGHT TENNESSEE",
              },
            ],
          },
          {
            category_name: "oil_gas_energy",
            industry_slug: "oil_gas_energy",
            support_oppose: "support",
            amount: 25000,
            contributor_count: 1,
            evidence: [
              {
                organization_name: "GREEN ENERGY LLC",
                amount: 25000,
                committee_key: "RIGHT TENNESSEE",
                committee_name: "RIGHT TENNESSEE",
              },
            ],
          },
        ],
        matched_outside_contribution_row_count: 3,
        included_outside_contribution_row_count: 2,
        skipped_outside_contribution_row_count: 1,
      },
      source_row_counts: {
        contribution_rows: 2,
        expenditure_rows: 1,
        outside_group_contribution_rows: 3,
      },
    });
    expect(client.loadContributionDataForCandidate).toHaveBeenCalledWith({
      candidateName: "Bill Lee",
      ownerName: "LEE, BILL",
      electionYear: 2022,
      clientOptions: { timeoutMs: 30000 },
    });
  });

  it("rejects malformed required options", () => {
    expect(() => parseProbeTennesseeCandidateFinanceArgs(["--year=2022", "--office=Governor"])).toThrow(
      "Missing required --candidate-name"
    );
    expect(() =>
      parseProbeTennesseeCandidateFinanceArgs(["--candidate-name=Bill Lee", "--year=2022", "--office=Governor", "--scope=city"])
    ).toThrow("Invalid --scope value");
    expect(() =>
      parseProbeTennesseeCandidateFinanceArgs(["--candidate-name=Bill Lee", "--year=2022x", "--office=Governor"])
    ).toThrow("Invalid --year value");
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() =>
      parseProbeTennesseeCandidateFinanceArgs([
        "--candidate-name=Bill Lee",
        "--year=2022",
        "--office=Governor",
        "--limit=9007199254740993",
      ])
    ).toThrow("Invalid --limit value: 9007199254740993");
  });
});
