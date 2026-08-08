import { describe, expect, it } from "vitest";

import {
  parseProbeLouisianaCandidateFinanceArgs,
  runProbeLouisianaCandidateFinance,
} from "../../src/scripts/probeLouisianaCandidateFinance.js";
import type { LouisianaCampaignFinanceCsvRow } from "../../src/pipeline/louisianaFinance/louisianaCampaignFinanceArtifactReader.js";

function contribution(overrides: Partial<LouisianaCampaignFinanceCsvRow> = {}): LouisianaCampaignFinanceCsvRow {
  return {
    FilerNumber: "12345",
    FilerLastName: "Edwards",
    FilerFirstName: "John Bel",
    ReportCode: "10-G",
    ReportType: "Candidate",
    ReportNumber: "1",
    ContributorTypeCode: "IND",
    ContributorName: "Jane Donor",
    ContributorAddr1: "1 Main St",
    ContributorAddr2: "",
    ContributorCity: "Baton Rouge",
    ContributorrState: "LA",
    ContributorZip: "70801",
    ContributionType: "MONETARY",
    ContributionDescription: "",
    ContributionDate: "09/01/2027",
    ContributionAmt: "1000.00",
    ContributionDesignatedElectionAdditionInfo: "",
    ...overrides,
  };
}

function expenditure(overrides: Partial<LouisianaCampaignFinanceCsvRow> = {}): LouisianaCampaignFinanceCsvRow {
  return {
    FilerNumber: "PAC1",
    FilerLastName: "Better Louisiana PAC",
    FilerFirstName: "",
    ReportCode: "F202",
    ReportType: "PAC",
    ReportNumber: "1",
    Schedule: "E-3",
    RecipientName: "John Bel Edwards",
    RecipientAddr1: "",
    RecipientAddr2: "",
    RecipientCity: "",
    RecipientState: "LA",
    RecipientZip: "",
    ExpenditureDescription: "Campaign contribution",
    CandidateBeneficiary: "John Bel Edwards",
    ExpenditureDate: "09/15/2027",
    ExpenditureAmt: "5000.00",
    ...overrides,
  };
}

describe("probeLouisianaCandidateFinance script", () => {
  it("parses live probe options", () => {
    expect(
      parseProbeLouisianaCandidateFinanceArgs([
        "--candidate-name=John Bel Edwards",
        "--year=2027",
        "--office=Governor",
        "--scope=statewide",
        "--limit=3",
        "--cache-dir=/tmp/la",
        "--contributions-csv=/tmp/contributions.csv",
        "--expenditures-csv=/tmp/expenditures.csv",
        "--refresh-cache",
        "--force-refresh",
        "--start-year=2024",
        "--end-year=2027",
        "--expected-direct-total=$1,000.00",
        "--expected-outside-support-total=5000",
        "--pac-filer-number=PAC1",
        "--expected-pac-receipts-total=8000",
        "--ambiguous-candidate-name=John Edwards",
        "--expected-ambiguous-status=not_matched",
        "--expected-tolerance=0.05",
      ])
    ).toEqual({
      candidateName: "John Bel Edwards",
      electionYear: 2027,
      officeScope: "statewide",
      officeName: "Governor",
      district: null,
      limit: 3,
      cacheDir: "/tmp/la",
      contributionCsvPath: "/tmp/contributions.csv",
      expenditureCsvPath: "/tmp/expenditures.csv",
      refreshCache: true,
      forceRefresh: true,
      startYear: 2024,
      endYear: 2027,
      expectedDirectTotal: 1000,
      expectedOutsideSupportTotal: 5000,
      pacFilerNumber: "PAC1",
      expectedPacReceiptsTotal: 8000,
      ambiguousCandidateName: "John Edwards",
      expectedAmbiguousStatus: "not_matched",
      expectedTolerance: 0.05,
    });
  });

  it("builds a no-write Louisiana validation summary with portal total comparisons", async () => {
    const args = parseProbeLouisianaCandidateFinanceArgs([
      "--candidate-name=John Bel Edwards",
      "--year=2027",
      "--office=Governor",
      "--expected-direct-total=1000",
      "--expected-outside-support-total=5000",
      "--pac-filer-number=PAC1",
      "--expected-pac-receipts-total=8000",
      // A middle-conflicting namesake: the middle-evidence gate now matches
      // middle-less first+last alignments ("John Edwards"), so the probe's
      // must-not-match check uses a name whose middle contradicts the filer.
      "--ambiguous-candidate-name=John Paul Edwards",
    ]);

    const output = await runProbeLouisianaCandidateFinance({
      args,
      now: new Date("2026-06-25T12:00:00.000Z"),
      rows: {
        contributionRows: [
          contribution(),
          contribution({
            FilerNumber: "PAC1",
            FilerLastName: "Better Louisiana PAC",
            FilerFirstName: "",
            ReportCode: "F202",
            ContributorTypeCode: "BUS",
            ContributorName: "Google LLC",
            ContributionAmt: "5000.00",
          }),
          contribution({
            FilerNumber: "PAC1",
            FilerLastName: "Better Louisiana PAC",
            FilerFirstName: "",
            ReportCode: "F202",
            ContributorTypeCode: "PAC",
            ContributorName: "IBEW Local 300",
            ContributionAmt: "3000.00",
          }),
        ],
        expenditureRows: [expenditure()],
        contributionSourceUrl: "https://example.invalid/contributions.csv",
        expenditureSourceUrl: "https://example.invalid/expenditures.csv",
        cacheRefresh: null,
      },
    });

    expect(output).toMatchObject({
      type: "louisiana_candidate_finance_live_probe",
      ts: "2026-06-25T12:00:00.000Z",
      ok: true,
      resolution: {
        status: "matched",
        filerNumber: "12345",
        filerName: "Edwards, John Bel",
      },
      validation: {
        expected_total_comparison_ok: true,
        no_occupation_data: true,
        ambiguous_candidate_check: {
          candidate_name: "John Paul Edwards",
          expected_status: "not_matched",
          actual_status: "unmatched",
          ok: true,
        },
      },
      rows_loaded: {
        contributions: 3,
        expenditures: 1,
      },
      direct_campaign: {
        total_receipts: 1000,
        direct_contribution_total: 1000,
        top_occupations: [],
      },
      outside_spending: {
        support_total: 5000,
        oppose_total: null,
        top_supporting_groups: [
          {
            filer_number: "PAC1",
            filer_name: "Better Louisiana PAC",
            support_oppose: "support",
            support_mechanism: "la_pac_contribution_to_candidate",
            amount: 5000,
            expenditure_count: 1,
          },
        ],
        top_supporting_industries: [
          {
            category_name: "technology",
            industry_slug: "technology",
            support_oppose: "support",
            amount: 5000,
            contributor_count: 1,
          },
          {
            category_name: "labor_unions",
            industry_slug: "labor_unions",
            support_oppose: "support",
            amount: 3000,
            contributor_count: 1,
          },
        ],
      },
      known_pac: {
        filer_number: "PAC1",
        filer_name: "Better Louisiana PAC",
        receipts_total: 8000,
      },
      counters: {
        direct_matched_rows: 1,
        direct_included_rows: 1,
        outside_matched_rows: 1,
        outside_included_rows: 1,
        outside_group_matched_rows: 2,
        outside_group_included_rows: 2,
      },
    });

    expect(output.validation.expected_total_comparisons).toEqual([
      {
        label: "direct_contributions",
        expected_total: 1000,
        observed_total: 1000,
        delta: 0,
        tolerance: 0.01,
        ok: true,
      },
      {
        label: "outside_support",
        expected_total: 5000,
        observed_total: 5000,
        delta: 0,
        tolerance: 0.01,
        ok: true,
      },
      {
        label: "pac_receipts",
        expected_total: 8000,
        observed_total: 8000,
        delta: 0,
        tolerance: 0.01,
        ok: true,
      },
    ]);
    expect(output.known_pac?.top_donors.map((donor) => donor.category_name)).toEqual(["Google LLC", "IBEW Local 300"]);
  });

  it("requires a PAC filer number when validating expected PAC receipts", async () => {
    const args = parseProbeLouisianaCandidateFinanceArgs([
      "--candidate-name=John Bel Edwards",
      "--year=2027",
      "--office=Governor",
      "--expected-pac-receipts-total=8000",
    ]);

    await expect(
      runProbeLouisianaCandidateFinance({
        args,
        rows: {
          contributionRows: [],
          expenditureRows: [],
          contributionSourceUrl: null,
          expenditureSourceUrl: null,
          cacheRefresh: null,
        },
      })
    ).rejects.toThrow("--expected-pac-receipts-total requires --pac-filer-number");
  });
});
