import { describe, expect, it } from "vitest";

import {
  parseProbeAlaskaCandidateFinanceArgs,
  runProbeAlaskaCandidateFinance,
} from "../../src/scripts/probeAlaskaCandidateFinance.js";
import type {
  AlaskaApocCampaignIncomeRow,
  AlaskaApocIndependentContributionRow,
  AlaskaApocIndependentExpenditureRow,
} from "../../src/pipeline/alaskaFinance/alaskaApocClient.js";

function income(overrides: Partial<AlaskaApocCampaignIncomeRow> = {}): AlaskaApocCampaignIncomeRow {
  return {
    reportYear: 2026,
    filerId: "1001",
    filerName: "Jane Doe",
    filerType: "Candidate",
    name: "Jane Doe",
    date: "10/01/2026",
    type: "Income",
    contributor: "Smith, Pat",
    address: "1 Main",
    city: "Juneau",
    state: "AK",
    zip: "99801",
    country: "USA",
    paymentType: "Check",
    paymentDetail: "1001",
    occupation: "Attorney",
    employer: "Law Firm",
    purpose: "Contribution",
    amount: 250,
    submitted: "10/02/2026",
    status: "Complete",
    sourceUrl: null,
    ...overrides,
  };
}

function expenditure(overrides: Partial<AlaskaApocIndependentExpenditureRow> = {}): AlaskaApocIndependentExpenditureRow {
  return {
    reportYear: 2026,
    filerId: "8001",
    filerName: "Alaska Future PAC",
    filerType: "Group",
    businessPhone: "",
    businessType: "Super PAC",
    type: "Expenditure",
    date: "09/15/2026",
    recipient: "Vendor",
    address: "1 Main",
    city: "Anchorage",
    state: "AK",
    zip: "99501",
    country: "USA",
    position: "Support",
    candidateProposition: "Jane Doe",
    description: "Mailers",
    reportType: "24-hour",
    election: "General",
    paymentType: "Card",
    paymentDetail: "ad buy",
    amount: 50_000,
    submitted: "09/16/2026",
    status: "Complete",
    sourceUrl: null,
    ...overrides,
  };
}

function contribution(overrides: Partial<AlaskaApocIndependentContributionRow> = {}): AlaskaApocIndependentContributionRow {
  return {
    reportYear: 2026,
    filerId: "8001",
    filerName: "Alaska Future PAC",
    filerType: "Group",
    businessPhone: "",
    businessType: "Super PAC",
    type: "Contribution",
    date: "09/01/2026",
    contributor: "Energy Transfer LLC",
    contributorAddress: "2 Energy Rd",
    contributorCity: "Dallas",
    contributorState: "TX",
    contributorZip: "75001",
    contributorCountry: "USA",
    employer: "",
    occupation: "",
    reportType: "24-hour",
    election: "General",
    officers: "",
    amount: 40_000,
    submitted: "09/02/2026",
    status: "Complete",
    sourceUrl: null,
    ...overrides,
  };
}

describe("probeAlaskaCandidateFinance script", () => {
  it("parses CSV probe options", () => {
    expect(
      parseProbeAlaskaCandidateFinanceArgs([
        "--candidate-name=Jane Doe",
        "--year=2026",
        "--income-csv=/tmp/income.csv",
        "--ie-expenditures-csv=/tmp/ie-exp.csv",
        "--ie-contributions-csv=/tmp/ie-con.csv",
        "--income-url=https://example.test/income.csv",
        "--timeout-ms=1000",
        "--retry-count=1",
        "--retry-delay-ms=0",
        "--request-spacing-ms=0",
        "--candidate-filer-id=1001",
        "--limit=3",
        "--min-industry-amount=10000",
      ])
    ).toEqual({
      candidateName: "Jane Doe",
      electionYear: 2026,
      dataSourceMode: "csv",
      incomeCsvPath: "/tmp/income.csv",
      independentExpendituresCsvPath: "/tmp/ie-exp.csv",
      independentContributionsCsvPath: "/tmp/ie-con.csv",
      incomeUrl: "https://example.test/income.csv",
      independentExpendituresUrl: null,
      independentContributionsUrl: null,
      timeoutMs: 1000,
      retryCount: 1,
      retryDelayMs: 0,
      requestSpacingMs: 0,
      candidateFilerId: "1001",
      candidateFilerName: null,
      limit: 3,
      minIndustryAmount: 10000,
    });
    expect(parseProbeAlaskaCandidateFinanceArgs(["--candidate-name=Jane Doe", "--year=2026", "--live"])).toMatchObject({
      dataSourceMode: "live",
    });
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() =>
      parseProbeAlaskaCandidateFinanceArgs([
        "--candidate-name=Jane Doe",
        "--year=2026",
        "--limit=9007199254740993",
      ])
    ).toThrow("Invalid --limit value: 9007199254740993");
  });

  it("builds a no-write Alaska probe summary with occupations and outside industries", async () => {
    const args = parseProbeAlaskaCandidateFinanceArgs([
      "--candidate-name=Jane Doe",
      "--year=2026",
      "--candidate-filer-id=1001",
      "--limit=5",
      "--min-industry-amount=25000",
    ]);

    const output = await runProbeAlaskaCandidateFinance({
      args,
      datasets: {
        incomeRows: [
          income({ contributor: "Smith, Pat", occupation: "Attorney", amount: 250 }),
          income({ contributor: "Roe, Alex", occupation: "Attorney", amount: 500 }),
          income({ contributor: "Teacher, Robin", occupation: "Teacher", amount: 5_000 }),
        ],
        independentExpenditureRows: [expenditure()],
        independentContributionRows: [contribution()],
      },
      now: new Date("2026-06-25T12:00:00.000Z"),
    });

    expect(output).toMatchObject({
      type: "alaska_candidate_finance_probe",
      ts: "2026-06-25T12:00:00.000Z",
      ok: true,
      candidate_match: {
        status: "provided",
        candidate_name: "Jane Doe",
        candidate_filer_id: "1001",
        matched_row_count: null,
        source: "cli",
      },
      direct_campaign: {
        total_direct_contributions: 5750,
        total_receipts: 5750,
        top_occupations: [
          {
            category_name: "Teacher",
            amount: 5000,
            contributor_count: 1,
          },
          {
            category_name: "Attorney",
            amount: 750,
            contributor_count: 2,
          },
        ],
      },
      outside_spending: {
        top_supporting_groups: [
          {
            committee_id: "8001",
            committee_name: "Alaska Future PAC",
            support_oppose: "support",
            amount: 50000,
          },
        ],
        top_supporting_industries: [
          {
            category_name: "oil_gas_energy",
            industry_slug: "oil_gas_energy",
            support_oppose: "support",
            amount: 40000,
            contributor_count: 1,
            evidence: [
              {
                contributor_name: "Energy Transfer LLC",
                amount: 40000,
                committee_id: "8001",
                committee_name: "Alaska Future PAC",
                classified_label: "Energy Transfer LLC",
                classification_source: "rule",
              },
            ],
          },
        ],
      },
    });
  });

  it("resolves the candidate campaign filer from APOC income rows when not provided", async () => {
    const args = parseProbeAlaskaCandidateFinanceArgs([
      "--candidate-name=Jane Doe",
      "--year=2026",
      "--limit=5",
    ]);

    const output = await runProbeAlaskaCandidateFinance({
      args,
      datasets: {
        incomeRows: [
          income({ filerId: "1001", filerName: "Jane Doe", contributor: "Smith, Pat", occupation: "Attorney", amount: 250 }),
        ],
        independentExpenditureRows: [],
        independentContributionRows: [],
      },
      now: new Date("2026-06-25T12:00:00.000Z"),
    });

    expect(output.candidate_match).toEqual({
      status: "matched",
      candidate_name: "Jane Doe",
      candidate_filer_id: "1001",
      candidate_filer_name: "Jane Doe",
      matched_row_count: 1,
      source: "apoc_csv",
    });
    expect(output.direct_campaign.total_direct_contributions).toBe(250);
  });
});
