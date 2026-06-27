import { describe, expect, it, vi } from "vitest";

import { runIllinoisCandidateFinanceLiveSmoke } from "../../src/scripts/smokeIllinoisCandidateFinance.js";
import type {
  IllinoisSbeContributionRecord,
  IllinoisSbeExpenditureRecord,
} from "../../src/pipeline/illinoisFinance/illinoisSbeClient.js";

function contribution(overrides: Partial<IllinoisSbeContributionRecord> = {}): IllinoisSbeContributionRecord {
  return {
    contributorName: "Pat Person",
    contributorAddress: "1 Main St",
    occupation: "Attorney",
    employer: "Law LLP",
    amount: 1000,
    receivedDate: "3/1/2022",
    reportReceivedDate: null,
    contributionType: "Individual Contributions",
    recipientCommitteeName: "Friends of Jane Doe",
    description: null,
    vendorName: null,
    vendorAddress: null,
    sourceUrl: "fixture://contributions.csv",
    ...overrides,
  };
}

function expenditure(overrides: Partial<IllinoisSbeExpenditureRecord> = {}): IllinoisSbeExpenditureRecord {
  return {
    payeeName: "Media Vendor",
    payeeAddress: null,
    amount: 10000,
    expendedDate: "10/1/2022",
    reportReceivedDate: null,
    expenditureType: "Independent Expenditures",
    expendingCommitteeName: "Illinois Conservation Action",
    purpose: "Digital ads",
    candidateName: "Jane Doe",
    officeDistrict: "Governor",
    supportOppose: "support",
    sourceUrl: "fixture://expenditures.csv",
    ...overrides,
  };
}

describe("smokeIllinoisCandidateFinance script", () => {
  it("passes no-write Illinois smoke checks when live-shape data is present", async () => {
    const client = {
      getCandidateContributions: vi.fn(async () => [contribution()]),
      getIndependentExpenditures: vi.fn(async (input: { supportOppose?: "support" | "oppose" }) =>
        input.supportOppose === "support"
          ? [expenditure()]
          : [
              expenditure({
                expendingCommitteeName: "People Against Jane",
                amount: 7000,
                supportOppose: "oppose",
              }),
            ]
      ),
      getCommitteeContributions: vi.fn(async (input: { committeeName?: string | null }) =>
        input.committeeName === "Illinois Conservation Action"
          ? [
              contribution({
                contributorName: "Sierra Club",
                contributorAddress: null,
                occupation: null,
                employer: null,
                amount: 30000,
                contributionType: "Transfers In",
                recipientCommitteeName: "Illinois Conservation Action",
                receivedDate: "9/1/2022",
              }),
            ]
          : []
      ),
    };

    const result = await runIllinoisCandidateFinanceLiveSmoke({
      args: [
        "--candidate-name=Jane Doe",
        "--year=2022",
        "--office=Governor",
        "--limit=5",
        "--funder-limit=10",
        "--min-industry-amount=25000",
      ],
      client,
      now: new Date("2026-06-01T00:00:00.000Z"),
    });

    expect(result).toMatchObject({
      type: "illinois_candidate_finance_live_smoke",
      ts: "2026-06-01T00:00:00.000Z",
      ok: true,
      checks: [
        { name: "probe_ok", passed: true },
        { name: "top_occupations_present", passed: true },
        { name: "contribution_size_buckets_present", passed: true },
        { name: "outside_groups_present", passed: true },
        { name: "supporting_industry_evidence_present", passed: true },
      ],
      probe: {
        type: "illinois_candidate_finance_live_probe",
        direct_campaign: {
          top_occupations: [
            {
              category_name: "Attorney",
              amount: 1000,
              contributor_count: 1,
            },
          ],
        },
        outside_spending: {
          top_supporting_groups: [
            {
              committee_key: "ILLINOIS CONSERVATION ACTION",
              committee_name: "Illinois Conservation Action",
              support_oppose: "support",
              amount: 10000,
            },
          ],
          top_supporting_industries: [
            {
              industry_slug: "environmental_group",
              evidence: [
                {
                  organization_name: "Sierra Club",
                  committee_key: "ILLINOIS CONSERVATION ACTION",
                  committee_name: "Illinois Conservation Action",
                  amount: 30000,
                },
              ],
            },
          ],
        },
      },
    });
  });

  it("fails the smoke when required live-shape evidence is missing", async () => {
    const client = {
      getCandidateContributions: vi.fn(async () => []),
      getIndependentExpenditures: vi.fn(async () => []),
      getCommitteeContributions: vi.fn(async () => []),
    };

    const result = await runIllinoisCandidateFinanceLiveSmoke({
      args: ["--candidate-name=Jane Doe", "--year=2022", "--office=Governor"],
      client,
      now: new Date("2026-06-01T00:00:00.000Z"),
    });

    expect(result.ok).toBe(false);
    expect(result.checks).toEqual([
      { name: "probe_ok", passed: true },
      { name: "top_occupations_present", passed: false },
      { name: "contribution_size_buckets_present", passed: false },
      { name: "outside_groups_present", passed: false, detail: "outside_group_count=0" },
      {
        name: "supporting_industry_evidence_present",
        passed: false,
        detail: "supporting_industries_with_evidence=0",
      },
    ]);
  });
});
