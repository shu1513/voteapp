import { describe, expect, it, vi } from "vitest";

import {
  parseProbeIllinoisCandidateFinanceArgs,
  runProbeIllinoisCandidateFinance,
} from "../../src/scripts/probeIllinoisCandidateFinance.js";

describe("probeIllinoisCandidateFinance script", () => {
  it("parses probe options with election-cycle date defaults", () => {
    expect(
      parseProbeIllinoisCandidateFinanceArgs([
        "--candidate-name=JB Pritzker",
        "--year=2022",
        "--office=Governor",
        "--limit=3",
        "--funder-limit=7",
        "--min-industry-amount=25000",
        "--timeout-ms=5000",
      ])
    ).toEqual({
      candidateName: "JB Pritzker",
      electionYear: 2022,
      officeName: "Governor",
      fromDate: "1/1/2021",
      toDate: "12/31/2022",
      limit: 3,
      funderLimit: 7,
      minIndustryAmount: 25000,
      timeoutMs: 5000,
    });
  });

  it("builds a no-write probe summary with direct occupations and outside industries", async () => {
    const args = parseProbeIllinoisCandidateFinanceArgs([
      "--candidate-name=Jane Doe",
      "--year=2022",
      "--office=Governor",
      "--limit=5",
      "--min-industry-amount=25000",
    ]);
    const client = {
      getCandidateContributions: vi.fn(async () => [
        {
          contributorName: "Alice Attorney",
          contributorAddress: "1 Main St",
          occupation: "Attorney",
          employer: "Law LLP",
          amount: 1000,
          receivedDate: "3/1/2022",
          reportReceivedDate: null,
          contributionType: "Individual Contributions",
          recipientCommitteeName: "Friends of Jane",
          description: null,
          vendorName: null,
          vendorAddress: null,
          sourceUrl: "https://www.elections.il.gov/CampaignDisclosure/ContributionSearchByCandidates.aspx",
        },
        {
          contributorName: "Bob Builder",
          contributorAddress: "2 Main St",
          occupation: "Construction",
          employer: "Build Co",
          amount: 500,
          receivedDate: "3/2/2022",
          reportReceivedDate: null,
          contributionType: "Individual Contributions",
          recipientCommitteeName: "Friends of Jane",
          description: null,
          vendorName: null,
          vendorAddress: null,
          sourceUrl: "https://www.elections.il.gov/CampaignDisclosure/ContributionSearchByCandidates.aspx",
        },
      ]),
      getIndependentExpenditures: vi.fn(async (input: { supportOppose?: "support" | "oppose" | null }) =>
        input.supportOppose === "support"
          ? [
              {
                payeeName: "Vendor",
                payeeAddress: null,
                amount: 10000,
                expendedDate: "10/1/2022",
                reportReceivedDate: null,
                expenditureType: "Independent Expenditures",
                expendingCommitteeName: "Illinois Conservation Action",
                purpose: "Mail",
                candidateName: "Jane Doe",
                officeDistrict: "Governor",
                supportOppose: "support" as const,
                sourceUrl: "https://www.elections.il.gov/CampaignDisclosure/ExpenditureSearchByAllExpenditures.aspx",
              },
            ]
          : [
              {
                payeeName: "Vendor",
                payeeAddress: null,
                amount: 2500,
                expendedDate: "10/2/2022",
                reportReceivedDate: null,
                expenditureType: "Independent Expenditures",
                expendingCommitteeName: "Illinois Conservation Action",
                purpose: "Mail",
                candidateName: "Jane Doe",
                officeDistrict: "Governor",
                supportOppose: "oppose" as const,
                sourceUrl: "https://www.elections.il.gov/CampaignDisclosure/ExpenditureSearchByAllExpenditures.aspx",
              },
            ]
      ),
      getCommitteeContributions: vi.fn(async () => [
        {
          contributorName: "Sierra Club",
          contributorAddress: null,
          occupation: null,
          employer: null,
          amount: 30000,
          receivedDate: "9/1/2022",
          reportReceivedDate: null,
          contributionType: "Transfers In",
          recipientCommitteeName: "Illinois Conservation Action",
          description: null,
          vendorName: null,
          vendorAddress: null,
          sourceUrl: "https://www.elections.il.gov/CampaignDisclosure/ContributionSearchByCommittees.aspx",
        },
      ]),
    };

    const output = await runProbeIllinoisCandidateFinance({
      args,
      client,
      now: new Date("2026-06-25T12:00:00.000Z"),
    });

    expect(output).toMatchObject({
      type: "illinois_candidate_finance_live_probe",
      ts: "2026-06-25T12:00:00.000Z",
      ok: true,
      search: {
        candidate_first_name: "Jane",
        candidate_last_name: "Doe",
      },
      direct_campaign: {
        top_occupations: [
          {
            category_name: "Attorney",
            amount: 1000,
            contributor_count: 1,
          },
          {
            category_name: "Construction",
            amount: 500,
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
            expenditure_count: 1,
          },
        ],
        top_supporting_industries: [
          {
            category_name: "environmental_group",
            industry_slug: "environmental_group",
            amount: 30000,
            contributor_count: 1,
            evidence: [
              {
                organization_name: "Sierra Club",
                committee_name: "Illinois Conservation Action",
                amount: 30000,
              },
            ],
          },
        ],
        skipped_outside_funder_lookup_count: 0,
      },
    });
    expect(client.getCandidateContributions).toHaveBeenCalledWith(
      expect.objectContaining({ candidateLastName: "Doe", candidateFirstName: "Jane", electionYear: 2022 }),
      expect.objectContaining({ timeoutMs: 30000 })
    );
    expect(client.getCommitteeContributions).toHaveBeenCalledWith(
      expect.objectContaining({ committeeName: "Illinois Conservation Action", contributionType: "All Types" }),
      expect.objectContaining({ timeoutMs: 30000 })
    );
    expect(client.getCommitteeContributions).toHaveBeenCalledTimes(1);
  });

  it("rejects malformed required options", () => {
    expect(() => parseProbeIllinoisCandidateFinanceArgs(["--year=2022", "--office=Governor"])).toThrow(
      "Missing required --candidate-name"
    );
    expect(() =>
      parseProbeIllinoisCandidateFinanceArgs(["--candidate-name=Jane Doe", "--year=2022x", "--office=Governor"])
    ).toThrow("Invalid --year value");
    expect(() =>
      parseProbeIllinoisCandidateFinanceArgs([
        "--candidate-name=Jane Doe",
        "--year=2022",
        "--office=Governor",
        "--from-date=2022-01-01",
      ])
    ).toThrow("use m/d/yyyy");
  });
});
