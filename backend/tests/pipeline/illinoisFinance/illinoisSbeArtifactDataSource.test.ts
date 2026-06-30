import { describe, expect, it } from "vitest";

import {
  createIllinoisSbeArtifactCandidateCommitteeResolver,
  loadIllinoisFinanceDataForDueRowFromArtifacts,
  loadIllinoisSbeArtifactDataSet,
  type IllinoisCandidateFinanceDueRow,
} from "../../../src/pipeline/illinoisFinance/index.js";

const CONTRIBUTIONS_CSV = "tests/fixtures/illinoisFinance/contributions.csv";
const EXPENDITURES_CSV = "tests/fixtures/illinoisFinance/expenditures.csv";
const CONTRIBUTIONS_URL = "https://example.test/illinois-contributions.csv";
const EXPENDITURES_URL = "https://example.test/illinois-expenditures.csv";

function dueRow(overrides: Partial<IllinoisCandidateFinanceDueRow> = {}): IllinoisCandidateFinanceDueRow {
  return {
    candidateId: "candidate-1",
    electionId: "election-1",
    candidateName: "Jane Doe",
    electionYear: 2022,
    officeScope: "statewide",
    officeName: "Governor",
    district: null,
    committeeKey: "FRIENDS OF JANE DOE",
    committeeName: "Friends of Jane Doe",
    sourceUrl: null,
    lastSyncedAt: null,
    ...overrides,
  };
}

describe("illinoisSbeArtifactDataSource", () => {
  it("loads contribution and expenditure CSV artifacts with source URLs", async () => {
    const artifacts = await loadIllinoisSbeArtifactDataSet({
      contributionCsvPaths: [CONTRIBUTIONS_CSV],
      expenditureCsvPaths: [EXPENDITURES_CSV],
      contributionSourceUrl: CONTRIBUTIONS_URL,
      expenditureSourceUrl: EXPENDITURES_URL,
    });

    expect(artifacts.contributionRecords).toHaveLength(3);
    expect(artifacts.expenditureRecords).toHaveLength(3);
    expect(artifacts.contributionRecords[0]?.sourceUrl).toBe(CONTRIBUTIONS_URL);
    expect(artifacts.expenditureRecords?.[0]?.sourceUrl).toBe(EXPENDITURES_URL);
  });

  it("selects due-row direct contributions, outside expenditures, and outside group contributions", async () => {
    const artifacts = await loadIllinoisSbeArtifactDataSet({
      contributionCsvPaths: [CONTRIBUTIONS_CSV],
      expenditureCsvPaths: [EXPENDITURES_CSV],
      contributionSourceUrl: CONTRIBUTIONS_URL,
      expenditureSourceUrl: EXPENDITURES_URL,
    });

    const data = loadIllinoisFinanceDataForDueRowFromArtifacts({
      row: dueRow(),
      artifacts,
    });

    expect(data.directContributionRecords.map((record) => record.contributorName)).toEqual([
      "Alpha Attorney",
      "Old Donor",
    ]);
    expect(data.outsideExpenditureRecords?.map((record) => record.expendingCommitteeName)).toEqual([
      "Illinois Conservation Action",
      "People Against Jane",
    ]);
    expect(data.outsideGroupContributionRecords?.map((record) => record.contributorName)).toEqual(["Sierra Club"]);
  });

  it("keeps outside data unavailable when only a contribution artifact is provided", async () => {
    const artifacts = await loadIllinoisSbeArtifactDataSet({
      contributionCsvPaths: [CONTRIBUTIONS_CSV],
      contributionSourceUrl: CONTRIBUTIONS_URL,
    });

    const data = loadIllinoisFinanceDataForDueRowFromArtifacts({
      row: dueRow(),
      artifacts,
    });

    expect(data.directContributionRecords).toHaveLength(2);
    expect(data.outsideExpenditureRecords).toBeUndefined();
    expect(data.outsideGroupContributionRecords).toBeUndefined();
  });

  it("resolves candidate committees from contribution artifacts", async () => {
    const artifacts = await loadIllinoisSbeArtifactDataSet({
      contributionCsvPaths: [CONTRIBUTIONS_CSV],
      contributionSourceUrl: CONTRIBUTIONS_URL,
    });
    const resolveCandidateCommittee = createIllinoisSbeArtifactCandidateCommitteeResolver(artifacts);

    await expect(
      resolveCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2022,
        district: null,
      })
    ).resolves.toEqual({
      status: "matched",
      committeeKey: "FRIENDS OF JANE DOE",
      committeeName: "Friends of Jane Doe",
      confidence: "exact",
      source: "illinois_sbe",
      sourceUrl: CONTRIBUTIONS_URL,
      matchedContributionRowCount: 1,
    });
  });

  it("filters legislative outside expenditures by district", () => {
    const artifacts = {
      contributionRecords: [],
      contributionSourceUrl: CONTRIBUTIONS_URL,
      expenditureRecords: [
        {
          payeeName: "Mailer",
          payeeAddress: null,
          amount: 100,
          expendedDate: "10/1/2022",
          reportReceivedDate: null,
          expenditureType: "Independent Expenditures",
          expendingCommitteeName: "District 44 Support",
          purpose: "Mail",
          candidateName: "Jane Doe",
          officeDistrict: "State Representative - 44th District",
          supportOppose: "support" as const,
          sourceUrl: EXPENDITURES_URL,
        },
        {
          payeeName: "Mailer",
          payeeAddress: null,
          amount: 100,
          expendedDate: "10/1/2022",
          reportReceivedDate: null,
          expenditureType: "Independent Expenditures",
          expendingCommitteeName: "District 45 Support",
          purpose: "Mail",
          candidateName: "Jane Doe",
          officeDistrict: "State Representative - 45th District",
          supportOppose: "support" as const,
          sourceUrl: EXPENDITURES_URL,
        },
        {
          payeeName: "Mailer",
          payeeAddress: null,
          amount: 100,
          expendedDate: "10/1/2022",
          reportReceivedDate: null,
          expenditureType: "Independent Expenditures",
          expendingCommitteeName: "District 144 Support",
          purpose: "Mail",
          candidateName: "Jane Doe",
          officeDistrict: "State Representative - 144th District",
          supportOppose: "support" as const,
          sourceUrl: EXPENDITURES_URL,
        },
      ],
      expenditureSourceUrl: EXPENDITURES_URL,
    };

    const data = loadIllinoisFinanceDataForDueRowFromArtifacts({
      row: dueRow({
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        district: "44",
      }),
      artifacts,
    });

    expect(data.outsideExpenditureRecords?.map((record) => record.expendingCommitteeName)).toEqual([
      "District 44 Support",
    ]);
  });

  it("rejects missing contribution CSV paths", async () => {
    await expect(loadIllinoisSbeArtifactDataSet({ contributionCsvPaths: [" "] })).rejects.toThrow(
      "Illinois SBE contribution artifact requires at least one CSV path"
    );
  });
});
