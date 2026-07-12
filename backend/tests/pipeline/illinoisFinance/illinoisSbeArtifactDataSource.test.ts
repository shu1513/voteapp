import { describe, expect, it } from "vitest";

import {
  createIllinoisSbeArtifactCandidateCommitteeResolver,
  loadIllinoisFinanceDataForDueRowFromArtifacts,
  loadIllinoisSbeArtifactDataSet,
  type IllinoisCandidateFinanceDueRow,
} from "../../../src/pipeline/illinoisFinance/index.js";

const CONTRIBUTIONS_CSV = "tests/fixtures/illinoisFinance/contributions.csv";
const EXPENDITURES_CSV = "tests/fixtures/illinoisFinance/expenditures.csv";
const NORMALIZED_ARTIFACT = "tests/fixtures/illinoisFinance/normalized-artifact.json";
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
    sbeCandidateId: null,
    sbeDistrictType: null,
    sbeOffice: null,
    isAtLarge: null,
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
    ).resolves.toMatchObject({
      status: "matched",
      matches: [
        {
          committeeKey: "FRIENDS OF JANE DOE",
          committeeName: "Friends of Jane Doe",
          confidence: "name_fallback",
          source: "illinois_sbe",
          sourceUrl: CONTRIBUTIONS_URL,
          matchedContributionRowCount: 1,
        },
      ],
    });
  });

  it("uses official normalized relations and D-2 summaries for local offices", async () => {
    const artifacts = await loadIllinoisSbeArtifactDataSet({
      contributionCsvPaths: [],
      normalizedArtifactPath: NORMALIZED_ARTIFACT,
    });
    const resolver = createIllinoisSbeArtifactCandidateCommitteeResolver(artifacts);
    await expect(
      resolver({
        candidateName: "Jane Doe",
        officeScope: "place",
        officeName: "Mayor",
        electionYear: 2025,
        district: "Aurora city, Illinois",
      })
    ).resolves.toMatchObject({
      status: "matched",
      matches: [{ committeeKey: "SBE:201" }, { committeeKey: "SBE:202" }],
    });

    const data = loadIllinoisFinanceDataForDueRowFromArtifacts({
      row: dueRow({
        electionYear: 2025,
        officeScope: "place",
        officeName: "Mayor",
        district: "Aurora",
        sbeCandidateId: null,
        sbeDistrictType: "City",
        sbeOffice: "Mayor",
        isAtLarge: false,
        committeeKey: "SBE:201",
        committeeName: "Aurora Forward",
      }),
      artifacts,
    });
    expect(data.d2ReportSummaries?.map((report) => report.reportId)).toEqual(["report-201-q1"]);
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

  it("matches local outside expenditures to the exact municipality", () => {
    const makeExpenditure = (officeDistrict: string) => ({
      payeeName: "Mailer",
      payeeAddress: null,
      amount: 100,
      expendedDate: "3/1/2025",
      reportReceivedDate: null,
      expenditureType: "Independent Expenditures",
      expendingCommitteeName: officeDistrict,
      purpose: "Mail",
      candidateName: "Jane Doe",
      officeDistrict,
      supportOppose: "support" as const,
      sourceUrl: EXPENDITURES_URL,
    });
    const data = loadIllinoisFinanceDataForDueRowFromArtifacts({
      row: dueRow({
        electionYear: 2025,
        officeScope: "place",
        officeName: "Mayor",
        district: "Aurora city, Illinois",
        sbeDistrictType: "City",
        sbeOffice: "Mayor",
        isAtLarge: false,
      }),
      artifacts: {
        contributionRecords: [],
        contributionSourceUrl: CONTRIBUTIONS_URL,
        expenditureRecords: [makeExpenditure("Mayor - Aurora"), makeExpenditure("Mayor - North Aurora")],
        expenditureSourceUrl: EXPENDITURES_URL,
      },
    });

    expect(data.outsideExpenditureRecords?.map((record) => record.officeDistrict)).toEqual(["Mayor - Aurora"]);
  });

  it("preserves statewide matching when SBE appends statewide office text", () => {
    const officeDistrict = "Governor - State of Illinois";
    const data = loadIllinoisFinanceDataForDueRowFromArtifacts({
      row: dueRow(),
      artifacts: {
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
            expendingCommitteeName: "Statewide Support",
            purpose: "Mail",
            candidateName: "Jane Doe",
            officeDistrict,
            supportOppose: "support",
            sourceUrl: EXPENDITURES_URL,
          },
        ],
        expenditureSourceUrl: EXPENDITURES_URL,
      },
    });

    expect(data.outsideExpenditureRecords?.map((record) => record.officeDistrict)).toEqual([officeDistrict]);
  });

  it("rejects missing contribution CSV paths", async () => {
    await expect(loadIllinoisSbeArtifactDataSet({ contributionCsvPaths: [" "] })).rejects.toThrow(
      "Illinois SBE contribution artifact requires at least one CSV path"
    );
  });
});
