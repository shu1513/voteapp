import { describe, expect, it, vi } from "vitest";

import { syncIllinoisCandidateFinance } from "../../../src/pipeline/illinoisFinance/syncIllinoisCandidateFinance.js";
import type {
  IllinoisSbeContributionRecord,
  IllinoisSbeExpenditureRecord,
} from "../../../src/pipeline/illinoisFinance/illinoisSbeClient.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const LINK_ID = "33333333-3333-4333-8333-333333333333";
const SOURCE_URL = "https://www.elections.il.gov/CampaignDisclosure/ContributionSearchByCandidates.aspx";

function createMockDb() {
  const poolQuery = vi.fn(async (sql: string) => {
    if (String(sql).includes("FROM public.finance_label_classifications AS classification")) {
      return { rows: [], rowCount: 0 };
    }
    return { rows: [{ id: LINK_ID }], rowCount: 1 };
  });
  const clientQuery = vi.fn(async () => ({ rows: [{ id: LINK_ID }], rowCount: 1 }));
  const client = {
    query: clientQuery,
    release: vi.fn(),
  };
  return {
    query: poolQuery,
    connect: vi.fn().mockResolvedValue(client),
    client,
  };
}

function contribution(overrides: Partial<IllinoisSbeContributionRecord> = {}): IllinoisSbeContributionRecord {
  return {
    contributorName: "Pat Person",
    contributorAddress: "1 Main St",
    occupation: "Attorney",
    employer: "Law LLP",
    amount: 250,
    receivedDate: "3/1/2022",
    reportReceivedDate: null,
    contributionType: "Individual Contributions",
    recipientCommitteeName: "Friends of Jane",
    description: null,
    vendorName: null,
    vendorAddress: null,
    sourceUrl: SOURCE_URL,
    ...overrides,
  };
}

function expenditure(overrides: Partial<IllinoisSbeExpenditureRecord> = {}): IllinoisSbeExpenditureRecord {
  return {
    payeeName: "Media Vendor",
    payeeAddress: null,
    amount: 2500,
    expendedDate: "10/1/2022",
    reportReceivedDate: null,
    expenditureType: "Independent Expenditures",
    expendingCommitteeName: "Illinois Conservation Action",
    purpose: "Digital ads",
    candidateName: "Jane Doe",
    officeDistrict: "Governor",
    supportOppose: "support",
    sourceUrl: "https://www.elections.il.gov/CampaignDisclosure/ExpenditureSearchByAllExpenditures.aspx",
    ...overrides,
  };
}

function baseInput() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    candidateName: "Jane Doe",
    electionYear: 2022,
    officeName: "Governor",
    committeeKey: "FRIENDS OF JANE",
    committeeName: "Friends of Jane",
    sourceUrl: SOURCE_URL,
    now: new Date("2022-07-08T09:10:11.000Z"),
  };
}

describe("syncIllinoisCandidateFinance", () => {
  it("aggregates Illinois SBE rows and writes a finance snapshot", async () => {
    const db = createMockDb();

    const result = await syncIllinoisCandidateFinance({
      db,
      ...baseInput(),
      directContributionRecords: [
        contribution({ amount: 100, occupation: "Attorney", contributorName: "Pat Person" }),
        contribution({ amount: 250, occupation: "Attorney", contributorName: "Sam Person" }),
        contribution({ amount: 5000, occupation: "Teacher", contributorName: "Alex Person" }),
      ],
      outsideExpenditureRecords: [expenditure()],
      outsideGroupContributionRecords: [
        contribution({
          contributorName: "Sierra Club",
          contributorAddress: null,
          occupation: null,
          employer: null,
          amount: 35_000,
          contributionType: "Transfers In",
          recipientCommitteeName: "Illinois Conservation Action",
          receivedDate: "8/1/2022",
          sourceUrl: "https://www.elections.il.gov/CampaignDisclosure/ContributionSearchByCommittees.aspx",
        }),
      ],
    });

    expect(result).toMatchObject({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2022,
      dryRun: false,
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 5,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 2,
      totalReceipts: 5350,
      directContributionTotal: 5350,
      outsideExpenditureDataAvailable: true,
      outsideGroupContributionDataAvailable: true,
      outsideSupportTotal: 2500,
      outsideOpposeTotal: 0,
      matchedContributionRowCount: 3,
      includedContributionRowCount: 3,
      skippedContributionRowCount: 0,
      matchedOutsideExpenditureRowCount: 1,
      includedOutsideExpenditureRowCount: 1,
      skippedOutsideExpenditureRowCount: 0,
      matchedOutsideContributionRowCount: 1,
      includedOutsideContributionRowCount: 1,
      skippedOutsideContributionRowCount: 0,
    });

    expect(db.query).not.toHaveBeenCalled();
    expect(db.connect).toHaveBeenCalledTimes(1);
    expect(db.client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(db.client.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");

    const linkCall = db.client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.il_candidate_finance_links")
    );
    expect(linkCall?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2022,
      "JANE DOE",
      "Governor",
      null,
      "FRIENDS OF JANE",
      "Friends of Jane",
      "active",
      "illinois_sbe",
      SOURCE_URL,
      "2022-07-08T09:10:11.000Z",
    ]);

    const outsideBreakdownCalls = db.client.query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.il_candidate_finance_outside_group_breakdowns")
    );
    expect(outsideBreakdownCalls.map((call) => call[1])).toEqual([
      [
        LINK_ID,
        2022,
        "ILLINOIS CONSERVATION ACTION",
        "support",
        "donor",
        "Sierra Club",
        35000,
        1,
        "https://www.elections.il.gov/CampaignDisclosure/ContributionSearchByCommittees.aspx",
        "2022-07-08T09:10:11.000Z",
      ],
      [
        LINK_ID,
        2022,
        "ILLINOIS CONSERVATION ACTION",
        "support",
        "industry",
        "environmental_group",
        35000,
        1,
        "https://www.elections.il.gov/CampaignDisclosure/ContributionSearchByCommittees.aspx",
        "2022-07-08T09:10:11.000Z",
      ],
    ]);
  });

  it("aggregates but does not write in dry-run mode", async () => {
    const db = createMockDb();

    const result = await syncIllinoisCandidateFinance({
      db,
      ...baseInput(),
      dryRun: true,
      directContributionRecords: [contribution({ amount: 1000 })],
      outsideExpenditureRecords: [expenditure()],
      outsideGroupContributionRecords: [
        contribution({
          contributorName: "Sierra Club",
          contributorAddress: null,
          amount: 35_000,
          recipientCommitteeName: "Illinois Conservation Action",
          receivedDate: "8/1/2022",
        }),
      ],
    });

    expect(result).toMatchObject({
      dryRun: true,
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      totalReceipts: 1000,
      outsideExpenditureDataAvailable: true,
      outsideGroupContributionDataAvailable: true,
      outsideSupportTotal: 2500,
    });
    expect(db.query).not.toHaveBeenCalled();
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("preserves outside group breakdowns when outside funder rows were not fetched", async () => {
    const db = createMockDb();

    const result = await syncIllinoisCandidateFinance({
      db,
      ...baseInput(),
      directContributionRecords: [contribution({ amount: 1000 })],
      outsideExpenditureRecords: [expenditure()],
    });

    expect(result).toMatchObject({
      outsideExpenditureDataAvailable: true,
      outsideGroupContributionDataAvailable: false,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 0,
      matchedOutsideContributionRowCount: 0,
      includedOutsideContributionRowCount: 0,
      skippedOutsideContributionRowCount: 0,
    });
    const statements = db.client.query.mock.calls.map((call) => String(call[0]));
    expect(statements.some((statement) => statement.includes("INSERT INTO public.il_candidate_finance_outside_groups"))).toBe(true);
    expect(
      statements.some((statement) => statement.includes("INSERT INTO public.il_candidate_finance_outside_group_breakdowns"))
    ).toBe(false);
    expect(
      statements.some((statement) => statement.includes("DELETE FROM public.il_candidate_finance_outside_group_breakdowns"))
    ).toBe(false);
  });

  it("marks outside data unavailable when outside expenditure rows were not supplied", async () => {
    const db = createMockDb();

    const result = await syncIllinoisCandidateFinance({
      db,
      ...baseInput(),
      directContributionRecords: [contribution({ amount: 1000 })],
    });

    expect(result).toMatchObject({
      outsideExpenditureDataAvailable: false,
      outsideGroupContributionDataAvailable: false,
      outsideSupportTotal: null,
      outsideOpposeTotal: null,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      matchedOutsideExpenditureRowCount: 0,
      matchedOutsideContributionRowCount: 0,
    });

    const summaryCall = db.client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.il_candidate_finance_summaries")
    );
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2022,
      1000,
      1000,
      null,
      null,
      null,
      null,
      SOURCE_URL,
      "2022-07-08T09:10:11.000Z",
    ]);
    const statements = db.client.query.mock.calls.map((call) => String(call[0]));
    expect(statements.some((statement) => statement.includes("INSERT INTO public.il_candidate_finance_outside_groups"))).toBe(false);
    expect(
      statements.some((statement) => statement.includes("INSERT INTO public.il_candidate_finance_outside_group_breakdowns"))
    ).toBe(false);
  });

  it("uses shared AI classification for high-dollar unknown PAC funders", async () => {
    const db = createMockDb();
    const classifier = vi.fn().mockResolvedValue([
      {
        rawLabel: "Made Up Homes LLC",
        labelType: "donor",
        normalizedLabel: "MADE UP HOMES",
        industrySlug: "real_estate",
        confidence: "medium",
        classificationSource: "ai",
        matchedRule: null,
      },
    ]);

    await syncIllinoisCandidateFinance({
      db,
      ...baseInput(),
      financeIndustryClassifier: classifier,
      aiClassificationMinAmount: 25_000,
      directContributionRecords: [contribution({ amount: 100 })],
      outsideExpenditureRecords: [expenditure()],
      outsideGroupContributionRecords: [
        contribution({
          contributorName: "Made Up Homes LLC",
          contributorAddress: null,
          occupation: null,
          employer: null,
          amount: 25_000,
          contributionType: "Transfers In",
          recipientCommitteeName: "Illinois Conservation Action",
          receivedDate: "8/1/2022",
        }),
      ],
    });

    expect(classifier).toHaveBeenCalledWith({
      labels: [
        {
          rawLabel: "Made Up Homes LLC",
          labelType: "donor",
          normalizedLabel: "MADE UP HOMES",
          amount: 25000,
        },
      ],
    });
    const params = db.client.query.mock.calls.map((call) => call[1]).filter(Array.isArray);
    expect(params.some((paramList) => paramList.includes("real_estate"))).toBe(true);
  });
});
