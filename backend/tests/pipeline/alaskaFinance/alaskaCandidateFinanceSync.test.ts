import { describe, expect, it, vi } from "vitest";

import { syncAlaskaCandidateFinance } from "../../../src/pipeline/alaskaFinance/alaskaCandidateFinanceSync.js";
import type {
  AlaskaApocCampaignIncomeRow,
  AlaskaApocIndependentContributionRow,
  AlaskaApocIndependentExpenditureRow,
} from "../../../src/pipeline/alaskaFinance/alaskaApocClient.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";

function createMockDb() {
  const query = vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 });
  const client = { query, release: vi.fn() };
  return {
    query,
    connect: vi.fn().mockResolvedValue(client),
    client,
  };
}

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
    businessPhone: "907-555-0100",
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
    description: "Mailers supporting Jane Doe",
    reportType: "24-hour",
    election: "General",
    paymentType: "Card",
    paymentDetail: "ad buy",
    amount: 35_000,
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
    businessPhone: "907-555-0100",
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

function baseInput() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    candidateName: "Jane Doe",
    electionYear: 2026,
    officeName: "Governor",
    now: new Date("2026-06-25T12:00:00.000Z"),
    trustedCommittee: {
      candidateFilerId: "1001",
      candidateFilerName: "Jane Doe",
      sourceUrl: "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx",
    },
  };
}

describe("alaskaCandidateFinanceSync", () => {
  it("aggregates trusted APOC rows and writes an Alaska finance snapshot", async () => {
    const db = createMockDb();

    const result = await syncAlaskaCandidateFinance({
      db,
      ...baseInput(),
      incomeRows: [
        income({ contributor: "Smith, Pat", occupation: "Attorney", amount: 250 }),
        income({ contributor: "Roe, Alex", occupation: "Attorney", amount: 500 }),
        income({ contributor: "Teacher, Robin", occupation: "Teacher", amount: 5_000 }),
        income({ amount: 1_000, type: "Refund", status: "Rejected" }),
      ],
      independentExpenditureRows: [expenditure()],
      independentContributionRows: [contribution()],
    });

    expect(result).toMatchObject({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2026,
      dryRun: false,
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 5,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 2,
      totalReceipts: 5750,
      directContributionTotal: 5750,
      outsideSupportTotal: 35000,
      outsideOpposeTotal: 0,
      matchedContributionRowCount: 4,
      includedContributionRowCount: 3,
      skippedContributionRowCount: 1,
      matchedExpenditureRowCount: 1,
      includedExpenditureRowCount: 1,
      skippedExpenditureRowCount: 0,
      matchedOutsideContributionRowCount: 1,
      includedOutsideContributionRowCount: 1,
      skippedOutsideContributionRowCount: 0,
      resolution: {
        status: "matched",
        candidateFilerId: "1001",
        candidateFilerName: "Jane Doe",
        source: "apoc_csv",
      },
    });

    expect(db.query.mock.calls.some((call) => call[0] === "BEGIN")).toBe(true);
    expect(db.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");

    const linkCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ak_candidate_finance_links")
    );
    expect(linkCall?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "JANE DOE",
      "Governor",
      null,
      "1001",
      "Jane Doe",
      "active",
      "apoc_csv",
      "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx",
      "2026-06-25T12:00:00.000Z",
    ]);

    const summaryCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ak_candidate_finance_summaries")
    );
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2026,
      5750,
      5750,
      null,
      null,
      35000,
      0,
      "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx",
      "2026-06-25T12:00:00.000Z",
    ]);

    expect(
      db.query.mock.calls.filter((call) => String(call[0]).includes("INSERT INTO public.ak_candidate_finance_direct_breakdowns"))
    ).toHaveLength(5);
    expect(
      db.query.mock.calls.filter((call) => String(call[0]).includes("INSERT INTO public.ak_candidate_finance_outside_groups"))
    ).toHaveLength(1);
    expect(
      db.query.mock.calls.filter((call) => String(call[0]).includes("INSERT INTO public.ak_candidate_finance_outside_group_breakdowns"))
    ).toHaveLength(2);
  });

  it("does not write in dry-run mode but returns aggregation counts", async () => {
    const db = createMockDb();

    const result = await syncAlaskaCandidateFinance({
      db,
      ...baseInput(),
      dryRun: true,
      incomeRows: [income({ amount: 250 })],
      independentExpenditureRows: [expenditure()],
      independentContributionRows: [contribution()],
    });

    expect(result).toMatchObject({
      dryRun: true,
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      totalReceipts: 250,
      directContributionTotal: 250,
      outsideSupportTotal: 35000,
    });
    expect(db.query).not.toHaveBeenCalled();
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("requires trusted APOC candidate filer metadata", async () => {
    const db = createMockDb();

    await expect(
      syncAlaskaCandidateFinance({
        db,
        ...baseInput(),
        trustedCommittee: { candidateFilerId: " ", candidateFilerName: "Jane Doe" },
        incomeRows: [],
      })
    ).rejects.toThrow("trusted Alaska candidate filer id is required");
    expect(db.query).not.toHaveBeenCalled();
  });
});
