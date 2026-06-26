import { describe, expect, it, vi } from "vitest";

import {
  listDueAlaskaCandidateFinanceSyncRows,
  syncDueAlaskaCandidateFinance,
  type AlaskaApocFinanceDataSet,
} from "../../../src/pipeline/alaskaFinance/alaskaCandidateFinanceBatchSync.js";
import type {
  AlaskaApocCampaignIncomeRow,
  AlaskaApocIndependentContributionRow,
  AlaskaApocIndependentExpenditureRow,
} from "../../../src/pipeline/alaskaFinance/alaskaApocClient.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const SOURCE_URL = "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx";

function createMockDb(rows: unknown[] = []) {
  const query = vi.fn().mockResolvedValue({ rows });
  const client = { query, release: vi.fn() };
  return {
    query,
    connect: vi.fn().mockResolvedValue(client),
    client,
  };
}

function dueRow(overrides: Record<string, unknown> = {}) {
  return {
    candidate_id: CANDIDATE_ID,
    election_id: ELECTION_ID,
    candidate_name: "Jane Doe",
    election_year: 2026,
    office_scope: "statewide",
    office_name: "Governor",
    district: null,
    candidate_filer_id: "1001",
    candidate_filer_name: "Doe, Jane",
    source_url: SOURCE_URL,
    last_synced_at: null,
    total_due_rows: "1",
    ...overrides,
  };
}

function income(overrides: Partial<AlaskaApocCampaignIncomeRow> = {}): AlaskaApocCampaignIncomeRow {
  return {
    reportYear: 2026,
    filerId: "1001",
    filerName: "Doe, Jane",
    filerType: "Candidate",
    name: "Doe, Jane",
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
    sourceUrl: SOURCE_URL,
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

function apocData(overrides: Partial<AlaskaApocFinanceDataSet> = {}): AlaskaApocFinanceDataSet {
  return {
    incomeRows: [income()],
    independentExpenditureRows: [expenditure()],
    independentContributionRows: [contribution()],
    incomeSourceUrl: SOURCE_URL,
    ...overrides,
  };
}

describe("alaskaCandidateFinanceBatchSync", () => {
  it("lists due Alaska finance sync rows from active links", async () => {
    const db = createMockDb([dueRow()]);

    const result = await listDueAlaskaCandidateFinanceSyncRows(db, {
      now: new Date("2026-06-25T12:00:00.000Z"),
      staleAfterDays: 7,
      maxCandidates: 25,
      electionLookbackDays: 30,
      electionLookaheadDays: 730,
    });

    expect(result).toEqual({
      totalDueRows: 1,
      rows: [
        {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Jane Doe",
          electionYear: 2026,
          officeScope: "statewide",
          officeName: "Governor",
          district: null,
          candidateFilerId: "1001",
          candidateFilerName: "Doe, Jane",
          sourceUrl: SOURCE_URL,
          lastSyncedAt: null,
        },
      ],
    });

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.ak_candidate_finance_links AS link");
    expect(sql).toContain("link.link_status = 'active'");
    expect(sql).toContain("district.state = 'AK'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($6::text[])");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-25T12:00:00.000Z",
      7,
      25,
      30,
      730,
      expect.arrayContaining(["statewide::Governor", "state_upper::State Senator"]),
    ]);
  });

  it("syncs selected due links with injected APOC rows", async () => {
    const db = createMockDb([dueRow()]);
    const data = apocData();
    const syncAlaskaCandidateFinanceFn = vi.fn().mockResolvedValue({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2026,
      dryRun: false,
      resolution: { status: "matched", candidateFilerId: "1001" },
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 2,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 2,
      totalReceipts: 250,
      directContributionTotal: 250,
      outsideSupportTotal: 35000,
      outsideOpposeTotal: 0,
      matchedContributionRowCount: 1,
      includedContributionRowCount: 1,
      skippedContributionRowCount: 0,
      matchedExpenditureRowCount: 1,
      includedExpenditureRowCount: 1,
      skippedExpenditureRowCount: 0,
      matchedOutsideContributionRowCount: 1,
      includedOutsideContributionRowCount: 1,
      skippedOutsideContributionRowCount: 0,
    });

    const result = await syncDueAlaskaCandidateFinance({
      db,
      syncAlaskaCandidateFinanceFn,
      now: new Date("2026-06-25T12:00:00.000Z"),
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: 30,
      apocData: data,
      autoLinkMissingLinks: false,
    });

    expect(result).toMatchObject({
      dryRun: false,
      now: "2026-06-25T12:00:00.000Z",
      staleAfterDays: 3,
      maxCandidates: 2,
      dueCandidateCount: 1,
      selectedCandidateCount: 1,
      syncedCandidateCount: 1,
      failedCandidateCount: 0,
      autoLinkAttemptedCount: 0,
      autoLinkLinkedCount: 0,
    });
    expect(result.results[0]).toMatchObject({
      ok: true,
      candidateFilerId: "1001",
      result: {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        totalReceipts: 250,
      },
    });
    expect(syncAlaskaCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Jane Doe",
        electionYear: 2026,
        officeName: "Governor",
        district: null,
        sourceUrl: SOURCE_URL,
        incomeRows: data.incomeRows,
        independentExpenditureRows: data.independentExpenditureRows,
        independentContributionRows: data.independentContributionRows,
        trustedCommittee: {
          candidateFilerId: "1001",
          candidateFilerName: "Doe, Jane",
          sourceUrl: SOURCE_URL,
        },
      })
    );
  });

  it("auto-links missing Alaska finance links before listing due rows", async () => {
    const db = {
      query: vi.fn(async (sql: string) => {
        const text = String(sql);
        if (text.includes("FROM public.candidate_elections AS candidate_election")) {
          return {
            rows: [
              {
                candidate_id: CANDIDATE_ID,
                election_id: ELECTION_ID,
                candidate_name: "Jane Doe",
                election_year: 2026,
                office_scope: "statewide",
                office_name: "Governor",
                district: null,
              },
            ],
            rowCount: 1,
          };
        }
        if (text.includes("INSERT INTO public.ak_candidate_finance_links")) {
          return { rows: [{ id: "link-1" }], rowCount: 1 };
        }
        if (text.includes("FROM public.ak_candidate_finance_links AS link")) {
          return { rows: [dueRow()], rowCount: 1 };
        }
        throw new Error(`Unexpected query: ${text}`);
      }),
      connect: vi.fn(),
    };
    const syncAlaskaCandidateFinanceFn = vi.fn().mockResolvedValue({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2026,
      dryRun: false,
      resolution: { status: "matched", candidateFilerId: "1001" },
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 2,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      totalReceipts: 250,
      directContributionTotal: 250,
      outsideSupportTotal: 0,
      outsideOpposeTotal: 0,
      matchedContributionRowCount: 1,
      includedContributionRowCount: 1,
      skippedContributionRowCount: 0,
      matchedExpenditureRowCount: 0,
      includedExpenditureRowCount: 0,
      skippedExpenditureRowCount: 0,
      matchedOutsideContributionRowCount: 0,
      includedOutsideContributionRowCount: 0,
      skippedOutsideContributionRowCount: 0,
    });

    const result = await syncDueAlaskaCandidateFinance({
      db,
      syncAlaskaCandidateFinanceFn,
      now: new Date("2026-06-25T12:00:00.000Z"),
      apocData: apocData(),
      autoLinkMissingLinks: true,
    });

    expect(result).toMatchObject({
      dueCandidateCount: 1,
      selectedCandidateCount: 1,
      syncedCandidateCount: 1,
      failedCandidateCount: 0,
      autoLinkAttemptedCount: 1,
      autoLinkLinkedCount: 1,
    });
    expect(db.query).toHaveBeenCalledTimes(3);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("FROM public.candidate_elections AS candidate_election");
    expect(String(db.query.mock.calls[1]?.[0])).toContain("INSERT INTO public.ak_candidate_finance_links");
    expect(String(db.query.mock.calls[2]?.[0])).toContain("FROM public.ak_candidate_finance_links AS link");
  });

  it("does not auto-link missing finance links during dry-run", async () => {
    const db = createMockDb([dueRow()]);
    const syncAlaskaCandidateFinanceFn = vi.fn().mockResolvedValue({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2026,
      dryRun: true,
      resolution: { status: "matched", candidateFilerId: "1001" },
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      totalReceipts: 250,
      directContributionTotal: 250,
      outsideSupportTotal: 0,
      outsideOpposeTotal: 0,
      matchedContributionRowCount: 1,
      includedContributionRowCount: 1,
      skippedContributionRowCount: 0,
      matchedExpenditureRowCount: 0,
      includedExpenditureRowCount: 0,
      skippedExpenditureRowCount: 0,
      matchedOutsideContributionRowCount: 0,
      includedOutsideContributionRowCount: 0,
      skippedOutsideContributionRowCount: 0,
    });

    await syncDueAlaskaCandidateFinance({
      db,
      dryRun: true,
      syncAlaskaCandidateFinanceFn,
      now: new Date("2026-06-25T12:00:00.000Z"),
      apocData: apocData(),
    });

    expect(String(db.query.mock.calls[0]?.[0])).toContain("FROM public.ak_candidate_finance_links AS link");
    expect(db.query.mock.calls.map((call) => String(call[0])).some((sql) => sql.includes("INSERT INTO public.ak_candidate_finance_links"))).toBe(false);
  });
});
