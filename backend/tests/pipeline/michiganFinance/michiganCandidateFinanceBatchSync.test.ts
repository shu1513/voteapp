import { describe, expect, it, vi } from "vitest";

import {
  listDueMichiganCandidateFinanceSyncRows,
  syncDueMichiganCandidateFinance,
  type MichiganMitnLegacyDataForYear,
} from "../../../src/pipeline/michiganFinance/michiganCandidateFinanceBatchSync.js";
import type {
  MichiganMitnLegacyContributionRow,
  MichiganMitnLegacyExpenditureRow,
} from "../../../src/pipeline/michiganFinance/michiganMitnLegacyArchiveReader.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const SOURCE_URL = "https://www.michigan.gov/sos/example/2022_mi_cfr.7z";

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows }),
  };
}

function dueRow(overrides: Record<string, unknown> = {}) {
  return {
    candidate_id: CANDIDATE_ID,
    election_id: ELECTION_ID,
    candidate_name: "Gretchen Whitmer",
    election_year: 2022,
    office_scope: "statewide",
    office_name: "Governor",
    district: null,
    committee_id: "514456",
    committee_name: "WHITMER FOR GOVERNOR",
    source_url: SOURCE_URL,
    last_synced_at: null,
    total_due_rows: "1",
    ...overrides,
  };
}

function contribution(
  overrides: Partial<MichiganMitnLegacyContributionRow> = {}
): MichiganMitnLegacyContributionRow {
  return {
    doc_seq_no: "100",
    page_no: "1",
    contribution_id: "200",
    cont_detail_id: "300",
    doc_stmnt_year: "2022",
    doc_type_desc: "Post-General",
    com_legal_name: "WHITMER FOR GOVERNOR",
    common_name: "Whitmer for Governor",
    cfr_com_id: "514456",
    com_type: "Candidate Committee",
    can_first_name: "GRETCHEN",
    can_last_name: "WHITMER",
    contribtype: "Individual",
    f_name: "JANE",
    l_name_or_org: "DOE",
    address: "1 Main",
    city: "Lansing",
    state: "MI",
    zip: "48901",
    occupation: "Attorney",
    employer: "Law Firm",
    received_date: "10/01/2022",
    amount: "100.00",
    aggregate: "100.00",
    extra_desc: "",
    ...overrides,
  };
}

function expenditure(overrides: Partial<MichiganMitnLegacyExpenditureRow> = {}): MichiganMitnLegacyExpenditureRow {
  return {
    doc_seq_no: "900",
    doc_stmnt_year: "2022",
    doc_type_desc: "Post-General",
    com_legal_name: "GET MICHIGAN WORKING AGAIN (SUPERPAC)",
    common_name: "Get Michigan Working Again",
    cfr_com_id: "520012",
    com_type: "Independent Expenditure Committee",
    schedule_desc: "Independent Expenditure",
    supp_opp: "2",
    can_or_ballot: "GRETCHEN WHITMER",
    amount: "863076.75",
    ...overrides,
  };
}

function mitnData(overrides: Partial<MichiganMitnLegacyDataForYear> = {}): MichiganMitnLegacyDataForYear {
  return {
    year: 2022,
    extractedDir: "/tmp/2022_mi_cfr",
    sourceUrl: SOURCE_URL,
    contributionRows: [contribution()],
    expenditureRows: [expenditure()],
    ...overrides,
  };
}

describe("michiganCandidateFinanceBatchSync", () => {
  it("lists due Michigan finance sync rows from explicit active links", async () => {
    const db = createMockDb([dueRow()]);

    const result = await listDueMichiganCandidateFinanceSyncRows(db, {
      now: new Date("2022-06-01T00:00:00.000Z"),
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
          candidateName: "Gretchen Whitmer",
          electionYear: 2022,
          officeScope: "statewide",
          officeName: "Governor",
          district: null,
          committeeId: "514456",
          committeeName: "WHITMER FOR GOVERNOR",
          sourceUrl: SOURCE_URL,
          lastSyncedAt: null,
        },
      ],
    });

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.mi_candidate_finance_links AS link");
    expect(sql).toContain("link.link_status = 'active'");
    expect(sql).toContain("district.state = 'MI'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($6::text[])");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2022-06-01T00:00:00.000Z",
      7,
      25,
      30,
      730,
      expect.arrayContaining(["statewide::Governor", "state_upper::State Senator"]),
    ]);
  });

  it("uses a one-day post-election grace window by default for due selection", async () => {
    const db = createMockDb();

    await syncDueMichiganCandidateFinance({
      db,
      syncMichiganCandidateFinanceFn: vi.fn(),
      now: new Date("2022-06-01T00:00:00.000Z"),
      autoLinkMissingLinks: false,
    });

    expect(String(db.query.mock.calls[0]?.[0])).toContain(
      "election.election_date >= ($1::date - make_interval(days => $4::int))"
    );
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2022-06-01T00:00:00.000Z",
      7,
      25,
      1,
      730,
      expect.arrayContaining(["statewide::Governor", "state_upper::State Senator"]),
    ]);
  });

  it("syncs selected due links with injected MiTN rows", async () => {
    const db = createMockDb([dueRow()]);
    const data = mitnData();
    const syncMichiganCandidateFinanceFn = vi.fn().mockResolvedValue({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2022,
      dryRun: false,
      resolution: { status: "matched", committeeId: "514456" },
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 3,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 2,
      totalReceipts: 100,
      directContributionTotal: 100,
      outsideSupportTotal: 0,
      outsideOpposeTotal: 863076.75,
      matchedContributionRowCount: 1,
      includedContributionRowCount: 1,
      skippedContributionRowCount: 0,
      matchedOutsideExpenditureRowCount: 1,
      includedOutsideExpenditureRowCount: 1,
      skippedOutsideExpenditureRowCount: 0,
      matchedOutsideContributionRowCount: 0,
      includedOutsideContributionRowCount: 0,
      skippedOutsideContributionRowCount: 0,
    });

    const result = await syncDueMichiganCandidateFinance({
      db,
      syncMichiganCandidateFinanceFn,
      now: new Date("2022-06-01T00:00:00.000Z"),
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: 30,
      mitnDataByYear: new Map([[2022, data]]),
      autoLinkMissingLinks: false,
    });

    expect(result).toMatchObject({
      dryRun: false,
      now: "2022-06-01T00:00:00.000Z",
      staleAfterDays: 3,
      maxCandidates: 2,
      dueCandidateCount: 1,
      selectedCandidateCount: 1,
      syncedCandidateCount: 1,
      failedCandidateCount: 0,
    });
    expect(result.results[0]).toMatchObject({
      ok: true,
      committeeId: "514456",
      result: {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        totalReceipts: 100,
      },
    });
    expect(syncMichiganCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Gretchen Whitmer",
        electionYear: 2022,
        officeScope: "statewide",
        officeName: "Governor",
        district: null,
        sourceUrl: SOURCE_URL,
        contributionSourceUrl: SOURCE_URL,
        outsideSourceUrl: SOURCE_URL,
        contributionRows: data.contributionRows,
        expenditureRows: data.expenditureRows,
        trustedCommittee: {
          committeeId: "514456",
          committeeName: "WHITMER FOR GOVERNOR",
          sourceUrl: SOURCE_URL,
        },
      })
    );
  });

  it("auto-links missing Michigan finance links before listing due rows", async () => {
    const db = {
      query: vi.fn(async (sql: string) => {
        const text = String(sql);
        if (text.includes("FROM public.candidate_elections AS candidate_election")) {
          return {
            rows: [
              {
                candidate_id: CANDIDATE_ID,
                election_id: ELECTION_ID,
                candidate_name: "Gretchen Whitmer",
                election_year: 2022,
                office_scope: "statewide",
                office_name: "Governor",
                district: null,
              },
            ],
            rowCount: 1,
          };
        }
        if (text.includes("INSERT INTO public.mi_candidate_finance_links")) {
          return { rows: [{ id: "link-1" }], rowCount: 1 };
        }
        if (text.includes("FROM public.mi_candidate_finance_links AS link")) {
          return { rows: [dueRow()], rowCount: 1 };
        }
        throw new Error(`Unexpected query: ${text}`);
      }),
    };
    const syncMichiganCandidateFinanceFn = vi.fn().mockResolvedValue({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2022,
      dryRun: false,
      resolution: { status: "matched", committeeId: "514456" },
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 3,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 2,
      totalReceipts: 100,
      directContributionTotal: 100,
      outsideSupportTotal: 0,
      outsideOpposeTotal: 863076.75,
      matchedContributionRowCount: 1,
      includedContributionRowCount: 1,
      skippedContributionRowCount: 0,
      matchedOutsideExpenditureRowCount: 1,
      includedOutsideExpenditureRowCount: 1,
      skippedOutsideExpenditureRowCount: 0,
      matchedOutsideContributionRowCount: 0,
      includedOutsideContributionRowCount: 0,
      skippedOutsideContributionRowCount: 0,
    });

    const result = await syncDueMichiganCandidateFinance({
      db,
      syncMichiganCandidateFinanceFn,
      now: new Date("2022-06-01T00:00:00.000Z"),
      mitnDataByYear: new Map([[2022, mitnData()]]),
    });

    expect(result).toMatchObject({
      dueCandidateCount: 1,
      selectedCandidateCount: 1,
      syncedCandidateCount: 1,
      failedCandidateCount: 0,
    });
    expect(db.query).toHaveBeenCalledTimes(3);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("FROM public.candidate_elections AS candidate_election");
    expect(String(db.query.mock.calls[1]?.[0])).toContain("INSERT INTO public.mi_candidate_finance_links");
    expect(String(db.query.mock.calls[2]?.[0])).toContain("FROM public.mi_candidate_finance_links AS link");
  });

  it("validates positive integer options", async () => {
    const db = createMockDb();

    await expect(
      syncDueMichiganCandidateFinance({
        db,
        maxCandidates: 0,
        autoLinkMissingLinks: false,
      })
    ).rejects.toThrow("Invalid Michigan finance batch sync maxCandidates");
    await expect(
      syncDueMichiganCandidateFinance({
        db,
        staleAfterDays: -1,
        autoLinkMissingLinks: false,
      })
    ).rejects.toThrow("Invalid Michigan finance batch sync staleAfterDays");
  });
});
