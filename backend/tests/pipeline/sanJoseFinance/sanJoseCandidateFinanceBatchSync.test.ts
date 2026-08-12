import { describe, expect, it, vi } from "vitest";
import { syncDueSanJoseCandidateFinance } from "../../../src/pipeline/sanJoseFinance/sanJoseCandidateFinanceBatchSync.js";
import type { EfileCalWorkbook } from "../../../src/pipeline/efileCalFinance/efileCalWorkbookParser.js";

const emptyWorkbook: EfileCalWorkbook = {
  summary: [],
  scheduleA: [],
  scheduleC: [],
  scheduleB1: [],
  scheduleD: [],
  s496: [],
  s497: [],
};

const NOW = new Date("2026-08-11T00:00:00Z");
const ELECTION_UUID = "0b0b0b0b-0000-4000-8000-000000000001";

function dueRow(over: Record<string, unknown> = {}) {
  return {
    candidate_id: "c1",
    election_id: "e1",
    election_year: 2026,
    candidate_name: "Jane Doe",
    office_name: "City Council Member",
    official_ballot_title: "Member, City Council, District 5",
    fppc_id: "1234567",
    last_synced_at: null,
    total_due_rows: "2",
    ...over,
  };
}

function makeDb(input: { missingLinkRows?: unknown[]; dueRows: unknown[] }) {
  const query = vi.fn().mockImplementation((sql: unknown) => {
    const s = String(sql);
    if (s.startsWith("WITH due AS"))
      return Promise.resolve({ rows: input.dueRows });
    if (s.startsWith("SELECT candidate.id::text"))
      return Promise.resolve({ rows: input.missingLinkRows ?? [] });
    if (s.startsWith("INSERT INTO public.sjc_candidate_finance_links"))
      return Promise.resolve({ rows: [{ id: "link-1" }] });
    return Promise.resolve({ rows: [] });
  });
  return { query, connect: vi.fn() };
}

function workbookLoader() {
  return vi.fn().mockImplementation(async (input: { electionYear: number }) => ({
    electionYear: input.electionYear,
    years: [input.electionYear - 1, input.electionYear],
    workbook: emptyWorkbook,
    sources: [
      { year: input.electionYear - 1, status: "cached" as const },
      { year: input.electionYear, status: "cached" as const },
    ],
  }));
}

describe("syncDueSanJoseCandidateFinance", () => {
  it("loads the cycle workbooks once and syncs due candidates stalest-first", async () => {
    const db = makeDb({
      dueRows: [
        dueRow(),
        dueRow({ candidate_id: "c2", office_name: "Mayor", official_ballot_title: "Mayor" }),
      ],
    });
    const loadWorkbookData = workbookLoader();
    const syncCandidateFn = vi.fn().mockResolvedValue({});
    const result = await syncDueSanJoseCandidateFinance({
      db: db as never,
      now: NOW,
      autoLinkMissingLinks: false,
      loadWorkbookData,
      syncCandidateFn,
    });
    expect(loadWorkbookData).toHaveBeenCalledTimes(1);
    expect(syncCandidateFn).toHaveBeenCalledTimes(2);
    expect(syncCandidateFn.mock.calls[0]?.[0]).toMatchObject({
      candidateId: "c1",
      officeName: "City Council Member",
      seatNumber: 5,
      fppcId: "1234567",
      workbook: emptyWorkbook,
    });
    expect(syncCandidateFn.mock.calls[1]?.[0]).toMatchObject({
      candidateId: "c2",
      officeName: "Mayor",
      seatNumber: null,
    });
    expect(result).toMatchObject({
      dueCandidateCount: 2,
      selectedCandidateCount: 2,
      syncedCandidateCount: 2,
      failedCandidateCount: 0,
      workbookSources: [
        { year: 2025, status: "cached" },
        { year: 2026, status: "cached" },
      ],
    });
  });

  it("runs the auto-link leg through the real resolver before the due loop", async () => {
    const db = makeDb({
      missingLinkRows: [
        {
          candidate_id: "c1",
          election_id: "e1",
          candidate_name: "Jane Doe",
          election_date: "2026-11-03",
          state: "CA",
          district_type: "place",
          geoid_compact: "0668000",
          office_scope: "place",
          office_name: "City Council Member",
          official_ballot_title: "Member, City Council, District 5",
          state_filing_ids: [],
        },
      ],
      dueRows: [],
    });
    const loadWorkbookData = vi.fn().mockResolvedValue({
      electionYear: 2026,
      years: [2025, 2026],
      workbook: {
        ...emptyWorkbook,
        summary: [
          {
            filerId: "1234567",
            filerName: "Jane Doe for City Council District 5 2026",
            reportNum: "000",
            eFilingId: "100",
            origEFilingId: "100",
            cmtteType: "C",
            rptDate: null,
            fromDate: null,
            thruDate: null,
            electDate: null,
            formType: "F460",
            lineItem: "1",
            amountACents: 0,
            amountBCents: null,
            amountCCents: null,
          },
        ],
      },
      sources: [{ year: 2026, status: "cached" as const }],
    });
    const result = await syncDueSanJoseCandidateFinance({
      db: db as never,
      now: NOW,
      loadWorkbookData,
      syncCandidateFn: vi.fn().mockResolvedValue({}),
    });
    expect(result.autoLinkAttemptedCount).toBe(1);
    expect(result.autoLinkLinkedCount).toBe(1);
    const insert = db.query.mock.calls.find((call) =>
      String(call[0]).startsWith(
        "INSERT INTO public.sjc_candidate_finance_links",
      ),
    );
    expect(insert?.[1]).toEqual(expect.arrayContaining(["1234567"]));
  });

  it("fails each candidate with the cached workbook error instead of refetching", async () => {
    const db = makeDb({ dueRows: [dueRow(), dueRow({ candidate_id: "c2" })] });
    const loadWorkbookData = vi
      .fn()
      .mockRejectedValue(new Error("workbook is not cached"));
    const result = await syncDueSanJoseCandidateFinance({
      db: db as never,
      now: NOW,
      autoLinkMissingLinks: false,
      loadWorkbookData,
      syncCandidateFn: vi.fn(),
    });
    expect(loadWorkbookData).toHaveBeenCalledTimes(1);
    expect(result.failedCandidateCount).toBe(2);
    expect(result.results.every((row) => /not cached/.test(row.error ?? ""))).toBe(
      true,
    );
  });

  it("election targeting skips auto-link and swaps the due scope", async () => {
    const db = makeDb({ dueRows: [dueRow()] });
    const syncCandidateFn = vi.fn().mockResolvedValue({});
    await syncDueSanJoseCandidateFinance({
      db: db as never,
      now: NOW,
      electionId: ELECTION_UUID,
      loadWorkbookData: workbookLoader(),
      syncCandidateFn,
    });
    const listCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).startsWith("SELECT candidate.id::text"),
    );
    expect(listCalls).toHaveLength(0);
    const dueCall = db.query.mock.calls.find((call) =>
      String(call[0]).startsWith("WITH due AS"),
    );
    expect(String(dueCall?.[0])).toContain("election.id=$1::uuid");
    expect(String(dueCall?.[0])).not.toContain("withdrawn");
    expect(dueCall?.[1]).toEqual([ELECTION_UUID, 25]);
    expect(syncCandidateFn).toHaveBeenCalledTimes(1);
  });

  it("rejects a malformed electionId before any work", async () => {
    const db = makeDb({ dueRows: [] });
    await expect(
      syncDueSanJoseCandidateFinance({
        db: db as never,
        now: NOW,
        electionId: "not-a-uuid",
        loadWorkbookData: workbookLoader(),
        syncCandidateFn: vi.fn(),
      }),
    ).rejects.toThrow(/Invalid San José finance electionId/);
    expect(db.query).not.toHaveBeenCalled();
  });

  it("errors a council row whose ballot title has no parseable district", async () => {
    const db = makeDb({
      dueRows: [dueRow({ official_ballot_title: "City Council" })],
    });
    const syncCandidateFn = vi.fn();
    const result = await syncDueSanJoseCandidateFinance({
      db: db as never,
      now: NOW,
      autoLinkMissingLinks: false,
      loadWorkbookData: workbookLoader(),
      syncCandidateFn,
    });
    expect(syncCandidateFn).not.toHaveBeenCalled();
    expect(result.results[0]).toMatchObject({
      ok: false,
      error: expect.stringContaining("Cannot parse a council district"),
    });
  });

  it("passes dryRun through and skips the auto-link leg", async () => {
    const db = makeDb({ dueRows: [dueRow()] });
    const syncCandidateFn = vi.fn().mockResolvedValue({});
    const result = await syncDueSanJoseCandidateFinance({
      db: db as never,
      now: NOW,
      dryRun: true,
      loadWorkbookData: workbookLoader(),
      syncCandidateFn,
    });
    expect(result.dryRun).toBe(true);
    expect(
      db.query.mock.calls.filter((call) =>
        String(call[0]).startsWith("SELECT candidate.id::text"),
      ),
    ).toHaveLength(0);
    expect(syncCandidateFn.mock.calls[0]?.[0]).toMatchObject({ dryRun: true });
  });
});
