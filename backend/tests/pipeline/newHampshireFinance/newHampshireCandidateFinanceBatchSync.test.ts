import { describe, expect, it, vi } from "vitest";

import {
  chooseSyncCandidateName,
  createSharedNewHampshireCfsBatchClient,
  syncDueNewHampshireCandidateFinance,
  type NewHampshireCfsBatchClient,
} from "../../../src/pipeline/newHampshireFinance/newHampshireCandidateFinanceBatchSync.js";
import type { NewHampshireCandidateFinanceDueRow } from "../../../src/pipeline/newHampshireFinance/newHampshireCandidateFinanceDueList.js";
import { CYCLE_2026_ID, ELECTION_CYCLES, filingEntity } from "./newHampshireTestFixtures.js";

const NOW = new Date("2026-09-03T00:00:00.000Z");
const OTHER_ENTITY = filingEntity({
  filingEntityId: 50_451,
  filerName: "Friends of Other Person",
  candidateName: "Other Person",
  firstName: "Other",
  lastName: "Person",
  district: "2",
});
const REGISTRY = [filingEntity(), OTHER_ENTITY];

function dueRow(overrides: Partial<NewHampshireCandidateFinanceDueRow> = {}): NewHampshireCandidateFinanceDueRow {
  return {
    candidateId: "candidate-1",
    electionId: "election-1",
    candidateName: "Sample Candidate",
    candidateNames: ["Sample Candidate"],
    electionYear: 2026,
    electionDate: "2026-11-03",
    officeScope: "state_upper",
    officeName: "State Senate",
    district: "1",
    filingEntityId: 50_450,
    filerName: "Sample Candidate Committee",
    linkSource: "cfs_registration",
    sourceUrl: "https://cfs.sos.nh.gov/",
    lastSyncedAt: null,
    ...overrides,
  };
}

const otherRow = () =>
  dueRow({
    candidateId: "candidate-2",
    candidateName: "Other Person",
    candidateNames: ["Other Person"],
    district: "2",
    filingEntityId: 50_451,
    filerName: "Friends of Other Person",
  });

function createClient(overrides: Partial<NewHampshireCfsBatchClient> = {}): NewHampshireCfsBatchClient {
  return {
    getElectionCycles: vi.fn().mockResolvedValue(ELECTION_CYCLES),
    getFilingEntities: vi.fn().mockResolvedValue(REGISTRY),
    getReceipts: vi.fn().mockResolvedValue([]),
    getIndependentExpenditures: vi.fn().mockResolvedValue([]),
    ...overrides,
  };
}

/** What the real sync returns for a linked filer whose two sections both loaded. */
function synced(filingEntityId = 50_450) {
  return {
    resolution: { status: "matched", filingEntityId },
    directAggregation: { summary: {} },
    outsideAggregation: { summary: {} },
    directSkippedReason: null,
    outsideSkippedReason: null,
  };
}

const db = { query: vi.fn(), connect: vi.fn() } as never;

describe("createSharedNewHampshireCfsBatchClient", () => {
  it("pulls the cycle list, each cycle's registry, and each cycle's IE list once, and passes receipts through", async () => {
    const base = createClient();
    const shared = createSharedNewHampshireCfsBatchClient(base);
    await Promise.all([shared.getElectionCycles(), shared.getElectionCycles()]);
    await shared.getFilingEntities({ electionCycleId: CYCLE_2026_ID });
    await shared.getFilingEntities({ electionCycleId: CYCLE_2026_ID });
    await shared.getFilingEntities({ electionCycleId: 27 });
    await shared.getIndependentExpenditures({ electionCycleId: CYCLE_2026_ID });
    await shared.getIndependentExpenditures({ electionCycleId: CYCLE_2026_ID });
    await shared.getReceipts({ filerName: "A", electionCycleId: CYCLE_2026_ID });
    await shared.getReceipts({ filerName: "A", electionCycleId: CYCLE_2026_ID });
    expect(base.getElectionCycles).toHaveBeenCalledTimes(1);
    expect(base.getFilingEntities).toHaveBeenCalledTimes(2);
    expect(base.getIndependentExpenditures).toHaveBeenCalledTimes(1);
    expect(base.getReceipts).toHaveBeenCalledTimes(2);
  });

  it("memoizes a rejection so an outage costs one call", async () => {
    const base = createClient({ getFilingEntities: vi.fn().mockRejectedValue(new Error("cfs down")) });
    const shared = createSharedNewHampshireCfsBatchClient(base);
    await expect(shared.getFilingEntities({ electionCycleId: CYCLE_2026_ID })).rejects.toThrow("cfs down");
    await expect(shared.getFilingEntities({ electionCycleId: CYCLE_2026_ID })).rejects.toThrow("cfs down");
    expect(base.getFilingEntities).toHaveBeenCalledTimes(1);
  });
});

describe("chooseSyncCandidateName", () => {
  const base = { electionCycleId: CYCLE_2026_ID, filingEntityRows: REGISTRY };

  it("returns the first spelling that resolves to the linked filer", () => {
    expect(chooseSyncCandidateName({ ...base, row: dueRow() })).toBe("Sample Candidate");
    // Auto-link may have linked from the structured spelling; the display name misses.
    expect(
      chooseSyncCandidateName({ ...base, row: dueRow({ candidateNames: ["Sam \"Sammy\" Candidate-Smith", "Sample Candidate"] }) })
    ).toBe("Sample Candidate");
  });

  it("refuses when no spelling resolves, or when the spelling now resolves to another filer", () => {
    expect(() => chooseSyncCandidateName({ ...base, row: dueRow({ candidateNames: ["Nobody Here"] }) })).toThrow(
      'no candidate spelling resolves to linked filer 50450 ("Nobody Here" -> unmatched: no_candidate_filer_match)'
    );
    // Link holds 50451 but "Sample Candidate" in district 1 resolves to 50450.
    expect(() => chooseSyncCandidateName({ ...base, row: dueRow({ filingEntityId: 50_451 }) })).toThrow(
      'no candidate spelling resolves to linked filer 50451 ("Sample Candidate" -> filer 50450)'
    );
  });
});

describe("syncDueNewHampshireCandidateFinance", () => {
  it("runs the auto-link pass, lists due rows with the defaults, resolves the cycle once, and syncs every link through the shared client", async () => {
    const client = createClient();
    const autoLink = vi.fn().mockResolvedValue([{ status: "linked" }]);
    const listDueRows = vi.fn().mockResolvedValue({ rows: [dueRow(), otherRow()], totalDueRows: 2 });
    const sync = vi.fn().mockResolvedValueOnce(synced(50_450)).mockResolvedValueOnce(synced(50_451));
    const result = await syncDueNewHampshireCandidateFinance({
      db,
      now: NOW,
      cfsClient: client,
      cfsClientOptions: { timeoutMs: 5 },
      autoLinkFn: autoLink,
      listDueRowsFn: listDueRows,
      syncCandidateFn: sync,
      log: () => {},
    });

    expect(autoLink).toHaveBeenCalledWith(
      expect.objectContaining({ db, now: NOW, maxCandidates: null, electionLookbackDays: 30, electionLookaheadDays: 730, cfsClientOptions: { timeoutMs: 5 } })
    );
    expect(listDueRows).toHaveBeenCalledWith(db, {
      now: NOW,
      staleAfterDays: 7,
      maxCandidates: 25,
      electionLookbackDays: 30,
      electionLookaheadDays: 730,
    });
    // The registry is read once for the pre-resolution of both links.
    expect(client.getElectionCycles).toHaveBeenCalledTimes(1);
    expect(client.getFilingEntities).toHaveBeenCalledTimes(1);
    expect(sync).toHaveBeenCalledTimes(2);
    const firstCall = sync.mock.calls[0]![0];
    expect(firstCall).toMatchObject({
      db,
      candidateId: "candidate-1",
      electionId: "election-1",
      candidateName: "Sample Candidate",
      electionYear: 2026,
      electionCycleId: CYCLE_2026_ID,
      officeScope: "state_upper",
      officeName: "State Senate",
      district: "1",
      sourceUrl: "https://cfs.sos.nh.gov/",
      cfsClientOptions: { timeoutMs: 5 },
      now: NOW,
      dryRun: false,
    });
    expect(sync.mock.calls[1]![0]).toMatchObject({ candidateId: "candidate-2", candidateName: "Other Person" });
    // The auto-link and every sync receive the same memoizing client.
    expect(autoLink.mock.calls[0]![0].cfsClient).toBe(firstCall.cfsClient);
    expect(sync.mock.calls[1]![0].cfsClient).toBe(firstCall.cfsClient);
    await firstCall.cfsClient.getFilingEntities({ electionCycleId: CYCLE_2026_ID });
    expect(client.getFilingEntities).toHaveBeenCalledTimes(1);
    expect(result).toMatchObject({
      dryRun: false,
      autoLinkResults: [{ status: "linked" }],
      autoLinkError: null,
      totalDueRows: 2,
      attempted: 2,
      succeeded: 2,
      failed: 0,
      electionCycleIds: { "2026": CYCLE_2026_ID },
    });
  });

  it("syncs with the structured spelling when the display name no longer resolves", async () => {
    const sync = vi.fn().mockResolvedValue(synced());
    const result = await syncDueNewHampshireCandidateFinance({
      db,
      now: NOW,
      cfsClient: createClient(),
      autoLinkMissingLinks: false,
      listDueRowsFn: vi.fn().mockResolvedValue({
        rows: [dueRow({ candidateName: "Sam \"Sammy\" Candidate-Smith", candidateNames: ["Sam \"Sammy\" Candidate-Smith", "Sample Candidate"] })],
        totalDueRows: 1,
      }),
      syncCandidateFn: sync,
      log: () => {},
    });
    expect(sync.mock.calls[0]![0]).toMatchObject({ candidateName: "Sample Candidate" });
    expect(result).toMatchObject({ succeeded: 1, failed: 0 });
  });

  it("fails a link without calling the sync when no spelling resolves to the linked filer", async () => {
    const sync = vi.fn();
    const logs: string[] = [];
    const result = await syncDueNewHampshireCandidateFinance({
      db,
      now: NOW,
      cfsClient: createClient(),
      autoLinkMissingLinks: false,
      listDueRowsFn: vi.fn().mockResolvedValue({
        rows: [dueRow({ candidateNames: ["Nobody Here"] }), dueRow({ filingEntityId: 50_451, filerName: "Friends of Other Person" })],
        totalDueRows: 2,
      }),
      syncCandidateFn: sync,
      log: (message) => logs.push(message),
    });
    expect(sync).not.toHaveBeenCalled();
    expect(result).toMatchObject({ attempted: 2, succeeded: 0, failed: 2 });
    expect(logs).toEqual([
      'New Hampshire finance sync failed for Sample Candidate (50450): no candidate spelling resolves to linked filer 50450 ("Nobody Here" -> unmatched: no_candidate_filer_match)',
      'New Hampshire finance sync failed for Sample Candidate (50451): no candidate spelling resolves to linked filer 50451 ("Sample Candidate" -> filer 50450)',
    ]);
  });

  it("records per-link failures without stopping, fails a sync that wrote nothing, and logs skipped sections", async () => {
    const sync = vi
      .fn()
      .mockRejectedValueOnce(new Error("receipts down"))
      .mockResolvedValueOnce({ ...synced(), resolution: { status: "unmatched", reason: "no_candidate_filer_match" } })
      .mockResolvedValueOnce({ ...synced(), resolution: { status: "matched", filingEntityId: 50_451 } })
      .mockResolvedValueOnce({
        ...synced(),
        directAggregation: null,
        outsideAggregation: null,
        directSkippedReason: "receipt search failed",
        outsideSkippedReason: "ie search failed",
      })
      .mockResolvedValueOnce({ ...synced(), outsideAggregation: null, outsideSkippedReason: "ie search failed" });
    const logs: string[] = [];
    const rows = ["candidate-1", "candidate-2", "candidate-3", "candidate-4", "candidate-5"].map((candidateId) => dueRow({ candidateId }));
    const result = await syncDueNewHampshireCandidateFinance({
      db,
      now: NOW,
      cfsClient: createClient(),
      autoLinkMissingLinks: false,
      listDueRowsFn: vi.fn().mockResolvedValue({ rows, totalDueRows: rows.length }),
      syncCandidateFn: sync,
      log: (message) => logs.push(message),
    });
    expect(result).toMatchObject({ attempted: 5, succeeded: 1, failed: 4 });
    expect(result.candidates.map((candidate) => candidate.ok)).toEqual([false, false, false, false, true]);
    expect(result.candidates.map((candidate) => candidate.error)).toEqual([
      "receipts down",
      "filer resolution unmatched: no_candidate_filer_match",
      "filer resolution landed on 50451, link holds 50450",
      "nothing written: direct receipt search failed; outside ie search failed",
      undefined,
    ]);
    expect(logs).toEqual([
      "New Hampshire finance sync failed for Sample Candidate (50450): receipts down",
      "New Hampshire finance sync failed for Sample Candidate (50450): filer resolution unmatched: no_candidate_filer_match",
      "New Hampshire finance sync failed for Sample Candidate (50450): filer resolution landed on 50451, link holds 50450",
      "New Hampshire finance sync failed for Sample Candidate (50450): nothing written: direct receipt search failed; outside ie search failed",
      "New Hampshire outside money skipped for Sample Candidate (50450): ie search failed",
    ]);
  });

  it("fails every link once when the cycle list cannot be pulled, without a retry per link", async () => {
    const client = createClient({ getElectionCycles: vi.fn().mockRejectedValue(new Error("cycles down")) });
    const sync = vi.fn();
    const result = await syncDueNewHampshireCandidateFinance({
      db,
      now: NOW,
      cfsClient: client,
      autoLinkMissingLinks: false,
      listDueRowsFn: vi.fn().mockResolvedValue({ rows: [dueRow(), otherRow()], totalDueRows: 2 }),
      syncCandidateFn: sync,
      log: () => {},
    });
    expect(result).toMatchObject({ attempted: 2, succeeded: 0, failed: 2, electionCycleIds: {} });
    expect(result.candidates.map((candidate) => candidate.error)).toEqual(["cycles down", "cycles down"]);
    expect(client.getElectionCycles).toHaveBeenCalledTimes(1);
    expect(sync).not.toHaveBeenCalled();
  });

  it("continues with existing links when the auto-link pass fails, exposes the error, and pulls nothing when nothing is due", async () => {
    const client = createClient();
    const logs: string[] = [];
    const result = await syncDueNewHampshireCandidateFinance({
      db,
      now: NOW,
      cfsClient: client,
      autoLinkFn: vi.fn().mockRejectedValue(new Error("db down")),
      listDueRowsFn: vi.fn().mockResolvedValue({ rows: [], totalDueRows: 0 }),
      syncCandidateFn: vi.fn(),
      log: (message) => logs.push(message),
    });
    expect(result).toEqual({
      dryRun: false,
      autoLinkResults: [],
      autoLinkError: "db down",
      totalDueRows: 0,
      attempted: 0,
      succeeded: 0,
      failed: 0,
      electionCycleIds: {},
      candidates: [],
    });
    expect(logs).toEqual(["New Hampshire auto-link pass failed (continuing with existing links): db down"]);
    expect(client.getElectionCycles).not.toHaveBeenCalled();
    expect(client.getFilingEntities).not.toHaveBeenCalled();
  });

  it("skips the auto-link pass in dry-run mode and passes dryRun to every sync", async () => {
    const autoLink = vi.fn();
    const sync = vi.fn().mockResolvedValue(synced());
    const result = await syncDueNewHampshireCandidateFinance({
      db,
      now: NOW,
      dryRun: true,
      cfsClient: createClient(),
      autoLinkFn: autoLink,
      listDueRowsFn: vi.fn().mockResolvedValue({ rows: [dueRow()], totalDueRows: 1 }),
      syncCandidateFn: sync,
      log: () => {},
    });
    expect(autoLink).not.toHaveBeenCalled();
    expect(sync.mock.calls[0]![0]).toMatchObject({ dryRun: true });
    expect(result).toMatchObject({ dryRun: true, autoLinkError: null, attempted: 1, succeeded: 1 });
  });

  it("honours --no-auto-link and custom limits", async () => {
    const autoLink = vi.fn();
    const listDueRows = vi.fn().mockResolvedValue({ rows: [], totalDueRows: 0 });
    await syncDueNewHampshireCandidateFinance({
      db,
      now: NOW,
      autoLinkMissingLinks: false,
      maxCandidates: 3,
      staleAfterDays: 1,
      electionLookbackDays: 10,
      electionLookaheadDays: 20,
      cfsClient: createClient(),
      autoLinkFn: autoLink,
      listDueRowsFn: listDueRows,
      log: () => {},
    });
    expect(autoLink).not.toHaveBeenCalled();
    expect(listDueRows).toHaveBeenCalledWith(db, {
      now: NOW,
      staleAfterDays: 1,
      maxCandidates: 3,
      electionLookbackDays: 10,
      electionLookaheadDays: 20,
    });
  });

  it("rejects non-positive batch limits", async () => {
    await expect(syncDueNewHampshireCandidateFinance({ db, now: NOW, maxCandidates: 0, log: () => {} })).rejects.toThrow(
      "Invalid New Hampshire finance batch maxCandidates: 0"
    );
  });
});
