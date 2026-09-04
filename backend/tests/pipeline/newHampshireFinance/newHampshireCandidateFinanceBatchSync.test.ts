import { describe, expect, it, vi } from "vitest";

import {
  createSharedNewHampshireCfsBatchClient,
  syncDueNewHampshireCandidateFinance,
  type NewHampshireCfsBatchClient,
} from "../../../src/pipeline/newHampshireFinance/newHampshireCandidateFinanceBatchSync.js";
import type { NewHampshireCandidateFinanceDueRow } from "../../../src/pipeline/newHampshireFinance/newHampshireCandidateFinanceDueList.js";
import { CYCLE_2026_ID, ELECTION_CYCLES, filingEntity } from "./newHampshireTestFixtures.js";

const NOW = new Date("2026-09-03T00:00:00.000Z");
const REGISTRY = [filingEntity()];

function dueRow(overrides: Partial<NewHampshireCandidateFinanceDueRow> = {}): NewHampshireCandidateFinanceDueRow {
  return {
    candidateId: "candidate-1",
    electionId: "election-1",
    candidateName: "Sample Candidate",
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

function createClient(overrides: Partial<NewHampshireCfsBatchClient> = {}): NewHampshireCfsBatchClient {
  return {
    getElectionCycles: vi.fn().mockResolvedValue(ELECTION_CYCLES),
    getFilingEntities: vi.fn().mockResolvedValue(REGISTRY),
    getReceipts: vi.fn().mockResolvedValue([]),
    getIndependentExpenditures: vi.fn().mockResolvedValue([]),
    ...overrides,
  };
}

const matched = { resolution: { status: "matched" }, directSkippedReason: null, outsideSkippedReason: null };
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

describe("syncDueNewHampshireCandidateFinance", () => {
  it("runs the auto-link pass, lists due rows with the defaults, resolves the cycle once, and syncs every link through the shared client", async () => {
    const client = createClient();
    const autoLink = vi.fn().mockResolvedValue([{ status: "linked" }]);
    const listDueRows = vi.fn().mockResolvedValue({
      rows: [dueRow(), dueRow({ candidateId: "candidate-2", filingEntityId: 50_451 })],
      totalDueRows: 2,
    });
    const sync = vi.fn().mockResolvedValue(matched);
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
    expect(client.getElectionCycles).toHaveBeenCalledTimes(1);
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
    // The auto-link and every sync receive the same memoizing client.
    expect(autoLink.mock.calls[0]![0].cfsClient).toBe(firstCall.cfsClient);
    expect(sync.mock.calls[1]![0].cfsClient).toBe(firstCall.cfsClient);
    await firstCall.cfsClient.getFilingEntities({ electionCycleId: CYCLE_2026_ID });
    await firstCall.cfsClient.getFilingEntities({ electionCycleId: CYCLE_2026_ID });
    expect(client.getFilingEntities).toHaveBeenCalledTimes(1);
    expect(result).toMatchObject({
      dryRun: false,
      autoLinkResults: [{ status: "linked" }],
      totalDueRows: 2,
      attempted: 2,
      succeeded: 2,
      failed: 0,
      electionCycleIds: { "2026": CYCLE_2026_ID },
    });
  });

  it("records per-link failures without stopping, treats an unresolved filer as a failure, and logs skipped sections", async () => {
    const sync = vi
      .fn()
      .mockRejectedValueOnce(new Error("receipts down"))
      .mockResolvedValueOnce({ resolution: { status: "unmatched", reason: "no_candidate_filer_match" } })
      .mockResolvedValueOnce({ ...matched, directSkippedReason: "receipt search failed", outsideSkippedReason: "ie search failed" });
    const logs: string[] = [];
    const result = await syncDueNewHampshireCandidateFinance({
      db,
      now: NOW,
      cfsClient: createClient(),
      autoLinkMissingLinks: false,
      listDueRowsFn: vi.fn().mockResolvedValue({
        rows: [dueRow(), dueRow({ candidateId: "candidate-2" }), dueRow({ candidateId: "candidate-3" })],
        totalDueRows: 3,
      }),
      syncCandidateFn: sync,
      log: (message) => logs.push(message),
    });
    expect(result).toMatchObject({ attempted: 3, succeeded: 1, failed: 2 });
    expect(result.candidates[0]).toMatchObject({ ok: false, error: "receipts down" });
    expect(result.candidates[1]).toMatchObject({ ok: false, error: "filer resolution unmatched: no_candidate_filer_match" });
    expect(result.candidates[2]).toMatchObject({ ok: true });
    expect(logs).toEqual([
      "New Hampshire finance sync failed for Sample Candidate (50450): receipts down",
      "New Hampshire finance sync failed for Sample Candidate (50450): filer resolution unmatched: no_candidate_filer_match",
      "New Hampshire direct money skipped for Sample Candidate (50450): receipt search failed",
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
      listDueRowsFn: vi.fn().mockResolvedValue({ rows: [dueRow(), dueRow({ candidateId: "candidate-2" })], totalDueRows: 2 }),
      syncCandidateFn: sync,
      log: () => {},
    });
    expect(result).toMatchObject({ attempted: 2, succeeded: 0, failed: 2, electionCycleIds: {} });
    expect(result.candidates.map((candidate) => candidate.error)).toEqual(["cycles down", "cycles down"]);
    expect(client.getElectionCycles).toHaveBeenCalledTimes(1);
    expect(sync).not.toHaveBeenCalled();
  });

  it("continues with existing links when the auto-link pass fails, and pulls nothing when nothing is due", async () => {
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
    const sync = vi.fn().mockResolvedValue(matched);
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
    expect(result).toMatchObject({ dryRun: true, attempted: 1, succeeded: 1 });
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
