import { beforeEach, describe, expect, it, vi } from "vitest";
import { syncDueSanFranciscoCandidateFinance } from "../../src/pipeline/sanFranciscoFinance/sanFranciscoCandidateFinanceBatchSync.js";
import {
  autoLinkMissingSanFranciscoCandidateFinanceLinks,
  listSanFranciscoCandidateElectionsMissingFinanceLinks,
} from "../../src/pipeline/sanFranciscoFinance/sanFranciscoCandidateFinanceAutoLink.js";
import {
  checkSanFranciscoSourceFreshness,
  syncSanFranciscoCandidateFinance,
} from "../../src/pipeline/sanFranciscoFinance/sanFranciscoCandidateFinanceSync.js";
import { getSanFranciscoContestManifest } from "../../src/pipeline/sanFranciscoFinance/sanFranciscoDashboardManifestClient.js";

vi.mock(
  "../../src/pipeline/sanFranciscoFinance/sanFranciscoCandidateFinanceAutoLink.js",
  () => ({
    autoLinkMissingSanFranciscoCandidateFinanceLinks: vi.fn(),
    listSanFranciscoCandidateElectionsMissingFinanceLinks: vi.fn(),
  }),
);
vi.mock(
  "../../src/pipeline/sanFranciscoFinance/sanFranciscoCandidateFinanceSync.js",
  () => ({
    checkSanFranciscoSourceFreshness: vi.fn(),
    syncSanFranciscoCandidateFinance: vi.fn(),
  }),
);
vi.mock(
  "../../src/pipeline/sanFranciscoFinance/sanFranciscoDashboardManifestClient.js",
  async (importOriginal) => ({
    ...(await importOriginal<object>()),
    getSanFranciscoContestManifest: vi.fn(),
  }),
);

const NOW = new Date("2026-08-09T12:00:00.000Z");
const FRESHNESS = {
  summary: { dataAsOf: "a", dataLoadedAt: "b" },
  transactions: { dataAsOf: "a", dataLoadedAt: "b" },
};
const MANIFEST = { candidates: [], outsideRelations: [] };

function dueRow(candidate: string, contest = "bos04") {
  return {
    candidate_id: `cand-${candidate}`,
    election_id: `elec-${contest}`,
    election_year: 2026,
    election_date: "2026-11-03",
    contest_code: contest,
    fppc_id: `149${candidate}`,
    last_synced_at: null,
    total_due_rows: "2",
  };
}

function fakeDb(input: { staleRows?: unknown[]; dueRows?: unknown[] }) {
  const query = vi.fn(async (sql: string) => {
    if (sql.includes("DISTINCT ON")) return { rows: input.staleRows ?? [] };
    if (sql.includes("WITH due")) return { rows: input.dueRows ?? [] };
    return { rows: [] };
  });
  return { query, connect: vi.fn() };
}

beforeEach(() => {
  vi.mocked(listSanFranciscoCandidateElectionsMissingFinanceLinks)
    .mockReset()
    .mockResolvedValue([]);
  vi.mocked(autoLinkMissingSanFranciscoCandidateFinanceLinks)
    .mockReset()
    .mockResolvedValue({
      results: [],
      diagnostics: {
        unmatchedManifestCandidates: [],
        unresolvedOutsideTargets: [],
        flaggedLinkIds: [],
        electionErrors: [],
      },
    });
  vi.mocked(checkSanFranciscoSourceFreshness)
    .mockReset()
    .mockResolvedValue(FRESHNESS);
  vi.mocked(syncSanFranciscoCandidateFinance).mockReset().mockResolvedValue({
    linkWritten: true,
  } as never);
  vi.mocked(getSanFranciscoContestManifest)
    .mockReset()
    .mockResolvedValue(MANIFEST as never);
});

describe("syncDueSanFranciscoCandidateFinance", () => {
  it("syncs due candidates with one manifest fetch and one freshness check per batch", async () => {
    const db = fakeDb({ dueRows: [dueRow("1"), dueRow("2")] });
    const result = await syncDueSanFranciscoCandidateFinance({ db, now: NOW });
    expect(result).toMatchObject({
      dueCandidateCount: 2,
      selectedCandidateCount: 2,
      syncedCandidateCount: 2,
      failedCandidateCount: 0,
      staleElectionRefreshCount: 0,
    });
    expect(getSanFranciscoContestManifest).toHaveBeenCalledTimes(1);
    expect(checkSanFranciscoSourceFreshness).toHaveBeenCalledTimes(1);
    expect(syncSanFranciscoCandidateFinance).toHaveBeenCalledTimes(2);
    expect(
      vi.mocked(syncSanFranciscoCandidateFinance).mock.calls[0]![0],
    ).toMatchObject({
      candidateId: "cand-1",
      electionDate: "2026-11-03",
      contestCode: "bos04",
      fppcId: "1491",
      manifest: MANIFEST,
      sourceFreshness: FRESHNESS,
      now: NOW,
    });
  });

  it("fails every due candidate with the freshness error and never syncs", async () => {
    vi.mocked(checkSanFranciscoSourceFreshness).mockRejectedValue(
      new Error("summary-totals dataset is stale"),
    );
    const db = fakeDb({ dueRows: [dueRow("1"), dueRow("2")] });
    const result = await syncDueSanFranciscoCandidateFinance({ db, now: NOW });
    expect(result.failedCandidateCount).toBe(2);
    expect(result.results.every((row) => !row.ok)).toBe(true);
    expect(result.results[0]!.error).toMatch(/dataset is stale/);
    expect(syncSanFranciscoCandidateFinance).not.toHaveBeenCalled();
  });

  it("keeps syncing when one candidate fails", async () => {
    vi.mocked(syncSanFranciscoCandidateFinance)
      .mockRejectedValueOnce(new Error("missing from the bos04 manifest"))
      .mockResolvedValueOnce({ linkWritten: true } as never);
    const db = fakeDb({ dueRows: [dueRow("1"), dueRow("2")] });
    const result = await syncDueSanFranciscoCandidateFinance({ db, now: NOW });
    expect(result.syncedCandidateCount).toBe(1);
    expect(result.failedCandidateCount).toBe(1);
    expect(result.results[0]).toMatchObject({
      ok: false,
      error: expect.stringMatching(/missing from the bos04 manifest/),
    });
  });

  it("wholesale-refreshes elections whose active links went stale", async () => {
    const db = fakeDb({
      staleRows: [
        {
          candidate_id: "cand-9",
          election_id: "elec-bos06",
          candidate_name: "Sam Sample",
          election_date: "2026-11-03",
          contest_code: "bos06",
        },
      ],
    });
    const result = await syncDueSanFranciscoCandidateFinance({ db, now: NOW });
    expect(result.staleElectionRefreshCount).toBe(1);
    expect(
      vi
        .mocked(autoLinkMissingSanFranciscoCandidateFinanceLinks)
        .mock.calls.at(-1)![0].candidates,
    ).toEqual([
      {
        candidateId: "cand-9",
        electionId: "elec-bos06",
        candidateName: "Sam Sample",
        electionDate: "2026-11-03",
        electionYear: 2026,
        contestCode: "bos06",
      },
    ]);
  });

  it("skips the stale-election refresh for elections leg 1 already refreshed", async () => {
    vi.mocked(
      listSanFranciscoCandidateElectionsMissingFinanceLinks,
    ).mockResolvedValue([
      {
        candidateId: "cand-new",
        electionId: "elec-bos06",
        candidateName: "New Candidate",
        electionDate: "2026-11-03",
        electionYear: 2026,
        contestCode: "bos06",
      },
    ]);
    const db = fakeDb({
      staleRows: [
        {
          candidate_id: "cand-9",
          election_id: "elec-bos06",
          candidate_name: "Sam Sample",
          election_date: "2026-11-03",
          contest_code: "bos06",
        },
      ],
    });
    const result = await syncDueSanFranciscoCandidateFinance({ db, now: NOW });
    expect(result.staleElectionRefreshCount).toBe(0);
    // Only the leg-1 auto-link call happened.
    expect(
      autoLinkMissingSanFranciscoCandidateFinanceLinks,
    ).toHaveBeenCalledTimes(1);
  });

  it("continues to existing links when auto-link fails", async () => {
    vi.mocked(
      listSanFranciscoCandidateElectionsMissingFinanceLinks,
    ).mockRejectedValue(new Error("manifest host down"));
    const db = fakeDb({ dueRows: [dueRow("1")] });
    const result = await syncDueSanFranciscoCandidateFinance({ db, now: NOW });
    expect(result.syncedCandidateCount).toBe(1);
    expect(result.autoLinkAttemptedCount).toBe(0);
  });

  it("dry-run skips both refresh legs and passes dryRun through", async () => {
    const db = fakeDb({ dueRows: [dueRow("1")] });
    const result = await syncDueSanFranciscoCandidateFinance({
      db,
      now: NOW,
      dryRun: true,
    });
    expect(result.dryRun).toBe(true);
    expect(
      listSanFranciscoCandidateElectionsMissingFinanceLinks,
    ).not.toHaveBeenCalled();
    expect(
      autoLinkMissingSanFranciscoCandidateFinanceLinks,
    ).not.toHaveBeenCalled();
    expect(
      vi.mocked(syncSanFranciscoCandidateFinance).mock.calls[0]![0].dryRun,
    ).toBe(true);
  });

  it("rejects invalid batch options loudly", async () => {
    const db = fakeDb({});
    await expect(
      syncDueSanFranciscoCandidateFinance({
        db,
        now: NOW,
        maxCandidates: 0,
      }),
    ).rejects.toThrow(/Invalid San Francisco finance maxCandidates: 0/);
    await expect(
      syncDueSanFranciscoCandidateFinance({
        db,
        now: NOW,
        staleAfterDays: Number.NaN,
      }),
    ).rejects.toThrow(/Invalid San Francisco finance staleAfterDays/);
  });
});
