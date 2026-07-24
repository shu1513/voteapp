import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { strToU8, zipSync } from "fflate";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  listDueColoradoCandidateFinanceSyncRows,
  syncDueColoradoCandidateFinance,
  type ColoradoContributionDataForYear,
} from "../../../src/pipeline/coloradoFinance/coloradoCandidateFinanceBatchSync.js";
import { getColoradoTracerContributionArtifactCachePaths } from "../../../src/pipeline/coloradoFinance/coloradoTracerContributionArtifactCache.js";
import {
  COLORADO_TRACER_CONTRIBUTION_COLUMNS,
  coloradoTracerContributionCsvFileName,
  type ColoradoTracerContributionRow,
} from "../../../src/pipeline/coloradoFinance/coloradoTracerContributionReader.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const tempDirs: string[] = [];

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

function contributionCsv(rows: readonly ColoradoTracerContributionRow[]): string {
  const escape = (value: string): string => (/[",\n\r]/.test(value) ? `"${value.replace(/"/g, '""')}"` : value);
  return [
    COLORADO_TRACER_CONTRIBUTION_COLUMNS.join(","),
    ...rows.map((row) => COLORADO_TRACER_CONTRIBUTION_COLUMNS.map((column) => escape(row[column])).join(",")),
  ].join("\n");
}

async function writeContributionArtifact(cacheDir: string, year: number, rows: readonly ColoradoTracerContributionRow[]) {
  const paths = getColoradoTracerContributionArtifactCachePaths({ cacheDir, year });
  const memberName = coloradoTracerContributionCsvFileName(year);
  await writeFile(paths.zipPath, Buffer.from(zipSync({ [memberName]: strToU8(contributionCsv(rows)) })));
}

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows }),
  };
}

function contribution(overrides: Partial<ColoradoTracerContributionRow> = {}): ColoradoTracerContributionRow {
  return {
    CO_ID: "202650001",
    ContributionAmount: "100.00",
    ContributionDate: "01/10/2026",
    LastName: "Doe",
    FirstName: "Jane",
    MI: "",
    Suffix: "",
    Address1: "",
    Address2: "",
    City: "Denver",
    State: "CO",
    Zip: "80203",
    Explanation: "",
    RecordID: "R1",
    FiledDate: "02/01/2026",
    ContributionType: "Monetary",
    ReceiptType: "Contribution",
    ContributorType: "Individual",
    Electioneering: "",
    CommitteeType: "Candidate Committee",
    CommitteeName: "Jane Doe for Colorado Governor",
    CandidateName: "Jane Doe",
    Employer: "Acme Inc",
    Occupation: "Engineer",
    Amended: "False",
    Amendment: "",
    AmendedRecordID: "",
    Jurisdiction: "STATEWIDE",
    OccupationComments: "",
    ...overrides,
  };
}

function contributionDataForYear(input: {
  year: number;
  sourceUrl?: string;
  rowsByCommitteeId: Map<string, ColoradoTracerContributionRow[]>;
}): ColoradoContributionDataForYear {
  return {
    year: input.year,
    zipPath: `/tmp/${input.year}_ContributionData.csv.zip`,
    sourceUrl:
      input.sourceUrl ??
      `https://tracer.sos.colorado.gov/PublicSite/Docs/BulkDataDownloads/${input.year}_ContributionData.csv.zip`,
    rowsByCommitteeId: input.rowsByCommitteeId,
  };
}

describe("coloradoCandidateFinanceBatchSync", () => {
  it("lists due Colorado finance sync rows from explicit active links", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Jane Doe",
        election_year: 2026,
        office_name: "Governor",
        committee_id: "202650001",
        committee_name: "Jane Doe for Colorado Governor",
        tracer_candidate_id: "TRACER-123",
        source_url: "https://tracer.sos.colorado.gov/",
        last_synced_at: null,
        total_due_rows: "2",
      },
      {
        candidate_id: "33333333-3333-4333-8333-333333333333",
        election_id: "44444444-4444-4444-8444-444444444444",
        candidate_name: "Alex Example",
        election_year: 2026,
        office_name: "State Senator",
        committee_id: "202650002",
        committee_name: "Alex Example for Senate",
        tracer_candidate_id: null,
        source_url: null,
        last_synced_at: "2026-01-01 00:00:00+00",
        total_due_rows: "2",
      },
    ]);

    const result = await listDueColoradoCandidateFinanceSyncRows(db, {
      now: new Date("2026-06-01T00:00:00.000Z"),
      staleAfterDays: 7,
      maxCandidates: 25,
      electionLookbackDays: 30,
      electionLookaheadDays: 730,
    });

    expect(result).toEqual({
      totalDueRows: 2,
      rows: [
        {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Jane Doe",
          electionYear: 2026,
          officeName: "Governor",
          committeeId: "202650001",
          committeeName: "Jane Doe for Colorado Governor",
          tracerCandidateId: "TRACER-123",
          sourceUrl: "https://tracer.sos.colorado.gov/",
          lastSyncedAt: null,
        },
        {
          candidateId: "33333333-3333-4333-8333-333333333333",
          electionId: "44444444-4444-4444-8444-444444444444",
          candidateName: "Alex Example",
          electionYear: 2026,
          officeName: "State Senator",
          committeeId: "202650002",
          committeeName: "Alex Example for Senate",
          tracerCandidateId: null,
          sourceUrl: null,
          lastSyncedAt: "2026-01-01 00:00:00+00",
        },
      ],
    });

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.co_candidate_finance_links AS link");
    expect(sql).toContain("link.link_status = 'active'");
    expect(sql).toContain("district.state = 'CO'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("election.election_date >= ($1::date - make_interval(days => $4::int))");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      7,
      25,
      30,
      730,
    ]);
  });

  it("uses a one-day post-election grace window by default for due selection", async () => {
    const db = createMockDb();

    await syncDueColoradoCandidateFinance({
      db,
      syncColoradoCandidateFinanceFn: vi.fn(),
      now: new Date("2026-06-01T00:00:00.000Z"),
      autoLinkMissingLinks: false,
    });

    expect(String(db.query.mock.calls[0]?.[0])).toContain(
      "election.election_date >= ($1::date - make_interval(days => $4::int))"
    );
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      7,
      25,
      1,
      730,
    ]);
  });

  it("syncs selected due links with cached yearly contribution rows and continues after failures", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Jane Doe",
        election_year: 2026,
        office_name: "Governor",
        committee_id: "202650001",
        committee_name: "Jane Doe for Colorado Governor",
        tracer_candidate_id: "TRACER-123",
        source_url: "https://tracer.sos.colorado.gov/",
        last_synced_at: null,
        total_due_rows: "2",
      },
      {
        candidate_id: "33333333-3333-4333-8333-333333333333",
        election_id: "44444444-4444-4444-8444-444444444444",
        candidate_name: "Alex Example",
        election_year: 2026,
        office_name: "State Senator",
        committee_id: "202650002",
        committee_name: "Alex Example for Senate",
        tracer_candidate_id: null,
        source_url: null,
        last_synced_at: null,
        total_due_rows: "2",
      },
    ]);
    const syncColoradoCandidateFinanceFn = vi
      .fn()
      .mockResolvedValueOnce({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        electionYear: 2026,
        dryRun: false,
        linkWritten: true,
        summaryWritten: true,
        directBreakdownsWritten: 3,
        totalReceipts: 100,
        matchedContributionRowCount: 1,
        includedContributionRowCount: 1,
        skippedContributionRowCount: 0,
      })
      .mockRejectedValueOnce(new Error("TRACER row parse failed"));
    const row = contribution({ CO_ID: "202650001", ContributionAmount: "100.00" });

    const result = await syncDueColoradoCandidateFinance({
      db,
      syncColoradoCandidateFinanceFn,
      now: new Date("2026-06-01T00:00:00.000Z"),
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: 30,
      autoLinkMissingLinks: false,
      contributionDataByYear: new Map([
        [
          2026,
          contributionDataForYear({
            year: 2026,
            rowsByCommitteeId: new Map([["202650001", [row]]]),
          }),
        ],
      ]),
    });

    expect(result).toMatchObject({
      dryRun: false,
      now: "2026-06-01T00:00:00.000Z",
      staleAfterDays: 3,
      maxCandidates: 2,
      dueCandidateCount: 2,
      selectedCandidateCount: 2,
      syncedCandidateCount: 1,
      failedCandidateCount: 1,
    });
    expect(result.results[0]).toMatchObject({ ok: true, committeeId: "202650001" });
    expect(result.results[1]).toMatchObject({
      ok: false,
      committeeId: "202650002",
      error: "TRACER row parse failed",
    });
    expect(syncColoradoCandidateFinanceFn).toHaveBeenCalledTimes(2);
    expect(syncColoradoCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Jane Doe",
        electionYear: 2026,
        officeName: "Governor",
        committeeId: "202650001",
        committeeName: "Jane Doe for Colorado Governor",
        tracerCandidateId: "TRACER-123",
        sourceUrl: "https://tracer.sos.colorado.gov/",
        contributionRows: [row],
        contributionSourceUrl:
          "https://tracer.sos.colorado.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionData.csv.zip",
      })
    );
    expect(syncColoradoCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        committeeId: "202650002",
        contributionRows: [],
      })
    );
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      3,
      2,
      30,
      730,
    ]);
  });

  it("continues syncing already-linked candidates when auto-link contribution ZIP is missing", async () => {
    const db = {
      query: vi
        .fn()
        .mockResolvedValueOnce({
          rows: [
            {
              candidate_id: "55555555-5555-4555-8555-555555555555",
              election_id: "66666666-6666-4666-8666-666666666666",
              candidate_name: "Missing Zip Candidate",
              election_year: 2025,
              office_name: "Governor",
            },
          ],
        })
        .mockResolvedValueOnce({
          rows: [
            {
              candidate_id: CANDIDATE_ID,
              election_id: ELECTION_ID,
              candidate_name: "Jane Doe",
              election_year: 2026,
              office_name: "Governor",
              committee_id: "202650001",
              committee_name: "Jane Doe for Colorado Governor",
              tracer_candidate_id: "TRACER-123",
              source_url: "https://tracer.sos.colorado.gov/",
              last_synced_at: null,
              total_due_rows: "1",
            },
          ],
        }),
    };
    const row = contribution({ CO_ID: "202650001", ContributionAmount: "100.00" });
    const syncColoradoCandidateFinanceFn = vi.fn().mockResolvedValue({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2026,
      dryRun: false,
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 3,
      totalReceipts: 100,
      matchedContributionRowCount: 1,
      includedContributionRowCount: 1,
      skippedContributionRowCount: 0,
    });
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

    try {
      const result = await syncDueColoradoCandidateFinance({
        db,
        syncColoradoCandidateFinanceFn,
        now: new Date("2026-06-01T00:00:00.000Z"),
        rawDataCacheDir: "/tmp/voteapp-missing-colorado-finance-cache-for-test",
        contributionDataByYear: new Map([
          [
            2026,
            contributionDataForYear({
              year: 2026,
              rowsByCommitteeId: new Map([["202650001", [row]]]),
            }),
          ],
        ]),
      });

      expect(result).toMatchObject({
        dueCandidateCount: 1,
        selectedCandidateCount: 1,
        syncedCandidateCount: 1,
        failedCandidateCount: 0,
      });
      expect(syncColoradoCandidateFinanceFn).toHaveBeenCalledTimes(1);
      expect(syncColoradoCandidateFinanceFn).toHaveBeenCalledWith(
        expect.objectContaining({
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          contributionRows: [row],
        })
      );
      expect(warnSpy).toHaveBeenCalledWith(
        "Colorado finance auto-link skipped; continuing with already-linked candidate sync:",
        expect.stringContaining("Colorado TRACER contribution ZIP not found for 2024")
      );
      expect(db.query).toHaveBeenCalledTimes(2);
    } finally {
      warnSpy.mockRestore();
    }
  });

  it("auto-links and syncs money found only in the prior cycle-year artifact", async () => {
    const rawDataCacheDir = await mkdtemp(join(tmpdir(), "voteapp-co-cycle-"));
    tempDirs.push(rawDataCacheDir);
    const priorYearRow = contribution({
      RecordID: "PRIOR-YEAR",
      ContributionDate: "10/15/2025",
      ContributionAmount: "250.00",
    });
    const duplicateAcrossYears = contribution({
      RecordID: "BOTH-YEARS",
      ContributionDate: "11/15/2025",
      ContributionAmount: "50.00",
    });
    await writeContributionArtifact(rawDataCacheDir, 2025, [priorYearRow, duplicateAcrossYears]);
    await writeContributionArtifact(rawDataCacheDir, 2026, [duplicateAcrossYears]);

    const db = {
      query: vi
        .fn()
        .mockResolvedValueOnce({
          rows: [{
            candidate_id: CANDIDATE_ID,
            election_id: ELECTION_ID,
            candidate_name: "Jane Doe",
            election_year: 2026,
            office_name: "Governor",
          }],
          rowCount: 1,
        })
        .mockResolvedValueOnce({ rows: [{ id: "link-1" }], rowCount: 1 })
        .mockResolvedValueOnce({
          rows: [{
            candidate_id: CANDIDATE_ID,
            election_id: ELECTION_ID,
            candidate_name: "Jane Doe",
            election_year: 2026,
            office_name: "Governor",
            committee_id: "202650001",
            committee_name: "Jane Doe for Colorado Governor",
            tracer_candidate_id: null,
            source_url: "https://tracer.sos.colorado.gov/",
            last_synced_at: null,
            total_due_rows: "1",
          }],
          rowCount: 1,
        }),
    };
    const syncFn = vi.fn().mockResolvedValue({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2026,
      totalReceipts: 250,
    });
    const result = await syncDueColoradoCandidateFinance({
      db,
      now: new Date("2026-06-01T00:00:00.000Z"),
      rawDataCacheDir,
      syncColoradoCandidateFinanceFn: syncFn,
    });
    expect(result).toMatchObject({ syncedCandidateCount: 1, failedCandidateCount: 0 });
    expect(String(db.query.mock.calls[1]?.[0])).toContain("INSERT INTO public.co_candidate_finance_links");
    expect(syncFn).toHaveBeenCalledWith(
      expect.objectContaining({
        contributionRows: [priorYearRow, duplicateAcrossYears],
        contributionSourceUrl: expect.stringContaining("/2025_ContributionData.csv.zip"),
      })
    );
  });

  it("rejects invalid batch options before querying", async () => {
    const db = createMockDb();

    await expect(
      syncDueColoradoCandidateFinance({
        db,
        maxCandidates: 0,
      })
    ).rejects.toThrow("Invalid Colorado finance batch sync maxCandidates");
    expect(db.query).not.toHaveBeenCalled();
  });
});
