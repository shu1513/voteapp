import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { strToU8, zipSync } from "fflate";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  listDueOklahomaCandidateFinanceSyncRows,
  syncDueOklahomaCandidateFinance,
  type OklahomaContributionDataForYear,
} from "../../../src/pipeline/oklahomaFinance/oklahomaCandidateFinanceBatchSync.js";
import { getOklahomaGuardianContributionArtifactCachePaths } from "../../../src/pipeline/oklahomaFinance/oklahomaGuardianContributionArtifactCache.js";
import {
  OKLAHOMA_GUARDIAN_CONTRIBUTION_COLUMNS,
  oklahomaGuardianContributionCsvFileName,
  type OklahomaGuardianContributionRow,
} from "../../../src/pipeline/oklahomaFinance/oklahomaGuardianContributionReader.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const tempDirs: string[] = [];

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

function contributionCsv(rows: readonly OklahomaGuardianContributionRow[]): string {
  const escape = (value: string): string => (/[",\n\r]/.test(value) ? `"${value.replace(/"/g, '""')}"` : value);
  return [
    OKLAHOMA_GUARDIAN_CONTRIBUTION_COLUMNS.join(","),
    ...rows.map((row) => OKLAHOMA_GUARDIAN_CONTRIBUTION_COLUMNS.map((column) => escape(row[column])).join(",")),
  ].join("\n");
}

async function writeContributionArtifact(cacheDir: string, year: number, rows: readonly OklahomaGuardianContributionRow[]) {
  const paths = getOklahomaGuardianContributionArtifactCachePaths({ cacheDir, year });
  const memberName = oklahomaGuardianContributionCsvFileName(year);
  await writeFile(paths.zipPath, Buffer.from(zipSync({ [memberName]: strToU8(contributionCsv(rows)) })));
}

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows }),
  };
}

function contribution(overrides: Partial<OklahomaGuardianContributionRow> = {}): OklahomaGuardianContributionRow {
  return {
    "Receipt ID": "R1",
    "Org ID": "11954",
    "Receipt Type": "Contribution",
    "Receipt Date": "01/10/2026",
    "Receipt Amount": "100.00",
    Description: "",
    "Receipt Source Type": "Individual",
    "Last Name": "Doe",
    "First Name": "Jane",
    "Middle Name": "",
    Suffix: "",
    "Address 1": "",
    "Address 2": "",
    City: "Oklahoma City",
    State: "OK",
    Zip: "73102",
    "Filed Date": "02/01/2026",
    "Committee Type": "Candidate Committee",
    "Committee Name": "Dishman for Senate",
    "Candidate Name": "C. Brent Dishman",
    Amended: "",
    Employer: "Acme Inc",
    Occupation: "Attorney",
    ...overrides,
  };
}

function contributionDataForYear(input: {
  year: number;
  sourceUrl?: string;
  rowsByCommitteeId: Map<string, OklahomaGuardianContributionRow[]>;
}): OklahomaContributionDataForYear {
  return {
    year: input.year,
    zipPath: `/tmp/${input.year}_ContributionLoanExtract.csv.zip`,
    sourceUrl:
      input.sourceUrl ??
      `https://guardian.ok.gov/PublicSite/Docs/BulkDataDownloads/${input.year}_ContributionLoanExtract.csv.zip`,
    rowsByCommitteeId: input.rowsByCommitteeId,
  };
}

describe("oklahomaCandidateFinanceBatchSync", () => {
  it("lists due Oklahoma finance sync rows from explicit active links", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Brent Dishman",
        election_year: 2026,
        office_scope: "state_upper",
        office_name: "State Senator",
        district: "47",
        committee_id: "11954",
        committee_name: "Dishman for Senate",
        source_url: "https://guardian.ok.gov/PublicSite/DataDownload.aspx",
        last_synced_at: null,
        total_due_rows: "2",
      },
      {
        candidate_id: "33333333-3333-4333-8333-333333333333",
        election_id: "44444444-4444-4444-8444-444444444444",
        candidate_name: "Jane Doe",
        election_year: 2026,
        office_scope: "statewide",
        office_name: "Governor",
        district: null,
        committee_id: "9001",
        committee_name: "Doe for Governor",
        source_url: null,
        last_synced_at: "2026-01-01 00:00:00+00",
        total_due_rows: "2",
      },
    ]);

    const result = await listDueOklahomaCandidateFinanceSyncRows(db, {
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
          candidateName: "Brent Dishman",
          electionYear: 2026,
          officeScope: "state_upper",
          officeName: "State Senator",
          district: "47",
          committeeId: "11954",
          committeeName: "Dishman for Senate",
          sourceUrl: "https://guardian.ok.gov/PublicSite/DataDownload.aspx",
          lastSyncedAt: null,
        },
        {
          candidateId: "33333333-3333-4333-8333-333333333333",
          electionId: "44444444-4444-4444-8444-444444444444",
          candidateName: "Jane Doe",
          electionYear: 2026,
          officeScope: "statewide",
          officeName: "Governor",
          district: null,
          committeeId: "9001",
          committeeName: "Doe for Governor",
          sourceUrl: null,
          lastSyncedAt: "2026-01-01 00:00:00+00",
        },
      ],
    });

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.ok_candidate_finance_links AS link");
    expect(sql).toContain("link.link_status = 'active'");
    expect(sql).toContain("district.state = 'OK'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("election.election_date >= ($1::date - make_interval(days => $4::int))");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($6::text[])");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      7,
      25,
      30,
      730,
      expect.arrayContaining(["statewide::Governor", "state_upper::State Senator"]),
    ]);
  });

  it("uses a one-day post-election grace window by default for due selection", async () => {
    const db = createMockDb();

    await syncDueOklahomaCandidateFinance({
      db,
      syncOklahomaCandidateFinanceFn: vi.fn(),
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
      expect.arrayContaining(["statewide::Governor", "state_upper::State Senator"]),
    ]);
  });

  it("syncs selected due links with cached yearly contribution rows and continues after failures", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Brent Dishman",
        election_year: 2026,
        office_scope: "state_upper",
        office_name: "State Senator",
        district: "47",
        committee_id: "11954",
        committee_name: "Dishman for Senate",
        source_url: "https://guardian.ok.gov/PublicSite/DataDownload.aspx",
        last_synced_at: null,
        total_due_rows: "2",
      },
      {
        candidate_id: "33333333-3333-4333-8333-333333333333",
        election_id: "44444444-4444-4444-8444-444444444444",
        candidate_name: "Jane Doe",
        election_year: 2026,
        office_scope: "statewide",
        office_name: "Governor",
        district: null,
        committee_id: "9001",
        committee_name: "Doe for Governor",
        source_url: null,
        last_synced_at: null,
        total_due_rows: "2",
      },
    ]);
    const syncOklahomaCandidateFinanceFn = vi
      .fn()
      .mockResolvedValueOnce({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        electionYear: 2026,
        dryRun: false,
        resolution: { status: "matched", committeeId: "11954" },
        linkWritten: true,
        summaryWritten: true,
        directBreakdownsWritten: 3,
        totalReceipts: 100,
        directContributionTotal: 90,
        matchedContributionRowCount: 1,
        includedContributionRowCount: 1,
        skippedContributionRowCount: 0,
      })
      .mockRejectedValueOnce(new Error("Guardian row parse failed"));
    const row = contribution({ "Org ID": "11954", "Receipt Amount": "100.00" });

    const result = await syncDueOklahomaCandidateFinance({
      db,
      syncOklahomaCandidateFinanceFn,
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
            rowsByCommitteeId: new Map([["11954", [row]]]),
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
    expect(result.results[0]).toMatchObject({
      ok: true,
      committeeId: "11954",
      result: {
        totalReceipts: 100,
        directContributionTotal: 90,
      },
    });
    expect(result.results[1]).toMatchObject({
      ok: false,
      committeeId: "9001",
      error: "Guardian row parse failed",
    });
    expect(syncOklahomaCandidateFinanceFn).toHaveBeenCalledTimes(2);
    expect(syncOklahomaCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Brent Dishman",
        electionYear: 2026,
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "47",
        sourceUrl: "https://guardian.ok.gov/PublicSite/DataDownload.aspx",
        contributionRows: [row],
        contributionSourceUrl:
          "https://guardian.ok.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip",
      })
    );
    expect(syncOklahomaCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: "33333333-3333-4333-8333-333333333333",
        contributionRows: [],
      })
    );
  });

  it("auto-links and syncs money found only in the prior cycle-year artifact", async () => {
    const rawDataCacheDir = await mkdtemp(join(tmpdir(), "voteapp-ok-cycle-"));
    tempDirs.push(rawDataCacheDir);
    const priorYearRow = contribution({
      "Receipt ID": "PRIOR-YEAR",
      "Receipt Date": "10/15/2025",
      "Receipt Amount": "250.00",
    });
    await writeContributionArtifact(rawDataCacheDir, 2025, [priorYearRow]);
    await writeContributionArtifact(rawDataCacheDir, 2026, []);

    const db = {
      query: vi
        .fn()
        .mockResolvedValueOnce({
          rows: [{
            candidate_id: CANDIDATE_ID,
            election_id: ELECTION_ID,
            candidate_name: "Brent Dishman",
            election_year: 2026,
            office_scope: "state_upper",
            office_name: "State Senator",
            district: "47",
          }],
          rowCount: 1,
        })
        .mockResolvedValueOnce({ rows: [{ id: "link-1" }], rowCount: 1 })
        .mockResolvedValueOnce({
          rows: [{
            candidate_id: CANDIDATE_ID,
            election_id: ELECTION_ID,
            candidate_name: "Brent Dishman",
            election_year: 2026,
            office_scope: "state_upper",
            office_name: "State Senator",
            district: "47",
            committee_id: "11954",
            committee_name: "Dishman for Senate",
            source_url: "https://guardian.ok.gov/PublicSite/DataDownload.aspx",
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
      directContributionTotal: 250,
    });
    const result = await syncDueOklahomaCandidateFinance({
      db,
      now: new Date("2026-06-01T00:00:00.000Z"),
      rawDataCacheDir,
      syncOklahomaCandidateFinanceFn: syncFn,
    });
    expect(result).toMatchObject({ syncedCandidateCount: 1, failedCandidateCount: 0 });
    expect(String(db.query.mock.calls[1]?.[0])).toContain("INSERT INTO public.ok_candidate_finance_links");
    expect(syncFn).toHaveBeenCalledWith(expect.objectContaining({ contributionRows: [priorYearRow] }));
  });

  it("rejects invalid batch options before querying", async () => {
    const db = createMockDb();

    await expect(
      syncDueOklahomaCandidateFinance({
        db,
        maxCandidates: 0,
      })
    ).rejects.toThrow("Invalid Oklahoma finance batch sync maxCandidates");
    expect(db.query).not.toHaveBeenCalled();
  });
});
