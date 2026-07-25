import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  listDueMichiganCandidateFinanceSyncRows,
  syncDueMichiganCandidateFinance,
  type MichiganMitnLegacyDataForYear,
} from "../../../src/pipeline/michiganFinance/michiganCandidateFinanceBatchSync.js";
import {
  MICHIGAN_MITN_LEGACY_CONTRIBUTION_COLUMNS,
  MICHIGAN_MITN_LEGACY_EXPENDITURE_COLUMNS,
  type MichiganMitnLegacyContributionRow,
  type MichiganMitnLegacyExpenditureRow,
} from "../../../src/pipeline/michiganFinance/michiganMitnLegacyArchiveReader.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const SOURCE_URL = "https://www.michigan.gov/sos/example/2022_mi_cfr.7z";
const tempDirs: string[] = [];

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

function csvCell(value: string): string {
  return /[",\n\r]/.test(value) ? `"${value.replace(/"/g, '""')}"` : value;
}

function csv(headers: readonly string[], rows: readonly Record<string, string>[]): string {
  return [
    headers.join(","),
    ...rows.map((row) => headers.map((header) => csvCell(row[header] ?? "")).join(",")),
  ].join("\n");
}

async function writeYearFilesInto(input: {
  dir: string;
  year: number;
  contributionRows: readonly Record<string, string>[];
  expenditureRows: readonly Record<string, string>[];
}): Promise<void> {
  await mkdir(input.dir, { recursive: true });
  await writeFile(
    join(input.dir, `${input.year}_mi_cfr_contributions.csv`),
    `${csv(MICHIGAN_MITN_LEGACY_CONTRIBUTION_COLUMNS, input.contributionRows)}\n`,
    "utf8"
  );
  // The official export carries expense_id / detail_id beyond the required
  // typed columns; include them so the cross-year merge identity is exercised.
  await writeFile(
    join(input.dir, `${input.year}_mi_cfr_expenditures.csv`),
    `${csv([...MICHIGAN_MITN_LEGACY_EXPENDITURE_COLUMNS, "expense_id", "detail_id"], input.expenditureRows)}\n`,
    "utf8"
  );
}

async function writeExtractedYearDir(input: {
  cacheDir: string;
  year: number;
  contributionRows: readonly Record<string, string>[];
  expenditureRows: readonly Record<string, string>[];
}): Promise<void> {
  await writeYearFilesInto({ ...input, dir: join(input.cacheDir, `${input.year}_mi_cfr`) });
}

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
    _column_29: "GOVERNOR",
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

  it("does not auto-link missing finance links during dry-run", async () => {
    const db = createMockDb([dueRow()]);
    const syncMichiganCandidateFinanceFn = vi.fn().mockResolvedValue({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2022,
      dryRun: true,
      resolution: { status: "matched", committeeId: "514456" },
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
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

    await syncDueMichiganCandidateFinance({
      db,
      dryRun: true,
      syncMichiganCandidateFinanceFn,
      now: new Date("2022-06-01T00:00:00.000Z"),
      mitnDataByYear: new Map([[2022, mitnData()]]),
    });

    expect(String(db.query.mock.calls[0]?.[0])).toContain("FROM public.mi_candidate_finance_links AS link");
    expect(db.query.mock.calls.map((call) => String(call[0])).some((sql) => sql.includes("INSERT INTO public.mi_candidate_finance_links"))).toBe(false);
  });

  it("merges contribution and expenditure rows from both cycle filing-year archives", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "voteapp-mi-cycle-"));
    tempDirs.push(cacheDir);
    const priorYearOnly = contribution({
      doc_seq_no: "1",
      contribution_id: "10",
      cont_detail_id: "0",
      doc_stmnt_year: "2021",
      received_date: "11/15/2021",
      amount: "250.00",
    });
    const duplicateAcrossYears = contribution({
      doc_seq_no: "2",
      contribution_id: "20",
      cont_detail_id: "0",
      doc_stmnt_year: "2021",
      received_date: "12/01/2021",
      amount: "50.00",
    });
    const electionYearRow = contribution({
      doc_seq_no: "3",
      contribution_id: "30",
      cont_detail_id: "0",
      doc_stmnt_year: "2022",
      received_date: "10/01/2022",
      amount: "100.00",
    });
    // The election-year receipt reported on the FOLLOWING January's annual
    // statement lives in the next filing year's archive.
    const nextYearFiledRow = contribution({
      doc_seq_no: "4",
      contribution_id: "40",
      cont_detail_id: "0",
      doc_stmnt_year: "2023",
      received_date: "12/15/2022",
      amount: "75.00",
    });
    const outsideExpenditure = {
      ...expenditure({ doc_seq_no: "900", doc_stmnt_year: "2022" }),
      expense_id: "500",
      detail_id: "0",
    };
    await writeExtractedYearDir({
      cacheDir,
      year: 2021,
      contributionRows: [priorYearOnly, duplicateAcrossYears],
      expenditureRows: [],
    });
    await writeExtractedYearDir({
      cacheDir,
      year: 2022,
      contributionRows: [{ ...duplicateAcrossYears, doc_stmnt_year: "2022" }, electionYearRow],
      expenditureRows: [outsideExpenditure],
    });
    await writeExtractedYearDir({
      cacheDir,
      year: 2023,
      contributionRows: [nextYearFiledRow],
      expenditureRows: [],
    });

    const db = createMockDb([dueRow({ source_url: null })]);
    const syncMichiganCandidateFinanceFn = vi.fn().mockResolvedValue({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2022,
      totalReceipts: 475,
    });

    const result = await syncDueMichiganCandidateFinance({
      db,
      syncMichiganCandidateFinanceFn,
      now: new Date("2022-06-01T00:00:00.000Z"),
      rawDataCacheDir: cacheDir,
      autoLinkMissingLinks: false,
    });

    expect(result).toMatchObject({ syncedCandidateCount: 1, failedCandidateCount: 0 });
    const syncArgs = syncMichiganCandidateFinanceFn.mock.calls[0]?.[0];
    // The prior filing year contributes rows the election-year archive never
    // carries, the receipt filed in both years is not double counted (the
    // newer artifact's copy wins), and the next filing year contributes the
    // election-year receipt reported the following January.
    expect(syncArgs.contributionRows.map((row: MichiganMitnLegacyContributionRow) => row.contribution_id)).toEqual([
      "10",
      "20",
      "30",
      "40",
    ]);
    expect(
      syncArgs.contributionRows.find((row: MichiganMitnLegacyContributionRow) => row.contribution_id === "20")
        ?.doc_stmnt_year
    ).toBe("2022");
    expect(
      syncArgs.expenditureRows.map((row: MichiganMitnLegacyExpenditureRow & { expense_id?: string }) => row.expense_id)
    ).toEqual(["500"]);
    // Provenance follows each row set's own first contributing archive.
    expect(syncArgs.contributionSourceUrl).toContain("/2021_mi_cfr.7z");
    expect(syncArgs.outsideSourceUrl).toContain("/2022_mi_cfr.7z");
  });

  it("clamps a 2020 election's cycle start to the first archive that exists", async () => {
    const cacheDir = await mkdtemp(join(tmpdir(), "voteapp-mi-first-year-"));
    tempDirs.push(cacheDir);
    // A January-2020 annual statement carries a 2019 received date — the only
    // place a 2020 election's prior-year receipts can exist, because there is
    // no 2019 archive to clamp toward.
    const priorYearReceiptOnAnnualStatement = contribution({
      doc_seq_no: "1",
      contribution_id: "10",
      cont_detail_id: "0",
      doc_stmnt_year: "2020",
      received_date: "12/15/2019",
    });
    const electionYearRow = contribution({
      doc_seq_no: "3",
      contribution_id: "30",
      cont_detail_id: "0",
      doc_stmnt_year: "2020",
      received_date: "10/01/2020",
    });
    await writeExtractedYearDir({
      cacheDir,
      year: 2020,
      contributionRows: [priorYearReceiptOnAnnualStatement, electionYearRow],
      expenditureRows: [],
    });
    await writeExtractedYearDir({ cacheDir, year: 2021, contributionRows: [], expenditureRows: [] });

    const db = createMockDb([dueRow({ election_year: 2020, source_url: null })]);
    const syncMichiganCandidateFinanceFn = vi.fn().mockResolvedValue({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2020,
      totalReceipts: 200,
    });

    const result = await syncDueMichiganCandidateFinance({
      db,
      syncMichiganCandidateFinanceFn,
      now: new Date("2020-06-01T00:00:00.000Z"),
      rawDataCacheDir: cacheDir,
      autoLinkMissingLinks: false,
    });

    // No demand for a nonexistent 2019 archive, and the clamped duplicate of
    // the election year does not read the 2020 archive twice.
    expect(result).toMatchObject({ syncedCandidateCount: 1, failedCandidateCount: 0 });
    const syncArgs = syncMichiganCandidateFinanceFn.mock.calls[0]?.[0];
    expect(syncArgs.contributionRows.map((row: MichiganMitnLegacyContributionRow) => row.contribution_id)).toEqual([
      "10",
      "30",
    ]);
  });

  it("reads every cycle filing year from a single shared extracted directory override", async () => {
    const sharedDir = await mkdtemp(join(tmpdir(), "voteapp-mi-shared-"));
    tempDirs.push(sharedDir);
    const priorYearOnly = contribution({
      doc_seq_no: "1",
      contribution_id: "10",
      cont_detail_id: "0",
      doc_stmnt_year: "2021",
      received_date: "11/15/2021",
    });
    const electionYearRow = contribution({
      doc_seq_no: "3",
      contribution_id: "30",
      cont_detail_id: "0",
      doc_stmnt_year: "2022",
      received_date: "10/01/2022",
    });
    await writeYearFilesInto({ dir: sharedDir, year: 2021, contributionRows: [priorYearOnly], expenditureRows: [] });
    await writeYearFilesInto({ dir: sharedDir, year: 2022, contributionRows: [electionYearRow], expenditureRows: [] });
    await writeYearFilesInto({ dir: sharedDir, year: 2023, contributionRows: [], expenditureRows: [] });

    const db = createMockDb([dueRow({ source_url: null })]);
    const syncMichiganCandidateFinanceFn = vi.fn().mockResolvedValue({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2022,
      totalReceipts: 350,
    });

    const result = await syncDueMichiganCandidateFinance({
      db,
      syncMichiganCandidateFinanceFn,
      now: new Date("2022-06-01T00:00:00.000Z"),
      rawDataExtractedDir: sharedDir,
      autoLinkMissingLinks: false,
    });

    expect(result).toMatchObject({ syncedCandidateCount: 1, failedCandidateCount: 0 });
    const syncArgs = syncMichiganCandidateFinanceFn.mock.calls[0]?.[0];
    // The readers partition one directory by the `{year}_` file-name prefix,
    // so each filing year's rows load exactly once.
    expect(syncArgs.contributionRows.map((row: MichiganMitnLegacyContributionRow) => row.contribution_id)).toEqual([
      "10",
      "30",
    ]);
  });

  it("marks only rows from a failed MiTN data year as failed", async () => {
    const db = createMockDb([
      dueRow({ total_due_rows: "2" }),
      dueRow({ election_year: 2024, committee_id: "888888", total_due_rows: "2" }),
    ]);
    const syncMichiganCandidateFinanceFn = vi.fn().mockResolvedValue({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2022,
      dryRun: false,
      resolution: { status: "matched", committeeId: "514456" },
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 1,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      totalReceipts: 100,
      directContributionTotal: 100,
      outsideSupportTotal: 0,
      outsideOpposeTotal: 0,
      matchedContributionRowCount: 1,
      includedContributionRowCount: 1,
      skippedContributionRowCount: 0,
      matchedOutsideExpenditureRowCount: 0,
      includedOutsideExpenditureRowCount: 0,
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
      autoLinkMissingLinks: false,
    });

    expect(result).toMatchObject({
      dueCandidateCount: 2,
      selectedCandidateCount: 2,
      syncedCandidateCount: 1,
      failedCandidateCount: 1,
    });
    expect(syncMichiganCandidateFinanceFn).toHaveBeenCalledTimes(1);
    expect(result.results).toEqual([
      expect.objectContaining({ electionYear: 2022, ok: true }),
      expect.objectContaining({
        electionYear: 2024,
        ok: false,
        error: expect.stringContaining("Michigan MiTN data load failed for 2024"),
      }),
    ]);
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
