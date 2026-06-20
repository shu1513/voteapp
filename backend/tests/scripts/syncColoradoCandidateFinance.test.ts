import { describe, expect, it } from "vitest";

import {
  parseSyncColoradoCandidateFinanceScriptArgs,
  toSyncColoradoCandidateFinanceScriptOutput,
} from "../../src/scripts/syncColoradoCandidateFinance.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";

describe("syncColoradoCandidateFinance script", () => {
  it("parses required and optional flags", () => {
    expect(
      parseSyncColoradoCandidateFinanceScriptArgs([
        "--candidate-id",
        CANDIDATE_ID.toUpperCase(),
        "--election-id",
        ELECTION_ID,
        "--candidate-name=Jane Doe",
        "--year",
        "2026",
        "--office=Governor",
        "--committee-id=202650001",
        "--committee-name",
        "Jane Doe for Colorado Governor",
        "--tracer-candidate-id=TRACER-123",
        "--source-url=https://tracer.sos.colorado.gov/PublicSite/SearchPages/CandidateDetail.aspx",
        "--contribution-source-url=https://tracer.sos.colorado.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionData.csv.zip",
        "--dry-run",
        "--force",
        "--raw-zip=/tmp/2026_ContributionData.csv.zip",
        "--raw-cache-dir=/tmp/co-tracer",
        "--max-breakdowns=25",
      ])
    ).toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      candidateName: "Jane Doe",
      electionYear: 2026,
      officeName: "Governor",
      committeeId: "202650001",
      committeeName: "Jane Doe for Colorado Governor",
      tracerCandidateId: "TRACER-123",
      sourceUrl: "https://tracer.sos.colorado.gov/PublicSite/SearchPages/CandidateDetail.aspx",
      contributionSourceUrl:
        "https://tracer.sos.colorado.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionData.csv.zip",
      dryRun: true,
      force: true,
      rawZipPath: "/tmp/2026_ContributionData.csv.zip",
      rawCacheDir: "/tmp/co-tracer",
      directMaxBreakdownsPerCategory: 25,
    });
  });

  it("defaults optional flags", () => {
    expect(
      parseSyncColoradoCandidateFinanceScriptArgs([
        `--candidate-id=${CANDIDATE_ID}`,
        `--election-id=${ELECTION_ID}`,
        "--candidate-name=Jane Doe",
        "--year=2026",
        "--office=Governor",
        "--committee-id=202650001",
        "--committee-name=Jane Doe for Colorado Governor",
      ])
    ).toMatchObject({
      dryRun: false,
      force: false,
      rawZipPath: undefined,
      rawCacheDir: undefined,
      directMaxBreakdownsPerCategory: undefined,
    });
  });

  it("rejects malformed flags strictly", () => {
    expect(() => parseSyncColoradoCandidateFinanceScriptArgs(["--year=2026"])).toThrow(
      "Missing required --candidate-id flag"
    );
    expect(() =>
      parseSyncColoradoCandidateFinanceScriptArgs([
        "--candidate-id=not-a-uuid",
        `--election-id=${ELECTION_ID}`,
        "--candidate-name=Jane Doe",
        "--year=2026",
        "--office=Governor",
        "--committee-id=202650001",
        "--committee-name=Jane Doe for Colorado Governor",
      ])
    ).toThrow("Invalid --candidate-id value");
    expect(() =>
      parseSyncColoradoCandidateFinanceScriptArgs([
        `--candidate-id=${CANDIDATE_ID}`,
        `--election-id=${ELECTION_ID}`,
        "--candidate-name=Jane Doe",
        "--year=20x6",
        "--office=Governor",
        "--committee-id=202650001",
        "--committee-name=Jane Doe for Colorado Governor",
      ])
    ).toThrow("Invalid --year value");
    expect(() =>
      parseSyncColoradoCandidateFinanceScriptArgs([
        `--candidate-id=${CANDIDATE_ID}`,
        `--election-id=${ELECTION_ID}`,
        "--candidate-name=Jane Doe",
        "--year=2026",
        "--office=Governor",
        "--committee-id=202650001",
        "--committee-name=Jane Doe for Colorado Governor",
        "--max-breakdowns=10abc",
      ])
    ).toThrow("Invalid --max-breakdowns value");
  });

  it("formats script output", () => {
    const output = toSyncColoradoCandidateFinanceScriptOutput({
      startedAt: new Date("2026-01-02T03:04:05.000Z"),
      options: {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Jane Doe",
        electionYear: 2026,
        officeName: "Governor",
        committeeId: "202650001",
        committeeName: "Jane Doe for Colorado Governor",
        dryRun: true,
        force: false,
      },
      zipPath: "/tmp/2026_ContributionData.csv.zip",
      contributionRowCount: 2,
      result: {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        electionYear: 2026,
        dryRun: true,
        linkWritten: false,
        summaryWritten: false,
        directBreakdownsWritten: 0,
        totalReceipts: 400,
        matchedContributionRowCount: 2,
        includedContributionRowCount: 2,
        skippedContributionRowCount: 0,
      },
    });

    expect(output).toMatchObject({
      type: "colorado_candidate_finance_sync",
      started_at: "2026-01-02T03:04:05.000Z",
      candidate_id: CANDIDATE_ID,
      election_id: ELECTION_ID,
      candidate_name: "Jane Doe",
      election_year: 2026,
      office_name: "Governor",
      committee_id: "202650001",
      dry_run: true,
      raw_zip_path: "/tmp/2026_ContributionData.csv.zip",
      contribution_rows_loaded: 2,
      result: {
        totalReceipts: 400,
      },
    });
    expect(typeof output.ts).toBe("string");
  });
});
