import { describe, expect, it } from "vitest";

import {
  parseSyncAlaskaCandidateFinanceScriptArgs,
  toSyncAlaskaCandidateFinanceScriptOutput,
} from "../../src/scripts/syncAlaskaCandidateFinance.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";

describe("syncAlaskaCandidateFinance script", () => {
  it("parses required and optional flags", () => {
    expect(
      parseSyncAlaskaCandidateFinanceScriptArgs([
        "--candidate-id",
        CANDIDATE_ID.toUpperCase(),
        "--election-id",
        ELECTION_ID,
        "--candidate-name=Jane Doe",
        "--year",
        "2026",
        "--office=Governor",
        "--district=4",
        "--candidate-filer-id=1001",
        "--candidate-filer-name",
        "Doe, Jane",
        "--source-url=https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx",
        "--income-csv=/tmp/alaska-income.csv",
        "--ie-expenditures-csv=/tmp/alaska-ie-exp.csv",
        "--ie-contributions-csv=/tmp/alaska-ie-con.csv",
        "--write",
        "--force",
      ])
    ).toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      candidateName: "Jane Doe",
      electionYear: 2026,
      officeName: "Governor",
      district: "4",
      candidateFilerId: "1001",
      candidateFilerName: "Doe, Jane",
      sourceUrl: "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx",
      incomeCsvPath: "/tmp/alaska-income.csv",
      independentExpendituresCsvPath: "/tmp/alaska-ie-exp.csv",
      independentContributionsCsvPath: "/tmp/alaska-ie-con.csv",
      dryRun: false,
      force: true,
    });
  });

  it("defaults to dry-run for explicit candidate sync", () => {
    expect(
      parseSyncAlaskaCandidateFinanceScriptArgs([
        `--candidate-id=${CANDIDATE_ID}`,
        `--election-id=${ELECTION_ID}`,
        "--candidate-name=Jane Doe",
        "--year=2026",
        "--office=Governor",
        "--candidate-filer-id=1001",
        "--candidate-filer-name=Doe, Jane",
        "--income-csv=/tmp/alaska-income.csv",
      ])
    ).toMatchObject({
      dryRun: true,
      force: false,
    });
  });

  it("rejects malformed flags strictly", () => {
    expect(() => parseSyncAlaskaCandidateFinanceScriptArgs(["--year=2026"])).toThrow(
      "Missing required --candidate-id flag"
    );
    expect(() =>
      parseSyncAlaskaCandidateFinanceScriptArgs([
        "--candidate-id=not-a-uuid",
        `--election-id=${ELECTION_ID}`,
        "--candidate-name=Jane Doe",
        "--year=2026",
        "--office=Governor",
        "--candidate-filer-id=1001",
        "--candidate-filer-name=Doe, Jane",
        "--income-csv=/tmp/alaska-income.csv",
      ])
    ).toThrow("Invalid --candidate-id value");
    expect(() =>
      parseSyncAlaskaCandidateFinanceScriptArgs([
        `--candidate-id=${CANDIDATE_ID}`,
        `--election-id=${ELECTION_ID}`,
        "--candidate-name=Jane Doe",
        "--year=20x6",
        "--office=Governor",
        "--candidate-filer-id=1001",
        "--candidate-filer-name=Doe, Jane",
        "--income-csv=/tmp/alaska-income.csv",
      ])
    ).toThrow("Invalid --year value");
    expect(() =>
      parseSyncAlaskaCandidateFinanceScriptArgs([
        `--candidate-id=${CANDIDATE_ID}`,
        `--election-id=${ELECTION_ID}`,
        "--candidate-name=Jane Doe",
        "--year=2026",
        "--office=Governor",
        "--candidate-filer-id=1001",
        "--candidate-filer-name=Doe, Jane",
        "--income-csv=/tmp/alaska-income.csv",
        "--dry-run",
        "--write",
      ])
    ).toThrow("Provide either --dry-run or --write, not both");
  });

  it("formats script output", () => {
    const output = toSyncAlaskaCandidateFinanceScriptOutput({
      startedAt: new Date("2026-01-02T03:04:05.000Z"),
      options: {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Jane Doe",
        electionYear: 2026,
        officeName: "Governor",
        candidateFilerId: "1001",
        candidateFilerName: "Doe, Jane",
        incomeCsvPath: "/tmp/alaska-income.csv",
        dryRun: true,
        force: false,
      },
      result: {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        electionYear: 2026,
        dryRun: true,
        resolution: {
          status: "matched",
          candidateFilerId: "1001",
          candidateFilerName: "Doe, Jane",
          source: "apoc_csv",
          sourceUrl: null,
        },
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
      },
    });

    expect(output).toMatchObject({
      type: "alaska_candidate_finance_sync",
      started_at: "2026-01-02T03:04:05.000Z",
      candidate_id: CANDIDATE_ID,
      election_id: ELECTION_ID,
      candidate_name: "Jane Doe",
      election_year: 2026,
      office_name: "Governor",
      candidate_filer_id: "1001",
      dry_run: true,
      result: {
        totalReceipts: 250,
        directContributionTotal: 250,
      },
    });
    expect(typeof output.ts).toBe("string");
  });
});
