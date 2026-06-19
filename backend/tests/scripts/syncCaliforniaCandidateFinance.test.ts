import { describe, expect, it } from "vitest";

import {
  parseSyncCaliforniaCandidateFinanceScriptArgs,
  toSyncCaliforniaCandidateFinanceScriptOutput,
} from "../../src/scripts/syncCaliforniaCandidateFinance.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";

describe("syncCaliforniaCandidateFinance script", () => {
  it("parses required and optional flags", () => {
    expect(
      parseSyncCaliforniaCandidateFinanceScriptArgs([
        "--candidate-id",
        CANDIDATE_ID.toUpperCase(),
        "--election-id",
        ELECTION_ID,
        "--candidate-name=Newsom, Gavin",
        "--year",
        "2026",
        "--office=Governor",
        "--committee-id=1456045",
        "--committee-name",
        "Newsom for California Governor 2026",
        "--source-url=https://powersearch.sos.ca.gov/advanced.php",
        "--dry-run",
        "--skip-outside",
        "--force",
        "--timeout-ms=5000",
      ])
    ).toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      candidateName: "Newsom, Gavin",
      electionYear: 2026,
      officeName: "Governor",
      controlledCommitteeId: "1456045",
      controlledCommitteeName: "Newsom for California Governor 2026",
      sourceUrl: "https://powersearch.sos.ca.gov/advanced.php",
      dryRun: true,
      includeOutside: false,
      force: true,
      timeoutMs: 5000,
    });
  });

  it("defaults to outside-spending sync enabled", () => {
    expect(
      parseSyncCaliforniaCandidateFinanceScriptArgs([
        `--candidate-id=${CANDIDATE_ID}`,
        `--election-id=${ELECTION_ID}`,
        "--candidate-name=Newsom, Gavin",
        "--year=2026",
        "--office=Governor",
        "--committee-id=1456045",
        "--committee-name=Newsom for California Governor 2026",
      ])
    ).toMatchObject({
      dryRun: false,
      includeOutside: true,
      force: false,
    });
  });

  it("rejects malformed flags strictly", () => {
    expect(() => parseSyncCaliforniaCandidateFinanceScriptArgs(["--year=2026"])).toThrow(
      "Missing required --candidate-id flag"
    );
    expect(() =>
      parseSyncCaliforniaCandidateFinanceScriptArgs([
        "--candidate-id=not-a-uuid",
        `--election-id=${ELECTION_ID}`,
        "--candidate-name=Newsom, Gavin",
        "--year=2026",
        "--office=Governor",
        "--committee-id=1456045",
        "--committee-name=Newsom for California Governor 2026",
      ])
    ).toThrow("Invalid --candidate-id value");
    expect(() =>
      parseSyncCaliforniaCandidateFinanceScriptArgs([
        `--candidate-id=${CANDIDATE_ID}`,
        `--election-id=${ELECTION_ID}`,
        "--candidate-name=Newsom, Gavin",
        "--year=20x6",
        "--office=Governor",
        "--committee-id=1456045",
        "--committee-name=Newsom for California Governor 2026",
      ])
    ).toThrow("Invalid --year value");
    expect(() =>
      parseSyncCaliforniaCandidateFinanceScriptArgs([
        `--candidate-id=${CANDIDATE_ID}`,
        `--election-id=${ELECTION_ID}`,
        "--candidate-name=Newsom, Gavin",
        "--year=2026",
        "--office=Governor",
        "--committee-id=1456045",
        "--committee-name=Newsom for California Governor 2026",
        "--timeout-ms=10abc",
      ])
    ).toThrow("Invalid --timeout-ms value");
  });

  it("formats script output", () => {
    const output = toSyncCaliforniaCandidateFinanceScriptOutput({
      startedAt: new Date("2026-01-02T03:04:05.000Z"),
      options: {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Newsom, Gavin",
        electionYear: 2026,
        officeName: "Governor",
        controlledCommitteeId: "1456045",
        controlledCommitteeName: "Newsom for California Governor 2026",
        dryRun: true,
        includeOutside: true,
        force: false,
      },
      result: {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        electionYear: 2026,
        dryRun: true,
        outsideIncluded: true,
        linkWritten: false,
        summaryWritten: false,
        directBreakdownsWritten: 0,
        outsideGroupsWritten: 0,
        outsideGroupBreakdownsWritten: 0,
        outsideSupportTotal: 300,
        outsideOpposeTotal: 50,
      },
    });

    expect(output).toMatchObject({
      type: "california_candidate_finance_sync",
      started_at: "2026-01-02T03:04:05.000Z",
      candidate_id: CANDIDATE_ID,
      election_id: ELECTION_ID,
      candidate_name: "Newsom, Gavin",
      election_year: 2026,
      office_name: "Governor",
      controlled_committee_id: "1456045",
      dry_run: true,
      include_outside: true,
      result: {
        outsideSupportTotal: 300,
        outsideOpposeTotal: 50,
      },
    });
    expect(typeof output.ts).toBe("string");
  });
});
