import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { aggregateIllinoisD2Summaries } from "../../../src/pipeline/illinoisFinance/illinoisD2SummaryAggregator.js";
import {
  produceIllinoisSbeNormalizedArtifact,
  type IllinoisSbeBulkDataPaths,
} from "../../../src/pipeline/illinoisFinance/illinoisSbeBulkDataProducer.js";

const FIXTURE = new URL("../../fixtures/illinoisFinance/official-bulk/", import.meta.url);
const ACQUIRED_AT = "2026-07-12T12:00:00.000Z";
const temporaryDirectories: string[] = [];

function fixturePaths(): IllinoisSbeBulkDataPaths {
  return {
    candidates: new URL("Candidates.txt", FIXTURE).pathname,
    candidateElections: new URL("CanElections.txt", FIXTURE).pathname,
    committeeCandidateLinks: new URL("CmteCandidateLinks.txt", FIXTURE).pathname,
    committees: new URL("Committees.txt", FIXTURE).pathname,
    filedDocuments: new URL("FiledDocs.txt", FIXTURE).pathname,
    d2Totals: new URL("D2Totals.txt", FIXTURE).pathname,
  };
}

afterEach(async () => {
  await Promise.all(temporaryDirectories.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

describe("Illinois SBE bulk data producer", () => {
  it("normalizes real official rows and rejects ward candidates", async () => {
    const { artifact, stats } = await produceIllinoisSbeNormalizedArtifact({
      paths: fixturePaths(),
      acquiredAt: ACQUIRED_AT,
    });

    expect(stats).toEqual({
      eligibleCandidates: 7,
      rejectedCandidates: 1,
      candidatesWithoutElection: 0,
      candidatesWithoutCommittee: 0,
      candidateCommitteeRelations: 17,
      d2ReportSummaries: 3,
      d2RowsWithoutUsableDocument: 0,
    });
    expect(artifact.candidateCommitteeRelations).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ candidateId: "46454", electionYear: 2023, office: "Mayor", district: "Chicago" }),
        expect.objectContaining({ candidateId: "37835", electionYear: 2023, office: "Clerk", district: "Chicago" }),
        expect.objectContaining({ candidateId: "42345", electionYear: 2023, office: "Treasurer", district: "Chicago" }),
        expect.objectContaining({ candidateId: "46519", districtType: "City", district: "Elgin", isAtLarge: true }),
        expect.objectContaining({ candidateId: "9811", districtType: "Village", district: "Inverness", isAtLarge: true }),
        expect.objectContaining({ candidateId: "19135", districtType: "Town", district: "Cicero", office: "President" }),
      ])
    );
    expect(artifact.candidateCommitteeRelations.some((relation) => relation.candidateId === "46284")).toBe(false);
    expect(
      artifact.candidateCommitteeRelations.filter(
        (relation) => relation.candidateId === "46454" && relation.electionYear === 2023
      )
    ).toHaveLength(2);
    expect(new Set(artifact.candidateCommitteeRelations.map((relation) => relation.committeeId)).size).toBe(12);
    expect(artifact.d2ReportSummaries.map((report) => report.filedAt)).toEqual([
      "2000-10-23T21:47:11.000Z",
      "2000-11-22T17:15:19.000Z",
      "2001-01-31T17:51:38.000Z",
    ]);

    expect(
      aggregateIllinoisD2Summaries({
        electionYear: 2000,
        committeeId: "1078",
        reports: artifact.d2ReportSummaries,
      })
    ).toMatchObject({ totalReceipts: 54724, includedReportCount: 2 });
  });

  it("fails closed when a bulk file is truncated", async () => {
    const directory = await mkdtemp(join(tmpdir(), "illinois-sbe-producer-"));
    temporaryDirectories.push(directory);
    const truncated = join(directory, "Candidates.txt");
    const candidates = await readFile(fixturePaths().candidates, "utf8");
    await writeFile(truncated, candidates.slice(0, -1), "utf8");

    await expect(
      produceIllinoisSbeNormalizedArtifact({
        paths: { ...fixturePaths(), candidates: truncated },
        acquiredAt: ACQUIRED_AT,
      })
    ).rejects.toThrow("final newline is missing");
  });

  it("fails closed when an official header changes", async () => {
    const directory = await mkdtemp(join(tmpdir(), "illinois-sbe-producer-"));
    temporaryDirectories.push(directory);
    const changed = join(directory, "Candidates.txt");
    const candidates = await readFile(fixturePaths().candidates, "utf8");
    await writeFile(changed, candidates.replace("ID\tLastName", "ID\tSurname"), "utf8");

    await expect(
      produceIllinoisSbeNormalizedArtifact({
        paths: { ...fixturePaths(), candidates: changed },
        acquiredAt: ACQUIRED_AT,
      })
    ).rejects.toThrow("header does not match the published schema");
  });
});
