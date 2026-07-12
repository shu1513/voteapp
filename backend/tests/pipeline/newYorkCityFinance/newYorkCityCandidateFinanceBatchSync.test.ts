import { describe, expect, it, vi } from "vitest";

import { syncDueNewYorkCityCandidateFinance } from "../../../src/pipeline/newYorkCityFinance/newYorkCityCandidateFinanceBatchSync.js";

const dueRow = {
  candidateId: "candidate-1", electionId: "election-1", candidateName: "Jane Doe", electionYear: 2029,
  officeScope: "place", officeCanonicalName: "Mayor", districtGeoid: "3651000", cfbCandidateId: null,
};

describe("newYorkCityCandidateFinanceBatchSync", () => {
  it("defers unpublished future files without failing", async () => {
    const refreshArtifact = vi.fn().mockImplementation(async (input: { electionYear: number; kind: "contributions" | "financial_analysis" }) => ({
      status: "not_yet_published", electionYear: input.electionYear, kind: input.kind, url: "https://example.test/missing.csv",
      checkedAt: "2026-07-11T00:00:00Z", nextCheckAt: "2026-07-12T00:00:00Z",
    }));
    const result = await syncDueNewYorkCityCandidateFinance({
      db: {} as never,
      dataSource: {
        listDueRows: vi.fn().mockResolvedValue({ rows: [dueRow], totalDueRows: 1 }),
        refreshArtifact,
      },
    });
    expect(result).toMatchObject({ syncedCandidateCount: 0, deferredCandidateCount: 1, failedCandidateCount: 0 });
    expect(result.results[0]?.status).toBe("not_yet_published");
    expect(result.results[0]?.nextCheckAt).toBe("2026-07-12T00:00:00Z");
  });

  it("resolves once, filters contributions, and syncs candidate", async () => {
    const syncCandidate = vi.fn().mockResolvedValue({ dryRun: false, breakdownsWritten: 1, acceptedContributionRows: 1 });
    const refreshArtifact = vi.fn().mockImplementation(async (input: { electionYear: number; kind: "contributions" | "financial_analysis" }) => ({
      status: "downloaded",
      current: {
        version: 1, electionYear: input.electionYear, kind: input.kind, url: "https://example.test/file.csv",
        filePath: `/tmp/${input.kind}.csv`, downloadedAt: new Date().toISOString(), bytes: 100, etag: null, lastModified: null,
      },
    }));
    const result = await syncDueNewYorkCityCandidateFinance({
      db: {} as never,
      dataSource: {
        listDueRows: vi.fn().mockResolvedValue({ rows: [dueRow], totalDueRows: 1 }),
        refreshArtifact,
        readAnalysis: vi.fn().mockResolvedValue({
          rawRowCount: 1, malformedRowCount: 0, rows: [{
            electionYear: 2029, fromStatement: 1, toStatement: 1, officeCode: "1", candidateName: "DOE, JANE",
            candidateId: "A1", boroughCode: null, privateContributions: 100, publicFunds: 20,
            netExpenditures: 50, outstandingBills: 0,
          }],
        }),
        readContributions: vi.fn().mockResolvedValue({ rows: [], rawRowCount: 0, malformedRowCount: 0 }),
        syncCandidate,
      },
    });
    expect(result).toMatchObject({ syncedCandidateCount: 1, deferredCandidateCount: 0, failedCandidateCount: 0 });
    expect(syncCandidate).toHaveBeenCalledWith(expect.objectContaining({ candidateId: "candidate-1", electionYear: 2029 }));
  });

  it("reports ambiguity as deferred, not failed", async () => {
    const refreshArtifact = vi.fn().mockImplementation(async (input: { electionYear: number; kind: "contributions" | "financial_analysis" }) => ({
      status: "downloaded",
      current: { version: 1, electionYear: input.electionYear, kind: input.kind, url: "x", filePath: `/tmp/${input.kind}.csv`, downloadedAt: "x", bytes: 1, etag: null, lastModified: null },
    }));
    const analysisBase = {
      electionYear: 2029, fromStatement: 1, toStatement: 1, officeCode: "1" as const, candidateName: "DOE, JANE",
      boroughCode: null, privateContributions: 0, publicFunds: 0, netExpenditures: 0, outstandingBills: 0,
    };
    const result = await syncDueNewYorkCityCandidateFinance({
      db: {} as never,
      dataSource: {
        listDueRows: vi.fn().mockResolvedValue({ rows: [dueRow], totalDueRows: 1 }),
        refreshArtifact,
        readAnalysis: vi.fn().mockResolvedValue({ rawRowCount: 2, malformedRowCount: 0, rows: [
          { ...analysisBase, candidateId: "A1" }, { ...analysisBase, candidateId: "A2" },
        ] }),
        readContributions: vi.fn(),
      },
    });
    expect(result).toMatchObject({ deferredCandidateCount: 1, failedCandidateCount: 0 });
    expect(result.results[0]?.status).toBe("ambiguous");
  });
});
