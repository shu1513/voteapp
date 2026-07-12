import { describe, expect, it, vi } from "vitest";

import { replaceNewYorkCityCandidateFinanceSnapshot } from "../../../src/pipeline/newYorkCityFinance/newYorkCityFinanceWriter.js";

describe("newYorkCityFinanceWriter", () => {
  it("replaces one snapshot transactionally", async () => {
    const query = vi.fn()
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: "link-1" }] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] });
    const client = { query, release: vi.fn() };
    const db = { query: vi.fn(), connect: vi.fn().mockResolvedValue(client) };
    const result = await replaceNewYorkCityCandidateFinanceSnapshot({
      db: db as never,
      link: {
        candidateId: "candidate-1", electionId: "election-1", electionYear: 2025,
        candidateNameNormalized: "JANE DOE", officeCode: "1", boroughCode: null,
        cfbCandidateId: "A1", cfbCandidateName: "DOE, JANE", linkSource: "cfb_csv",
        sourceUrl: "https://example.test", lastVerifiedAt: new Date("2025-01-01T00:00:00Z"),
      },
      summary: {
        privateContributions: 100, netExpenditures: 50, outstandingBills: 5, publicFunds: 20,
        sourceUrl: "https://example.test", lastSyncedAt: new Date("2025-01-02T00:00:00Z"),
      },
      breakdowns: [{ categoryType: "occupation", categoryName: "Teacher", amount: 100, contributorCount: 1, sourceUrl: "https://example.test" }],
      outside: {
        supportTotal: 25,
        opposeTotal: 0,
        groups: [{ spenderId: "Z1", spenderName: "Outside Group", supportOppose: "support", amount: 25, expenditureCount: 1, sourceUrl: "https://example.test/outside" }],
        breakdowns: [{ spenderId: "Z1", supportOppose: "support", categoryType: "donor", categoryName: "Business Group", amount: 50, contributorCount: 1, sourceUrl: "https://example.test/funders" }],
      },
    });
    expect(result).toEqual({ linkId: "link-1", breakdownsWritten: 1, outsideGroupsWritten: 1 });
    expect(query.mock.calls.map((call) => String(call[0]).trim().split(/\s+/, 1)[0])).toEqual([
      "BEGIN", "UPDATE", "INSERT", "INSERT", "DELETE", "INSERT", "DELETE", "INSERT", "INSERT", "COMMIT",
    ]);
    expect(client.release).toHaveBeenCalledOnce();
  });

  it("rolls back failed writes", async () => {
    const query = vi.fn()
      .mockResolvedValueOnce({ rows: [] })
      .mockRejectedValueOnce(new Error("write failed"))
      .mockResolvedValueOnce({ rows: [] });
    const client = { query, release: vi.fn() };
    const db = { query: vi.fn(), connect: vi.fn().mockResolvedValue(client) };
    await expect(replaceNewYorkCityCandidateFinanceSnapshot({
      db: db as never,
      link: {
        candidateId: "candidate-1", electionId: "election-1", electionYear: 2025,
        candidateNameNormalized: "JANE DOE", officeCode: "1", boroughCode: null,
        cfbCandidateId: "A1", cfbCandidateName: "DOE, JANE", linkSource: "cfb_csv",
        sourceUrl: null, lastVerifiedAt: new Date(),
      },
      summary: {
        privateContributions: null, netExpenditures: null, outstandingBills: null, publicFunds: null,
        sourceUrl: null, lastSyncedAt: new Date(),
      },
      breakdowns: [],
    })).rejects.toThrow("write failed");
    expect(query).toHaveBeenLastCalledWith("ROLLBACK");
    expect(client.release).toHaveBeenCalledOnce();
  });

  it("preserves existing outside data when a direct-only snapshot is written", async () => {
    const query = vi.fn()
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: "link-1" }] })
      .mockResolvedValue({ rows: [] });
    const client = { query, release: vi.fn() };
    await replaceNewYorkCityCandidateFinanceSnapshot({
      db: { query: vi.fn(), connect: vi.fn().mockResolvedValue(client) } as never,
      link: {
        candidateId: "candidate-1", electionId: "election-1", electionYear: 2025,
        candidateNameNormalized: "JANE DOE", officeCode: "1", boroughCode: null,
        cfbCandidateId: "A1", cfbCandidateName: "DOE, JANE", linkSource: "cfb_csv",
        sourceUrl: "https://example.test", lastVerifiedAt: new Date("2025-01-01T00:00:00Z"),
      },
      summary: {
        privateContributions: 100, netExpenditures: 50, outstandingBills: 5, publicFunds: 20,
        sourceUrl: "https://example.test", lastSyncedAt: new Date("2025-01-02T00:00:00Z"),
      },
      breakdowns: [],
    });
    expect(query.mock.calls.some((call) => String(call[0]).includes("nyc_candidate_finance_outside_groups"))).toBe(false);
    const summaryCall = query.mock.calls.find((call) => String(call[0]).includes("nyc_candidate_finance_summaries"));
    expect(summaryCall?.[1]?.at(-1)).toBe(false);
  });
});
