import { describe, expect, it, vi } from "vitest";

import {
  replaceColoradoCandidateFinanceSnapshot,
  upsertColoradoFinanceLink,
} from "../../../src/pipeline/coloradoFinance/coloradoFinanceWriter.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";

function createMockDb() {
  return {
    query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
  };
}

function baseLink() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    electionYear: 2026,
    candidateNameNormalized: "JANE DOE",
    officeName: "Governor",
    tracerCandidateId: "TRACER-123",
    committeeId: "202650001",
    committeeName: "Jane Doe for Colorado Governor",
    linkSource: "tracer_candidate_search" as const,
    sourceUrl: "https://tracer.sos.colorado.gov/PublicSite/SearchPages/CandidateDetail.aspx",
    lastVerifiedAt: new Date("2026-01-01T00:00:00.000Z"),
  };
}

describe("coloradoFinanceWriter", () => {
  it("upserts Colorado finance links and returns the link id", async () => {
    const db = createMockDb();

    await expect(upsertColoradoFinanceLink({ db, link: baseLink() })).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.co_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT (candidate_id, election_id, committee_id)");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("RETURNING id");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "JANE DOE",
      "Governor",
      "TRACER-123",
      "202650001",
      "Jane Doe for Colorado Governor",
      "active",
      "tracer_candidate_search",
      "https://tracer.sos.colorado.gov/PublicSite/SearchPages/CandidateDetail.aspx",
      "2026-01-01T00:00:00.000Z",
    ]);
  });

  it("replaces a Colorado finance snapshot inside a transaction", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };
    const db = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    };

    const result = await replaceColoradoCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 1000,
        sourceUrl: "https://tracer.sos.colorado.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionData.csv.zip",
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 700,
          contributorCount: 3,
          sourceUrl: "https://tracer.sos.colorado.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionData.csv.zip",
        },
        {
          categoryType: "employer",
          categoryName: "Google",
          amount: 300,
          contributorCount: 2,
          sourceUrl: "https://tracer.sos.colorado.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionData.csv.zip",
        },
      ],
    });

    expect(result).toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 2,
    });
    expect(db.connect).toHaveBeenCalledTimes(1);
    expect(db.query).not.toHaveBeenCalled();
    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
    expect(client.release).toHaveBeenCalledTimes(1);

    const sql = client.query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("INSERT INTO public.co_candidate_finance_summaries"))).toBe(true);
    const summarySql = sql.find((statement) => statement.includes("INSERT INTO public.co_candidate_finance_summaries"));
    expect(summarySql).toContain(
      "total_receipts = COALESCE(EXCLUDED.total_receipts, co_candidate_finance_summaries.total_receipts)"
    );
    expect(summarySql).toContain("source_url = COALESCE(EXCLUDED.source_url, co_candidate_finance_summaries.source_url)");
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.co_candidate_finance_direct_breakdowns"))).toHaveLength(2);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.co_candidate_finance_direct_breakdowns"))).toBe(true);
  });

  it("does not delete omitted direct breakdowns", async () => {
    const db = createMockDb();

    const result = await replaceColoradoCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 1000,
      },
    });

    expect(result).toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 0,
    });
    const sql = db.query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("DELETE FROM public.co_candidate_finance_direct_breakdowns"))).toBe(false);
  });

  it("uses current snapshot keys when cleaning repeated direct writes with the same timestamp", async () => {
    const db = createMockDb();
    const syncedAt = new Date("2026-02-03T04:05:06.000Z");

    await replaceColoradoCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt,
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 700,
        },
      ],
    });
    await replaceColoradoCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt,
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Teacher",
          amount: 500,
        },
      ],
    });

    const directDeleteCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("DELETE FROM public.co_candidate_finance_direct_breakdowns")
    );
    const lastDelete = directDeleteCalls.at(-1);
    expect(String(lastDelete?.[0])).toContain("jsonb_to_recordset");
    expect(String(lastDelete?.[0])).not.toContain("last_synced_at <");
    expect(lastDelete?.[1]).toEqual([
      LINK_ID,
      2026,
      JSON.stringify([{ category_type: "occupation", category_name: "Teacher" }]),
    ]);
  });

  it("rolls back and releases the client when a transactional write fails", async () => {
    const client = {
      query: vi
        .fn()
        .mockResolvedValueOnce({ rows: [], rowCount: 0 })
        .mockRejectedValueOnce(new Error("write failed"))
        .mockResolvedValueOnce({ rows: [], rowCount: 0 }),
      release: vi.fn(),
    };
    const db = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    };

    await expect(
      replaceColoradoCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        summary: { totalReceipts: 1000 },
      })
    ).rejects.toThrow("write failed");

    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("ROLLBACK");
    expect(client.release).toHaveBeenCalledTimes(1);
  });

  it("validates core inputs before writing", async () => {
    const db = createMockDb();

    await expect(
      replaceColoradoCandidateFinanceSnapshot({
        db,
        link: {
          ...baseLink(),
          committeeId: " ",
        },
      })
    ).rejects.toThrow("Colorado committee id is required");

    expect(db.query).not.toHaveBeenCalled();
  });

  it("rejects invalid election years", async () => {
    const db = createMockDb();

    await expect(
      upsertColoradoFinanceLink({
        db,
        link: {
          ...baseLink(),
          electionYear: 2000,
        },
      })
    ).rejects.toThrow("Invalid Colorado finance election year");

    expect(db.query).not.toHaveBeenCalled();
  });
});

describe("coloradoFinanceWriter pool boundary", () => {
  it("rejects a supplied PoolClient before issuing any statement, so it can never commit an outer transaction", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      connect: vi.fn(),
      release: vi.fn(),
    };

    await expect(
      replaceColoradoCandidateFinanceSnapshot({
        db: client as never,
        link: baseLink(),
        syncedAt: new Date("2026-02-03T04:05:06.000Z"),
        summary: { totalReceipts: 1000 },
      } as never)
    ).rejects.toThrow("Colorado finance snapshot writes must receive a Pool, not a PoolClient");
    expect(client.query).not.toHaveBeenCalled();
    expect(client.connect).not.toHaveBeenCalled();
  });
});
