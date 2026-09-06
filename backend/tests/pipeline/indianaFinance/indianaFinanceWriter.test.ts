import { describe, expect, it, vi } from "vitest";

import {
  replaceIndianaCandidateFinanceSnapshot,
  upsertIndianaFinanceLink,
} from "../../../src/pipeline/indianaFinance/indianaFinanceWriter.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";
const CONTRIBUTION_SOURCE_URL =
  "https://campaignfinance.in.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionData.csv.zip";

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
    candidateNameNormalized: "CESAR DIEGO MORALES",
    officeName: "State Senator",
    district: "30",
    committeeId: "422",
    committeeName: "Diego for Indiana",
    linkSource: "public_bulk" as const,
    sourceUrl: "https://campaignfinance.in.gov/PublicSite/Reporting/DataDownload.aspx",
    lastVerifiedAt: new Date("2026-01-01T00:00:00.000Z"),
  };
}

describe("indianaFinanceWriter", () => {
  it("upserts Indiana finance links and returns the link id", async () => {
    const db = createMockDb();

    await expect(upsertIndianaFinanceLink({ db, link: baseLink() })).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.in_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT (candidate_id, election_id, committee_id)");
    expect(String(db.query.mock.calls[0]?.[0])).toContain(
      "WHEN in_candidate_finance_links.link_source = 'manual' THEN in_candidate_finance_links.link_source"
    );
    expect(String(db.query.mock.calls[0]?.[0])).toContain("RETURNING id");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "CESAR DIEGO MORALES",
      "State Senator",
      "30",
      "422",
      "Diego for Indiana",
      "active",
      "public_bulk",
      "https://campaignfinance.in.gov/PublicSite/Reporting/DataDownload.aspx",
      "2026-01-01T00:00:00.000Z",
    ]);
  });

  it("replaces an Indiana finance snapshot inside a transaction", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };
    const db = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    };

    const result = await replaceIndianaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 5350,
        directContributionTotal: 5350,
        sourceUrl: CONTRIBUTION_SOURCE_URL,
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Teacher/Education",
          amount: 5000,
          contributorCount: 1,
          sourceUrl: CONTRIBUTION_SOURCE_URL,
        },
        {
          categoryType: "contribution_size",
          categoryName: "$5,000+",
          amount: 5000,
          contributorCount: 1,
          sourceUrl: CONTRIBUTION_SOURCE_URL,
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
    expect(sql.some((statement) => statement.includes("INSERT INTO public.in_candidate_finance_summaries"))).toBe(true);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.in_candidate_finance_direct_breakdowns"))).toHaveLength(2);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.in_candidate_finance_direct_breakdowns"))).toBe(true);
  });

  it("replaces nullable summary values and does not delete omitted direct breakdowns", async () => {
    const db = createMockDb();

    const result = await replaceIndianaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: { totalReceipts: 1000 },
    });

    expect(result).toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 0,
    });
    const sql = db.query.mock.calls.map((call) => String(call[0]));
    const summarySql = sql.find((statement) => statement.includes("INSERT INTO public.in_candidate_finance_summaries"));
    expect(summarySql).toContain("total_receipts = EXCLUDED.total_receipts");
    expect(summarySql).toContain("direct_contribution_total = EXCLUDED.direct_contribution_total");
    expect(summarySql).toContain("source_url = EXCLUDED.source_url");
    expect(sql.some((statement) => statement.includes("DELETE FROM public.in_candidate_finance_direct_breakdowns"))).toBe(false);
  });

  it("uses current snapshot keys when cleaning repeated direct writes with the same timestamp", async () => {
    const db = createMockDb();
    const syncedAt = new Date("2026-02-03T04:05:06.000Z");

    await replaceIndianaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt,
      directBreakdowns: [{ categoryType: "occupation", categoryName: "Attorney", amount: 700 }],
    });
    await replaceIndianaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt,
      directBreakdowns: [{ categoryType: "occupation", categoryName: "Teacher", amount: 500 }],
    });

    const directDeleteCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("DELETE FROM public.in_candidate_finance_direct_breakdowns")
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
      replaceIndianaCandidateFinanceSnapshot({
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
      replaceIndianaCandidateFinanceSnapshot({
        db,
        link: {
          ...baseLink(),
          committeeId: " ",
        },
      })
    ).rejects.toThrow("Indiana committee id is required");

    expect(db.query).not.toHaveBeenCalled();
  });
});

describe("indianaFinanceWriter pool boundary", () => {
  it("rejects a supplied PoolClient before issuing any statement, so it can never commit an outer transaction", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      connect: vi.fn(),
      release: vi.fn(),
    };

    await expect(
      replaceIndianaCandidateFinanceSnapshot({
        db: client as never,
        link: baseLink(),
        syncedAt: new Date("2026-02-03T04:05:06.000Z"),
        summary: { totalReceipts: 1000 },
      } as never)
    ).rejects.toThrow("Indiana finance snapshot writes must receive a Pool, not a PoolClient");
    expect(client.query).not.toHaveBeenCalled();
    expect(client.connect).not.toHaveBeenCalled();
  });
});
