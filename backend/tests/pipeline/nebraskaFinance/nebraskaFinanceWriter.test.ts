import { describe, expect, it, vi } from "vitest";

import {
  replaceNebraskaCandidateFinanceSnapshot,
  upsertNebraskaFinanceLink,
} from "../../../src/pipeline/nebraskaFinance/nebraskaFinanceWriter.js";

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
    candidateNameNormalized: "RICK VEST",
    officeName: "State Senator",
    district: "30",
    committeeId: "7569",
    committeeName: "VOTE VEST",
    linkSource: "nadc_bulk" as const,
    sourceUrl: "https://nadc-e.nebraska.gov/PublicSite/DataDownload.aspx",
    lastVerifiedAt: new Date("2026-01-01T00:00:00.000Z"),
  };
}

describe("nebraskaFinanceWriter", () => {
  it("upserts Nebraska finance links and returns the link id", async () => {
    const db = createMockDb();

    await expect(upsertNebraskaFinanceLink({ db, link: baseLink() })).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.ne_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT (candidate_id, election_id, committee_id)");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("RETURNING id");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "RICK VEST",
      "State Senator",
      "30",
      "7569",
      "VOTE VEST",
      "active",
      "nadc_bulk",
      "https://nadc-e.nebraska.gov/PublicSite/DataDownload.aspx",
      "2026-01-01T00:00:00.000Z",
    ]);
  });

  it("replaces a Nebraska finance snapshot inside a transaction", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };
    const db = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    };

    const result = await replaceNebraskaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 81880.74,
        directContributionTotal: 75389,
        sourceUrl: "https://nadc-e.nebraska.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip",
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "PROPRIETOR",
          amount: 500,
          contributorCount: 1,
          sourceUrl: "https://nadc-e.nebraska.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip",
        },
        {
          categoryType: "contribution_size",
          categoryName: "$5,000+",
          amount: 66389,
          contributorCount: 7,
          sourceUrl: "https://nadc-e.nebraska.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip",
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
    expect(sql.some((statement) => statement.includes("INSERT INTO public.ne_candidate_finance_summaries"))).toBe(true);
    const summaryCall = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ne_candidate_finance_summaries")
    );
    expect(String(summaryCall?.[0])).toContain("direct_contribution_total");
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2026,
      81880.74,
      75389,
      "https://nadc-e.nebraska.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip",
      "2026-02-03T04:05:06.000Z",
    ]);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.ne_candidate_finance_direct_breakdowns"))).toHaveLength(2);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ne_candidate_finance_direct_breakdowns"))).toBe(true);
  });

  it("wraps a supplied queryable in a transaction", async () => {
    const db = createMockDb();

    const result = await replaceNebraskaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 1000,
      },
    });

    expect(result.summaryWritten).toBe(true);
    expect(db.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(db.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
  });

  it("does not delete omitted direct breakdowns", async () => {
    const db = createMockDb();

    const result = await replaceNebraskaCandidateFinanceSnapshot({
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
    const summarySql = sql.find((statement) => statement.includes("INSERT INTO public.ne_candidate_finance_summaries"));
    expect(summarySql).toContain(
      "total_receipts = COALESCE(EXCLUDED.total_receipts, ne_candidate_finance_summaries.total_receipts)"
    );
    expect(summarySql).toContain(
      "direct_contribution_total = COALESCE(EXCLUDED.direct_contribution_total, ne_candidate_finance_summaries.direct_contribution_total)"
    );
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ne_candidate_finance_direct_breakdowns"))).toBe(false);
  });

  it("uses current snapshot keys when cleaning repeated direct writes with the same timestamp", async () => {
    const db = createMockDb();
    const syncedAt = new Date("2026-02-03T04:05:06.000Z");

    await replaceNebraskaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt,
      directBreakdowns: [
        {
          categoryType: "contributor_source_type",
          categoryName: "individuals",
          amount: 700,
        },
      ],
    });
    await replaceNebraskaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt,
      directBreakdowns: [
        {
          categoryType: "contributor_source_type",
          categoryName: "business_nonprofit_entities",
          amount: 500,
        },
      ],
    });

    const directDeleteCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("DELETE FROM public.ne_candidate_finance_direct_breakdowns")
    );
    const lastDelete = directDeleteCalls.at(-1);
    expect(String(lastDelete?.[0])).toContain("jsonb_to_recordset");
    expect(String(lastDelete?.[0])).not.toContain("last_synced_at <");
    expect(lastDelete?.[1]).toEqual([
      LINK_ID,
      2026,
      JSON.stringify([{ category_type: "contributor_source_type", category_name: "business_nonprofit_entities" }]),
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
      replaceNebraskaCandidateFinanceSnapshot({
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
      replaceNebraskaCandidateFinanceSnapshot({
        db,
        link: {
          ...baseLink(),
          committeeId: " ",
        },
      })
    ).rejects.toThrow("Nebraska committee id is required");

    expect(db.query).not.toHaveBeenCalled();
  });

  it("rejects invalid election years", async () => {
    const db = createMockDb();

    await expect(
      upsertNebraskaFinanceLink({
        db,
        link: {
          ...baseLink(),
          electionYear: 2020,
        },
      })
    ).rejects.toThrow("Invalid Nebraska finance election year");

    expect(db.query).not.toHaveBeenCalled();
  });
});
