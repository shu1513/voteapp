import { describe, expect, it, vi } from "vitest";

import {
  replaceVirginiaCandidateFinanceSnapshot,
  upsertVirginiaFinanceLink,
} from "../../../src/pipeline/virginiaFinance/virginiaFinanceWriter.js";

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
    electionYear: 2025,
    candidateNameNormalized: "JANE DOE",
    officeName: "Governor",
    district: null,
    committeeId: "committee-123",
    committeeCode: "CC-123",
    committeeName: "Jane Doe for Governor",
    linkSource: "cfreports_search" as const,
    sourceUrl: "https://cfreports.elections.virginia.gov/Committee/Index/committee-123",
    lastVerifiedAt: new Date("2025-01-01T00:00:00.000Z"),
  };
}

describe("virginiaFinanceWriter", () => {
  it("upserts Virginia finance links and returns the link id", async () => {
    const db = createMockDb();

    await expect(upsertVirginiaFinanceLink({ db, link: baseLink() })).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.va_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT (candidate_id, election_id, committee_id)");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("RETURNING id");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2025,
      "JANE DOE",
      "Governor",
      null,
      "committee-123",
      "CC-123",
      "Jane Doe for Governor",
      "active",
      "cfreports_search",
      "https://cfreports.elections.virginia.gov/Committee/Index/committee-123",
      "2025-01-01T00:00:00.000Z",
    ]);
  });

  it("replaces a Virginia finance snapshot inside a transaction", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };
    const db = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    };

    const result = await replaceVirginiaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2025-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 1000,
        directContributionTotal: 1000,
        sourceUrl: "https://cfreports.elections.virginia.gov/Report/Xml/123",
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 700,
          contributorCount: 3,
          sourceUrl: "https://cfreports.elections.virginia.gov/Report/Xml/123",
        },
        {
          categoryType: "contribution_size",
          categoryName: "$250-$499",
          amount: 300,
          contributorCount: 2,
          sourceUrl: "https://cfreports.elections.virginia.gov/Report/Xml/123",
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
    expect(sql.some((statement) => statement.includes("INSERT INTO public.va_candidate_finance_summaries"))).toBe(true);
    const summarySql = sql.find((statement) => statement.includes("INSERT INTO public.va_candidate_finance_summaries"));
    expect(summarySql).toContain(
      "total_receipts = COALESCE(EXCLUDED.total_receipts, va_candidate_finance_summaries.total_receipts)"
    );
    expect(summarySql).toContain(
      "direct_contribution_total = COALESCE(EXCLUDED.direct_contribution_total, va_candidate_finance_summaries.direct_contribution_total)"
    );
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.va_candidate_finance_direct_breakdowns"))).toHaveLength(2);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.va_candidate_finance_direct_breakdowns"))).toBe(true);
  });

  it("does not delete omitted direct breakdowns", async () => {
    const db = createMockDb();

    const result = await replaceVirginiaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2025-02-03T04:05:06.000Z"),
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
    expect(sql.some((statement) => statement.includes("DELETE FROM public.va_candidate_finance_direct_breakdowns"))).toBe(false);
  });

  it("uses current snapshot keys when cleaning repeated direct writes with the same timestamp", async () => {
    const db = createMockDb();
    const syncedAt = new Date("2025-02-03T04:05:06.000Z");

    await replaceVirginiaCandidateFinanceSnapshot({
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
    await replaceVirginiaCandidateFinanceSnapshot({
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
      String(call[0]).includes("DELETE FROM public.va_candidate_finance_direct_breakdowns")
    );
    const lastDelete = directDeleteCalls.at(-1);
    expect(String(lastDelete?.[0])).toContain("jsonb_to_recordset");
    expect(String(lastDelete?.[0])).not.toContain("last_synced_at <");
    expect(lastDelete?.[1]).toEqual([
      LINK_ID,
      2025,
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
      replaceVirginiaCandidateFinanceSnapshot({
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
      replaceVirginiaCandidateFinanceSnapshot({
        db,
        link: {
          ...baseLink(),
          committeeId: " ",
        },
      })
    ).rejects.toThrow("Virginia committee id is required");

    expect(db.query).not.toHaveBeenCalled();
  });

  it("rejects invalid election years", async () => {
    const db = createMockDb();

    await expect(
      upsertVirginiaFinanceLink({
        db,
        link: {
          ...baseLink(),
          electionYear: 1999,
        },
      })
    ).rejects.toThrow("Invalid Virginia finance election year");

    expect(db.query).not.toHaveBeenCalled();
  });
});
