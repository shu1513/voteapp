import { describe, expect, it, vi } from "vitest";

import {
  replaceConnecticutCandidateFinanceSnapshot,
  upsertConnecticutFinanceLink,
} from "../../../src/pipeline/connecticutFinance/connecticutFinanceWriter.js";

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
    candidateNameNormalized: "TIMOTHY ACKERT",
    officeName: "State Lower Chamber Legislator",
    district: "8",
    committeeId: "14376",
    committeeName: "ACKERT FOR THE 8TH",
    linkSource: "ecris_bulk" as const,
    sourceUrl: "https://seec.ct.gov/portal/ecris/CurPreYears",
    lastVerifiedAt: new Date("2026-01-01T00:00:00.000Z"),
  };
}

describe("connecticutFinanceWriter", () => {
  it("upserts Connecticut finance links and returns the link id", async () => {
    const db = createMockDb();

    await expect(upsertConnecticutFinanceLink({ db, link: baseLink() })).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.ct_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT (candidate_id, election_id, committee_id)");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("RETURNING id");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "TIMOTHY ACKERT",
      "State Lower Chamber Legislator",
      "8",
      "14376",
      "ACKERT FOR THE 8TH",
      "active",
      "ecris_bulk",
      "https://seec.ct.gov/portal/ecris/CurPreYears",
      "2026-01-01T00:00:00.000Z",
    ]);
  });

  it("replaces a Connecticut finance snapshot inside a transaction", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };
    const db = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    };

    const result = await replaceConnecticutCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 1000,
        sourceUrl: "https://seec.ct.gov/portal/ecris/CurPreYears",
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 700,
          contributorCount: 3,
          sourceUrl: "https://seec.ct.gov/portal/ecris/CurPreYears",
        },
        {
          categoryType: "contribution_size",
          categoryName: "$250-$499",
          amount: 300,
          contributorCount: 2,
          sourceUrl: "https://seec.ct.gov/portal/ecris/CurPreYears",
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
    expect(sql.some((statement) => statement.includes("INSERT INTO public.ct_candidate_finance_summaries"))).toBe(true);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.ct_candidate_finance_direct_breakdowns"))).toHaveLength(2);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ct_candidate_finance_direct_breakdowns"))).toBe(true);
  });

  it("wraps a supplied queryable in a transaction", async () => {
    const db = createMockDb();

    const result = await replaceConnecticutCandidateFinanceSnapshot({
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

    const result = await replaceConnecticutCandidateFinanceSnapshot({
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
    const summarySql = sql.find((statement) => statement.includes("INSERT INTO public.ct_candidate_finance_summaries"));
    expect(summarySql).toContain(
      "total_receipts = COALESCE(EXCLUDED.total_receipts, ct_candidate_finance_summaries.total_receipts)"
    );
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ct_candidate_finance_direct_breakdowns"))).toBe(false);
  });

  it("uses current snapshot keys when cleaning repeated direct writes with the same timestamp", async () => {
    const db = createMockDb();
    const syncedAt = new Date("2026-02-03T04:05:06.000Z");

    await replaceConnecticutCandidateFinanceSnapshot({
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
    await replaceConnecticutCandidateFinanceSnapshot({
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
      String(call[0]).includes("DELETE FROM public.ct_candidate_finance_direct_breakdowns")
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
      replaceConnecticutCandidateFinanceSnapshot({
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
      replaceConnecticutCandidateFinanceSnapshot({
        db,
        link: {
          ...baseLink(),
          committeeId: " ",
        },
      })
    ).rejects.toThrow("Connecticut committee id is required");

    expect(db.query).not.toHaveBeenCalled();
  });

  it("rejects invalid election years", async () => {
    const db = createMockDb();

    await expect(
      upsertConnecticutFinanceLink({
        db,
        link: {
          ...baseLink(),
          electionYear: 2007,
        },
      })
    ).rejects.toThrow("Invalid Connecticut finance election year");

    expect(db.query).not.toHaveBeenCalled();
  });
});
