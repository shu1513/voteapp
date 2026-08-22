import { describe, expect, it, vi } from "vitest";

import {
  replaceNewHampshireCandidateFinanceSnapshot,
  upsertNewHampshireFinanceLink,
} from "../../../src/pipeline/newHampshireFinance/newHampshireFinanceWriter.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const LINK_ID = "33333333-3333-4333-8333-333333333333";
const SOURCE_URL = "https://cfs.sos.nh.gov/";
const VERIFIED_AT = new Date("2026-08-20T00:00:00.000Z");

function baseLink() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    electionYear: 2026,
    candidateNameNormalized: "JANE EXAMPLE",
    officeName: "State Senate",
    district: "District 1",
    filingEntityId: 12345,
    filerName: "Friends of Jane Example",
    linkSource: "cfs_registration" as const,
    sourceUrl: SOURCE_URL,
    lastVerifiedAt: VERIFIED_AT,
  };
}

function successfulQuery(sql: unknown) {
  if (String(sql).includes("INSERT INTO public.nh_candidate_finance_links")) {
    return Promise.resolve({ rows: [{ id: LINK_ID }], rowCount: 1 });
  }
  return Promise.resolve({ rows: [], rowCount: 0 });
}

describe("newHampshireFinanceWriter", () => {
  it("writes official filing-entity link identity and protects manual links", async () => {
    const db = { query: vi.fn(successfulQuery) };

    await expect(upsertNewHampshireFinanceLink({ db, link: baseLink() })).resolves.toEqual({
      linkId: LINK_ID,
    });

    expect(db.query).toHaveBeenCalledTimes(2);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("link_source='manual'");
    const insert = db.query.mock.calls[1];
    expect(String(insert?.[0])).toContain("filing_entity_id");
    expect(String(insert?.[0])).toContain("filer_name");
    expect(insert?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "JANE EXAMPLE",
      "State Senate",
      "District 1",
      "12345",
      "Friends of Jane Example",
      "active",
      "cfs_registration",
      SOURCE_URL,
      VERIFIED_AT.toISOString(),
    ]);
  });

  it("replaces the available NH snapshot transactionally", async () => {
    const client = { query: vi.fn(successfulQuery), release: vi.fn() };
    const db = { query: vi.fn(), connect: vi.fn().mockResolvedValue(client) };

    await expect(
      replaceNewHampshireCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        syncedAt: new Date("2026-08-21T00:00:00.000Z"),
        summary: {
          totalReceipts: 12_500,
          directContributionTotal: 10_000,
          outsideSupportTotal: 4_000,
          outsideOpposeTotal: 1_500,
          sourceUrl: SOURCE_URL,
        },
        directBreakdowns: [
          { categoryType: "industry", categoryName: "health-care", amount: 6_000 },
          { categoryType: "contribution_size", categoryName: "$100-$249", amount: 4_000 },
        ],
        outsideGroups: [
          {
            filingEntityId: 98765,
            filerName: "New Hampshire Example PAC",
            supportOppose: "oppose",
            amount: 1_500,
          },
        ],
      })
    ).resolves.toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 2,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 0,
    });

    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
    expect(client.release).toHaveBeenCalledOnce();

    const calls = client.query.mock.calls;
    const summarySql = String(
      calls.find((call) => String(call[0]).includes("nh_candidate_finance_summaries"))?.[0]
    );
    expect(summarySql).toContain(
      "total_receipts = COALESCE(EXCLUDED.total_receipts, nh_candidate_finance_summaries.total_receipts)"
    );
    expect(summarySql).toContain(
      "outside_support_total = COALESCE(EXCLUDED.outside_support_total, nh_candidate_finance_summaries.outside_support_total)"
    );
    expect(summarySql).toContain(
      "total_disbursements = COALESCE(EXCLUDED.total_disbursements"
    );

    const directInserts = calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.nh_candidate_finance_direct_breakdowns")
    );
    expect(directInserts.map((call) => call[1]?.[2])).toEqual(["industry", "contribution_size"]);

    const outsideInsert = calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.nh_candidate_finance_outside_groups")
    );
    expect(String(outsideInsert?.[0])).toContain("filing_entity_id");
    expect(String(outsideInsert?.[0])).toContain("filer_name");
    expect(outsideInsert?.[1]?.[2]).toBe("98765");

    const supersede = calls.find(
      (call) =>
        String(call[0]).includes("UPDATE public.nh_candidate_finance_links") &&
        String(call[0]).includes("id <>")
    );
    expect(String(supersede?.[0])).toContain("link_source = 'cfs_registration'");
  });

  it("preserves stored totals and detail rows when source sections are unavailable", async () => {
    const client = { query: vi.fn(successfulQuery), release: vi.fn() };
    const db = { query: vi.fn(), connect: vi.fn().mockResolvedValue(client) };

    await replaceNewHampshireCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      summary: {
        totalReceipts: null,
        directContributionTotal: null,
        outsideSupportTotal: null,
        outsideOpposeTotal: null,
        sourceUrl: null,
      },
    });

    const calls = client.query.mock.calls;
    const summarySql = String(
      calls.find((call) => String(call[0]).includes("nh_candidate_finance_summaries"))?.[0]
    );
    expect(summarySql).toContain(
      "direct_contribution_total = COALESCE(EXCLUDED.direct_contribution_total"
    );
    expect(summarySql).toContain(
      "outside_oppose_total = COALESCE(EXCLUDED.outside_oppose_total"
    );
    expect(
      calls.some((call) => String(call[0]).includes("DELETE FROM public.nh_candidate_finance_"))
    ).toBe(false);
  });

  it("writes zeros and clears stale detail rows after successful empty fetches", async () => {
    const client = { query: vi.fn(successfulQuery), release: vi.fn() };
    const db = { query: vi.fn(), connect: vi.fn().mockResolvedValue(client) };

    await replaceNewHampshireCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      summary: {
        totalReceipts: 0,
        directContributionTotal: 0,
        outsideSupportTotal: 0,
        outsideOpposeTotal: 0,
        sourceUrl: SOURCE_URL,
      },
      directBreakdowns: [],
      outsideGroups: [],
    });

    const calls = client.query.mock.calls;
    const summary = calls.find((call) => String(call[0]).includes("nh_candidate_finance_summaries"));
    expect(summary?.[1]?.slice(2, 9)).toEqual([0, 0, null, null, 0, 0, SOURCE_URL]);
    expect(
      calls.some((call) =>
        String(call[0]).includes("DELETE FROM public.nh_candidate_finance_direct_breakdowns")
      )
    ).toBe(true);
    expect(
      calls.some((call) =>
        String(call[0]).includes("DELETE FROM public.nh_candidate_finance_outside_groups")
      )
    ).toBe(true);
  });

  it("rejects occupation data before opening a transaction", async () => {
    const db = { query: vi.fn(), connect: vi.fn() };

    await expect(
      replaceNewHampshireCandidateFinanceSnapshot({
        db: db as never,
        link: baseLink(),
        directBreakdowns: [
          {
            categoryType: "occupation" as never,
            categoryName: "Attorney",
            amount: 100,
          },
        ],
      })
    ).rejects.toThrow(
      "New Hampshire finance direct breakdown category type is not allowed: occupation"
    );
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("rejects invalid filing entity IDs before database writes", async () => {
    const db = { query: vi.fn(), connect: vi.fn() };

    await expect(
      replaceNewHampshireCandidateFinanceSnapshot({
        db: db as never,
        link: { ...baseLink(), filingEntityId: 0 },
      })
    ).rejects.toThrow("Invalid New Hampshire filing entity ID: 0");
    await expect(
      replaceNewHampshireCandidateFinanceSnapshot({
        db: db as never,
        link: baseLink(),
        outsideGroups: [
          {
            filingEntityId: 1.5,
            filerName: "Example PAC",
            supportOppose: "support",
            amount: 100,
          },
        ],
      })
    ).rejects.toThrow("Invalid New Hampshire filing entity ID: 1.5");
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("rolls back and releases the client when a snapshot query fails", async () => {
    const client = {
      query: vi.fn((sql: unknown) => {
        if (String(sql).includes("nh_candidate_finance_summaries")) {
          return Promise.reject(new Error("summary failed"));
        }
        return successfulQuery(sql);
      }),
      release: vi.fn(),
    };
    const db = { query: vi.fn(), connect: vi.fn().mockResolvedValue(client) };

    await expect(
      replaceNewHampshireCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        summary: {
          totalReceipts: 1,
          directContributionTotal: 1,
          outsideSupportTotal: 0,
          outsideOpposeTotal: 0,
          sourceUrl: SOURCE_URL,
        },
      })
    ).rejects.toThrow("summary failed");

    expect(client.query.mock.calls.at(-1)?.[0]).toBe("ROLLBACK");
    expect(client.query.mock.calls.some((call) => call[0] === "COMMIT")).toBe(false);
    expect(client.release).toHaveBeenCalledOnce();
  });
});
