import { describe, expect, it, vi } from "vitest";

import {
  replaceNorthCarolinaCandidateFinanceSnapshot,
  upsertNorthCarolinaFinanceLink,
} from "../../../src/pipeline/northCarolinaFinance/northCarolinaFinanceWriter.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";
const SOURCE_URL = "https://cf.ncsbe.gov/CFOrgLkup/ReportDetail/?RID=229931&TP=ALL";

function createMockDb() {
  return {
    query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
  };
}

function createMockPool() {
  const client = {
    query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
    release: vi.fn(),
  };
  return {
    client,
    db: {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    },
  };
}

function baseLink() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    electionYear: 2026,
    candidateNameNormalized: "JANE EXAMPLE",
    officeName: "State Senator",
    committeeId: "STA-JV516O-C-001",
    committeeName: "COMMITTEE TO ELECT JANE EXAMPLE",
    linkSource: "ncsbe_portal" as const,
    sourceUrl: SOURCE_URL,
    lastVerifiedAt: new Date("2026-01-01T00:00:00.000Z"),
  };
}

describe("northCarolinaFinanceWriter", () => {
  it("upserts North Carolina finance links and returns the link id", async () => {
    const db = createMockDb();

    await expect(upsertNorthCarolinaFinanceLink({ db, link: baseLink() })).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.nc_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT (candidate_id, election_id, committee_id)");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("RETURNING id");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "JANE EXAMPLE",
      "State Senator",
      null,
      "STA-JV516O-C-001",
      "COMMITTEE TO ELECT JANE EXAMPLE",
      "active",
      "ncsbe_portal",
      SOURCE_URL,
      "2026-01-01T00:00:00.000Z",
    ]);
  });

  it("upper-cases committee ids everywhere they are written", async () => {
    const db = createMockDb();

    await upsertNorthCarolinaFinanceLink({
      db,
      link: { ...baseLink(), committeeId: " sta-jv516o-c-001 " },
    });

    expect(db.query.mock.calls[0]?.[1]).toContain("STA-JV516O-C-001");

    const { db: pool, client } = createMockPool();
    await replaceNorthCarolinaCandidateFinanceSnapshot({
      db: pool,
      link: baseLink(),
      outsideGroups: [
        {
          committeeId: "nc-ogid:57190",
          committeeName: "Advance NC",
          supportOppose: "support",
          amount: 1000,
        },
      ],
      outsideGroupBreakdowns: [
        {
          committeeId: "NC-OGID:57190",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Rolling Sea Fund",
          amount: 1000,
        },
      ],
    });

    const groupCall = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.nc_candidate_finance_outside_groups")
    );
    expect(groupCall?.[1]).toContain("NC-OGID:57190");
  });

  it("replaces a North Carolina finance snapshot inside a transaction", async () => {
    const { db, client } = createMockPool();

    const result = await replaceNorthCarolinaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 6073.24,
        directContributionTotal: 5800,
        totalDisbursements: 2500,
        cashOnHand: 3573.24,
        outsideSupportTotal: 29306.3,
        outsideOpposeTotal: 1200.5,
        sourceUrl: SOURCE_URL,
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 5000,
          contributorCount: 4,
          sourceUrl: SOURCE_URL,
        },
        {
          categoryType: "contribution_size",
          categoryName: "$250-$499",
          amount: 750,
          contributorCount: 3,
          sourceUrl: SOURCE_URL,
        },
      ],
      outsideGroups: [
        {
          committeeId: "STA-C4368N-C-002",
          committeeName: "Advance Carolina Example PAC",
          supportOppose: "support",
          amount: 29306.3,
          sourceUrl: SOURCE_URL,
        },
        {
          committeeId: "NC-OGID:57190",
          committeeName: "Carolina Accountability Fund",
          supportOppose: "oppose",
          amount: 1200.5,
          sourceUrl: SOURCE_URL,
        },
      ],
      outsideGroupBreakdowns: [
        {
          committeeId: "STA-C4368N-C-002",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Rolling Sea Fund",
          amount: 24506,
          contributorCount: 1,
          sourceUrl: SOURCE_URL,
        },
      ],
    });

    expect(result).toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 2,
      outsideGroupsWritten: 2,
      outsideGroupBreakdownsWritten: 1,
    });
    expect(db.connect).toHaveBeenCalledTimes(1);
    expect(db.query).not.toHaveBeenCalled();
    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
    expect(client.release).toHaveBeenCalledTimes(1);

    const sql = client.query.mock.calls.map((call) => String(call[0]));
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.nc_candidate_finance_summaries"))).toHaveLength(1);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.nc_candidate_finance_direct_breakdowns"))).toHaveLength(2);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.nc_candidate_finance_outside_groups"))).toHaveLength(2);
    expect(
      sql.filter((statement) => statement.includes("INSERT INTO public.nc_candidate_finance_outside_group_breakdowns"))
    ).toHaveLength(1);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.nc_candidate_finance_direct_breakdowns"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.nc_candidate_finance_outside_groups"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.nc_candidate_finance_outside_group_breakdowns"))).toBe(
      true
    );

    const summaryCall = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.nc_candidate_finance_summaries")
    );
    expect(String(summaryCall?.[0])).toContain("total_receipts = EXCLUDED.total_receipts");
    expect(String(summaryCall?.[0])).toContain("cash_on_hand = EXCLUDED.cash_on_hand");
    expect(String(summaryCall?.[0])).toContain("outside_support_total = COALESCE(EXCLUDED.outside_support_total");
    expect(String(summaryCall?.[0])).toContain("outside_oppose_total = COALESCE(EXCLUDED.outside_oppose_total");
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2026,
      6073.24,
      5800,
      2500,
      3573.24,
      29306.3,
      1200.5,
      SOURCE_URL,
      "2026-02-03T04:05:06.000Z",
    ]);
  });

  it("deactivates other active portal links for the same candidate election", async () => {
    const { db, client } = createMockPool();

    await replaceNorthCarolinaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: { totalReceipts: 1000 },
    });

    const deactivationCall = client.query.mock.calls.find((call) =>
      String(call[0]).includes("UPDATE public.nc_candidate_finance_links")
    );
    expect(String(deactivationCall?.[0])).toContain("link_source = 'ncsbe_portal'");
    expect(deactivationCall?.[1]).toEqual([CANDIDATE_ID, ELECTION_ID, LINK_ID]);
  });

  it("does not deactivate portal links when the incoming link is manual", async () => {
    const { db, client } = createMockPool();

    await replaceNorthCarolinaCandidateFinanceSnapshot({
      db,
      link: { ...baseLink(), linkSource: "manual" as const },
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: { totalReceipts: 1000 },
    });

    const sql = client.query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("UPDATE public.nc_candidate_finance_links"))).toBe(false);
  });

  it("preserves prior outside totals when a direct-only refresh has no IE data", async () => {
    const { db, client } = createMockPool();

    await replaceNorthCarolinaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 6073.24,
        directContributionTotal: 5800,
        outsideSupportTotal: null,
        outsideOpposeTotal: null,
      },
    });

    const summaryCall = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.nc_candidate_finance_summaries")
    );
    expect(String(summaryCall?.[0])).toContain(
      "outside_support_total = COALESCE(EXCLUDED.outside_support_total, nc_candidate_finance_summaries.outside_support_total)"
    );
    expect(String(summaryCall?.[0])).toContain("total_disbursements = EXCLUDED.total_disbursements");
  });

  it("requires each outside group breakdown to pair with an outside group in the snapshot", async () => {
    const { db, client } = createMockPool();

    await expect(
      replaceNorthCarolinaCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        outsideGroups: [
          {
            committeeId: "STA-C4368N-C-002",
            committeeName: "Advance Carolina Example PAC",
            supportOppose: "support",
            amount: 1000,
          },
        ],
        outsideGroupBreakdowns: [
          {
            committeeId: "STA-C4368N-C-002",
            supportOppose: "oppose",
            categoryType: "donor",
            categoryName: "Rolling Sea Fund",
            amount: 1000,
          },
        ],
      })
    ).rejects.toThrow(/outside group/i);
    expect(client.query).not.toHaveBeenCalled();
  });

  it("rejects empty committee ids before writing", async () => {
    const db = createMockDb();

    await expect(
      upsertNorthCarolinaFinanceLink({
        db,
        link: { ...baseLink(), committeeId: "   " },
      })
    ).rejects.toThrow(/committee id/i);

    expect(db.query).not.toHaveBeenCalled();
  });

  it("rejects a supplied PoolClient so it cannot commit an outer transaction", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };

    await expect(
      replaceNorthCarolinaCandidateFinanceSnapshot({
        db: client as never,
        link: baseLink(),
        summary: { totalReceipts: 1000 },
      })
    ).rejects.toThrow("North Carolina finance snapshot writes must receive a Pool, not a PoolClient");
    expect(client.query).not.toHaveBeenCalled();
    expect(client.release).not.toHaveBeenCalled();
  });

  it("rolls back and releases the client when a transactional write fails", async () => {
    const client = {
      query: vi
        .fn()
        .mockResolvedValueOnce({ rows: [], rowCount: 0 })
        .mockRejectedValueOnce(new Error("write failed")),
      release: vi.fn(),
    };
    const db = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    };

    await expect(
      replaceNorthCarolinaCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        summary: { totalReceipts: 1000 },
      })
    ).rejects.toThrow("write failed");

    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("ROLLBACK");
    expect(client.release).toHaveBeenCalledTimes(1);
  });

  it("rejects election years below the North Carolina floor", async () => {
    const db = createMockDb();

    await expect(
      upsertNorthCarolinaFinanceLink({
        db,
        link: { ...baseLink(), electionYear: 1999 },
      })
    ).rejects.toThrow("Invalid North Carolina finance election year");

    expect(db.query).not.toHaveBeenCalled();
  });
});
