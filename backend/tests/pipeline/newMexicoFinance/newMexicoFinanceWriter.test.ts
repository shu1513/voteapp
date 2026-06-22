import { describe, expect, it, vi } from "vitest";

import {
  replaceNewMexicoCandidateFinanceSnapshot,
  upsertNewMexicoFinanceLink,
} from "../../../src/pipeline/newMexicoFinance/newMexicoFinanceWriter.js";

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
    candidateNameNormalized: "DEB HAALAND",
    officeName: "Governor",
    committeeId: "1001",
    committeeName: "Haaland for New Mexico",
    linkSource: "cfis_bulk" as const,
    sourceUrl: "https://login.cfis.sos.state.nm.us/",
    lastVerifiedAt: new Date("2026-01-01T00:00:00.000Z"),
  };
}

describe("newMexicoFinanceWriter", () => {
  it("upserts New Mexico finance links and returns the link id", async () => {
    const db = createMockDb();

    await expect(upsertNewMexicoFinanceLink({ db, link: baseLink() })).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.nm_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT (candidate_id, election_id, committee_id)");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("RETURNING id");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "DEB HAALAND",
      "Governor",
      null,
      "1001",
      "Haaland for New Mexico",
      "active",
      "cfis_bulk",
      "https://login.cfis.sos.state.nm.us/",
      "2026-01-01T00:00:00.000Z",
    ]);
  });

  it("replaces a full New Mexico finance snapshot inside a transaction", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };
    const db = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    };

    const result = await replaceNewMexicoCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 100000.5,
        directContributionTotal: 80000.25,
        outsideSupportTotal: 12000,
        outsideOpposeTotal: 3000,
        sourceUrl: "https://login.cfis.sos.state.nm.us/api/DataDownload/GetCSVDownloadReport",
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 700,
          contributorCount: 3,
          sourceUrl: "https://login.cfis.sos.state.nm.us/api/DataDownload/GetCSVDownloadReport",
        },
        {
          categoryType: "contributor_source_type",
          categoryName: "individuals",
          amount: 800,
          contributorCount: 2,
        },
      ],
      outsideGroups: [
        {
          committeeId: "9001",
          committeeName: "Accountable New Mexico",
          supportOppose: "support",
          amount: 12000,
          sourceUrl: "https://login.cfis.sos.state.nm.us/api/DataDownload/GetCSVDownloadReport",
        },
      ],
      outsideGroupBreakdowns: [
        {
          committeeId: "9001",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "finance_investment",
          amount: 12000,
          contributorCount: 1,
          sourceUrl: "https://login.cfis.sos.state.nm.us/api/DataDownload/GetCSVDownloadReport",
        },
      ],
    });

    expect(result).toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 2,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 1,
    });
    expect(db.connect).toHaveBeenCalledTimes(1);
    expect(db.query).not.toHaveBeenCalled();
    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
    expect(client.release).toHaveBeenCalledTimes(1);

    const sql = client.query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("INSERT INTO public.nm_candidate_finance_summaries"))).toBe(true);
    const summaryCall = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.nm_candidate_finance_summaries")
    );
    expect(String(summaryCall?.[0])).toContain("direct_contribution_total");
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2026,
      100000.5,
      80000.25,
      null,
      12000,
      3000,
      "https://login.cfis.sos.state.nm.us/api/DataDownload/GetCSVDownloadReport",
      "2026-02-03T04:05:06.000Z",
    ]);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.nm_candidate_finance_direct_breakdowns"))).toHaveLength(2);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.nm_candidate_finance_outside_groups"))).toHaveLength(1);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.nm_candidate_finance_outside_group_breakdowns"))).toHaveLength(1);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.nm_candidate_finance_direct_breakdowns"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.nm_candidate_finance_outside_groups"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.nm_candidate_finance_outside_group_breakdowns"))).toBe(true);
  });

  it("wraps a supplied queryable in a transaction", async () => {
    const db = createMockDb();

    const result = await replaceNewMexicoCandidateFinanceSnapshot({
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

  it("does not delete omitted direct or outside sections", async () => {
    const db = createMockDb();

    const result = await replaceNewMexicoCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: {
        outsideSupportTotal: 1000,
      },
    });

    expect(result).toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
    });
    const sql = db.query.mock.calls.map((call) => String(call[0]));
    const summarySql = sql.find((statement) => statement.includes("INSERT INTO public.nm_candidate_finance_summaries"));
    expect(summarySql).toContain(
      "total_receipts = COALESCE(EXCLUDED.total_receipts, nm_candidate_finance_summaries.total_receipts)"
    );
    expect(summarySql).toContain(
      "outside_support_total = COALESCE(EXCLUDED.outside_support_total, nm_candidate_finance_summaries.outside_support_total)"
    );
    expect(sql.some((statement) => statement.includes("DELETE FROM public.nm_candidate_finance_direct_breakdowns"))).toBe(false);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.nm_candidate_finance_outside_groups"))).toBe(false);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.nm_candidate_finance_outside_group_breakdowns"))).toBe(
      false
    );
  });

  it("preserves outside group breakdowns when only outside groups are refreshed", async () => {
    const db = createMockDb();

    const result = await replaceNewMexicoCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      outsideGroups: [
        {
          committeeId: "9001",
          committeeName: "Accountable New Mexico",
          supportOppose: "support",
          amount: 500,
        },
      ],
    });

    expect(result.outsideGroupsWritten).toBe(1);
    expect(result.outsideGroupBreakdownsWritten).toBe(0);
    const sql = db.query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("DELETE FROM public.nm_candidate_finance_outside_groups"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.nm_candidate_finance_outside_group_breakdowns"))).toBe(
      false
    );
  });

  it("uses current snapshot keys when cleaning repeated direct writes with the same timestamp", async () => {
    const db = createMockDb();
    const syncedAt = new Date("2026-02-03T04:05:06.000Z");

    await replaceNewMexicoCandidateFinanceSnapshot({
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
    await replaceNewMexicoCandidateFinanceSnapshot({
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
      String(call[0]).includes("DELETE FROM public.nm_candidate_finance_direct_breakdowns")
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
      replaceNewMexicoCandidateFinanceSnapshot({
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
      replaceNewMexicoCandidateFinanceSnapshot({
        db,
        link: {
          ...baseLink(),
          committeeId: " ",
        },
      })
    ).rejects.toThrow("New Mexico committee id is required");

    expect(db.query).not.toHaveBeenCalled();
  });

  it("rejects invalid election years", async () => {
    const db = createMockDb();

    await expect(
      upsertNewMexicoFinanceLink({
        db,
        link: {
          ...baseLink(),
          electionYear: 2019,
        },
      })
    ).rejects.toThrow("Invalid New Mexico finance election year");

    expect(db.query).not.toHaveBeenCalled();
  });
});
