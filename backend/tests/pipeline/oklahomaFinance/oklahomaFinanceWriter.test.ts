import { describe, expect, it, vi } from "vitest";

import {
  replaceOklahomaCandidateFinanceSnapshot,
  upsertOklahomaFinanceLink,
} from "../../../src/pipeline/oklahomaFinance/oklahomaFinanceWriter.js";

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
    candidateNameNormalized: "BRENT DISHMAN",
    officeName: "State Senator",
    district: "47",
    committeeId: "11954",
    committeeName: "Dishman for Senate",
    linkSource: "guardian_bulk" as const,
    sourceUrl: "https://guardian.ok.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip",
    lastVerifiedAt: new Date("2026-01-01T00:00:00.000Z"),
  };
}

describe("oklahomaFinanceWriter", () => {
  it("upserts Oklahoma finance links and returns the link id", async () => {
    const db = createMockDb();

    await expect(upsertOklahomaFinanceLink({ db, link: baseLink() })).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.ok_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT (candidate_id, election_id, committee_id)");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("RETURNING id");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "BRENT DISHMAN",
      "State Senator",
      "47",
      "11954",
      "Dishman for Senate",
      "active",
      "guardian_bulk",
      "https://guardian.ok.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip",
      "2026-01-01T00:00:00.000Z",
    ]);
  });

  it("replaces an Oklahoma finance snapshot inside a transaction", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };
    const db = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    };

    const result = await replaceOklahomaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 6250,
        directContributionTotal: 1000,
        outsideSupportTotal: 300,
        outsideOpposeTotal: 50,
        sourceUrl: "https://guardian.ok.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip",
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 1000,
          contributorCount: 1,
          sourceUrl: "https://guardian.ok.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip",
        },
        {
          categoryType: "contribution_size",
          categoryName: "$1,000-$4,999",
          amount: 1000,
          contributorCount: 1,
          sourceUrl: "https://guardian.ok.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip",
        },
      ],
    });

    expect(result).toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 2,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
    });
    expect(db.connect).toHaveBeenCalledTimes(1);
    expect(db.query).not.toHaveBeenCalled();
    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
    expect(client.release).toHaveBeenCalledTimes(1);

    const sql = client.query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("INSERT INTO public.ok_candidate_finance_summaries"))).toBe(true);
    const summaryCall = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ok_candidate_finance_summaries")
    );
    expect(String(summaryCall?.[0])).toContain("direct_contribution_total");
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2026,
      6250,
      1000,
      300,
      50,
      "https://guardian.ok.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip",
      "2026-02-03T04:05:06.000Z",
    ]);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.ok_candidate_finance_direct_breakdowns"))).toHaveLength(2);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ok_candidate_finance_direct_breakdowns"))).toBe(true);
  });

  it("wraps a supplied queryable in a transaction", async () => {
    const db = createMockDb();

    const result = await replaceOklahomaCandidateFinanceSnapshot({
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

    const result = await replaceOklahomaCandidateFinanceSnapshot({
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
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
    });
    const sql = db.query.mock.calls.map((call) => String(call[0]));
    const summarySql = sql.find((statement) => statement.includes("INSERT INTO public.ok_candidate_finance_summaries"));
    expect(summarySql).toContain("total_receipts = COALESCE(EXCLUDED.total_receipts");
    expect(summarySql).toContain("direct_contribution_total = COALESCE(EXCLUDED.direct_contribution_total");
    expect(summarySql).toContain("outside_support_total = COALESCE(EXCLUDED.outside_support_total");
    expect(summarySql).toContain("outside_oppose_total = COALESCE(EXCLUDED.outside_oppose_total");
    expect(summarySql).toContain("source_url = COALESCE(EXCLUDED.source_url");
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ok_candidate_finance_direct_breakdowns"))).toBe(false);
  });

  it("writes outside groups and cleans stale outside groups only when provided", async () => {
    const db = createMockDb();
    const syncedAt = new Date("2026-02-03T04:05:06.000Z");

    const result = await replaceOklahomaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt,
      summary: {
        outsideSupportTotal: 0,
        outsideOpposeTotal: 61597.12,
        sourceUrl: "https://guardian.ok.gov/PublicSite/PublicReports/IndependentExpenditure.aspx",
      },
      outsideGroups: [
        {
          committeeId: "THE OKLAHOMA PROJECT",
          committeeName: "THE OKLAHOMA PROJECT",
          supportOppose: "oppose",
          amount: 61597.12,
          sourceUrl: "https://guardian.ok.gov/PublicSite/PublicReports/IndependentExpenditure.aspx",
        },
      ],
    });

    expect(result).toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 0,
    });

    const outsideGroupCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ok_candidate_finance_outside_groups")
    );
    expect(outsideGroupCall?.[1]).toEqual([
      LINK_ID,
      2026,
      "THE OKLAHOMA PROJECT",
      "THE OKLAHOMA PROJECT",
      "oppose",
      61597.12,
      "https://guardian.ok.gov/PublicSite/PublicReports/IndependentExpenditure.aspx",
      "2026-02-03T04:05:06.000Z",
    ]);

    const outsideDeleteCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("DELETE FROM public.ok_candidate_finance_outside_groups")
    );
    expect(String(outsideDeleteCall?.[0])).toContain("jsonb_to_recordset");
    expect(outsideDeleteCall?.[1]).toEqual([
      LINK_ID,
      2026,
      JSON.stringify([{ committee_id: "THE OKLAHOMA PROJECT", support_oppose: "oppose" }]),
    ]);
  });

  it("writes outside group breakdowns and finance label classifications", async () => {
    const db = createMockDb();

    const result = await replaceOklahomaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      outsideGroups: [
        {
          committeeId: "THE OKLAHOMA PROJECT",
          committeeName: "THE OKLAHOMA PROJECT",
          supportOppose: "oppose",
          amount: 61597.12,
        },
      ],
      outsideGroupBreakdowns: [
        {
          committeeId: "THE OKLAHOMA PROJECT",
          supportOppose: "oppose",
          categoryType: "donor",
          categoryName: "Energy Transfer",
          amount: 50000,
          contributorCount: 1,
        },
        {
          committeeId: "THE OKLAHOMA PROJECT",
          supportOppose: "oppose",
          categoryType: "industry",
          categoryName: "oil_gas_energy",
          amount: 50000,
          contributorCount: 1,
        },
      ],
      classifications: [
        {
          rawLabel: "Energy Transfer",
          labelType: "donor",
          normalizedLabel: "ENERGY TRANSFER",
          industrySlug: "oil_gas_energy",
          confidence: "high",
          classificationSource: "rule",
          matchedRule: "organization_exact_energy_transfer",
        },
      ],
    });

    expect(result).toEqual({
      linkId: LINK_ID,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 2,
    });

    expect(
      db.query.mock.calls.filter((call) =>
        String(call[0]).includes("INSERT INTO public.ok_candidate_finance_outside_group_breakdowns")
      )
    ).toHaveLength(2);
    expect(
      db.query.mock.calls.some((call) =>
        String(call[0]).includes("DELETE FROM public.ok_candidate_finance_outside_group_breakdowns")
      )
    ).toBe(true);
    expect(
      db.query.mock.calls.some((call) => String(call[0]).includes("INSERT INTO public.finance_label_classifications"))
    ).toBe(true);
  });

  it("uses current snapshot keys when cleaning repeated direct writes with the same timestamp", async () => {
    const db = createMockDb();
    const syncedAt = new Date("2026-02-03T04:05:06.000Z");

    await replaceOklahomaCandidateFinanceSnapshot({
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
    await replaceOklahomaCandidateFinanceSnapshot({
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
      String(call[0]).includes("DELETE FROM public.ok_candidate_finance_direct_breakdowns")
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
      replaceOklahomaCandidateFinanceSnapshot({
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
      replaceOklahomaCandidateFinanceSnapshot({
        db,
        link: {
          ...baseLink(),
          committeeId: " ",
        },
      })
    ).rejects.toThrow("Oklahoma committee id is required");

    expect(db.query).not.toHaveBeenCalled();
  });

  it("rejects invalid election years", async () => {
    const db = createMockDb();

    await expect(
      upsertOklahomaFinanceLink({
        db,
        link: {
          ...baseLink(),
          electionYear: 2013,
        },
      })
    ).rejects.toThrow("Invalid Oklahoma finance election year");

    expect(db.query).not.toHaveBeenCalled();
  });
});
