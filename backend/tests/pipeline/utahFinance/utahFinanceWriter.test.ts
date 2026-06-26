import { describe, expect, it, vi } from "vitest";

import {
  replaceUtahCandidateFinanceSnapshot,
  upsertUtahFinanceLink,
} from "../../../src/pipeline/utahFinance/utahFinanceWriter.js";

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
    electionYear: 2024,
    candidateNameNormalized: "JANE DOE",
    officeName: "Governor",
    district: null,
    folderId: "98765",
    committeeName: "Friends of Jane Doe",
    linkSource: "disclosures_advanced_search" as const,
    sourceUrl: "https://disclosures.utah.gov/Search/AdvancedSearch/FolderDetails/98765",
    lastVerifiedAt: new Date("2026-01-01T00:00:00.000Z"),
  };
}

describe("utahFinanceWriter", () => {
  it("upserts Utah finance links and returns the link id", async () => {
    const db = createMockDb();

    await expect(upsertUtahFinanceLink({ db, link: baseLink() })).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.ut_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT (candidate_id, election_id, folder_id)");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("RETURNING id");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2024,
      "JANE DOE",
      "Governor",
      null,
      "98765",
      "Friends of Jane Doe",
      "active",
      "disclosures_advanced_search",
      "https://disclosures.utah.gov/Search/AdvancedSearch/FolderDetails/98765",
      "2026-01-01T00:00:00.000Z",
    ]);
  });

  it("replaces a Utah finance snapshot inside a transaction", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };
    const db = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    };

    const result = await replaceUtahCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 1000,
        directContributionTotal: 750,
        totalDisbursements: 125,
        cashOnHand: 400,
        sourceUrl: "https://disclosures.utah.gov/Search/AdvancedSearch/GenerateReport/98765?ReportYear=2024",
      },
      directBreakdowns: [
        {
          categoryType: "contribution_size",
          categoryName: "$500-$999",
          amount: 750,
          contributorCount: 3,
          sourceUrl: "https://disclosures.utah.gov/Search/AdvancedSearch/GenerateReport/98765?ReportYear=2024",
        },
      ],
    });

    expect(result).toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 1,
      supportingCommitteesWritten: 0,
      supportingCommitteeIndustriesWritten: 0,
    });
    expect(db.connect).toHaveBeenCalledTimes(1);
    expect(db.query).not.toHaveBeenCalled();
    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
    expect(client.release).toHaveBeenCalledTimes(1);

    const sql = client.query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("INSERT INTO public.ut_candidate_finance_summaries"))).toBe(true);
    const summaryCall = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ut_candidate_finance_summaries")
    );
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2024,
      1000,
      750,
      125,
      400,
      "https://disclosures.utah.gov/Search/AdvancedSearch/GenerateReport/98765?ReportYear=2024",
      "2026-02-03T04:05:06.000Z",
    ]);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.ut_candidate_finance_direct_breakdowns"))).toHaveLength(1);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ut_candidate_finance_direct_breakdowns"))).toBe(true);
  });

  it("wraps a supplied queryable in a transaction", async () => {
    const db = createMockDb();

    const result = await replaceUtahCandidateFinanceSnapshot({
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

    const result = await replaceUtahCandidateFinanceSnapshot({
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
      supportingCommitteesWritten: 0,
      supportingCommitteeIndustriesWritten: 0,
    });
    const sql = db.query.mock.calls.map((call) => String(call[0]));
    const summarySql = sql.find((statement) => statement.includes("INSERT INTO public.ut_candidate_finance_summaries"));
    expect(summarySql).toContain(
      "total_receipts = COALESCE(EXCLUDED.total_receipts, ut_candidate_finance_summaries.total_receipts)"
    );
    expect(summarySql).toContain(
      "cash_on_hand = COALESCE(EXCLUDED.cash_on_hand, ut_candidate_finance_summaries.cash_on_hand)"
    );
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ut_candidate_finance_direct_breakdowns"))).toBe(false);
  });

  it("uses current snapshot keys when cleaning repeated direct writes with the same timestamp", async () => {
    const db = createMockDb();
    const syncedAt = new Date("2026-02-03T04:05:06.000Z");

    await replaceUtahCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt,
      directBreakdowns: [
        {
          categoryType: "contribution_size",
          categoryName: "$100-$249",
          amount: 200,
        },
      ],
    });
    await replaceUtahCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt,
      directBreakdowns: [
        {
          categoryType: "contribution_size",
          categoryName: "$250-$499",
          amount: 300,
        },
      ],
    });

    const directDeleteCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("DELETE FROM public.ut_candidate_finance_direct_breakdowns")
    );
    const lastDelete = directDeleteCalls.at(-1);
    expect(String(lastDelete?.[0])).toContain("jsonb_to_recordset");
    expect(String(lastDelete?.[0])).not.toContain("last_synced_at <");
    expect(lastDelete?.[1]).toEqual([
      LINK_ID,
      2024,
      JSON.stringify([{ category_type: "contribution_size", category_name: "$250-$499" }]),
    ]);
  });

  it("replaces Utah supporting committee and industry rows", async () => {
    const db = createMockDb();

    const result = await replaceUtahCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      supportingCommittees: [
        {
          committeeName: "Utah Builders PAC",
          amount: 3000,
          contributorCount: 2,
          sourceUrl: "https://disclosures.utah.gov/Search/AdvancedSearch/GenerateReport/98765?ReportYear=2024",
        },
      ],
      supportingCommitteeIndustries: [
        {
          supportingCommitteeName: "Utah Builders PAC",
          industrySlug: "construction",
          amount: 25000,
          contributorCount: 2,
          sourceUrl: "https://disclosures.utah.gov/Search/AdvancedSearch/GenerateReport?ReportYear=2024&EntityType=PAC",
        },
      ],
    });

    expect(result).toEqual({
      linkId: LINK_ID,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      supportingCommitteesWritten: 1,
      supportingCommitteeIndustriesWritten: 1,
    });
    const supportingCommitteeCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ut_candidate_finance_supporting_committees")
    );
    expect(supportingCommitteeCall?.[1]).toEqual([
      LINK_ID,
      2024,
      "Utah Builders PAC",
      3000,
      2,
      "https://disclosures.utah.gov/Search/AdvancedSearch/GenerateReport/98765?ReportYear=2024",
      "2026-02-03T04:05:06.000Z",
    ]);
    const industryCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ut_candidate_finance_supporting_committee_industries")
    );
    expect(industryCall?.[1]).toEqual([
      LINK_ID,
      2024,
      "Utah Builders PAC",
      "construction",
      25000,
      2,
      "https://disclosures.utah.gov/Search/AdvancedSearch/GenerateReport?ReportYear=2024&EntityType=PAC",
      "2026-02-03T04:05:06.000Z",
    ]);
    expect(
      db.query.mock.calls.some((call) => String(call[0]).includes("DELETE FROM public.ut_candidate_finance_supporting_committees"))
    ).toBe(true);
    expect(
      db.query.mock.calls.some((call) =>
        String(call[0]).includes("DELETE FROM public.ut_candidate_finance_supporting_committee_industries")
      )
    ).toBe(true);
  });

  it("rejects supporting committee industries without matching supporting committees", async () => {
    const db = createMockDb();

    await expect(
      replaceUtahCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        supportingCommitteeIndustries: [
          {
            supportingCommitteeName: "Utah Builders PAC",
            industrySlug: "construction",
            amount: 25000,
          },
        ],
      })
    ).rejects.toThrow("matching supporting committees");

    expect(db.query).not.toHaveBeenCalled();
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
      replaceUtahCandidateFinanceSnapshot({
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
      replaceUtahCandidateFinanceSnapshot({
        db,
        link: {
          ...baseLink(),
          folderId: " ",
        },
      })
    ).rejects.toThrow("Utah disclosures folder id is required");

    expect(db.query).not.toHaveBeenCalled();
  });

  it("rejects invalid election years", async () => {
    const db = createMockDb();

    await expect(
      upsertUtahFinanceLink({
        db,
        link: {
          ...baseLink(),
          electionYear: 1997,
        },
      })
    ).rejects.toThrow("Invalid Utah finance election year");

    expect(db.query).not.toHaveBeenCalled();
  });
});
