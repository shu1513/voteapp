import { describe, expect, it, vi } from "vitest";

import { replaceArizonaCandidateFinanceSnapshot } from "../../../src/pipeline/arizonaFinance/arizonaFinanceWriter.js";

describe("arizonaFinanceWriter", () => {
  it("writes a full Arizona finance snapshot with narrow categories", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: "link-1" }] })
      .mockResolvedValue({ rows: [] });

    const result = await replaceArizonaCandidateFinanceSnapshot({
      db: { query },
      syncedAt: new Date("2026-06-25T12:00:00.000Z"),
      link: {
        candidateId: "11111111-1111-4111-8111-111111111111",
        electionId: "22222222-2222-4222-8222-222222222222",
        electionYear: 2026,
        candidateNameNormalized: "JANE ARIZONAN",
        officeName: "Governor",
        committeeId: "AZ100",
        committeeName: "Jane Arizonan for Governor",
        linkSource: "spotlight",
      },
      summary: {
        totalReceipts: 1000,
        directContributionTotal: 1000,
        outsideSupportTotal: 500,
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 1000,
          contributorCount: 1,
        },
        {
          categoryType: "contribution_size",
          categoryName: "$1,000-$4,999",
          amount: 1000,
          contributorCount: 1,
        },
      ],
      outsideGroups: [
        {
          committeeId: "AZPAC1",
          committeeName: "Arizona Progress PAC",
          supportOppose: "support",
          amount: 500,
        },
      ],
      outsideGroupBreakdowns: [
        {
          committeeId: "AZPAC1",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Desert AI Labs LLC",
          amount: 500,
          contributorCount: 1,
        },
        {
          committeeId: "AZPAC1",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "technology",
          amount: 500,
          contributorCount: 1,
        },
      ],
    });

    expect(result).toEqual({
      linkId: "link-1",
      summaryWritten: true,
      directBreakdownsWritten: 2,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 2,
    });
    const sql = query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("INSERT INTO public.az_candidate_finance_links"))).toBe(true);
    expect(sql.some((statement) => statement.includes("INSERT INTO public.az_candidate_finance_summaries"))).toBe(true);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.az_candidate_finance_direct_breakdowns"))).toHaveLength(2);
    expect(sql.some((statement) => statement.includes("INSERT INTO public.az_candidate_finance_outside_groups"))).toBe(true);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.az_candidate_finance_outside_group_breakdowns"))).toHaveLength(2);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.az_candidate_finance_direct_breakdowns"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.az_candidate_finance_outside_groups"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.az_candidate_finance_outside_group_breakdowns"))).toBe(true);
  });

  it("requires outside groups when writing outside group breakdowns", async () => {
    await expect(
      replaceArizonaCandidateFinanceSnapshot({
        db: { query: vi.fn() },
        link: {
          candidateId: "11111111-1111-4111-8111-111111111111",
          electionId: "22222222-2222-4222-8222-222222222222",
          electionYear: 2026,
          candidateNameNormalized: "JANE ARIZONAN",
          officeName: "Governor",
          committeeId: "AZ100",
          committeeName: "Jane Arizonan for Governor",
        },
        outsideGroupBreakdowns: [
          {
            committeeId: "AZPAC1",
            supportOppose: "support",
            categoryType: "industry",
            categoryName: "technology",
            amount: 500,
          },
        ],
      })
    ).rejects.toThrow("outside group breakdowns require outside groups");
  });
});
