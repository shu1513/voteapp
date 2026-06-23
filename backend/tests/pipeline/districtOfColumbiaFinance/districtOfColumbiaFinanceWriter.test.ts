import { describe, expect, it, vi } from "vitest";

import {
  replaceDistrictOfColumbiaCandidateFinanceSnapshot,
  upsertDistrictOfColumbiaFinanceLink,
} from "../../../src/pipeline/districtOfColumbiaFinance/districtOfColumbiaFinanceWriter.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const LINK_ID = "33333333-3333-4333-8333-333333333333";

function baseLink() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    electionYear: 2026,
    candidateNameNormalized: "JANE DOE",
    officeName: "Mayor",
    district: " ",
    committeeKey: "COMMITTEE TO ELECT JANE DOE",
    committeeName: "Committee To Elect Jane Doe",
    linkStatus: "active" as const,
    linkSource: "ocf_export" as const,
    sourceUrl: "https://efiling.ocf.dc.gov/DataDownload",
    lastVerifiedAt: new Date("2026-06-01T00:00:00.000Z"),
  };
}

function createMockDb() {
  const query = vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 });
  const client = {
    query,
    release: vi.fn(),
  };
  return {
    query,
    connect: vi.fn().mockResolvedValue(client),
    client,
  };
}

describe("districtOfColumbiaFinanceWriter", () => {
  it("upserts D.C. finance links with normalized nullable fields", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
    };

    await expect(upsertDistrictOfColumbiaFinanceLink({ db, link: baseLink() })).resolves.toEqual({ linkId: LINK_ID });

    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.dc_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT (candidate_id, election_id, committee_key)");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "JANE DOE",
      "Mayor",
      null,
      "COMMITTEE TO ELECT JANE DOE",
      "Committee To Elect Jane Doe",
      "active",
      "ocf_export",
      "https://efiling.ocf.dc.gov/DataDownload",
      "2026-06-01T00:00:00.000Z",
    ]);
  });

  it("replaces a D.C. finance snapshot inside a transaction", async () => {
    const db = createMockDb();

    const result = await replaceDistrictOfColumbiaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-07-08T09:10:11.000Z"),
      summary: {
        totalReceipts: 120000,
        directContributionTotal: 120000,
        outsideSupportTotal: 35000,
        outsideOpposeTotal: 5000,
        sourceUrl: "https://efiling.ocf.dc.gov/DataDownload/Export?exportType=CSV",
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 90000,
          contributorCount: 12,
          sourceUrl: "https://efiling.ocf.dc.gov/DataDownload/Export?exportType=CSV",
        },
      ],
      outsideGroups: [
        {
          committeeKey: "DCCSA IEC",
          committeeName: "DCCSA IEC",
          supportOppose: "support",
          amount: 35000,
          sourceUrl: "https://efiling.ocf.dc.gov/DataDownload/Export?exportType=CSV",
        },
      ],
      outsideGroupBreakdowns: [
        {
          committeeKey: "DCCSA IEC",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "construction",
          amount: 35000,
          contributorCount: 1,
          sourceUrl: "https://efiling.ocf.dc.gov/DataDownload/Export?exportType=CSV",
        },
      ],
    });

    expect(result).toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 1,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 1,
    });
    expect(db.connect).toHaveBeenCalledTimes(1);
    expect(db.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(db.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
    expect(db.client.release).toHaveBeenCalledTimes(1);

    const sql = db.query.mock.calls.map((call) => String(call[0]));
    const summaryCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.dc_candidate_finance_summaries")
    );
    expect(String(summaryCall?.[0])).toContain(
      "total_receipts = COALESCE(EXCLUDED.total_receipts, dc_candidate_finance_summaries.total_receipts)"
    );
    expect(String(summaryCall?.[0])).toContain("outside_support_total = EXCLUDED.outside_support_total");
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2026,
      120000,
      120000,
      null,
      null,
      35000,
      5000,
      "https://efiling.ocf.dc.gov/DataDownload/Export?exportType=CSV",
      "2026-07-08T09:10:11.000Z",
    ]);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.dc_candidate_finance_direct_breakdowns"))).toHaveLength(1);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.dc_candidate_finance_outside_groups"))).toHaveLength(1);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.dc_candidate_finance_outside_group_breakdowns"))).toHaveLength(1);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.dc_candidate_finance_direct_breakdowns"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.dc_candidate_finance_outside_groups"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.dc_candidate_finance_outside_group_breakdowns"))).toBe(true);
  });

  it("does not delete omitted breakdown sections", async () => {
    const db = createMockDb();

    const result = await replaceDistrictOfColumbiaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-07-08T09:10:11.000Z"),
      summary: {
        outsideSupportTotal: 0,
        outsideOpposeTotal: 0,
      },
    });

    expect(result).toMatchObject({
      summaryWritten: true,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
    });
    const sql = db.query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("DELETE FROM public.dc_candidate_finance_direct_breakdowns"))).toBe(false);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.dc_candidate_finance_outside_groups"))).toBe(false);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.dc_candidate_finance_outside_group_breakdowns"))).toBe(false);
  });

  it("rejects outside group breakdowns without matching outside groups", async () => {
    const db = createMockDb();

    await expect(
      replaceDistrictOfColumbiaCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        outsideGroups: [],
        outsideGroupBreakdowns: [
          {
            committeeKey: "DCCSA IEC",
            supportOppose: "support",
            categoryType: "donor",
            categoryName: "Guzman Construction Solutions LLC",
            amount: 1000,
          },
        ],
      })
    ).rejects.toThrow("D.C. outside group breakdowns require matching outside groups in the same snapshot");
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("rejects invalid link inputs", async () => {
    const db = {
      query: vi.fn(),
    };

    await expect(
      upsertDistrictOfColumbiaFinanceLink({
        db,
        link: {
          ...baseLink(),
          candidateId: " ",
        },
      })
    ).rejects.toThrow("candidate id is required");

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
      replaceDistrictOfColumbiaCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        summary: { totalReceipts: 1000 },
      })
    ).rejects.toThrow("write failed");

    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("ROLLBACK");
    expect(client.release).toHaveBeenCalledTimes(1);
  });
});
