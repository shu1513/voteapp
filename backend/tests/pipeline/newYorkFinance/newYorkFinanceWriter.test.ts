import { describe, expect, it, vi } from "vitest";

import {
  replaceNewYorkCandidateFinanceSnapshot,
  upsertNewYorkFinanceLink,
} from "../../../src/pipeline/newYorkFinance/newYorkFinanceWriter.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";

function baseLink() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    electionYear: 2026,
    candidateNameNormalized: "KATHY HOCHUL",
    officeName: "Governor",
    filerId: "16851",
    filerName: "Friends for Kathy Hochul",
    linkSource: "ny_soda_api" as const,
    sourceUrl: "https://data.ny.gov/d/7x2g-h32p",
    lastVerifiedAt: new Date("2026-07-11T00:00:00.000Z"),
  };
}

describe("newYorkFinanceWriter", () => {
  it("upserts New York finance links and returns the link id", async () => {
    const db = { query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }) };

    await expect(upsertNewYorkFinanceLink({ db, link: baseLink() })).resolves.toEqual({ linkId: LINK_ID });

    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.ny_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT (candidate_id, election_id, filer_id)");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "KATHY HOCHUL",
      "Governor",
      null,
      "16851",
      "Friends for Kathy Hochul",
      "active",
      "ny_soda_api",
      "https://data.ny.gov/d/7x2g-h32p",
      "2026-07-11T00:00:00.000Z",
    ]);
  });

  it("replaces a New York finance snapshot inside a transaction and prunes stale rows", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };
    const db = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    };

    const result = await replaceNewYorkCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-07-11T04:05:06.000Z"),
      summary: {
        outsideSupportTotal: 12_320_650.23,
        outsideOpposeTotal: 0,
        sourceUrl: "https://data.ny.gov/d/e9ss-239a",
      },
      outsideGroups: [
        {
          filerId: "590891",
          filerName: "Citizens for Affordable Rates PAC",
          supportOppose: "support",
          amount: 12_320_650.23,
          sourceUrl: "https://data.ny.gov/d/e9ss-239a",
        },
      ],
      outsideGroupBreakdowns: [
        {
          filerId: "590891",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Uber Technologies Inc.",
          amount: 11_686_700.23,
          contributorCount: 23,
          sourceUrl: "https://data.ny.gov/d/e9ss-239a",
        },
        {
          filerId: "590891",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "transportation",
          amount: 11_686_700.23,
          contributorCount: 23,
          sourceUrl: "https://data.ny.gov/d/e9ss-239a",
        },
      ],
    });

    expect(result).toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 2,
    });

    const statements = client.query.mock.calls.map((call) => String(call[0]));
    expect(statements[0]).toBe("BEGIN");
    expect(statements.at(-1)).toBe("COMMIT");
    expect(statements.some((sql) => sql.includes("INSERT INTO public.ny_candidate_finance_summaries"))).toBe(true);
    expect(statements.some((sql) => sql.includes("INSERT INTO public.ny_candidate_finance_outside_groups"))).toBe(true);
    expect(
      statements.some((sql) => sql.includes("INSERT INTO public.ny_candidate_finance_outside_group_breakdowns"))
    ).toBe(true);
    expect(statements.some((sql) => sql.includes("DELETE FROM public.ny_candidate_finance_outside_groups"))).toBe(true);
    expect(
      statements.some((sql) => sql.includes("DELETE FROM public.ny_candidate_finance_outside_group_breakdowns"))
    ).toBe(true);
    // No direct breakdowns provided in Phase 1: their delete never runs.
    expect(statements.some((sql) => sql.includes("DELETE FROM public.ny_candidate_finance_direct_breakdowns"))).toBe(
      false
    );
    expect(client.release).toHaveBeenCalled();
  });

  it("rolls back and rethrows when a write fails", async () => {
    const client = {
      query: vi.fn(async (sql: string) => {
        if (String(sql).includes("INSERT INTO public.ny_candidate_finance_summaries")) {
          throw new Error("summary write failed");
        }
        return { rows: [{ id: LINK_ID }], rowCount: 1 };
      }),
      release: vi.fn(),
    };
    const db = { query: vi.fn(), connect: vi.fn().mockResolvedValue(client) };

    await expect(
      replaceNewYorkCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        summary: { outsideSupportTotal: 1, outsideOpposeTotal: 0 },
      })
    ).rejects.toThrow("summary write failed");

    expect(client.query.mock.calls.map((call) => String(call[0]))).toContain("ROLLBACK");
    expect(client.release).toHaveBeenCalled();
  });

  it("rejects breakdowns without their groups and non-pool databases", async () => {
    const clientLike = { query: vi.fn(), release: vi.fn() };
    await expect(
      replaceNewYorkCandidateFinanceSnapshot({
        db: clientLike as never,
        link: baseLink(),
      })
    ).rejects.toThrow("must receive a Pool, not a PoolClient");

    const db = { query: vi.fn(), connect: vi.fn() };
    await expect(
      replaceNewYorkCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        outsideGroupBreakdowns: [
          {
            filerId: "590891",
            supportOppose: "support",
            categoryType: "donor",
            categoryName: "Uber Technologies Inc.",
            amount: 1,
          },
        ],
      })
    ).rejects.toThrow("require outside groups");
  });
});
