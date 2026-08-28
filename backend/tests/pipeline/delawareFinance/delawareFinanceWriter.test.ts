import { describe, expect, it, vi } from "vitest";

import {
  normalizeDelawareCfId,
  replaceDelawareCandidateFinanceSnapshot,
  upsertDelawareFinanceLink,
} from "../../../src/pipeline/delawareFinance/delawareFinanceWriter.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const LINK_ID = "33333333-3333-4333-8333-333333333333";
const SOURCE_URL = "https://cfrs.elections.delaware.gov/";
const VERIFIED_AT = new Date("2026-08-28T00:00:00.000Z");

function baseLink() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    electionYear: 2026,
    candidateNameNormalized: "JANE EXAMPLE",
    officeName: "Attorney General",
    district: null,
    committeeId: "01005311",
    committeeName: "Jane Example for Delaware",
    linkSource: "cfrs_portal" as const,
    sourceUrl: SOURCE_URL,
    lastVerifiedAt: VERIFIED_AT,
  };
}

function automaticQueryResult(sql: unknown) {
  const statement = String(sql);
  if (statement.includes("INSERT INTO public.de_candidate_finance_links")) {
    return Promise.resolve({ rows: [{ id: LINK_ID }], rowCount: 1 });
  }
  return Promise.resolve({ rows: [], rowCount: 0 });
}

describe("delawareFinanceWriter", () => {
  it("normalizes CF_IDs and rejects malformed ones", () => {
    expect(normalizeDelawareCfId(" 01005311 ")).toBe("01005311");
    expect(() => normalizeDelawareCfId("1005311")).toThrow("Invalid Delaware CF_ID");
    expect(() => normalizeDelawareCfId("A1005311")).toThrow("Invalid Delaware CF_ID");
  });

  it("upserts links through the canonical Delaware table with manual protection", async () => {
    const db = { query: vi.fn(automaticQueryResult) };

    await expect(
      upsertDelawareFinanceLink({ db, link: { ...baseLink(), committeeId: " 01005311 " } })
    ).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query).toHaveBeenCalledTimes(2);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("link_source='manual'");
    const insertCall = db.query.mock.calls[1];
    expect(String(insertCall?.[0])).toContain("INSERT INTO public.de_candidate_finance_links");
    expect(String(insertCall?.[0])).toContain(
      "WHERE de_candidate_finance_links.link_source <> 'manual' OR EXCLUDED.link_source = 'manual'"
    );
    expect(insertCall?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "JANE EXAMPLE",
      "Attorney General",
      null,
      "01005311",
      "Jane Example for Delaware",
      "active",
      "cfrs_portal",
      SOURCE_URL,
      VERIFIED_AT.toISOString(),
    ]);
  });

  it("fails closed against protected manual links", async () => {
    const conflictDb = {
      query: vi.fn().mockResolvedValue({
        rows: [{ id: LINK_ID, committee_id: "04006103", link_status: "active" }],
        rowCount: 1,
      }),
    };
    await expect(upsertDelawareFinanceLink({ db: conflictDb, link: baseLink() })).rejects.toThrow(
      "Delaware automatic finance link conflicts with protected manual link"
    );

    const disabledDb = {
      query: vi.fn().mockResolvedValue({
        rows: [{ id: LINK_ID, committee_id: "01005311", link_status: "inactive" }],
        rowCount: 1,
      }),
    };
    await expect(upsertDelawareFinanceLink({ db: disabledDb, link: baseLink() })).rejects.toThrow(
      "Delaware automatic finance link matches an operator-disabled manual link"
    );
  });

  it("replaces a snapshot transactionally and supersedes stale portal links", async () => {
    const client = { query: vi.fn(automaticQueryResult), release: vi.fn() };
    const db = { query: vi.fn(), connect: vi.fn().mockResolvedValue(client) };

    await expect(
      replaceDelawareCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        syncedAt: new Date("2026-08-28T00:00:00.000Z"),
        summary: {
          totalReceipts: 396_903.93,
          directContributionTotal: 350_000,
          totalDisbursements: 148_590.22,
          cashOnHand: 12_345.67,
          sourceUrl: SOURCE_URL,
        },
        directBreakdowns: [
          { categoryType: "occupation", categoryName: "Attorney", amount: 10_000 },
          { categoryType: "contribution_size", categoryName: "$1-$99", amount: 20_000 },
        ],
      })
    ).resolves.toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 2,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
    });

    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
    expect(client.release).toHaveBeenCalledTimes(1);
    const calls = client.query.mock.calls;
    const summarySql = String(
      calls.find((call) => String(call[0]).includes("INSERT INTO public.de_candidate_finance_summaries"))?.[0]
    );
    // Outside totals are never written for Delaware; the preserveWhenNull
    // policy must keep any stored values untouched.
    expect(summarySql).toContain("outside_support_total = COALESCE(EXCLUDED.outside_support_total");
    expect(summarySql).toContain("outside_oppose_total = COALESCE(EXCLUDED.outside_oppose_total");
    const supersede = calls.find(
      (call) => String(call[0]).includes("UPDATE public.de_candidate_finance_links") && String(call[0]).includes("id <>")
    );
    expect(String(supersede?.[0])).toContain("link_source = 'cfrs_portal'");
    expect(supersede?.[1]).toEqual([CANDIDATE_ID, ELECTION_ID, LINK_ID]);
    expect(calls.some((call) => String(call[0]).includes("de_candidate_finance_outside_groups"))).toBe(false);
  });

  it("rejects out-of-scope years and malformed CF_IDs before DB writes", async () => {
    const db = { query: vi.fn(automaticQueryResult) };

    await expect(
      upsertDelawareFinanceLink({ db, link: { ...baseLink(), electionYear: 2025 } })
    ).rejects.toThrow("Invalid Delaware finance election year: 2025");
    await expect(
      upsertDelawareFinanceLink({ db, link: { ...baseLink(), committeeId: "558171" } })
    ).rejects.toThrow("Invalid Delaware CF_ID: 558171");
    expect(db.query).not.toHaveBeenCalled();
  });
});
