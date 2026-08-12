import { describe, expect, it, vi } from "vitest";
import {
  replaceDenverCandidateFinanceSnapshot,
  upsertDenverFinanceLink,
} from "../../../src/pipeline/denverFinance/denverFinanceWriter.js";

// A query mock that answers the link INSERT with an id and everything else
// with empty rows — the writer's control flow (manual SELECT, deactivate
// UPDATE, detail deletes/inserts) all tolerate empty results.
function queryMock(overrides?: (sql: string) => { rows: unknown[] } | null) {
  return vi.fn().mockImplementation((sql: unknown) => {
    const s = String(sql);
    const override = overrides?.(s);
    if (override) return Promise.resolve(override);
    if (s.startsWith("INSERT INTO public.denver_candidate_finance_links"))
      return Promise.resolve({ rows: [{ id: "link-1" }] });
    return Promise.resolve({ rows: [] });
  });
}

const link = {
  candidateId: "c",
  electionId: "e",
  electionYear: 2026,
  candidateNameNormalized: "MIKE JOHNSTON",
  officeName: "City Council At-Large",
  filerId: 658,
  committeeEntityIds: [641, 807],
  committeeName: "Mike For Denver",
};

// Johnston cycle-26 fixtures (plan-denver-finance.md): receipts include FEF,
// direct is private donor money only, cash can be negative.
const summary = {
  totalReceiptsCents: 201_626_363,
  directContributionTotalCents: 124_933_988,
  totalDisbursementsCents: 201_464_423,
  cashOnHandCents: -73_805,
  outsideSupportCents: 500_995_460,
  outsideOpposeCents: 15_701_593,
  sourceUrl: "https://denver.maplight.com",
};

describe("Denver finance writer", () => {
  it("writes a full snapshot in one transaction with exact dollar strings", async () => {
    const query = queryMock();
    const release = vi.fn();
    const db = { connect: vi.fn().mockResolvedValue({ query, release }) };
    await replaceDenverCandidateFinanceSnapshot({
      db: db as never,
      link,
      summary,
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Retired",
          amountCents: 123456,
          contributorCount: 7,
        },
      ],
      outsideGroups: [
        {
          spenderId: "Ind787",
          spenderName: "Advancing Denver",
          supportOppose: "support",
          amountCents: 496_241_547,
        },
      ],
    });
    const sql = query.mock.calls.map((call) => String(call[0]));
    expect(sql[0]).toBe("BEGIN");
    expect(sql.at(-1)).toBe("COMMIT");
    expect(sql).toEqual(
      expect.arrayContaining([
        expect.stringContaining("denver_candidate_finance_summaries"),
        expect.stringContaining(
          "DELETE FROM public.denver_candidate_finance_direct_breakdowns",
        ),
        expect.stringContaining(
          "DELETE FROM public.denver_candidate_finance_outside_groups",
        ),
      ]),
    );
    const summaryCall = query.mock.calls.find((call) =>
      String(call[0]).includes("denver_candidate_finance_summaries"),
    );
    // Cents → exact dollar strings, never floats: receipts, direct,
    // disbursements, signed cash, outside support/oppose.
    expect(summaryCall?.[1]).toEqual(
      expect.arrayContaining([
        "2016263.63",
        "1249339.88",
        "2014644.23",
        "-738.05",
        "5009954.60",
        "157015.93",
      ]),
    );
    // The link write carries the filer id as digits text plus the entity ids.
    const linkCall = query.mock.calls.find((call) =>
      String(call[0]).startsWith(
        "INSERT INTO public.denver_candidate_finance_links",
      ),
    );
    expect(linkCall?.[1]).toEqual(expect.arrayContaining(["658"]));
    expect(linkCall?.[1]).toEqual(expect.arrayContaining([[641, 807]]));
    expect(release).toHaveBeenCalled();
  });

  it("rejects a negative flow and rolls the snapshot back", async () => {
    const query = queryMock();
    const release = vi.fn();
    const db = { connect: vi.fn().mockResolvedValue({ query, release }) };
    await expect(
      replaceDenverCandidateFinanceSnapshot({
        db: db as never,
        link,
        summary: { ...summary, totalReceiptsCents: -1 },
        directBreakdowns: [],
        outsideGroups: [],
      }),
    ).rejects.toThrow(/total receipts must be nonnegative/);
    const sql = query.mock.calls.map((call) => String(call[0]));
    expect(sql).toContain("ROLLBACK");
    expect(sql).not.toContain("COMMIT");
    expect(release).toHaveBeenCalled();
  });

  it("rejects non-integer cents", async () => {
    const db = {
      connect: vi
        .fn()
        .mockResolvedValue({ query: queryMock(), release: vi.fn() }),
    };
    await expect(
      replaceDenverCandidateFinanceSnapshot({
        db: db as never,
        link,
        summary: { ...summary, totalDisbursementsCents: 100.5 },
        directBreakdowns: [],
        outsideGroups: [],
      }),
    ).rejects.toThrow(/total disbursements must be integer cents/);
  });

  it("rejects a filer id that is not a positive integer", async () => {
    for (const filerId of [0, -658, 1.5, Number.NaN]) {
      await expect(
        upsertDenverFinanceLink({
          db: { query: queryMock() } as never,
          link: { ...link, filerId },
        }),
      ).rejects.toThrow(/filer id must be a positive integer/);
    }
  });

  it("rejects empty or malformed committee entity ids", async () => {
    await expect(
      upsertDenverFinanceLink({
        db: { query: queryMock() } as never,
        link: { ...link, committeeEntityIds: [] },
      }),
    ).rejects.toThrow(/committee entity ids are required/);
    for (const ids of [[641, 0], [641, 2.5], [-807]]) {
      await expect(
        upsertDenverFinanceLink({
          db: { query: queryMock() } as never,
          link: { ...link, committeeEntityIds: ids },
        }),
      ).rejects.toThrow(/entity ids must be positive integers/);
    }
  });

  it("rejects an outside spender id that is not a resolved Ind uniqueId", async () => {
    const db = {
      connect: vi
        .fn()
        .mockResolvedValue({ query: queryMock(), release: vi.fn() }),
    };
    for (const spenderId of ["A Better Denver", "com658", "ind787", ""]) {
      await expect(
        replaceDenverCandidateFinanceSnapshot({
          db: db as never,
          link,
          summary,
          directBreakdowns: [],
          outsideGroups: [
            {
              spenderId,
              spenderName: "A Better Denver",
              supportOppose: "oppose",
              amountCents: 15_665_916,
            },
          ],
        }),
      ).rejects.toThrow(/spender id must be a resolved "Ind…" uniqueId/);
    }
  });

  it("reuses a matching protected manual link, refreshing entity ids and last_verified_at", async () => {
    const query = queryMock((sql) =>
      sql.startsWith("SELECT id::text,filer_id")
        ? { rows: [{ id: "manual-1", filer_id: "658", link_status: "active" }] }
        : null,
    );
    const verifiedAt = new Date("2026-08-12T00:00:00Z");
    const result = await upsertDenverFinanceLink({
      db: { query } as never,
      link: { ...link, linkSource: "searchlight", lastVerifiedAt: verifiedAt },
    });
    expect(result.linkId).toBe("manual-1");
    const sql = query.mock.calls.map((call) => String(call[0]));
    // Only the manual probe and the entity-ids/last_verified_at touch — never
    // an INSERT that would rewrite the operator's row.
    expect(sql.some((s) => s.startsWith("INSERT INTO"))).toBe(false);
    const touch = query.mock.calls.find((call) =>
      String(call[0]).includes("SET committee_entity_ids"),
    );
    expect(touch?.[1]).toEqual([
      "manual-1",
      [641, 807],
      verifiedAt.toISOString(),
    ]);
  });

  it("errors when an automatic link conflicts with a protected manual link", async () => {
    const query = queryMock((sql) =>
      sql.startsWith("SELECT id::text,filer_id")
        ? { rows: [{ id: "manual-1", filer_id: "517", link_status: "active" }] }
        : null,
    );
    await expect(
      upsertDenverFinanceLink({
        db: { query } as never,
        link: { ...link, linkSource: "searchlight" },
      }),
    ).rejects.toThrow(/conflicts with protected manual link/);
  });

  it("never resurrects an operator-disabled manual link with the same filer id", async () => {
    // The disabled manual row is the ON CONFLICT target — without the
    // any-status probe the upsert would silently flip it back to
    // active/searchlight.
    for (const linkStatus of ["inactive", "needs_review"]) {
      const query = queryMock((sql) =>
        sql.startsWith("SELECT id::text,filer_id")
          ? {
              rows: [
                { id: "manual-1", filer_id: "658", link_status: linkStatus },
              ],
            }
          : null,
      );
      await expect(
        upsertDenverFinanceLink({
          db: { query } as never,
          link: { ...link, linkSource: "searchlight" },
        }),
      ).rejects.toThrow(/matches an operator-disabled manual link/);
      const sql = query.mock.calls.map((call) => String(call[0]));
      expect(sql.some((s) => s.startsWith("INSERT INTO"))).toBe(false);
      expect(sql.some((s) => s.includes("SET committee_entity_ids"))).toBe(
        false,
      );
    }
  });

  it("allows a new automatic identity past a disabled manual link with a different filer id", async () => {
    // The operator disabled that association, not the candidate.
    const query = queryMock((sql) =>
      sql.startsWith("SELECT id::text,filer_id")
        ? {
            rows: [
              { id: "manual-1", filer_id: "517", link_status: "inactive" },
            ],
          }
        : null,
    );
    const result = await upsertDenverFinanceLink({
      db: { query } as never,
      link: { ...link, linkSource: "searchlight" },
    });
    expect(result.linkId).toBe("link-1");
    const sql = query.mock.calls.map((call) => String(call[0]));
    expect(
      sql.some((s) =>
        s.startsWith("INSERT INTO public.denver_candidate_finance_links"),
      ),
    ).toBe(true);
  });

  it("deactivates other automatic links before an active upsert", async () => {
    const query = queryMock();
    await upsertDenverFinanceLink({ db: { query } as never, link });
    const deactivate = query.mock.calls.find((call) =>
      String(call[0]).includes("SET link_status='inactive'"),
    );
    expect(String(deactivate?.[0])).toContain("link_source<>'manual'");
    expect(deactivate?.[1]).toEqual(["c", "e", "658"]);
  });
});
