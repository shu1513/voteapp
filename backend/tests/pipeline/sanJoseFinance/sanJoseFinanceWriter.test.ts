import { describe, expect, it, vi } from "vitest";
import {
  replaceSanJoseCandidateFinanceSnapshot,
  upsertSanJoseFinanceLink,
} from "../../../src/pipeline/sanJoseFinance/sanJoseFinanceWriter.js";

// A query mock that answers the link INSERT with an id and everything else
// with empty rows — the writer's control flow (manual SELECT, deactivate
// UPDATE, detail deletes/inserts) all tolerate empty results.
function queryMock(overrides?: (sql: string) => { rows: unknown[] } | null) {
  return vi.fn().mockImplementation((sql: unknown) => {
    const s = String(sql);
    const override = overrides?.(s);
    if (override) return Promise.resolve(override);
    if (s.startsWith("INSERT INTO public.sjc_candidate_finance_links"))
      return Promise.resolve({ rows: [{ id: "link-1" }] });
    return Promise.resolve({ rows: [] });
  });
}

const link = {
  candidateId: "c",
  electionId: "e",
  electionYear: 2026,
  candidateNameNormalized: "BIEN DOAN",
  fppcId: "1484291",
  committeeName: "Bien Doan for City Council 2026",
};

const summary = {
  totalRaisedCents: 11712537,
  totalSpentCents: 10890571,
  cashOnHandCents: 3266866,
  debtsOwedCents: 0,
  loansReceivedCents: 2000000,
  outsideSupportCents: 10124954,
  outsideOpposeCents: 0,
  directCoverageNote: null,
  methodologyVersion: "sj-cal-v1",
  sourceUrl: "https://efile.sanjoseca.gov",
  reportedThrough: "2026-06-30",
};

describe("San José finance writer", () => {
  it("writes a full snapshot in one transaction with exact dollar strings", async () => {
    const query = queryMock();
    const release = vi.fn();
    const db = { connect: vi.fn().mockResolvedValue({ query, release }) };
    await replaceSanJoseCandidateFinanceSnapshot({
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
          spenderFilerId: "1487288",
          spenderName: "SOME IE COMMITTEE",
          supportOppose: "support",
          amountCents: 10124954,
          expenditureCount: 12,
        },
      ],
    });
    const sql = query.mock.calls.map((call) => String(call[0]));
    expect(sql[0]).toBe("BEGIN");
    expect(sql.at(-1)).toBe("COMMIT");
    expect(sql).toEqual(
      expect.arrayContaining([
        expect.stringContaining("sjc_candidate_finance_summaries"),
        expect.stringContaining(
          "DELETE FROM public.sjc_candidate_finance_direct_breakdowns",
        ),
        expect.stringContaining(
          "DELETE FROM public.sjc_candidate_finance_outside_groups",
        ),
      ]),
    );
    const summaryCall = query.mock.calls.find((call) =>
      String(call[0]).includes("sjc_candidate_finance_summaries"),
    );
    // Cents → exact dollar strings, never floats: raised, spent, cash, loans.
    expect(summaryCall?.[1]).toEqual(
      expect.arrayContaining(["117125.37", "108905.71", "32668.66", "20000.00"]),
    );
    expect(release).toHaveBeenCalled();
  });

  it("passes negative cash on hand through as a signed balance", async () => {
    const query = queryMock();
    const db = {
      connect: vi.fn().mockResolvedValue({ query, release: vi.fn() }),
    };
    await replaceSanJoseCandidateFinanceSnapshot({
      db: db as never,
      link,
      summary: { ...summary, cashOnHandCents: -4200 },
      directBreakdowns: [],
      outsideGroups: [],
    });
    const summaryCall = query.mock.calls.find((call) =>
      String(call[0]).includes("sjc_candidate_finance_summaries"),
    );
    expect(summaryCall?.[1]).toEqual(expect.arrayContaining(["-42.00"]));
  });

  it("rejects a negative flow and rolls the snapshot back", async () => {
    const query = queryMock();
    const release = vi.fn();
    const db = { connect: vi.fn().mockResolvedValue({ query, release }) };
    await expect(
      replaceSanJoseCandidateFinanceSnapshot({
        db: db as never,
        link,
        summary: { ...summary, totalRaisedCents: -1 },
        directBreakdowns: [],
        outsideGroups: [],
      }),
    ).rejects.toThrow(/total raised must be nonnegative/);
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
      replaceSanJoseCandidateFinanceSnapshot({
        db: db as never,
        link,
        summary: { ...summary, totalSpentCents: 100.5 },
        directBreakdowns: [],
        outsideGroups: [],
      }),
    ).rejects.toThrow(/total spent must be integer cents/);
  });

  it('never links the "Pending" placeholder filer id, in any casing', async () => {
    // Live data says "Pending", but an upstream re-casing must still fail
    // loudly here rather than store a placeholder as a durable identity.
    for (const fppcId of ["Pending", "PENDING", "pending", " Pending "]) {
      await expect(
        upsertSanJoseFinanceLink({
          db: { query: queryMock() } as never,
          link: { ...link, fppcId },
        }),
      ).rejects.toThrow(/assigned FPPC id, not Pending/);
    }
  });

  it("reuses a matching protected manual link and advances last_verified_at", async () => {
    const query = queryMock((sql) =>
      sql.startsWith("SELECT id::text,fppc_id")
        ? { rows: [{ id: "manual-1", fppc_id: "1484291" }] }
        : null,
    );
    const verifiedAt = new Date("2026-08-11T00:00:00Z");
    const result = await upsertSanJoseFinanceLink({
      db: { query } as never,
      link: { ...link, linkSource: "efile_export", lastVerifiedAt: verifiedAt },
    });
    expect(result.linkId).toBe("manual-1");
    const sql = query.mock.calls.map((call) => String(call[0]));
    // Only the manual probe and the last_verified_at touch — never an INSERT
    // that would rewrite the operator's row.
    expect(sql.some((s) => s.startsWith("INSERT INTO"))).toBe(false);
    const touch = query.mock.calls.find((call) =>
      String(call[0]).includes("SET last_verified_at"),
    );
    expect(touch?.[1]).toEqual(["manual-1", verifiedAt.toISOString()]);
  });

  it("errors when an automatic link conflicts with a protected manual link", async () => {
    const query = queryMock((sql) =>
      sql.startsWith("SELECT id::text,fppc_id")
        ? { rows: [{ id: "manual-1", fppc_id: "1480385" }] }
        : null,
    );
    await expect(
      upsertSanJoseFinanceLink({
        db: { query } as never,
        link: { ...link, linkSource: "efile_export" },
      }),
    ).rejects.toThrow(/conflicts with protected manual link/);
  });

  it("deactivates other automatic links before an active upsert", async () => {
    const query = queryMock();
    await upsertSanJoseFinanceLink({ db: { query } as never, link });
    const deactivate = query.mock.calls.find((call) =>
      String(call[0]).includes("SET link_status='inactive'"),
    );
    expect(String(deactivate?.[0])).toContain("link_source<>'manual'");
    expect(deactivate?.[1]).toEqual(["c", "e", "1484291"]);
  });
});
