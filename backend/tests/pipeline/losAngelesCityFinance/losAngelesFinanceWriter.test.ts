import { describe, expect, it, vi } from "vitest";
import {
  replaceLosAngelesCandidateFinanceSnapshot,
  upsertLosAngelesFinanceLink,
} from "../../../src/pipeline/losAngelesCityFinance/losAngelesFinanceWriter.js";

// A query mock that answers the link INSERT with an id and everything else
// with empty rows — the writer's control flow (manual SELECT, deactivate
// UPDATE) all tolerate empty results.
function queryMock(overrides?: (sql: string) => { rows: unknown[] } | null) {
  return vi.fn().mockImplementation((sql: unknown) => {
    const s = String(sql);
    const override = overrides?.(s);
    if (override) return Promise.resolve(override);
    if (s.startsWith("INSERT INTO public.lacity_candidate_finance_links"))
      return Promise.resolve({ rows: [{ id: "link-1" }] });
    return Promise.resolve({ rows: [] });
  });
}

const AUTOMATIC_LINK = {
  candidateId: "c",
  electionId: "e",
  electionYear: 2026,
  candidateNameNormalized: "KAREN BASS",
  officeName: "Mayor",
  ethicsElectionId: "76",
  ethicsCandidatePersonId: "172",
  ethicsSeatCandidateId: "1509",
  fppcCommitteeId: "1471359",
  committeeName: "Bass",
  linkSource: "lacity_ethics" as const,
};

describe("upsertLosAngelesFinanceLink manual protection", () => {
  it("reuses a protected manual link with the same committee", async () => {
    const query = queryMock((sql) =>
      sql.startsWith("SELECT id::text,fppc_committee_id")
        ? {
            rows: [
              {
                id: "manual-1",
                fppc_committee_id: "1471359",
                link_status: "active",
              },
            ],
          }
        : null,
    );
    const result = await upsertLosAngelesFinanceLink({
      db: { query } as never,
      link: AUTOMATIC_LINK,
    });
    expect(result.linkId).toBe("manual-1");
    const sql = query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((s) => s.startsWith("INSERT INTO"))).toBe(false);
  });

  it("protects a manual link from a needs_review automatic write too", async () => {
    // Without status-independent protection, this upsert would hit
    // ON CONFLICT on the manual row and rewrite it to lacity_ethics.
    const query = queryMock((sql) =>
      sql.startsWith("SELECT id::text,fppc_committee_id")
        ? {
            rows: [
              {
                id: "manual-1",
                fppc_committee_id: "1471359",
                link_status: "active",
              },
            ],
          }
        : null,
    );
    const result = await upsertLosAngelesFinanceLink({
      db: { query } as never,
      link: { ...AUTOMATIC_LINK, linkStatus: "needs_review" },
    });
    expect(result.linkId).toBe("manual-1");
    const sql = query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((s) => s.startsWith("INSERT INTO"))).toBe(false);
  });

  it("errors when an automatic link conflicts with a protected manual link", async () => {
    const query = queryMock((sql) =>
      sql.startsWith("SELECT id::text,fppc_committee_id")
        ? {
            rows: [
              {
                id: "manual-1",
                fppc_committee_id: "9999999",
                link_status: "active",
              },
            ],
          }
        : null,
    );
    await expect(
      upsertLosAngelesFinanceLink({
        db: { query } as never,
        link: AUTOMATIC_LINK,
      }),
    ).rejects.toThrow(/conflicts with protected manual link/);
  });

  it("never resurrects an operator-disabled manual link", async () => {
    // The disabled manual row is the ON CONFLICT target — without the
    // any-status probe the upsert would silently flip it back to
    // active/lacity_ethics.
    for (const linkStatus of ["inactive", "needs_review"]) {
      const query = queryMock((sql) =>
        sql.startsWith("SELECT id::text,fppc_committee_id")
          ? {
              rows: [
                {
                  id: "manual-1",
                  fppc_committee_id: "1471359",
                  link_status: linkStatus,
                },
              ],
            }
          : null,
      );
      await expect(
        upsertLosAngelesFinanceLink({
          db: { query } as never,
          link: AUTOMATIC_LINK,
        }),
      ).rejects.toThrow(/matches an operator-disabled manual link/);
      const sql = query.mock.calls.map((call) => String(call[0]));
      expect(sql.some((s) => s.startsWith("INSERT INTO"))).toBe(false);
    }
  });

  it("allows a new automatic identity past a disabled manual link with a different committee", async () => {
    // The operator disabled that association, not the candidate.
    const query = queryMock((sql) =>
      sql.startsWith("SELECT id::text,fppc_committee_id")
        ? {
            rows: [
              {
                id: "manual-1",
                fppc_committee_id: "9999999",
                link_status: "inactive",
              },
            ],
          }
        : null,
    );
    const result = await upsertLosAngelesFinanceLink({
      db: { query } as never,
      link: AUTOMATIC_LINK,
    });
    expect(result.linkId).toBe("link-1");
    const sql = query.mock.calls.map((call) => String(call[0]));
    expect(
      sql.some((s) =>
        s.startsWith("INSERT INTO public.lacity_candidate_finance_links"),
      ),
    ).toBe(true);
  });
});

describe("Los Angeles finance writer", () => {
  it("writes a full snapshot in one transaction", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: "link" }] })
      .mockResolvedValue({ rows: [] });
    const release = vi.fn();
    const db = { connect: vi.fn().mockResolvedValue({ query, release }) };
    await replaceLosAngelesCandidateFinanceSnapshot({
      db: db as never,
      link: {
        candidateId: "c",
        electionId: "e",
        electionYear: 2026,
        candidateNameNormalized: "KAREN BASS",
        officeName: "Mayor",
        ethicsElectionId: "76",
        ethicsCandidatePersonId: "172",
        ethicsSeatCandidateId: "1509",
        fppcCommitteeId: "1471359",
        committeeName: "Bass",
        linkSource: "lacity_ethics",
      },
      summary: {
        totalReceipts: 1,
        totalDisbursements: 1,
        cashOnHand: 1,
        matchingFunds: 1,
        outsideSupportTotal: 1,
        outsideOpposeTotal: 1,
        membershipSupportTotal: 1,
        membershipOpposeTotal: 1,
        sourceUrl: null,
        reportedThrough: null,
      },
      directBreakdowns: [],
      outsideGroups: [],
    });
    const sql = query.mock.calls.map((call) => String(call[0]));
    expect(sql[0]).toBe("BEGIN");
    expect(sql).toEqual(
      expect.arrayContaining([
        expect.stringContaining("lacity_candidate_finance_summaries"),
        expect.stringContaining(
          "DELETE FROM public.lacity_candidate_finance_direct_breakdowns",
        ),
        expect.stringContaining(
          "DELETE FROM public.lacity_candidate_finance_outside_groups",
        ),
      ]),
    );
    expect(sql.at(-1)).toBe("COMMIT");
    expect(release).toHaveBeenCalled();
  });

  it("stores a validated council seat on every snapshot upsert", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: "link" }] })
      .mockResolvedValue({ rows: [] });
    const db = {
      connect: vi.fn().mockResolvedValue({ query, release: vi.fn() }),
    };
    await replaceLosAngelesCandidateFinanceSnapshot({
      db: db as never,
      link: {
        candidateId: "c",
        electionId: "e",
        electionYear: 2026,
        candidateNameNormalized: "JORDAN LEE",
        officeName: "City Council Member",
        seatNumber: 3,
        ethicsElectionId: "76",
        ethicsCandidatePersonId: "303",
        ethicsSeatCandidateId: "1503",
        fppcCommitteeId: "1471303",
        committeeName: "Lee",
        linkSource: "lacity_ethics",
      },
      summary: {
        totalReceipts: 1,
        totalDisbursements: 1,
        cashOnHand: 1,
        matchingFunds: 1,
        outsideSupportTotal: 1,
        outsideOpposeTotal: 1,
        membershipSupportTotal: 1,
        membershipOpposeTotal: 1,
        sourceUrl: null,
        reportedThrough: null,
      },
      directBreakdowns: [],
      outsideGroups: [],
    });
    const insert = query.mock.calls.find((call) =>
      String(call[0]).startsWith(
        "INSERT INTO public.lacity_candidate_finance_links",
      ),
    );
    expect(String(insert?.[0])).toContain("office_name,seat_number");
    expect(insert?.[1]?.[5]).toBe(3);
  });

  it("rejects missing, out-of-range, and citywide seat numbers", async () => {
    const db = {
      connect: vi.fn().mockResolvedValue({ query: vi.fn(), release: vi.fn() }),
    };
    const base = {
      candidateId: "c",
      electionId: "e",
      electionYear: 2026,
      candidateNameNormalized: "JORDAN LEE",
      ethicsElectionId: "76",
      ethicsCandidatePersonId: "303",
      ethicsSeatCandidateId: "1503",
      fppcCommitteeId: "1471303",
      committeeName: "Lee",
    };
    const summary = {
      totalReceipts: 1,
      totalDisbursements: 1,
      cashOnHand: 1,
      matchingFunds: 1,
      outsideSupportTotal: 1,
      outsideOpposeTotal: 1,
      membershipSupportTotal: 1,
      membershipOpposeTotal: 1,
      sourceUrl: null,
      reportedThrough: null,
    };
    for (const link of [
      { ...base, officeName: "City Council Member" },
      { ...base, officeName: "City Council Member", seatNumber: 16 },
      { ...base, officeName: "School Board Member" },
      { ...base, officeName: "School Board Member", seatNumber: 8 },
      { ...base, officeName: "Mayor", seatNumber: 3 },
    ]) {
      await expect(
        replaceLosAngelesCandidateFinanceSnapshot({
          db: db as never,
          link,
          summary,
          directBreakdowns: [],
          outsideGroups: [],
        }),
      ).rejects.toThrow(/seat number/);
    }
  });
});
