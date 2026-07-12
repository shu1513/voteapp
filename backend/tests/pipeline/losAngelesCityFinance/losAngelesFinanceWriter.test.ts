import { describe, expect, it, vi } from "vitest";
import { replaceLosAngelesCandidateFinanceSnapshot } from "../../../src/pipeline/losAngelesCityFinance/losAngelesFinanceWriter.js";

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
