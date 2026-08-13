import { describe, expect, it, vi } from "vitest";
import {
  autoLinkMissingSanJoseCandidateFinanceLinks,
  listSanJoseCandidateElectionsMissingFinanceLinks,
  type SanJoseFinanceAutoLinkCandidate,
} from "../../../src/pipeline/sanJoseFinance/sanJoseCandidateFinanceAutoLink.js";
import type { EfileCalWorkbook } from "../../../src/pipeline/efileCalFinance/efileCalWorkbookParser.js";

const emptyWorkbook: EfileCalWorkbook = {
  summary: [],
  scheduleA: [],
  scheduleC: [],
  scheduleB1: [],
  scheduleD: [],
  s496: [],
  s497: [],
};

// The roster re-read must return the input candidate (the auto-link skips
// candidates absent from it instead of linking from a stale selector row),
// so every mock serves a roster unless a test overrides it.
const doeRosterRow = {
  candidate_id: "c1",
  candidate_name: "Jane Doe",
  state_filing_ids: [],
};

function linkWriterQueryMock(
  rosterRows: Record<string, unknown>[] = [doeRosterRow],
) {
  return vi.fn().mockImplementation((sql: unknown) => {
    const s = String(sql);
    if (s.startsWith("SELECT candidate.id::text candidate_id,COALESCE"))
      return Promise.resolve({ rows: rosterRows });
    if (s.startsWith("INSERT INTO public.sjc_candidate_finance_links"))
      return Promise.resolve({ rows: [{ id: "link-1" }] });
    return Promise.resolve({ rows: [] });
  });
}

const doeCandidate: SanJoseFinanceAutoLinkCandidate = {
  candidateId: "c1",
  electionId: "e1",
  candidateName: "Jane Doe",
  electionYear: 2026,
  officeName: "City Council Member",
  seatNumber: 5,
  stateFilingIds: [],
};

const doeCommittee = {
  filerId: "1234567",
  committeeNames: ["Jane Doe for City Council District 5 2026"],
  committeeTypes: ["C"],
};

describe("listSanJoseCandidateElectionsMissingFinanceLinks", () => {
  it("applies the office-level gate in TS and derives seat numbers", async () => {
    const row = {
      candidate_id: "c1",
      election_id: "e1",
      candidate_name: "Jane Doe",
      election_date: "2026-11-03",
      state: "CA",
      district_type: "place",
      geoid_compact: "0668000",
      office_scope: "place",
      office_name: "City Council Member",
      official_ballot_title: "Member, City Council, District 5",
      state_filing_ids: ["1234567", 42],
    };
    const query = vi.fn().mockResolvedValue({
      rows: [
        row,
        // Mayor: eligible, no seat number.
        {
          ...row,
          candidate_id: "c2",
          office_name: "Mayor",
          official_ballot_title: "Mayor",
          state_filing_ids: null,
        },
        // Not a finance office — the SQL predicate is district-level only,
        // so the TS gate must drop this row.
        {
          ...row,
          candidate_id: "c3",
          office_name: "City Clerk",
          official_ballot_title: "City Clerk",
        },
      ],
    });
    const rows = await listSanJoseCandidateElectionsMissingFinanceLinks(
      { query } as never,
      {
        now: new Date("2026-08-11T00:00:00Z"),
        maxCandidates: 25,
        electionLookbackDays: 45,
        electionLookaheadDays: 730,
      },
    );
    expect(rows).toEqual([
      {
        candidateId: "c1",
        electionId: "e1",
        candidateName: "Jane Doe",
        electionYear: 2026,
        officeName: "City Council Member",
        seatNumber: 5,
        // Non-string entries never survive into the id tier.
        stateFilingIds: ["1234567"],
      },
      {
        candidateId: "c2",
        electionId: "e1",
        candidateName: "Jane Doe",
        electionYear: 2026,
        officeName: "Mayor",
        seatNumber: null,
        stateFilingIds: [],
      },
    ]);
    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("geoid_compact='0668000'");
    expect(sql).toContain(
      "NOT EXISTS (SELECT 1 FROM public.sjc_candidate_finance_links",
    );
    expect(sql).toContain("NOT IN ('withdrawn','lost')");
    // A row whose every name column is blank resolves to a NULL name; the
    // resolver would throw on it, so the SQL must exclude such rows.
    expect(sql).toMatch(/COALESCE\(NULLIF\(trim\(candidate\.display_name\),''\),NULLIF\(trim\(candidate\.first_name\|\|' '\|\|candidate\.last_name\),''\)\) IS NOT NULL/);
  });
});

describe("autoLinkMissingSanJoseCandidateFinanceLinks", () => {
  it("links a resolved candidate with an active efile_export link", async () => {
    const query = linkWriterQueryMock();
    const results = await autoLinkMissingSanJoseCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-11T00:00:00Z"),
      candidates: [doeCandidate],
      workbook: emptyWorkbook,
      committees: [doeCommittee],
    });
    expect(results).toEqual([
      { candidateId: "c1", electionId: "e1", status: "linked" },
    ]);
    const insert = query.mock.calls.find((call) =>
      String(call[0]).startsWith(
        "INSERT INTO public.sjc_candidate_finance_links",
      ),
    );
    expect(insert?.[1]).toEqual(
      expect.arrayContaining([
        "JANE DOE",
        "1234567",
        "Jane Doe for City Council District 5 2026",
        "active",
        "efile_export",
      ]),
    );
  });

  it("collects committees from every workbook sheet when none are provided", async () => {
    const query = linkWriterQueryMock();
    const results = await autoLinkMissingSanJoseCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-11T00:00:00Z"),
      candidates: [doeCandidate],
      workbook: {
        ...emptyWorkbook,
        summary: [
          {
            filerId: "1234567",
            filerName: "Jane Doe for City Council District 5 2026",
            reportNum: "000",
            eFilingId: "100",
            origEFilingId: "100",
            cmtteType: "C",
            rptDate: null,
            fromDate: null,
            thruDate: null,
            electDate: null,
            formType: "F460",
            lineItem: "1",
            amountACents: 0,
            amountBCents: null,
            amountCCents: null,
          },
        ],
      },
    });
    expect(results[0]?.status).toBe("linked");
  });

  it("reports ambiguity as needs_review and writes nothing", async () => {
    const query = linkWriterQueryMock();
    const results = await autoLinkMissingSanJoseCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-11T00:00:00Z"),
      candidates: [doeCandidate],
      workbook: emptyWorkbook,
      committees: [
        doeCommittee,
        {
          filerId: "7654321",
          committeeNames: ["Jane Doe for San Jose City Council 2026"],
          committeeTypes: ["C"],
        },
      ],
    });
    expect(results[0]).toMatchObject({
      status: "needs_review",
      reason: expect.stringContaining("2 candidate-controlled committees"),
    });
    // The roster read runs, but nothing is written.
    expect(
      query.mock.calls.some((call) =>
        String(call[0]).startsWith("INSERT INTO"),
      ),
    ).toBe(false);
  });

  it("reports no committee without writing", async () => {
    const query = linkWriterQueryMock();
    const results = await autoLinkMissingSanJoseCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-11T00:00:00Z"),
      candidates: [doeCandidate],
      workbook: emptyWorkbook,
      committees: [],
    });
    expect(results[0]).toMatchObject({ status: "no_committee" });
    // The roster read runs, but nothing is written.
    expect(
      query.mock.calls.some((call) =>
        String(call[0]).startsWith("INSERT INTO"),
      ),
    ).toBe(false);
  });

  it("resolves against the full election roster, not only the unlinked slice", async () => {
    // Candidate A linked on an earlier run (or fell past maxCandidates), so
    // only B arrives here — but the committee matches BOTH roster entries.
    // Resolving just the input slice would link B and duplicate the money;
    // the full-roster resolution must fail B closed instead.
    const query = vi.fn().mockImplementation((sql: unknown) => {
      const s = String(sql);
      if (s.startsWith("SELECT candidate.id::text candidate_id,COALESCE"))
        return Promise.resolve({
          rows: [
            {
              candidate_id: "cA",
              candidate_name: "Jane Doe",
              state_filing_ids: [],
            },
            {
              candidate_id: "cB",
              candidate_name: "Jane Doe",
              state_filing_ids: [],
            },
          ],
        });
      if (s.startsWith("INSERT INTO public.sjc_candidate_finance_links"))
        return Promise.resolve({ rows: [{ id: "link-1" }] });
      return Promise.resolve({ rows: [] });
    });
    const results = await autoLinkMissingSanJoseCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-11T00:00:00Z"),
      candidates: [{ ...doeCandidate, candidateId: "cB" }],
      workbook: emptyWorkbook,
      committees: [doeCommittee],
    });
    // Only the input candidate is reported; the roster-only sibling shaped
    // the duplicate check but got no result row.
    expect(results).toEqual([
      {
        candidateId: "cB",
        electionId: "e1",
        status: "needs_review",
        reason: expect.stringContaining("multiple roster candidates"),
      },
    ]);
    expect(
      query.mock.calls.some((call) =>
        String(call[0]).startsWith(
          "INSERT INTO public.sjc_candidate_finance_links",
        ),
      ),
    ).toBe(false);
  });

  it("skips a candidate absent from the refreshed roster instead of linking from the stale selector row", async () => {
    // The roster re-read reflects a mid-run status change (withdrawn,
    // deleted, merged); the selector row is stale and must not produce a
    // link. The next run's selector decides fresh.
    const query = linkWriterQueryMock([]);
    const results = await autoLinkMissingSanJoseCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-11T00:00:00Z"),
      candidates: [doeCandidate],
      workbook: emptyWorkbook,
      committees: [doeCommittee],
    });
    expect(results).toEqual([
      {
        candidateId: "c1",
        electionId: "e1",
        status: "error",
        reason: expect.stringContaining("left the election roster"),
      },
    ]);
    expect(
      query.mock.calls.some((call) => String(call[0]).startsWith("INSERT INTO")),
    ).toBe(false);
  });

  it("surfaces a protected-manual-link conflict as a per-candidate error", async () => {
    const query = vi.fn().mockImplementation((sql: unknown) => {
      const s = String(sql);
      if (s.startsWith("SELECT candidate.id::text candidate_id,COALESCE"))
        return Promise.resolve({ rows: [doeRosterRow] });
      if (s.startsWith("SELECT id::text,fppc_id"))
        return Promise.resolve({
          rows: [{ id: "manual-1", fppc_id: "1480385", link_status: "active" }],
        });
      return Promise.resolve({ rows: [] });
    });
    const results = await autoLinkMissingSanJoseCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-11T00:00:00Z"),
      candidates: [doeCandidate],
      workbook: emptyWorkbook,
      committees: [doeCommittee],
    });
    expect(results[0]).toMatchObject({
      status: "error",
      reason: expect.stringContaining("protected manual link"),
    });
  });
});
