import { describe, expect, it, vi } from "vitest";
import {
  autoLinkMissingDenverCandidateFinanceLinks,
  listDenverCandidateElectionsMissingFinanceLinks,
  type DenverFinanceAutoLinkCandidate,
} from "../../../src/pipeline/denverFinance/denverCandidateFinanceAutoLink.js";
import type { DenverRegistrantRecord } from "../../../src/pipeline/denverFinance/denverCandidateCommitteeResolver.js";

const CYCLE = 36;

function linkWriterQueryMock() {
  return vi.fn().mockImplementation((sql: unknown) => {
    const s = String(sql);
    if (s.startsWith("INSERT INTO public.denver_candidate_finance_links"))
      return Promise.resolve({ rows: [{ id: "link-1" }] });
    return Promise.resolve({ rows: [] });
  });
}

function registrantRecord(
  fullName: string,
  filerId: number,
  committeeId: number,
  committeeName: string,
): DenverRegistrantRecord {
  return {
    registrant: {
      fullName,
      firstName: null,
      middleName: null,
      lastName: null,
      officeSoughtId: 10,
      officeSought: "City Council At-Large Seat B",
      district: null,
      committeeId,
      filerId,
    },
    filer: {
      filerId,
      filerTypeName: "Committee",
      filerStatusName: "New",
      isTerminated: false,
      committeeIds: [committeeId],
      independentExpenditureIds: [],
    },
    cycles: [{ electionCycleId: CYCLE, name: "cycle 36", electionDate: null }],
    details: {
      filerId,
      committeeId,
      committeeName,
      committeeTypeId: 1,
      committeeType: "Candidate Committee",
      candidateName: fullName,
      office: "City Council At-Large Seat B",
      officeId: 10,
      electionCycleId: CYCLE,
      electionDate: "2026-11-03T07:00:00",
    },
  };
}

const browne: DenverFinanceAutoLinkCandidate = {
  candidateId: "c1",
  electionId: "e1",
  candidateName: "Jake Browne",
  electionDate: "2026-11-03",
  electionYear: 2026,
  officeName: "City Council Member",
  atLargeSeatLetter: "B",
};

const browneRecord = registrantRecord("Jake Browne", 1326, 797, "Browne for Denver");

describe("listDenverCandidateElectionsMissingFinanceLinks", () => {
  it("applies the office-level gate in TS and derives seat letters", async () => {
    const row = {
      candidate_id: "c1",
      election_id: "e1",
      candidate_name: "Jake Browne",
      election_date: "2026-11-03",
      state: "CO",
      district_type: "place",
      geoid_compact: "0820000",
      office_scope: "place",
      office_name: "City Council Member",
      official_ballot_title: "City Council Member, At-Large Seat B",
    };
    const query = vi.fn().mockResolvedValue({
      rows: [
        row,
        // Not a finance office in v1 — the SQL predicate is district-level
        // only, so the TS gate must drop this row.
        {
          ...row,
          candidate_id: "c2",
          office_name: "Mayor",
          official_ballot_title: "Mayor",
        },
        // District council seat: out of scope until the 2027-cycle work.
        {
          ...row,
          candidate_id: "c3",
          official_ballot_title: "City Council Member, District 7",
        },
      ],
    });
    const rows = await listDenverCandidateElectionsMissingFinanceLinks(
      { query } as never,
      {
        now: new Date("2026-08-12T00:00:00Z"),
        maxCandidates: 25,
        electionLookbackDays: 45,
        electionLookaheadDays: 730,
      },
    );
    expect(rows).toEqual([
      {
        candidateId: "c1",
        electionId: "e1",
        candidateName: "Jake Browne",
        electionDate: "2026-11-03",
        electionYear: 2026,
        officeName: "City Council Member",
        atLargeSeatLetter: "B",
      },
    ]);
    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("geoid_compact='0820000'");
    expect(sql).toContain("district.state='CO'");
    // Office narrowing in SQL: ineligible Denver place races (Mayor, Clerk,
    // district seats…) must not consume the LIMIT before the TS gate runs.
    expect(sql).toContain("office.canonical_name IN ('City Council Member')");
    expect(sql).toContain(
      "NOT EXISTS (SELECT 1 FROM public.denver_candidate_finance_links",
    );
    expect(sql).toContain("NOT IN ('withdrawn','lost')");
    // A row whose every name column is blank resolves to a NULL name; the
    // resolver has nothing to match, so the SQL must exclude such rows.
    expect(sql).toMatch(
      /COALESCE\(NULLIF\(trim\(candidate\.display_name\),''\),NULLIF\(trim\(candidate\.first_name\|\|' '\|\|candidate\.last_name\),''\)\) IS NOT NULL/,
    );
  });
});

describe("autoLinkMissingDenverCandidateFinanceLinks", () => {
  it("links a resolved candidate with an active searchlight link", async () => {
    const query = linkWriterQueryMock();
    const results = await autoLinkMissingDenverCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-12T00:00:00Z"),
      electionCycleId: CYCLE,
      electionDate: "2026-11-03",
      candidates: [browne],
      registrants: [browneRecord],
    });
    expect(results).toEqual([
      { candidateId: "c1", electionId: "e1", status: "linked" },
    ]);
    const insert = query.mock.calls.find((call) =>
      String(call[0]).startsWith(
        "INSERT INTO public.denver_candidate_finance_links",
      ),
    );
    expect(insert?.[1]).toEqual(
      expect.arrayContaining([
        "JAKE BROWNE",
        "City Council Member",
        "1326", // filer id as digits text
        [797],
        "Browne for Denver",
        "active",
        "searchlight",
        "https://denver.maplight.com",
      ]),
    );
  });

  it("skips candidates whose election date is not the cycle's (cross-cycle guard)", async () => {
    // A hypothetical 2027 at-large election passes structural eligibility;
    // resolving it against cycle-36 registrants would hand a repeat candidate
    // the 2026 committee. Such candidates are another cycle's work: no
    // result row, no write.
    const query = linkWriterQueryMock();
    const results = await autoLinkMissingDenverCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-12T00:00:00Z"),
      electionCycleId: CYCLE,
      electionDate: "2026-11-03",
      candidates: [
        { ...browne, candidateId: "c2027", electionId: "e2027", electionDate: "2027-04-06", electionYear: 2027 },
        browne,
      ],
      registrants: [browneRecord],
    });
    expect(results).toEqual([
      { candidateId: "c1", electionId: "e1", status: "linked" },
    ]);
    const inserts = query.mock.calls.filter((call) =>
      String(call[0]).startsWith("INSERT INTO"),
    );
    expect(inserts).toHaveLength(1);
  });

  it("fails the run when a registrant record dates the cycle differently", async () => {
    // A wrong cycle-id/date pairing is a caller bug, not a per-candidate
    // condition — the whole run aborts before any resolution.
    const wrongDate = {
      ...browneRecord,
      details: { ...browneRecord.details, electionDate: "2027-04-06T06:00:00" },
    };
    await expect(
      autoLinkMissingDenverCandidateFinanceLinks({
        db: { query: linkWriterQueryMock() } as never,
        now: new Date("2026-08-12T00:00:00Z"),
        electionCycleId: CYCLE,
        electionDate: "2026-11-03",
        candidates: [browne],
        registrants: [wrongDate],
      }),
    ).rejects.toThrow(/wrong cycle\/date pairing/);
  });

  it("reports ambiguity as needs_review and writes nothing (duplicate names)", async () => {
    const query = linkWriterQueryMock();
    const results = await autoLinkMissingDenverCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-12T00:00:00Z"),
      electionCycleId: CYCLE,
      electionDate: "2026-11-03",
      candidates: [{ ...browne, candidateName: "Monica Martinez" }],
      registrants: [
        registrantRecord("Monica Martinez", 1322, 806, "Martinez for Denver"),
        registrantRecord("Monica Martinez", 1328, 799, "Monica for Denver"),
      ],
    });
    expect(results[0]).toMatchObject({
      status: "needs_review",
      reason: expect.stringContaining("1322, 1328"),
    });
    expect(
      query.mock.calls.some((call) => String(call[0]).startsWith("INSERT INTO")),
    ).toBe(false);
  });

  it("reports no committee without writing", async () => {
    const query = linkWriterQueryMock();
    const results = await autoLinkMissingDenverCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-12T00:00:00Z"),
      electionCycleId: CYCLE,
      electionDate: "2026-11-03",
      candidates: [browne],
      registrants: [],
    });
    expect(results[0]).toMatchObject({ status: "no_committee" });
    expect(
      query.mock.calls.some((call) => String(call[0]).startsWith("INSERT INTO")),
    ).toBe(false);
  });

  it("resolves against the full election roster, not only the unlinked slice", async () => {
    // Candidate A linked on an earlier run (or fell past maxCandidates), so
    // only B arrives here — but the registrant matches BOTH roster entries.
    // Resolving just the input slice would link B and duplicate the money;
    // the full-roster resolution must fail B closed instead.
    const query = vi.fn().mockImplementation((sql: unknown) => {
      const s = String(sql);
      if (s.startsWith("SELECT candidate.id::text candidate_id,COALESCE"))
        return Promise.resolve({
          rows: [
            { candidate_id: "cA", candidate_name: "Jake Browne" },
            { candidate_id: "cB", candidate_name: "Jake Browne" },
          ],
        });
      if (s.startsWith("INSERT INTO public.denver_candidate_finance_links"))
        return Promise.resolve({ rows: [{ id: "link-1" }] });
      return Promise.resolve({ rows: [] });
    });
    const results = await autoLinkMissingDenverCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-12T00:00:00Z"),
      electionCycleId: CYCLE,
      electionDate: "2026-11-03",
      candidates: [{ ...browne, candidateId: "cB" }],
      registrants: [browneRecord],
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
          "INSERT INTO public.denver_candidate_finance_links",
        ),
      ),
    ).toBe(false);
  });

  it("surfaces a protected-manual-link conflict as a per-candidate error", async () => {
    const query = vi.fn().mockImplementation((sql: unknown) => {
      const s = String(sql);
      if (s.startsWith("SELECT id::text,filer_id"))
        return Promise.resolve({
          rows: [{ id: "manual-1", filer_id: "9999", link_status: "active" }],
        });
      return Promise.resolve({ rows: [] });
    });
    const results = await autoLinkMissingDenverCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-12T00:00:00Z"),
      electionCycleId: CYCLE,
      electionDate: "2026-11-03",
      candidates: [browne],
      registrants: [browneRecord],
    });
    expect(results[0]).toMatchObject({
      status: "error",
      reason: expect.stringContaining("protected manual link"),
    });
  });
});
