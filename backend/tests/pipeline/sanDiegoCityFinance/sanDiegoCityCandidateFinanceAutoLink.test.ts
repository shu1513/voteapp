import { describe, expect, it, vi } from "vitest";
import {
  autoLinkMissingSanDiegoCityCandidateFinanceLinks,
  listSanDiegoCityCandidateElectionsMissingFinanceLinks,
  type SanDiegoCityFinanceAutoLinkCandidate,
} from "../../../src/pipeline/sanDiegoCityFinance/sanDiegoCityCandidateFinanceAutoLink.js";
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

function linkWriterQueryMock() {
  return vi.fn().mockImplementation((sql: unknown) => {
    const s = String(sql);
    if (s.startsWith("INSERT INTO public.sdcity_candidate_finance_links"))
      return Promise.resolve({ rows: [{ id: "link-1" }] });
    return Promise.resolve({ rows: [] });
  });
}

const martinezCandidate: SanDiegoCityFinanceAutoLinkCandidate = {
  candidateId: "c1",
  electionId: "e1",
  candidateName: "Antonio Martinez",
  electionYear: 2026,
  officeName: "City Council Member",
  seatNumber: 8,
  stateFilingIds: [],
};

const martinezCommittee = {
  filerId: "1460125",
  committeeNames: ["Antonio Martinez for City Council 2026"],
  committeeTypes: ["C"],
};

describe("listSanDiegoCityCandidateElectionsMissingFinanceLinks", () => {
  it("applies the office-level gate in TS and derives seat numbers", async () => {
    const row = {
      candidate_id: "c1",
      election_id: "e1",
      candidate_name: "Antonio Martinez",
      election_date: "2026-11-03",
      state: "CA",
      district_type: "place",
      geoid_compact: "0666000",
      office_scope: "place",
      office_name: "City Council Member",
      official_ballot_title: "Member of the City Council, District 8",
      state_filing_ids: ["1460125", 42],
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
          office_name: "Municipal Attorney",
          official_ballot_title: "City Attorney",
        },
      ],
    });
    const rows = await listSanDiegoCityCandidateElectionsMissingFinanceLinks(
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
        candidateName: "Antonio Martinez",
        electionYear: 2026,
        officeName: "City Council Member",
        seatNumber: 8,
        // Non-string entries never survive into the id tier.
        stateFilingIds: ["1460125"],
      },
      {
        candidateId: "c2",
        electionId: "e1",
        candidateName: "Antonio Martinez",
        electionYear: 2026,
        officeName: "Mayor",
        seatNumber: null,
        stateFilingIds: [],
      },
    ]);
    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("geoid_compact='0666000'");
    expect(sql).toContain(
      "NOT EXISTS (SELECT 1 FROM public.sdcity_candidate_finance_links",
    );
    expect(sql).toContain("NOT IN ('withdrawn','lost')");
    // A row whose every name column is blank resolves to a NULL name; the
    // resolver would throw on it, so the SQL must exclude such rows.
    expect(sql).toMatch(/COALESCE\(NULLIF\(trim\(candidate\.display_name\),''\),NULLIF\(trim\(candidate\.first_name\|\|' '\|\|candidate\.last_name\),''\)\) IS NOT NULL/);
  });
});

describe("autoLinkMissingSanDiegoCityCandidateFinanceLinks", () => {
  it("links a resolved candidate with an active efile_export link", async () => {
    const query = linkWriterQueryMock();
    const results = await autoLinkMissingSanDiegoCityCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-12T00:00:00Z"),
      candidates: [martinezCandidate],
      workbook: emptyWorkbook,
      committees: [martinezCommittee],
    });
    expect(results).toEqual([
      { candidateId: "c1", electionId: "e1", status: "linked" },
    ]);
    const insert = query.mock.calls.find((call) =>
      String(call[0]).startsWith(
        "INSERT INTO public.sdcity_candidate_finance_links",
      ),
    );
    expect(insert?.[1]).toEqual(
      expect.arrayContaining([
        "ANTONIO MARTINEZ",
        "1460125",
        "Antonio Martinez for City Council 2026",
        "active",
        "efile_export",
        "https://efile.sandiego.gov",
      ]),
    );
  });

  it("links a clerk-log candidate the name tier cannot resolve (Powell)", async () => {
    const query = linkWriterQueryMock();
    const results = await autoLinkMissingSanDiegoCityCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-12T00:00:00Z"),
      candidates: [
        {
          ...martinezCandidate,
          candidateId: "c9",
          candidateName: "Mark Powell",
          seatNumber: 6,
        },
      ],
      workbook: emptyWorkbook,
      committees: [
        {
          filerId: "1485884",
          committeeNames: ["POWELL FOR CITY COUNCIL 2026"],
          committeeTypes: ["C"],
        },
      ],
    });
    expect(results).toEqual([
      { candidateId: "c9", electionId: "e1", status: "linked" },
    ]);
    const insert = query.mock.calls.find((call) =>
      String(call[0]).startsWith(
        "INSERT INTO public.sdcity_candidate_finance_links",
      ),
    );
    expect(insert?.[1]).toEqual(
      expect.arrayContaining(["1485884", "POWELL FOR CITY COUNCIL 2026"]),
    );
  });

  it("collects committees from every workbook sheet when none are provided", async () => {
    const query = linkWriterQueryMock();
    const results = await autoLinkMissingSanDiegoCityCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-12T00:00:00Z"),
      candidates: [martinezCandidate],
      workbook: {
        ...emptyWorkbook,
        summary: [
          {
            filerId: "1460125",
            filerName: "Antonio Martinez for City Council 2026",
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
    const results = await autoLinkMissingSanDiegoCityCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-12T00:00:00Z"),
      candidates: [martinezCandidate],
      workbook: emptyWorkbook,
      committees: [
        martinezCommittee,
        {
          filerId: "7654321",
          committeeNames: ["Antonio Martinez for San Diego City Council 2026"],
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
    const results = await autoLinkMissingSanDiegoCityCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-12T00:00:00Z"),
      candidates: [martinezCandidate],
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
              candidate_name: "Antonio Martinez",
              state_filing_ids: [],
            },
            {
              candidate_id: "cB",
              candidate_name: "Antonio Martinez",
              state_filing_ids: [],
            },
          ],
        });
      if (s.startsWith("INSERT INTO public.sdcity_candidate_finance_links"))
        return Promise.resolve({ rows: [{ id: "link-1" }] });
      return Promise.resolve({ rows: [] });
    });
    const results = await autoLinkMissingSanDiegoCityCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-12T00:00:00Z"),
      candidates: [{ ...martinezCandidate, candidateId: "cB" }],
      workbook: emptyWorkbook,
      committees: [martinezCommittee],
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
          "INSERT INTO public.sdcity_candidate_finance_links",
        ),
      ),
    ).toBe(false);
  });

  it("surfaces a protected-manual-link conflict as a per-candidate error", async () => {
    const query = vi.fn().mockImplementation((sql: unknown) => {
      const s = String(sql);
      if (s.startsWith("SELECT id::text,fppc_id"))
        return Promise.resolve({
          rows: [{ id: "manual-1", fppc_id: "1489999" }],
        });
      return Promise.resolve({ rows: [] });
    });
    const results = await autoLinkMissingSanDiegoCityCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-12T00:00:00Z"),
      candidates: [martinezCandidate],
      workbook: emptyWorkbook,
      committees: [martinezCommittee],
    });
    expect(results[0]).toMatchObject({
      status: "error",
      reason: expect.stringContaining("protected manual link"),
    });
  });
});
