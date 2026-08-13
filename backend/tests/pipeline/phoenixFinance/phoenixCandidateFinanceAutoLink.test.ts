import { describe, expect, it, vi } from "vitest";
import {
  autoLinkMissingPhoenixCandidateFinanceLinks,
  listPhoenixCandidateElectionsMissingFinanceLinks,
  type PhoenixFinanceAutoLinkCandidate,
} from "../../../src/pipeline/phoenixFinance/phoenixCandidateFinanceAutoLink.js";
import type { PhoenixRegistrationRow } from "../../../src/pipeline/phoenixFinance/phoenixEfilingClient.js";

// The roster re-read must return the input candidate (the auto-link skips
// candidates absent from it instead of linking from a stale selector row),
// so every mock serves a roster unless a test overrides it.
const hermesRosterRow = {
  candidate_id: "c1",
  candidate_name: "Ed Hermes",
  state_filing_ids: ["CAN-25-4"],
};

function linkWriterQueryMock(
  rosterRows: Record<string, unknown>[] = [hermesRosterRow],
) {
  return vi.fn().mockImplementation((sql: unknown) => {
    const s = String(sql);
    if (s.startsWith("SELECT candidate.id::text candidate_id,COALESCE"))
      return Promise.resolve({ rows: rosterRows });
    if (s.startsWith("INSERT INTO public.phx_candidate_finance_links"))
      return Promise.resolve({ rows: [{ id: "link-1" }] });
    return Promise.resolve({ rows: [] });
  });
}

const hermesCandidate: PhoenixFinanceAutoLinkCandidate = {
  candidateId: "c1",
  electionId: "e1",
  candidateName: "Ed Hermes",
  electionYear: 2026,
  electionDate: "2026-11-03",
  officeName: "City Council Member",
  districtNumber: 4,
  stateFilingIds: ["CAN-25-4"],
};

const hermesRegistration: PhoenixRegistrationRow = {
  copId: "CAN-25-4",
  committeeName: "Ed Hermes for Phoenix",
  committeeType: "Candidate Committee",
  candidateName: "Ed Hermes",
  electionCycle: "2025 Election Cycle",
  officeSoughtElectionCycle: "2026",
  terminated: false,
  approved: true,
  approvedTimestamp: 1_748_632_357_450,
  isStandingCommittee: false,
};

describe("listPhoenixCandidateElectionsMissingFinanceLinks", () => {
  it("applies the office-level gate in TS and derives district numbers", async () => {
    const row = {
      candidate_id: "c1",
      election_id: "e1",
      candidate_name: "Ed Hermes",
      election_date: "2026-11-03",
      state: "AZ",
      district_type: "place",
      geoid_compact: "0455000",
      office_scope: "place",
      office_name: "City Council Member",
      official_ballot_title: "Phoenix City Council, District 4",
      state_filing_ids: ["CAN-25-4", 42],
    };
    const query = vi.fn().mockResolvedValue({
      rows: [
        row,
        // Mayor: eligible, no district number.
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
    const rows = await listPhoenixCandidateElectionsMissingFinanceLinks(
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
        candidateName: "Ed Hermes",
        electionYear: 2026,
        electionDate: "2026-11-03",
        officeName: "City Council Member",
        districtNumber: 4,
        // Non-string entries never survive into the id tier.
        stateFilingIds: ["CAN-25-4"],
      },
      {
        candidateId: "c2",
        electionId: "e1",
        candidateName: "Ed Hermes",
        electionYear: 2026,
        electionDate: "2026-11-03",
        officeName: "Mayor",
        districtNumber: null,
        stateFilingIds: [],
      },
    ]);
    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("geoid_compact='0455000'");
    expect(sql).toContain("district.state='AZ'");
    expect(sql).toContain(
      "NOT EXISTS (SELECT 1 FROM public.phx_candidate_finance_links",
    );
    expect(sql).toContain("NOT IN ('withdrawn','lost')");
    // A row whose every name column is blank resolves to a NULL name; the
    // resolver would throw on it, so the SQL must exclude such rows.
    expect(sql).toMatch(/COALESCE\(NULLIF\(trim\(candidate\.display_name\),''\),NULLIF\(trim\(candidate\.first_name\|\|' '\|\|candidate\.last_name\),''\)\) IS NOT NULL/);
  });
});

describe("autoLinkMissingPhoenixCandidateFinanceLinks", () => {
  it("links a resolved candidate with an active efiling_portal link and derived cycle bounds", async () => {
    const query = linkWriterQueryMock();
    const results = await autoLinkMissingPhoenixCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-12T00:00:00Z"),
      candidates: [hermesCandidate],
      committees: [hermesRegistration],
    });
    expect(results).toEqual([
      { candidateId: "c1", electionId: "e1", status: "linked" },
    ]);
    const insert = query.mock.calls.find((call) =>
      String(call[0]).startsWith(
        "INSERT INTO public.phx_candidate_finance_links",
      ),
    );
    expect(insert?.[1]).toEqual(
      expect.arrayContaining([
        "ED HERMES",
        "CAN-25-4",
        "Ed Hermes for Phoenix",
        // ElectionCycle display string verbatim; bounds from the documented
        // Apr-1-odd-year rule anchored on the election date.
        "2025 Election Cycle",
        "2025-04-01",
        "2027-03-31",
        "active",
        "efiling_portal",
        "https://apps-secure.phoenix.gov/CampaignFinance",
      ]),
    );
  });

  it("reports ambiguity as needs_review and writes nothing", async () => {
    const query = linkWriterQueryMock([
      { ...hermesRosterRow, state_filing_ids: ["CAN-25-4", "CAN-24-1"] },
    ]);
    const results = await autoLinkMissingPhoenixCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-12T00:00:00Z"),
      candidates: [
        { ...hermesCandidate, stateFilingIds: ["CAN-25-4", "CAN-24-1"] },
      ],
      committees: [
        hermesRegistration,
        { ...hermesRegistration, copId: "CAN-24-1" },
      ],
    });
    expect(results[0]).toMatchObject({
      status: "needs_review",
      reason: expect.stringContaining("2 registrations"),
    });
    expect(
      query.mock.calls.some((call) => String(call[0]).startsWith("INSERT INTO")),
    ).toBe(false);
  });

  it("reports no committee without writing", async () => {
    const query = linkWriterQueryMock();
    const results = await autoLinkMissingPhoenixCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-12T00:00:00Z"),
      candidates: [hermesCandidate],
      committees: [],
    });
    expect(results[0]).toMatchObject({ status: "no_committee" });
    expect(
      query.mock.calls.some((call) => String(call[0]).startsWith("INSERT INTO")),
    ).toBe(false);
  });

  it("resolves against the full election roster, not only the unlinked slice", async () => {
    // Candidate A linked on an earlier run (or fell past maxCandidates), so
    // only B arrives here — but both roster entries carry the same COP id.
    // Resolving just the input slice would link B and duplicate the money;
    // the full-roster resolution must fail B closed instead.
    const query = vi.fn().mockImplementation((sql: unknown) => {
      const s = String(sql);
      if (s.startsWith("SELECT candidate.id::text candidate_id,COALESCE"))
        return Promise.resolve({
          rows: [
            {
              candidate_id: "cA",
              candidate_name: "Ed Hermes",
              state_filing_ids: ["CAN-25-4"],
            },
            {
              candidate_id: "cB",
              candidate_name: "Ed Hermes",
              state_filing_ids: ["CAN-25-4"],
            },
          ],
        });
      if (s.startsWith("INSERT INTO public.phx_candidate_finance_links"))
        return Promise.resolve({ rows: [{ id: "link-1" }] });
      return Promise.resolve({ rows: [] });
    });
    const results = await autoLinkMissingPhoenixCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-12T00:00:00Z"),
      candidates: [{ ...hermesCandidate, candidateId: "cB" }],
      committees: [hermesRegistration],
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
          "INSERT INTO public.phx_candidate_finance_links",
        ),
      ),
    ).toBe(false);
  });

  it("skips a candidate absent from the refreshed roster instead of linking from the stale selector row", async () => {
    // The roster re-read reflects a mid-run status change (withdrawn,
    // deleted, merged); the selector row is stale and must not produce a
    // link. The next run's selector decides fresh.
    const query = linkWriterQueryMock([]);
    const results = await autoLinkMissingPhoenixCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-12T00:00:00Z"),
      candidates: [hermesCandidate],
      committees: [hermesRegistration],
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
        return Promise.resolve({ rows: [hermesRosterRow] });
      if (s.startsWith("SELECT id::text,cop_id,link_status"))
        return Promise.resolve({
          rows: [
            { id: "manual-1", cop_id: "CAN-99-9", link_status: "active" },
          ],
        });
      return Promise.resolve({ rows: [] });
    });
    const results = await autoLinkMissingPhoenixCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-12T00:00:00Z"),
      candidates: [hermesCandidate],
      committees: [hermesRegistration],
    });
    expect(results[0]).toMatchObject({
      status: "error",
      reason: expect.stringContaining("protected manual link"),
    });
  });

  it("reuses a matching manual link instead of writing a new row", async () => {
    const query = vi.fn().mockImplementation((sql: unknown) => {
      const s = String(sql);
      if (s.startsWith("SELECT candidate.id::text candidate_id,COALESCE"))
        return Promise.resolve({ rows: [hermesRosterRow] });
      if (s.startsWith("SELECT id::text,cop_id,link_status"))
        return Promise.resolve({
          rows: [
            { id: "manual-1", cop_id: "CAN-25-4", link_status: "active" },
          ],
        });
      return Promise.resolve({ rows: [] });
    });
    const results = await autoLinkMissingPhoenixCandidateFinanceLinks({
      db: { query } as never,
      now: new Date("2026-08-12T00:00:00Z"),
      candidates: [hermesCandidate],
      committees: [hermesRegistration],
    });
    expect(results).toEqual([
      { candidateId: "c1", electionId: "e1", status: "linked" },
    ]);
    expect(
      query.mock.calls.some((call) => String(call[0]).startsWith("INSERT INTO")),
    ).toBe(false);
  });
});
