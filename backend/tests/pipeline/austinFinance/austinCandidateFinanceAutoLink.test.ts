import { describe, expect, it, vi } from "vitest";
import {
  AUSTIN_FINANCE_LINK_SOURCE_URL,
  autoLinkMissingAustinCandidateFinanceLinks,
  listAustinCandidateElectionsMissingFinanceLinks,
  type AustinFinanceAutoLinkCandidate,
} from "../../../src/pipeline/austinFinance/austinCandidateFinanceAutoLink.js";
import type { AustinReportFiler } from "../../../src/pipeline/austinFinance/austinCandidateFilerResolver.js";

const NOW = new Date("2026-08-18T00:00:00Z");

// Post-#697 rule: only candidates present in the roster re-read resolve, so
// every linking test must serve a roster row for its input candidate.
function linkWriterQueryMock(
  roster: Array<{ candidate_id: string; candidate_name: string }> = [
    { candidate_id: "c1", candidate_name: 'Zohaib "Zo" Qadri' },
  ],
) {
  return vi.fn().mockImplementation((sql: unknown) => {
    const s = String(sql);
    if (s.startsWith("SELECT candidate.id::text candidate_id,COALESCE"))
      return Promise.resolve({ rows: roster });
    if (s.startsWith("INSERT INTO public.atx_candidate_finance_links"))
      return Promise.resolve({ rows: [{ id: "link-1" }] });
    return Promise.resolve({ rows: [] });
  });
}

function filer(filerName: string, officeCodes: string[] = ["COUNCIL_MBR_DISTRICT_09"]): AustinReportFiler {
  return { filerName, officeCodes: officeCodes as AustinReportFiler["officeCodes"], rowCount: 1 };
}

const qadri: AustinFinanceAutoLinkCandidate = {
  candidateId: "c1",
  electionId: "e9",
  candidateName: 'Zohaib "Zo" Qadri',
  electionDate: "2026-11-03",
  electionYear: 2026,
  officeName: "City Council Member",
  officeCode: "COUNCIL_MBR_DISTRICT_09",
};

const qadriFiler = filer("Qadri, Zohaib");

describe("listAustinCandidateElectionsMissingFinanceLinks", () => {
  it("selects by the allowlisted dates, applies the office gate in TS, and derives office codes", async () => {
    const row = {
      candidate_id: "c1",
      election_id: "e9",
      candidate_name: 'Zohaib "Zo" Qadri',
      election_date: "2026-11-03",
      state: "TX",
      district_type: "place",
      geoid_compact: "4805000",
      office_scope: "place",
      office_name: "City Council Member",
      official_ballot_title: "City Council Member District 9",
    };
    const query = vi.fn().mockResolvedValue({
      rows: [
        row,
        // A council title with no district number has no office code — the
        // SQL predicate is district-level only, so the TS gate must drop it.
        {
          ...row,
          candidate_id: "c2",
          election_id: "eX",
          official_ballot_title: "City Council Member",
        },
        {
          ...row,
          candidate_id: "c3",
          election_id: "eM",
          office_name: "Mayor",
          official_ballot_title: "Mayor",
        },
      ],
    });
    const rows = await listAustinCandidateElectionsMissingFinanceLinks({ query } as never, {
      electionDates: ["2026-11-03"],
      maxCandidates: 25,
    });
    expect(rows).toEqual([
      qadri,
      {
        candidateId: "c3",
        electionId: "eM",
        candidateName: 'Zohaib "Zo" Qadri',
        electionDate: "2026-11-03",
        electionYear: 2026,
        officeName: "Mayor",
        officeCode: "MAYOR",
      },
    ]);
    const sql = String(query.mock.calls[0]?.[0]);
    expect(query.mock.calls[0]?.[1]).toEqual([["2026-11-03"], 25]);
    expect(sql).toContain("geoid_compact='4805000'");
    expect(sql).toContain("district.state='TX'");
    // Office narrowing in SQL: ineligible Austin place races must not consume
    // the LIMIT before the TS gate runs.
    expect(sql).toContain("office.canonical_name IN ('Mayor','City Council Member')");
    // The allowlist IS the window — no lookback/lookahead arithmetic.
    expect(sql).toContain("election.election_date=ANY($1::date[])");
    expect(sql).not.toMatch(/make_interval/);
    expect(sql).toContain("NOT EXISTS (SELECT 1 FROM public.atx_candidate_finance_links");
    expect(sql).toContain("NOT IN ('withdrawn','lost')");
    // A row whose every name column is blank resolves to a NULL name; the
    // resolver has nothing to match, so the SQL must exclude such rows.
    expect(sql).toMatch(
      /COALESCE\(NULLIF\(trim\(candidate\.display_name\),''\),NULLIF\(trim\(candidate\.first_name\|\|' '\|\|candidate\.last_name\),''\)\) IS NOT NULL/,
    );
  });

  it("refuses dates outside the v1 allowlist and malformed dates before querying", async () => {
    const query = vi.fn();
    await expect(
      listAustinCandidateElectionsMissingFinanceLinks({ query } as never, {
        electionDates: ["2024-11-05"],
        maxCandidates: 25,
      }),
    ).rejects.toThrow(/not in the v1 allowlist/);
    await expect(
      listAustinCandidateElectionsMissingFinanceLinks({ query } as never, {
        electionDates: ["11/03/2026"],
        maxCandidates: 25,
      }),
    ).rejects.toThrow();
    expect(query).not.toHaveBeenCalled();
    expect(
      await listAustinCandidateElectionsMissingFinanceLinks({ query } as never, {
        electionDates: [],
        maxCandidates: 25,
      }),
    ).toEqual([]);
    expect(query).not.toHaveBeenCalled();
  });
});

describe("autoLinkMissingAustinCandidateFinanceLinks", () => {
  it("links a resolved candidate with an active austin_clerk link carrying the exact filer spelling", async () => {
    const query = linkWriterQueryMock();
    const results = await autoLinkMissingAustinCandidateFinanceLinks({
      db: { query } as never,
      now: NOW,
      electionDate: "2026-11-03",
      candidates: [qadri],
      filers: [qadriFiler, filer("Heyman, Richard")],
    });
    expect(results).toEqual([{ candidateId: "c1", electionId: "e9", status: "linked" }]);
    const insert = query.mock.calls.find((call) =>
      String(call[0]).startsWith("INSERT INTO public.atx_candidate_finance_links"),
    );
    expect(insert?.[1]).toEqual([
      "c1",
      "e9",
      2026,
      "ZOHAIB ZO QADRI",
      "City Council Member",
      "District 9",
      "QADRI ZOHAIB",
      "Qadri, Zohaib",
      "active",
      "austin_clerk",
      AUSTIN_FINANCE_LINK_SOURCE_URL,
      NOW.toISOString(),
    ]);
    expect(AUSTIN_FINANCE_LINK_SOURCE_URL).toBe("https://data.austintexas.gov/d/b2pc-2s8n");
  });

  it("stores a null district for Mayor links", async () => {
    const query = linkWriterQueryMock([{ candidate_id: "c1", candidate_name: "Kirk Watson" }]);
    await autoLinkMissingAustinCandidateFinanceLinks({
      db: { query } as never,
      now: NOW,
      electionDate: "2026-11-03",
      candidates: [{ ...qadri, candidateName: "Kirk Watson", officeName: "Mayor", officeCode: "MAYOR" }],
      filers: [filer("Watson, Kirk P.", ["MAYOR"])],
    });
    const insert = query.mock.calls.find((call) => String(call[0]).startsWith("INSERT INTO"));
    expect(insert?.[1]?.slice(4, 8)).toEqual(["Mayor", null, "WATSON KIRK P", "Watson, Kirk P."]);
  });

  it("skips candidates whose election date is not the filers' date (cross-date guard)", async () => {
    // Filers were read for one Report Detail election date; a candidate on
    // another allowlisted date is that date's work — no result row, no write.
    const query = linkWriterQueryMock();
    const results = await autoLinkMissingAustinCandidateFinanceLinks({
      db: { query } as never,
      now: NOW,
      electionDate: "2026-11-03",
      candidates: [
        { ...qadri, candidateId: "c2028", electionId: "e2028", electionDate: "2028-11-07", electionYear: 2028 },
        qadri,
      ],
      filers: [qadriFiler],
    });
    expect(results).toEqual([{ candidateId: "c1", electionId: "e9", status: "linked" }]);
    expect(query.mock.calls.filter((call) => String(call[0]).startsWith("INSERT INTO"))).toHaveLength(1);
  });

  it("refuses an election date outside the v1 allowlist before touching the database", async () => {
    const query = linkWriterQueryMock();
    await expect(
      autoLinkMissingAustinCandidateFinanceLinks({
        db: { query } as never,
        now: NOW,
        electionDate: "2024-11-05",
        candidates: [qadri],
        filers: [qadriFiler],
      }),
    ).rejects.toThrow(/not in the v1 allowlist/);
    expect(query).not.toHaveBeenCalled();
  });

  it("reports ambiguity as needs_review and writes nothing (two spellings of one filer)", async () => {
    const query = linkWriterQueryMock();
    const results = await autoLinkMissingAustinCandidateFinanceLinks({
      db: { query } as never,
      now: NOW,
      electionDate: "2026-11-03",
      candidates: [qadri],
      filers: [qadriFiler, filer("Qadri, Zohaib ")],
    });
    expect(results[0]).toMatchObject({
      status: "needs_review",
      reason: expect.stringContaining('"Qadri, Zohaib", "Qadri, Zohaib "'),
    });
    expect(query.mock.calls.some((call) => String(call[0]).startsWith("INSERT INTO"))).toBe(false);
  });

  it("reports no committee without writing", async () => {
    const query = linkWriterQueryMock();
    const results = await autoLinkMissingAustinCandidateFinanceLinks({
      db: { query } as never,
      now: NOW,
      electionDate: "2026-11-03",
      candidates: [qadri],
      filers: [filer("Heyman, Richard")],
    });
    expect(results[0]).toMatchObject({
      status: "no_committee",
      reason: "no Report Detail filer for COUNCIL_MBR_DISTRICT_09 name-matches",
    });
    expect(query.mock.calls.some((call) => String(call[0]).startsWith("INSERT INTO"))).toBe(false);
  });

  it("resolves against the full election roster, not only the unlinked slice", async () => {
    // Candidate A linked on an earlier run (or fell past maxCandidates), so
    // only B arrives here — but the filer matches BOTH roster entries.
    // Resolving just the input slice would link B and duplicate the money;
    // the full-roster resolution must fail B closed instead.
    const query = linkWriterQueryMock([
      { candidate_id: "cA", candidate_name: "Zohaib Qadri" },
      { candidate_id: "cB", candidate_name: "Zohaib Qadri" },
    ]);
    const results = await autoLinkMissingAustinCandidateFinanceLinks({
      db: { query } as never,
      now: NOW,
      electionDate: "2026-11-03",
      candidates: [{ ...qadri, candidateId: "cB" }],
      filers: [qadriFiler],
    });
    // Only the input candidate is reported; the roster-only sibling shaped
    // the duplicate check but got no result row.
    expect(results).toEqual([
      {
        candidateId: "cB",
        electionId: "e9",
        status: "needs_review",
        reason: expect.stringContaining("multiple roster candidates"),
      },
    ]);
    expect(query.mock.calls.some((call) => String(call[0]).startsWith("INSERT INTO"))).toBe(false);
  });

  it("reports an error for a candidate that left the roster between queries", async () => {
    // The selector saw the candidate; the roster re-read no longer does
    // (withdrawn, deleted, or merged in between). Never link from the stale
    // selector row — the #697 rule ported from SJ/SD/Phoenix.
    const query = linkWriterQueryMock([]);
    const results = await autoLinkMissingAustinCandidateFinanceLinks({
      db: { query } as never,
      now: NOW,
      electionDate: "2026-11-03",
      candidates: [qadri],
      filers: [qadriFiler],
    });
    expect(results).toEqual([
      {
        candidateId: "c1",
        electionId: "e9",
        status: "error",
        reason: "candidate left the election roster between selection and resolution; skipped",
      },
    ]);
    expect(query.mock.calls.some((call) => String(call[0]).startsWith("INSERT INTO"))).toBe(false);
  });

  it("surfaces a protected-manual-link conflict as a per-candidate error", async () => {
    const query = vi.fn().mockImplementation((sql: unknown) => {
      const s = String(sql);
      if (s.startsWith("SELECT candidate.id::text candidate_id,COALESCE"))
        return Promise.resolve({ rows: [{ candidate_id: "c1", candidate_name: "Zohaib Qadri" }] });
      if (s.startsWith("SELECT id::text,filer_key,link_status"))
        return Promise.resolve({
          rows: [{ id: "manual-1", filer_key: "SOMEONE ELSE", link_status: "active" }],
        });
      return Promise.resolve({ rows: [] });
    });
    const results = await autoLinkMissingAustinCandidateFinanceLinks({
      db: { query } as never,
      now: NOW,
      electionDate: "2026-11-03",
      candidates: [qadri],
      filers: [qadriFiler],
    });
    expect(results[0]).toMatchObject({
      status: "error",
      reason: expect.stringContaining("protected manual link"),
    });
    expect(query.mock.calls.some((call) => String(call[0]).startsWith("INSERT INTO"))).toBe(false);
  });
});
