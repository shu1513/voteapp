import { describe, expect, it, vi } from "vitest";

import {
  autoLinkMissingKansasCandidateFinanceLinks,
  createKansasFilerPoolLoader,
  KANSAS_CFR_FILER_SEARCHES,
  listKansasCandidateElectionsMissingFinanceLinks,
  searchKansasFilerRows,
  type KansasFilerPoolLoader,
  type KansasFinanceAutoLinkCandidateElection,
} from "../../../src/pipeline/kansasFinance/kansasCandidateFinanceAutoLink.js";
import type { KansasFilerRow } from "../../../src/pipeline/kansasFinance/kansasCandidateFilerResolver.js";
import {
  KANSAS_FINANCE_ELIGIBLE_OFFICE_KEYS,
  kansasCfrOfficeForRace,
} from "../../../src/pipeline/kansasFinance/kansasFinanceEligibleOffices.js";

const LINK_ID = "33333333-3333-4333-8333-333333333333";
const NOW = new Date("2026-09-01T12:00:00.000Z");

// Synthetic filer names on purpose (25-4154(d) posture).
function filerRow(overrides: Partial<KansasFilerRow>): KansasFilerRow {
  return {
    filedName: "HOLLOWAY MARGARET",
    district: "85",
    officeSought: "State Representative",
    filingKind: "report",
    fileDate: "07/27/2026",
    ...overrides,
  };
}

function candidateElection(
  overrides: Partial<KansasFinanceAutoLinkCandidateElection>
): KansasFinanceAutoLinkCandidateElection {
  return {
    candidateId: "candidate-1",
    electionId: "election-1",
    candidateName: "Margaret Holloway",
    electionYear: 2026,
    officeScope: "state_lower",
    officeName: "State Lower Chamber Legislator",
    district: "State House District 85 (2024); Kansas",
    legislativeDistrict: "85",
    ...overrides,
  };
}

function linkWritingDb() {
  return {
    query: vi.fn((sql: unknown) => {
      if (String(sql).includes("INSERT INTO public.ks_candidate_finance_links")) {
        return Promise.resolve({ rows: [{ id: LINK_ID }], rowCount: 1 });
      }
      return Promise.resolve({ rows: [], rowCount: 0 });
    }),
  };
}

function poolOf(rows: KansasFilerRow[]): KansasFilerPoolLoader {
  return vi.fn(async () => rows);
}

const runOptions = { now: NOW, maxCandidates: 25, electionLookbackDays: 98, electionLookaheadDays: 730 };

describe("listKansasCandidateElectionsMissingFinanceLinks", () => {
  it("selects eligible KS general candidate elections without an active link", async () => {
    const db = { query: vi.fn().mockResolvedValue({ rows: [] }) };
    await listKansasCandidateElectionsMissingFinanceLinks(db, runOptions);
    const [sql, params] = db.query.mock.calls[0]!;
    expect(String(sql)).toContain("district.state = 'KS'");
    expect(String(sql)).toContain("election.election_stage = 'general'");
    expect(String(sql)).toContain("ks_candidate_finance_links");
    expect(String(sql)).toContain("geoid_compact");
    expect(params?.[4]).toEqual([...KANSAS_FINANCE_ELIGIBLE_OFFICE_KEYS]);
  });
});

describe("autoLinkMissingKansasCandidateFinanceLinks", () => {
  it("links a resolved House candidate with the recipe key and cfr_viewer source", async () => {
    const db = linkWritingDb();
    const results = await autoLinkMissingKansasCandidateFinanceLinks({
      db,
      ...runOptions,
      candidateElections: [candidateElection({})],
      loadFilerPool: poolOf([filerRow({}), filerRow({ filedName: "HOLLOWAY MARGARET A", filingKind: "appointment_of_treasurer" })]),
    });
    expect(results).toEqual([
      {
        candidateId: "candidate-1",
        electionId: "election-1",
        status: "linked",
        committeeId: "7:85:HOLLOWAY:MARGARET",
        committeeName: "HOLLOWAY MARGARET",
        filedNames: ["HOLLOWAY MARGARET", "HOLLOWAY MARGARET A"],
        confidence: "name_exact",
      },
    ]);
    const insert = db.query.mock.calls.find(([sql]) => String(sql).includes("INSERT INTO public.ks_candidate_finance_links"));
    expect(insert?.[1]).toEqual([
      "candidate-1",
      "election-1",
      2026,
      "MARGARET HOLLOWAY",
      "State Lower Chamber Legislator",
      "85",
      "7:85:HOLLOWAY:MARGARET",
      "HOLLOWAY MARGARET",
      "active",
      "cfr_viewer",
      "https://sos.ks.gov/elections/cfr_viewer/cfr_examiner_entry.aspx",
      NOW.toISOString(),
    ]);
  });

  it("keys statewide links with an empty district and ignores stray grid districts", async () => {
    const db = linkWritingDb();
    const results = await autoLinkMissingKansasCandidateFinanceLinks({
      db,
      ...runOptions,
      candidateElections: [
        candidateElection({ candidateName: "Stacy Rowan", officeScope: "statewide", officeName: "Governor", district: "Kansas", legislativeDistrict: null }),
      ],
      loadFilerPool: poolOf([filerRow({ filedName: "ROWAN STACY", district: "4", officeSought: "Governor" })]),
    });
    expect(results[0]).toMatchObject({ status: "linked", committeeId: "1::ROWAN:STACY" });
    const insert = db.query.mock.calls.find(([sql]) => String(sql).includes("INSERT INTO public.ks_candidate_finance_links"));
    expect(insert?.[1]?.[5]).toBeNull();
  });

  it("reports unmatched, ambiguous, and manual-review outcomes without writing", async () => {
    const db = linkWritingDb();
    const results = await autoLinkMissingKansasCandidateFinanceLinks({
      db,
      ...runOptions,
      candidateElections: [
        candidateElection({ candidateName: "Nobody Here" }),
        candidateElection({ candidateId: "candidate-2" }),
        candidateElection({ candidateId: "candidate-3", legislativeDistrict: "86" }),
        candidateElection({ candidateId: "candidate-4", legislativeDistrict: null }),
        candidateElection({ candidateId: "candidate-5", officeScope: "county", officeName: "District Attorney", legislativeDistrict: null }),
      ],
      loadFilerPool: poolOf([
        filerRow({ filedName: "HOLLOWAY MARGARET B" }),
        filerRow({ filedName: "HOLLOWAY MARGARET T" }),
        filerRow({ filedName: "HOLLOWAY MARGARET", district: "" }),
      ]),
    });
    expect(results.map((result) => [result.status, result.reason])).toEqual([
      ["unmatched", "no_matching_filer"],
      ["ambiguous", "conflicting_filed_names"],
      ["manual_confirm_required", "filings_missing_district"],
      ["unmatched", "district_unparseable"],
      ["unmatched", "office_unmapped"],
    ]);
    expect(results[1]?.filedNames).toEqual(["HOLLOWAY MARGARET B", "HOLLOWAY MARGARET T"]);
    expect(db.query).not.toHaveBeenCalled();
  });

  it("resolves without writing in dry-run mode", async () => {
    const db = linkWritingDb();
    const results = await autoLinkMissingKansasCandidateFinanceLinks({
      db,
      ...runOptions,
      dryRun: true,
      candidateElections: [candidateElection({})],
      loadFilerPool: poolOf([filerRow({})]),
    });
    expect(results[0]).toMatchObject({ status: "linked", committeeId: "7:85:HOLLOWAY:MARGARET" });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("records a failed enumeration as per-candidate errors and continues", async () => {
    const db = linkWritingDb();
    const results = await autoLinkMissingKansasCandidateFinanceLinks({
      db,
      ...runOptions,
      candidateElections: [candidateElection({}), candidateElection({ candidateId: "candidate-2" })],
      loadFilerPool: vi.fn(async () => {
        throw new Error("viewer down");
      }),
    });
    expect(results).toEqual([
      expect.objectContaining({ candidateId: "candidate-1", status: "error", reason: "auto_link_failed", error: "viewer down" }),
      expect.objectContaining({ candidateId: "candidate-2", status: "error" }),
    ]);
    expect(db.query).not.toHaveBeenCalled();
  });
});

describe("createKansasFilerPoolLoader", () => {
  const house = kansasCfrOfficeForRace({ officeScope: "state_lower", officeCanonicalName: "State Lower Chamber Legislator" })!;

  it("runs the three filing-type searches once per office + year over the cycle window", async () => {
    const search = vi.fn(async (input: { filingType: string }) => [
      { name: "HOLLOWAY MARGARET", district: "85", officeSought: input.filingType === "Appointment of Treasurer" ? "STATE REPRESENTATIVE" : "State Representative", fileDate: "07/27/2026" },
    ]);
    const load = createKansasFilerPoolLoader({ now: NOW, search });
    const [first, second] = await Promise.all([load(house, 2026), load(house, 2026)]);
    expect(first).toBe(second);
    expect(search).toHaveBeenCalledTimes(3);
    expect(search.mock.calls.map(([input]) => [input.filingType, input.gridId])).toEqual(
      KANSAS_CFR_FILER_SEARCHES.map((entry) => [entry.filingType, entry.gridId])
    );
    expect(search.mock.calls[0]![0]).toMatchObject({ office: house, startDate: "01/01/2025", endDate: "09/01/2026" });
    expect(first.map((row) => row.filingKind)).toEqual(["report", "appointment_of_treasurer", "affidavit"]);
    // A different year is a different pool.
    await load(house, 2028);
    expect(search).toHaveBeenCalledTimes(6);
  });

  it("drops rows whose office text disagrees with the searched office", async () => {
    const onSkippedRows = vi.fn();
    const load = createKansasFilerPoolLoader({
      now: NOW,
      onSkippedRows,
      search: vi.fn(async () => [
        { name: "HOLLOWAY MARGARET", district: "85", officeSought: "State Representative", fileDate: "07/27/2026" },
        { name: "STRAY ROW", district: "", officeSought: "Governor", fileDate: "07/27/2026" },
      ]),
    });
    const rows = await load(house, 2026);
    expect(rows.map((row) => row.filedName)).toEqual(["HOLLOWAY MARGARET", "HOLLOWAY MARGARET", "HOLLOWAY MARGARET"]);
    expect(onSkippedRows).toHaveBeenCalledWith(house, 3);
  });
});

describe("searchKansasFilerRows", () => {
  const ENTRY = "https://sos.ks.gov/elections/cfr_viewer/cfr_examiner_entry.aspx";
  const FORM = "https://sos.ks.gov/elections/cfr_viewer/cfr_examiner.aspx";
  const RESULTS = "https://sos.ks.gov/elections/cfr_viewer/cfr_examiner_search_results.aspx";
  const hidden = (state: string) =>
    `<input type="hidden" name="__VIEWSTATE" value="${state}" /><input type="hidden" name="__EVENTVALIDATION" value="ev" />`;

  function fakeViewer(resultsHtml: string) {
    const posts: { url: string; body: URLSearchParams }[] = [];
    const fetchImpl = vi.fn(async (url: string, init: { method: string; body?: string }) => {
      if (init.method === "POST") {
        posts.push({ url, body: new URLSearchParams(init.body ?? "") });
        const target = url === ENTRY ? FORM : RESULTS;
        return new Response(null, { status: 302, headers: { location: target } });
      }
      if (url === ENTRY) return new Response(`<form>${hidden("entry")}</form>`, { status: 200 });
      if (url === FORM) return new Response(`<form>${hidden("form")}</form>`, { status: 200 });
      if (url === RESULTS) return new Response(resultsHtml, { status: 200 });
      return new Response("nope", { status: 404 });
    });
    return { posts, fetchImpl };
  }

  const house = kansasCfrOfficeForRace({ officeScope: "state_lower", officeCanonicalName: "State Lower Chamber Legislator" })!;

  it("posts the pinned form fields and reads every row of the results grid", async () => {
    const results = `<span id="lblRecordCount">1</span>${hidden("results")}
      <span id="grdviewApptOfTreas_lblDate_0">08/13/2026</span>
      <a id="grdviewApptOfTreas_lnkbtnLastName_0" href="javascript:__doPostBack(&#39;grdviewApptOfTreas$ctl02$lnkbtnLastName&#39;,&#39;&#39;)">HOLLOWAY</a>
      <a id="grdviewApptOfTreas_lnkbtnFirstName_0" href="javascript:__doPostBack(&#39;grdviewApptOfTreas$ctl02$lnkbtnFirstName&#39;,&#39;&#39;)">MARGARET</a>
      <span id="grdviewApptOfTreas_labelOfficeSought_0">STATE REPRESENTATIVE</span>
      <span id="grdviewApptOfTreas_lblDistrictNumber_0">/ 85</span>`;
    const viewer = fakeViewer(results);
    const rows = await searchKansasFilerRows({
      office: house,
      filingType: "Appointment of Treasurer",
      gridId: "grdviewApptOfTreas",
      startDate: "01/01/2025",
      endDate: "09/01/2026",
      sessionOptions: { fetchImpl: viewer.fetchImpl as never, spacingMs: 0 },
    });
    expect(rows).toEqual([{ name: "HOLLOWAY MARGARET", district: "85", officeSought: "STATE REPRESENTATIVE", fileDate: "08/13/2026" }]);
    const search = viewer.posts.find((post) => post.url === FORM)!;
    expect(Object.fromEntries(search.body)).toMatchObject({
      __VIEWSTATE: "form",
      __EVENTVALIDATION: "ev",
      txtFirstName: "",
      txtLastName: "",
      drpdownOffice: "7",
      txtDistrictNo: "",
      drpdownFilingType: "Appointment of Treasurer",
      txtStartDate: "01/01/2025",
      txtEndDate: "09/01/2026",
      btnSearch: "Submit Search",
    });
  });

  it("fails closed when the form re-renders with a validation message", async () => {
    const viewer = fakeViewer("");
    viewer.fetchImpl.mockImplementation(async (url: string, init: { method: string; body?: string }) => {
      if (init.method === "POST" && url === ENTRY) return new Response(null, { status: 302, headers: { location: FORM } });
      if (init.method === "POST") {
        return new Response(`<form>${hidden("form2")}<span id="lblMsg" style="color:Red;">Filing Type Required</span></form>`, { status: 200 });
      }
      return new Response(`<form>${hidden("x")}</form>`, { status: 200 });
    });
    await expect(
      searchKansasFilerRows({
        office: house,
        filingType: "",
        gridId: "grdviewCfrResults",
        startDate: "01/01/2025",
        endDate: "09/01/2026",
        sessionOptions: { fetchImpl: viewer.fetchImpl as never, spacingMs: 0 },
      })
    ).rejects.toThrow("Filing Type Required");
  });
});
