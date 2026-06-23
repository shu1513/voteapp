import { describe, expect, it, vi } from "vitest";

import {
  autoLinkMissingTexasCandidateFinanceLinks,
  autoLinkTexasCandidateFinanceForCandidateElection,
  listTexasCandidateElectionsMissingFinanceLinks,
} from "../../../src/pipeline/texasFinance/texasCandidateFinanceAutoLink.js";
import type { TexasTecFilerRow } from "../../../src/pipeline/texasFinance/texasTecCsvDatabaseReader.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const NOW = new Date("2026-06-01T00:00:00.000Z");
const SOURCE_URL = "https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip";

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows, rowCount: rows.length }),
  };
}

function filer(overrides: Partial<TexasTecFilerRow> = {}): TexasTecFilerRow {
  return {
    recordType: "FILER",
    filerIdent: "00019652",
    filerTypeCd: "COH",
    filerName: "Abbott, Greg (The Honorable)",
    committeeStatusCd: "ACTIVE",
    filerFilerpersStatusCd: "CURRENT",
    contestSeekOfficeCd: "GOVERNOR",
    contestSeekOfficeDistrict: "",
    contestSeekOfficePlace: "",
    contestSeekOfficeDescr: "Governor",
    contestSeekOfficeCountyCd: "",
    contestSeekOfficeCountyDescr: "",
    filerPersentTypeCd: "INDIVIDUAL",
    filerNameOrganization: "",
    filerNameLast: "ABBOTT",
    filerNameFirst: "GREG",
    filerNameShort: "",
    ...overrides,
  };
}

describe("texasCandidateFinanceAutoLink", () => {
  it("lists eligible Texas candidate elections missing active finance links", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Greg Abbott",
        election_year: 2026,
        office_scope: "statewide",
        office_name: "Governor",
        district: null,
      },
    ]);

    await expect(
      listTexasCandidateElectionsMissingFinanceLinks(db, {
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
      })
    ).resolves.toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Greg Abbott",
        electionYear: 2026,
        officeScope: "statewide",
        officeName: "Governor",
        district: null,
      },
    ]);

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.candidate_elections AS candidate_election");
    expect(sql).toContain("district.state = 'TX'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($5::text[])");
    expect(sql).toContain("FROM public.tx_candidate_finance_links AS link");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      25,
      1,
      730,
      expect.arrayContaining([
        "statewide::Governor",
        "statewide::Attorney General",
        "state_upper::State Senator",
        "state_lower::State Lower Chamber Legislator",
      ]),
    ]);
    expect(db.query.mock.calls[0]?.[1]?.[4]).not.toContain("statewide::State Board of Education");
  });

  it("links a matched candidate election to the resolved TEC candidate filer", async () => {
    const db = createMockDb([{ id: "link-1" }]);

    await expect(
      autoLinkTexasCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        sourceUrl: SOURCE_URL,
        filerRows: [
          filer(),
          filer({
            filerIdent: "00051153",
            filerTypeCd: "SPAC",
            filerName: "Texans for Greg Abbott",
            filerNameFirst: "",
            filerNameLast: "",
          }),
        ],
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Greg Abbott",
          electionYear: 2026,
          officeScope: "statewide",
          officeName: "Governor",
          district: null,
        },
      })
    ).resolves.toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      status: "linked",
      committeeId: "00019652",
    });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.tx_candidate_finance_links");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "GREG ABBOTT",
      "Governor",
      null,
      "00019652",
      "Abbott, Greg (The Honorable)",
      "active",
      "tec_bulk",
      SOURCE_URL,
      "2026-06-01T00:00:00.000Z",
    ]);
  });

  it("does not write a link when committee resolution is ambiguous", async () => {
    const db = createMockDb();

    await expect(
      autoLinkTexasCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        sourceUrl: SOURCE_URL,
        filerRows: [
          filer(),
          filer({
            filerIdent: "00099999",
            filerName: "ABBOTT, GREG CAMPAIGN",
          }),
        ],
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Greg Abbott",
          electionYear: 2026,
          officeScope: "statewide",
          officeName: "Governor",
          district: null,
        },
      })
    ).resolves.toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      status: "ambiguous",
      reason: "multiple_matching_committees",
    });

    expect(db.query).not.toHaveBeenCalled();
  });

  it("continues auto-linking later candidates when one candidate write fails", async () => {
    const db = {
      query: vi
        .fn()
        .mockRejectedValueOnce(new Error("temporary write failure"))
        .mockResolvedValueOnce({ rows: [{ id: "link-2" }], rowCount: 1 }),
    };
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

    try {
      await expect(
        autoLinkMissingTexasCandidateFinanceLinks({
          db,
          now: NOW,
          maxCandidates: 25,
          electionLookbackDays: 1,
          electionLookaheadDays: 730,
          filerRows: [filer()],
          sourceUrl: SOURCE_URL,
          candidateElections: [
            {
              candidateId: CANDIDATE_ID,
              electionId: ELECTION_ID,
              candidateName: "Greg Abbott",
              electionYear: 2026,
              officeScope: "statewide",
              officeName: "Governor",
              district: null,
            },
            {
              candidateId: "33333333-3333-4333-8333-333333333333",
              electionId: "44444444-4444-4444-8444-444444444444",
              candidateName: "Greg Abbott",
              electionYear: 2026,
              officeScope: "statewide",
              officeName: "Governor",
              district: null,
            },
          ],
        })
      ).resolves.toEqual([
        {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          status: "error",
          reason: "auto_link_failed",
          error: "temporary write failure",
        },
        {
          candidateId: "33333333-3333-4333-8333-333333333333",
          electionId: "44444444-4444-4444-8444-444444444444",
          status: "linked",
          committeeId: "00019652",
        },
      ]);

      expect(warnSpy).toHaveBeenCalledWith(
        "Texas finance auto-link failed for candidate election; continuing:",
        expect.objectContaining({
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          error: "temporary write failure",
        })
      );
    } finally {
      warnSpy.mockRestore();
    }

    expect(db.query).toHaveBeenCalledTimes(2);
  });
});
