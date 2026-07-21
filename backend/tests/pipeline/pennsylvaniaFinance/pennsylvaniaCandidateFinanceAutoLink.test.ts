import { describe, expect, it, vi } from "vitest";

import {
  autoLinkMissingPennsylvaniaCandidateFinanceLinks,
  listPennsylvaniaCandidateElectionsMissingFinanceLinks,
} from "../../../src/pipeline/pennsylvaniaFinance/pennsylvaniaCandidateFinanceAutoLink.js";
import type { PennsylvaniaCampaignFinanceFilerRow } from "../../../src/pipeline/pennsylvaniaFinance/pennsylvaniaCampaignFinanceReader.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";

function filerRow(overrides: Partial<PennsylvaniaCampaignFinanceFilerRow> = {}): PennsylvaniaCampaignFinanceFilerRow {
  return {
    CampaignfinanceID: "100",
    FILERID: "12345",
    EYEAR: "2026",
    SubmittedDate: "20260501",
    CYCLE: "2",
    AMMEND: "",
    TERMINATE: "",
    FILERTYPE: "1",
    FILERNAME: "JANE DOE FOR GOVERNOR",
    OFFICE: "GOV",
    DISTRICT: "",
    PARTY: "DEM",
    ADDRESS1: "",
    ADDRESS2: "",
    CITY: "",
    STATE: "PA",
    ZIPCODE: "",
    COUNTY: "",
    PHONE: "",
    BEGINNING: "",
    MONETARY: "",
    INKIND: "",
    ...overrides,
  };
}

function candidateElection() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    candidateName: "Jane Doe",
    electionYear: 2026,
    officeScope: "statewide",
    officeName: "Governor",
    district: null,
  };
}

describe("pennsylvaniaCandidateFinanceAutoLink", () => {
  it("queries missing Pennsylvania candidate elections", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({
        rows: [
          {
            candidate_id: CANDIDATE_ID,
            election_id: ELECTION_ID,
            candidate_name: "Jane Doe",
            election_year: 2026,
            office_scope: "statewide",
            office_name: "Governor",
            district: null,
          },
        ],
      }),
    };

    await expect(
      listPennsylvaniaCandidateElectionsMissingFinanceLinks(db, {
        now: new Date("2026-07-21T12:00:00.000Z"),
        maxCandidates: 10,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
      })
    ).resolves.toEqual([candidateElection()]);
    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("district.state = 'PA'");
    expect(sql).toContain("FROM public.pa_candidate_finance_links AS link");
    expect(sql).toContain("NOT EXISTS");
  });

  it("auto-links exactly matched Pennsylvania candidate filers", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: "link-1" }], rowCount: 1 }),
    };

    const results = await autoLinkMissingPennsylvaniaCandidateFinanceLinks({
      db,
      now: new Date("2026-07-21T12:00:00.000Z"),
      candidateElections: [candidateElection()],
      filerRowsByElectionYear: new Map([[2026, [filerRow()]]]),
      sourceUrlByElectionYear: new Map([[2026, "https://www.pa.gov/example/2026.zip"]]),
    });

    expect(results).toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        status: "linked",
        filerId: "12345",
      },
    ]);
    const insertSql = String(db.query.mock.calls[0]?.[0]);
    expect(insertSql).toContain("INSERT INTO public.pa_candidate_finance_links");
    const params = db.query.mock.calls[0]?.[1] as unknown[];
    expect(params).toContain(CANDIDATE_ID);
    expect(params).toContain("12345");
    expect(params).toContain("JANE DOE FOR GOVERNOR");
    expect(params).toContain("pa_bulk");
  });

  it("reports unmatched candidates without writing links", async () => {
    const db = { query: vi.fn() };

    const results = await autoLinkMissingPennsylvaniaCandidateFinanceLinks({
      db,
      now: new Date("2026-07-21T12:00:00.000Z"),
      candidateElections: [
        { ...candidateElection(), candidateName: "Somebody Unregistered" },
        { ...candidateElection(), electionYear: 2028 },
      ],
      filerRowsByElectionYear: new Map([[2026, [filerRow()]]]),
    });

    expect(results).toEqual([
      expect.objectContaining({ status: "unmatched", reason: "no_candidate_filer_match" }),
      expect.objectContaining({ status: "unmatched", reason: "no_candidate_filer_match" }),
    ]);
    expect(db.query).not.toHaveBeenCalled();
  });
});
