import { describe, expect, it, vi } from "vitest";

import {
  autoLinkAlaskaCandidateFinanceForCandidateElection,
  listAlaskaCandidateElectionsMissingFinanceLinks,
} from "../../../src/pipeline/alaskaFinance/alaskaCandidateFinanceAutoLink.js";
import type { AlaskaApocCampaignIncomeRow } from "../../../src/pipeline/alaskaFinance/alaskaApocClient.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const NOW = new Date("2026-06-25T12:00:00.000Z");
const SOURCE_URL = "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx";

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows, rowCount: rows.length }),
  };
}

function income(overrides: Partial<AlaskaApocCampaignIncomeRow> = {}): AlaskaApocCampaignIncomeRow {
  return {
    reportYear: 2026,
    filerId: "1001",
    filerName: "Doe, Jane",
    filerType: "Candidate",
    office: "",
    name: "Doe, Jane",
    date: "10/01/2026",
    type: "Income",
    contributor: "Smith, Pat",
    address: "1 Main",
    city: "Juneau",
    state: "AK",
    zip: "99801",
    country: "USA",
    paymentType: "Check",
    paymentDetail: "1001",
    occupation: "Attorney",
    employer: "Law Firm",
    purpose: "Contribution",
    amount: 250,
    submitted: "10/02/2026",
    status: "Complete",
    sourceUrl: SOURCE_URL,
    ...overrides,
  };
}

describe("alaskaCandidateFinanceAutoLink", () => {
  it("lists eligible Alaska candidate elections missing active finance links", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Jane Doe",
        election_year: 2026,
        office_scope: "statewide",
        office_name: "Governor",
        district: null,
      },
    ]);

    await expect(
      listAlaskaCandidateElectionsMissingFinanceLinks(db, {
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
      })
    ).resolves.toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Jane Doe",
        electionYear: 2026,
        officeScope: "statewide",
        officeName: "Governor",
        district: null,
      },
    ]);

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.candidate_elections AS candidate_election");
    expect(sql).toContain("district.state = 'AK'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($5::text[])");
    expect(sql).toContain("FROM public.ak_candidate_finance_links AS link");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-25T12:00:00.000Z",
      25,
      1,
      730,
      expect.arrayContaining(["statewide::Governor", "state_upper::State Senator"]),
    ]);
  });

  it("links a matched candidate election to the resolved APOC filer", async () => {
    const db = createMockDb([{ id: "link-1" }]);

    await expect(
      autoLinkAlaskaCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        sourceUrl: SOURCE_URL,
        incomeRows: [income()],
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Jane Doe",
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
      candidateFilerId: "1001",
    });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.ak_candidate_finance_links");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "JANE DOE",
      "Governor",
      null,
      "1001",
      "Doe, Jane",
      "active",
      "apoc_csv",
      SOURCE_URL,
      "2026-06-25T12:00:00.000Z",
    ]);
  });

  it("does not write a link when APOC filer resolution is ambiguous", async () => {
    const db = createMockDb();

    await expect(
      autoLinkAlaskaCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        sourceUrl: SOURCE_URL,
        incomeRows: [income(), income({ filerId: "1002", filerName: "Jane Doe for Alaska", name: "Jane Doe" })],
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Jane Doe",
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
      reason: "multiple_matching_filers",
    });
    expect(db.query).not.toHaveBeenCalled();
  });
});
