import { describe, expect, it, vi } from "vitest";

import {
  autoLinkMissingSouthCarolinaCandidateFinanceLinks,
  autoLinkSouthCarolinaCandidateFinanceForCandidateElection,
  normalizeSouthCarolinaCandidateNameForStorage,
  southCarolinaFilerNeedsReportFetch,
  type SouthCarolinaFinanceAutoLinkCandidateElection,
} from "../../../src/pipeline/southCarolinaFinance/southCarolinaCandidateFinanceAutoLink.js";
import type {
  SouthCarolinaCandidateReportRow,
  SouthCarolinaFilerSearchRow,
} from "../../../src/pipeline/southCarolinaFinance/southCarolinaEthicsClient.js";

const LINK_ID = "33333333-3333-4333-8333-333333333333";

function filerRow(overrides: Partial<SouthCarolinaFilerSearchRow>): SouthCarolinaFilerSearchRow {
  return {
    candidate: "Evette, Pamela S",
    candidateFilerId: 54395,
    officeName: "4",
    lastCampaignDisclosureReport: "07/14/2026",
    ...overrides,
  };
}

function reportRow(overrides: Partial<SouthCarolinaCandidateReportRow>): SouthCarolinaCandidateReportRow {
  return {
    reportId: 430061,
    reportName: "Pre-Election Report 2026",
    reportType: "Pre-Election Quarterly",
    electionDate: "6/9/2026",
    contributions: 100,
    expenses: 50,
    balance: 50,
    dateSubmitted: "2026-07-14T10:00:00",
    campaignId: 77609,
    candidateFilerId: 54395,
    filingStartDate: "2026-04-01T04:00:00",
    filingEndDate: "2026-05-20T00:00:00",
    isPrimary: true,
    isGeneral: false,
    isPreElection: true,
    isFinal: false,
    ...overrides,
  };
}

function candidateElection(
  overrides: Partial<SouthCarolinaFinanceAutoLinkCandidateElection> = {}
): SouthCarolinaFinanceAutoLinkCandidateElection {
  return {
    candidateId: "candidate-1",
    electionId: "election-1",
    candidateName: "Pamela Evette",
    electionDate: "2026-11-03",
    electionYear: 2026,
    officeScope: "statewide",
    officeName: "Governor",
    district: null,
    ...overrides,
  };
}

function linkWritingDb() {
  return {
    query: vi.fn((sql: unknown) => {
      if (String(sql).includes("INSERT INTO public.sc_candidate_finance_links")) {
        return Promise.resolve({ rows: [{ id: LINK_ID }], rowCount: 1 });
      }
      return Promise.resolve({ rows: [], rowCount: 0 });
    }),
  };
}

describe("normalizeSouthCarolinaCandidateNameForStorage", () => {
  it("strips diacritics and collapses punctuation", () => {
    expect(normalizeSouthCarolinaCandidateNameForStorage("José O'Neal-Smith Jr.")).toBe(
      "JOSE O NEAL SMITH JR"
    );
  });
});

describe("southCarolinaFilerNeedsReportFetch", () => {
  it("keeps recent filers and null last-report filers, drops stale ones", () => {
    expect(southCarolinaFilerNeedsReportFetch(filerRow({ lastCampaignDisclosureReport: "07/14/2026" }), 2026)).toBe(true);
    expect(southCarolinaFilerNeedsReportFetch(filerRow({ lastCampaignDisclosureReport: "01/10/2025" }), 2026)).toBe(true);
    expect(southCarolinaFilerNeedsReportFetch(filerRow({ lastCampaignDisclosureReport: "10/10/2018" }), 2026)).toBe(false);
    // Registered-not-yet-filed filers carry placeholder cycle rows — the
    // evidence the resolver needs — so a missing last report is KEPT.
    expect(southCarolinaFilerNeedsReportFetch(filerRow({ lastCampaignDisclosureReport: null }), 2026)).toBe(true);
    expect(southCarolinaFilerNeedsReportFetch(filerRow({ lastCampaignDisclosureReport: "garbage" }), 2026)).toBe(true);
  });
});

describe("autoLinkSouthCarolinaCandidateFinanceForCandidateElection", () => {
  it("links a full-name match with ethics_filer_search source and normalized name", async () => {
    const db = linkWritingDb();
    const result = await autoLinkSouthCarolinaCandidateFinanceForCandidateElection({
      db,
      candidateElection: candidateElection(),
      now: new Date("2026-08-27T00:00:00.000Z"),
      loadFilerReportSets: async () => ({
        filerReportSets: [{ filer: filerRow({}), reports: [reportRow({})] }],
        skippedFilers: [],
      }),
      fetchContributions: vi.fn().mockResolvedValue([]),
    });

    expect(result).toMatchObject({
      status: "linked",
      candidateFilerId: 54395,
      filerName: "Evette, Pamela S",
    });
    const insert = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.sc_candidate_finance_links")
    );
    expect(insert?.[1]).toEqual([
      "candidate-1",
      "election-1",
      2026,
      "PAMELA EVETTE",
      "Governor",
      null,
      "54395",
      "Evette, Pamela S",
      "active",
      "ethics_filer_search",
      "https://ethicsfiling.sc.gov/public",
      "2026-08-27T00:00:00.000Z",
    ]);
  });

  it("reports manual_confirm_required without writing (Wilson legal-name divergence)", async () => {
    const db = linkWritingDb();
    const result = await autoLinkSouthCarolinaCandidateFinanceForCandidateElection({
      db,
      candidateElection: candidateElection({ candidateName: "Alan Wilson" }),
      now: new Date("2026-08-27T00:00:00.000Z"),
      loadFilerReportSets: async () => ({
        filerReportSets: [
          {
            filer: filerRow({ candidate: "Wilson, Michael A", candidateFilerId: 54344 }),
            reports: [reportRow({ candidateFilerId: 54344, campaignId: 77574 })],
          },
        ],
        skippedFilers: [filerRow({ candidate: "Wilson, Amy F.", candidateFilerId: 31444 })],
      }),
    });

    expect(result.status).toBe("manual_confirm_required");
    expect(result.skippedFilerIds).toEqual([31444]);
    expect(result.candidates?.[0]).toMatchObject({ candidateFilerId: 54344 });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("reports ambiguity and unmatched without writing", async () => {
    const db = linkWritingDb();
    const ambiguous = await autoLinkSouthCarolinaCandidateFinanceForCandidateElection({
      db,
      candidateElection: candidateElection({ candidateName: "John Smith" }),
      now: new Date("2026-08-27T00:00:00.000Z"),
      loadFilerReportSets: async () => ({
        filerReportSets: [
          {
            filer: filerRow({ candidate: "Smith, John A", candidateFilerId: 70001 }),
            reports: [reportRow({ candidateFilerId: 70001, campaignId: 1 })],
          },
          {
            filer: filerRow({ candidate: "Smith, John B", candidateFilerId: 70002 }),
            reports: [reportRow({ candidateFilerId: 70002, campaignId: 2 })],
          },
        ],
        skippedFilers: [],
      }),
    });
    expect(ambiguous.status).toBe("ambiguous");

    const unmatched = await autoLinkSouthCarolinaCandidateFinanceForCandidateElection({
      db,
      candidateElection: candidateElection(),
      now: new Date("2026-08-27T00:00:00.000Z"),
      loadFilerReportSets: async () => ({ filerReportSets: [], skippedFilers: [] }),
    });
    expect(unmatched).toMatchObject({ status: "unmatched", reason: "no_matching_filer" });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("refuses to link when a possibly-matching filer's report fetch failed", async () => {
    const db = linkWritingDb();
    const result = await autoLinkSouthCarolinaCandidateFinanceForCandidateElection({
      db,
      candidateElection: candidateElection(),
      now: new Date("2026-08-27T00:00:00.000Z"),
      loadFilerReportSets: async () => ({
        filerReportSets: [{ filer: filerRow({}), reports: [reportRow({})] }],
        // Same full name as the candidate — its unreadable reports could
        // have made this resolution ambiguous.
        skippedFilers: [filerRow({ candidate: "Evette, Pamela", candidateFilerId: 99999 })],
      }),
      fetchContributions: vi.fn().mockResolvedValue([]),
    });

    expect(result).toMatchObject({
      status: "error",
      reason: "skipped_filer_may_match",
      skippedFilerIds: [99999],
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("demotes a match to manual confirmation when run office evidence contradicts the race", async () => {
    const db = linkWritingDb();
    const fetchContributions = vi.fn().mockResolvedValue([
      {
        contributionId: 1,
        candidateId: 54395,
        officeRunId: 77609,
        candidateName: "Evette, Pamela S",
        officeName: "SC House of Representatives District 23",
        electionDate: "2026-11-03T05:00:00",
        date: "2026-03-01T00:00:00",
        amount: 100,
        contributorName: "Jane Donor",
        contributorOccupation: "Attorney",
        group: "No",
        description: null,
      },
    ]);

    const result = await autoLinkSouthCarolinaCandidateFinanceForCandidateElection({
      db,
      // Statewide candidate, but the matched filer's run rows say SC House.
      candidateElection: candidateElection(),
      now: new Date("2026-08-27T00:00:00.000Z"),
      loadFilerReportSets: async () => ({
        filerReportSets: [{ filer: filerRow({}), reports: [reportRow({})] }],
        skippedFilers: [],
      }),
      fetchContributions,
    });

    expect(result.status).toBe("manual_confirm_required");
    expect(result.reason).toContain("office_evidence_conflict");
    expect(result.reason).toContain("SC House of Representatives District 23");
    expect(db.query).not.toHaveBeenCalled();
    expect(fetchContributions).toHaveBeenCalledWith({ candidate: "Evette", contributionYear: 2026 }, undefined);
  });
});

describe("autoLinkMissingSouthCarolinaCandidateFinanceLinks", () => {
  it("records a per-candidate error and continues the batch", async () => {
    const db = linkWritingDb();
    const failing = candidateElection({ candidateId: "candidate-err", candidateName: "Boom Error" });
    const fine = candidateElection();
    const results = await autoLinkMissingSouthCarolinaCandidateFinanceLinks({
      db,
      now: new Date("2026-08-27T00:00:00.000Z"),
      maxCandidates: 10,
      electionLookbackDays: 76,
      electionLookaheadDays: 730,
      fetchContributions: vi.fn().mockResolvedValue([]),
      candidateElections: [failing, fine],
      loadFilerReportSets: async (input) => {
        if (input.candidateName === "Boom Error") {
          throw new Error("network exploded");
        }
        return {
          filerReportSets: [{ filer: filerRow({}), reports: [reportRow({})] }],
          skippedFilers: [],
        };
      },
    });

    expect(results).toHaveLength(2);
    expect(results[0]).toMatchObject({
      candidateId: "candidate-err",
      status: "error",
      reason: "auto_link_failed",
      error: "network exploded",
    });
    expect(results[1]).toMatchObject({ status: "linked", candidateFilerId: 54395 });
  });
});
