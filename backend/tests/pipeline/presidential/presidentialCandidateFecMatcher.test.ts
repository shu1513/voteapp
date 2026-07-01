import { describe, expect, it, vi } from "vitest";

import {
  matchPresidentialRosterCandidateToFec,
  presidentialPartyToOpenFecPartyCode,
} from "../../../src/pipeline/presidential/presidentialCandidateFecMatcher.js";
import type { PresidentialRosterCandidate } from "../../../src/contracts/presidentialRosterPayloadContract.js";
import type {
  OpenFecClientOptions,
  OpenFecPresidentialCandidate,
} from "../../../src/pipeline/presidential/openFecClient.js";

function rosterCandidate(overrides: Partial<PresidentialRosterCandidate> = {}): PresidentialRosterCandidate {
  return {
    display_name: "Jane President",
    party: "Democratic",
    sources: ["https://example.org/jane"],
    qualification_evidence: [
      {
        kind: "official_campaign_website",
        source_url: "https://jane.example.org",
      },
    ],
    status: "active",
    ...overrides,
  } as PresidentialRosterCandidate;
}

function fecCandidate(overrides: Partial<OpenFecPresidentialCandidate> = {}): OpenFecPresidentialCandidate {
  const fecCandidateId = overrides.fecCandidateId ?? "P80000001";
  return {
    fecCandidateId,
    name: "Jane President",
    party: "DEM",
    partyFull: "Democratic Party",
    office: "P",
    officeFull: "President",
    electionYears: [2028],
    principalCommittees: [],
    fecCandidateUrl: `https://www.fec.gov/data/candidate/${fecCandidateId}`,
    ...overrides,
  };
}

function options(overrides: Partial<OpenFecClientOptions> = {}): OpenFecClientOptions {
  return {
    apiKeys: ["test-key"],
    timeoutMs: 1000,
    ...overrides,
  };
}

describe("presidentialPartyToOpenFecPartyCode", () => {
  it("maps common major-party labels to OpenFEC party codes", () => {
    expect(presidentialPartyToOpenFecPartyCode("Democratic")).toBe("DEM");
    expect(presidentialPartyToOpenFecPartyCode("Democrat")).toBe("DEM");
    expect(presidentialPartyToOpenFecPartyCode("Republican")).toBe("REP");
    expect(presidentialPartyToOpenFecPartyCode("GOP")).toBe("REP");
  });

  it("returns undefined for unknown parties", () => {
    expect(presidentialPartyToOpenFecPartyCode("Some New Party")).toBeUndefined();
  });
});

describe("matchPresidentialRosterCandidateToFec", () => {
  it("matches exact AI-provided FEC ID first", async () => {
    const getByFecId = vi.fn().mockResolvedValue(fecCandidate());
    const searchByName = vi.fn();

    const match = await matchPresidentialRosterCandidateToFec({
      electionYear: 2028,
      expectedParty: "Democratic",
      candidate: rosterCandidate({ fec_candidate_id: "P80000001" }),
      options: {
        ...options(),
        getByFecId,
        searchByName,
      },
    });

    expect(match).toMatchObject({
      matchStatus: "matched",
      method: "exact_fec_id",
      confidence: 1,
      matchedFecId: "P80000001",
      fecSourceUrls: ["https://www.fec.gov/data/candidate/P80000001"],
    });
    expect(getByFecId).toHaveBeenCalledWith("P80000001", expect.objectContaining({ apiKeys: ["test-key"] }));
    expect(searchByName).not.toHaveBeenCalled();
  });

  it("rejects exact FEC ID when it does not match expected party or year", async () => {
    const wrongParty = await matchPresidentialRosterCandidateToFec({
      electionYear: 2028,
      expectedParty: "Democratic",
      candidate: rosterCandidate({ fec_candidate_id: "P80000002" }),
      options: {
        ...options(),
        getByFecId: vi.fn().mockResolvedValue(
          fecCandidate({
            fecCandidateId: "P80000002",
            party: "REP",
            partyFull: "Republican Party",
          })
        ),
      },
    });

    expect(wrongParty).toMatchObject({
      matchStatus: "unmatched",
      method: "unmatched",
    });

    const wrongYear = await matchPresidentialRosterCandidateToFec({
      electionYear: 2028,
      expectedParty: "Democratic",
      candidate: rosterCandidate({ fec_candidate_id: "P80000003" }),
      options: {
        ...options(),
        getByFecId: vi.fn().mockResolvedValue(
          fecCandidate({
            fecCandidateId: "P80000003",
            electionYears: [2024],
          })
        ),
      },
    });

    expect(wrongYear).toMatchObject({
      matchStatus: "unmatched",
      method: "unmatched",
    });
  });

  it("matches exact normalized name plus party through OpenFEC search", async () => {
    const searchByName = vi.fn().mockResolvedValue([
      fecCandidate({
        name: "Jane   President",
      }),
    ]);

    const match = await matchPresidentialRosterCandidateToFec({
      electionYear: 2028,
      expectedParty: "Democratic",
      candidate: rosterCandidate({ display_name: "Jane President" }),
      options: {
        ...options(),
        searchByName,
      },
    });

    expect(searchByName).toHaveBeenCalledWith(
      {
        electionYear: 2028,
        name: "Jane President",
        partyCode: "DEM",
        perPage: 100,
      },
      expect.objectContaining({ apiKeys: ["test-key"] })
    );
    expect(match).toMatchObject({
      matchStatus: "matched",
      method: "exact_name_party",
      confidence: 0.99,
      matchedFecId: "P80000001",
    });
  });

  it("marks duplicate exact name matches as ambiguous", async () => {
    const match = await matchPresidentialRosterCandidateToFec({
      electionYear: 2028,
      expectedParty: "Democratic",
      candidate: rosterCandidate(),
      options: {
        ...options(),
        searchByName: vi.fn().mockResolvedValue([
          fecCandidate({ fecCandidateId: "P80000001" }),
          fecCandidate({ fecCandidateId: "P80000002" }),
        ]),
      },
    });

    expect(match).toMatchObject({
      matchStatus: "ambiguous",
      method: "ambiguous",
      confidence: 0.99,
    });
    expect(match.fecSourceUrls).toEqual([
      "https://www.fec.gov/data/candidate/P80000001",
      "https://www.fec.gov/data/candidate/P80000002",
    ]);
  });

  it("matches a single high-confidence fuzzy name when unambiguous", async () => {
    const match = await matchPresidentialRosterCandidateToFec({
      electionYear: 2028,
      expectedParty: "Republican",
      candidate: rosterCandidate({
        display_name: "John President",
        party: "Republican",
      }),
      options: {
        ...options(),
        searchByName: vi.fn().mockResolvedValue([
          fecCandidate({
            fecCandidateId: "P80000010",
            name: "John Q. President",
            party: "REP",
            partyFull: "Republican Party",
          }),
          fecCandidate({
            fecCandidateId: "P80000011",
            name: "Mary President",
            party: "REP",
            partyFull: "Republican Party",
          }),
        ]),
      },
    });

    expect(match).toMatchObject({
      matchStatus: "matched",
      method: "fuzzy_name_party",
      matchedFecId: "P80000010",
    });
    expect(match.confidence).toBeGreaterThanOrEqual(0.9);
  });

  it("marks close fuzzy candidates as ambiguous", async () => {
    const match = await matchPresidentialRosterCandidateToFec({
      electionYear: 2028,
      expectedParty: "Democratic",
      candidate: rosterCandidate({ display_name: "Jane President" }),
      options: {
        ...options(),
        searchByName: vi.fn().mockResolvedValue([
          fecCandidate({
            fecCandidateId: "P80000020",
            name: "Jane Q President",
          }),
          fecCandidate({
            fecCandidateId: "P80000021",
            name: "Jane R President",
          }),
        ]),
      },
    });

    expect(match).toMatchObject({
      matchStatus: "ambiguous",
      method: "ambiguous",
    });
  });

  it("returns unmatched when search results do not contain a compatible candidate", async () => {
    const match = await matchPresidentialRosterCandidateToFec({
      electionYear: 2028,
      expectedParty: "Democratic",
      candidate: rosterCandidate({ display_name: "Jane President" }),
      options: {
        ...options(),
        searchByName: vi.fn().mockResolvedValue([
          fecCandidate({
            fecCandidateId: "P80000030",
            name: "Different Person",
          }),
          fecCandidate({
            fecCandidateId: "P80000031",
            name: "Jane President",
            party: "REP",
            partyFull: "Republican Party",
          }),
        ]),
      },
    });

    expect(match).toMatchObject({
      matchStatus: "unmatched",
      method: "unmatched",
      confidence: 0,
    });
  });
});
