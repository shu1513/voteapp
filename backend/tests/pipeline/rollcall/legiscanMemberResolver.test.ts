import { describe, expect, it } from "vitest";

import {
  parseCandidateDistrictNumber,
  parseLegiscanCrosswalkFile,
  parseLegiscanDistrict,
  parseLegiscanPerson,
  parseLegiscanPeopleSnapshot,
  proposeLegiscanCrosswalk,
  resolveLegiscanMembers,
  type LegiscanCandidateForMatching,
  type LegiscanPerson,
} from "../../../src/pipeline/rollcall/legiscanMemberResolver.js";

const UUID_A = "11111111-1111-4111-8111-111111111111";
const UUID_B = "22222222-2222-4222-8222-222222222222";

function personElement(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    people_id: 16788,
    person_hash: "1s25cljm",
    party: "D",
    role: "Rep",
    // The manual's own sample pads name fields with stray spaces.
    name: " Joseph Preston",
    first_name: " Joseph ",
    last_name: "Preston",
    suffix: "",
    district: "HD-063",
    committee_sponsor: 0,
    ...overrides,
  };
}

function person(overrides: Partial<LegiscanPerson> = {}): LegiscanPerson {
  return {
    peopleId: 16788,
    name: "Joseph Preston",
    firstName: "Joseph",
    lastName: "Preston",
    party: "D",
    chamber: "house",
    district: "HD-063",
    ...overrides,
  };
}

function candidate(overrides: Partial<LegiscanCandidateForMatching> = {}): LegiscanCandidateForMatching {
  return {
    candidateId: UUID_A,
    name: "Joseph Preston",
    scope: "state_lower",
    districtName: "State House District 63 (2024); Texas",
    ...overrides,
  };
}

describe("parseLegiscanPerson", () => {
  it("trims the padded name fields and maps the role to a chamber", () => {
    const parsed = parseLegiscanPerson(personElement())!;
    expect(parsed.name).toBe("Joseph Preston");
    expect(parsed.firstName).toBe("Joseph");
    expect(parsed.chamber).toBe("house");
    expect(parseLegiscanPerson(personElement({ role: "Sen" }))!.chamber).toBe("senate");
    expect(parseLegiscanPerson(personElement({ role: "Del" }))!.chamber).toBeNull();
  });

  it("skips committee pseudo-persons and rejects a bad people_id", () => {
    expect(parseLegiscanPerson(personElement({ committee_sponsor: 1 }))).toBeNull();
    expect(() => parseLegiscanPerson(personElement({ people_id: "16788" }))).toThrow("people_id");
  });
});

describe("parseLegiscanPeopleSnapshot", () => {
  const expected = { jurisdiction: "TX", sessionId: 2172 };

  it("indexes persons by people_id and skips committee rows", () => {
    const snapshot = parseLegiscanPeopleSnapshot(
      {
        jurisdiction: "TX",
        sessionId: 2172,
        people: [personElement(), personElement({ people_id: 200, committee_sponsor: 1 })],
      },
      expected
    );
    expect([...snapshot.byPeopleId.keys()]).toEqual([16788]);
  });

  it("rejects the wrong state or session and duplicate people", () => {
    const people = [personElement()];
    expect(() => parseLegiscanPeopleSnapshot({ jurisdiction: "OH", sessionId: 2172, people }, expected)).toThrow(
      "jurisdiction"
    );
    expect(() => parseLegiscanPeopleSnapshot({ jurisdiction: "TX", sessionId: 1, people }, expected)).toThrow("sessionId");
    expect(() =>
      parseLegiscanPeopleSnapshot({ jurisdiction: "TX", sessionId: 2172, people: [personElement(), personElement()] }, expected)
    ).toThrow("twice");
  });
});

describe("parseLegiscanCrosswalkFile", () => {
  const file = {
    source: "legiscan",
    jurisdiction: "TX",
    entries: [
      { people_id: 16788, candidate_id: UUID_A.toUpperCase(), note: "  seat match  " },
      { people_id: 200, candidate_id: null, note: "retiring; not a candidate" },
    ],
  };

  it("reads entries and folds ids to lowercase", () => {
    const crosswalk = parseLegiscanCrosswalkFile(file, "TX");
    expect(crosswalk.byPeopleId.get(16788)).toEqual({ peopleId: 16788, candidateId: UUID_A, note: "seat match" });
    expect(crosswalk.byPeopleId.get(200)!.candidateId).toBeNull();
  });

  it("rejects the wrong source, state, duplicates, and bad ids", () => {
    expect(() => parseLegiscanCrosswalkFile({ ...file, source: "openstates" }, "TX")).toThrow('source must be "legiscan"');
    expect(() => parseLegiscanCrosswalkFile(file, "OH")).toThrow("jurisdiction");
    expect(() =>
      parseLegiscanCrosswalkFile({ ...file, entries: [...file.entries, { people_id: 16788, candidate_id: null }] }, "TX")
    ).toThrow("twice");
    expect(() =>
      parseLegiscanCrosswalkFile({ ...file, entries: [{ people_id: 1, candidate_id: "not-a-uuid" }] }, "TX")
    ).toThrow("UUID or null");
  });
});

describe("resolveLegiscanMembers", () => {
  const crosswalk = parseLegiscanCrosswalkFile(
    {
      source: "legiscan",
      jurisdiction: "TX",
      entries: [
        { people_id: 1, candidate_id: UUID_A },
        { people_id: 2, candidate_id: null, note: "no candidate" },
        { people_id: 3, candidate_id: UUID_B },
      ],
    },
    "TX"
  );
  const peopleById = new Map([[1, person({ peopleId: 1 })]]);
  const candidatesById = new Map([
    [UUID_A, { candidateId: UUID_A, name: "Joseph Preston", inScope: true }],
    [UUID_B, { candidateId: UUID_B, name: "Riley Poole", inScope: false }],
  ]);

  it("resolves every outcome, keeping sides", () => {
    const resolutions = resolveLegiscanMembers({ yeas: [1, 2], nays: [3, 4] }, crosswalk, peopleById, candidatesById);
    expect(resolutions.map((resolution) => [resolution.peopleId, resolution.side, resolution.outcome])).toEqual([
      [1, "yea", "matched"],
      [2, "yea", "unmatched_reviewed"],
      [3, "nay", "out_of_scope"],
      [4, "nay", "no_crosswalk"],
    ]);
    // A member absent from the snapshot still resolves through the file.
    expect(resolutions[2]!.person).toBeNull();
    expect(resolutions[0]!.person!.name).toBe("Joseph Preston");
  });

  it("throws on a crosswalk candidate the loader did not return", () => {
    expect(() => resolveLegiscanMembers({ yeas: [3], nays: [] }, crosswalk, peopleById, new Map())).toThrow(
      "unknown candidate"
    );
  });
});

describe("district parsing", () => {
  it("reads numeric LegiScan districts and rejects named ones", () => {
    expect(parseLegiscanDistrict("HD-063")).toEqual({ chamber: "house", number: 63 });
    expect(parseLegiscanDistrict("SD-01")).toEqual({ chamber: "senate", number: 1 });
    expect(parseLegiscanDistrict("HD-Hillsborough-37")).toBeNull();
  });

  it("reads the candidate district number out of our district names", () => {
    expect(parseCandidateDistrictNumber("State House District 83 (2024); Texas")).toBe(83);
    expect(parseCandidateDistrictNumber("State Senate District 07 (2024); Ohio")).toBe(7);
    expect(parseCandidateDistrictNumber("Chittenden-6-1 State House District; Vermont")).toBeNull();
  });
});

describe("proposeLegiscanCrosswalk", () => {
  it("proposes a unique name match and reports seat agreement", () => {
    const { proposals, unmatchedPeople, unmatchedCandidates } = proposeLegiscanCrosswalk([person()], [candidate()]);
    expect(proposals).toEqual([
      {
        peopleId: 16788,
        rosterName: "Joseph Preston",
        rosterSeat: "house HD-063",
        candidateId: UUID_A,
        candidateName: "Joseph Preston",
        candidacy: "state_lower: State House District 63 (2024); Texas",
        confidence: "first_and_last",
        seatAgrees: true,
      },
    ]);
    expect(unmatchedPeople).toEqual([]);
    expect(unmatchedCandidates).toEqual([]);
  });

  it("flags a seat mismatch without vetoing, and goes null when a side cannot parse", () => {
    const senateRun = candidate({ scope: "state_upper", districtName: "State Senate District 5 (2024); Texas" });
    const mismatch = proposeLegiscanCrosswalk([person()], [senateRun]).proposals[0]!;
    expect(mismatch.seatAgrees).toBe(false);
    const named = proposeLegiscanCrosswalk([person({ district: "HD-Hillsborough-37" })], [candidate()]).proposals[0]!;
    expect(named.seatAgrees).toBeNull();
  });

  it("proposes nothing when a name matches in two directions", () => {
    const twoCandidates = [candidate(), candidate({ candidateId: UUID_B, name: "Jo Preston" })];
    expect(proposeLegiscanCrosswalk([person()], twoCandidates).proposals).toEqual([]);
    const twoMembers = [person(), person({ peopleId: 2, firstName: "Jo", name: "Jo Preston" })];
    expect(proposeLegiscanCrosswalk(twoMembers, [candidate()]).proposals).toEqual([]);
  });

  it("requires the last name to be the token tail and first names to agree", () => {
    expect(proposeLegiscanCrosswalk([person()], [candidate({ name: "Joseph Preston Garcia" })]).proposals).toEqual([]);
    expect(proposeLegiscanCrosswalk([person()], [candidate({ name: "Martha Preston" })]).proposals).toEqual([]);
    // Prefix first names (Josephine/Joseph) still propose, at lower
    // confidence; a nickname that is not a literal prefix (Joe/Joseph) is a
    // known miss the reviewer resolves by hand, like Mike/Michael in Ohio.
    expect(proposeLegiscanCrosswalk([person({ firstName: "Josephine" })], [candidate()]).proposals[0]?.confidence).toBe(
      "first_prefix"
    );
    expect(proposeLegiscanCrosswalk([person({ firstName: "Joe" })], [candidate()]).proposals).toEqual([]);
  });
});
