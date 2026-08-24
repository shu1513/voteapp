import { describe, expect, it } from "vitest";

import type { OhioLegislator } from "../../../src/pipeline/rollcall/ohioMemberResolver.js";
import {
  parseOhioCrosswalkFile,
  parseOhioLegislators,
  proposeOhioCrosswalk,
  resolveOhioMembers,
} from "../../../src/pipeline/rollcall/ohioMemberResolver.js";

const UUID_A = "11111111-1111-4111-8111-111111111111";
const UUID_B = "22222222-2222-4222-8222-222222222222";

function legislator(overrides: Partial<OhioLegislator>): OhioLegislator {
  return {
    lpid: "rep_smith_kent_1",
    firstName: "Kent",
    lastName: "Smith",
    displayName: "Kent Smith",
    district: "21",
    party: "party_democrat_1",
    chamber: "house",
    active: true,
    ...overrides,
  };
}

describe("parseOhioLegislators", () => {
  it("reads the roster feed and rejects duplicates and odd chambers", () => {
    const roster = parseOhioLegislators({
      "0": {
        lpid: "sen_wilson_steve_1",
        firstname: "Steve",
        lastname: "Wilson",
        displayname: "Steve Wilson",
        district: "7",
        party: "party_republican_1",
        chamber: "Senate",
        active: true,
      },
    });
    expect(roster).toHaveLength(1);
    expect(roster[0]).toMatchObject({ lpid: "sen_wilson_steve_1", chamber: "senate", district: "7", active: true });
    const row = {
      lpid: "x_1",
      firstname: "A",
      lastname: "B",
      displayname: "A B",
      district: "1",
      chamber: "House",
      active: false,
    };
    expect(() => parseOhioLegislators({ "0": row, "1": row })).toThrow("twice");
    // Vacant seats are placeholder rows, not people.
    expect(
      parseOhioLegislators({
        "0": row,
        "1": { lpid: "rep_district_26", firstname: "", lastname: "Vacant", displayname: "", district: "26", chamber: "House", active: false },
      })
    ).toHaveLength(1);
    expect(() => parseOhioLegislators({ "0": { ...row, chamber: "Joint" } })).toThrow("chamber is Joint");
  });
});

describe("parseOhioCrosswalkFile", () => {
  const good = {
    jurisdiction: "OH",
    general_assembly: 136,
    entries: [
      { lpid: "rep_smith_kent_1", candidate_id: UUID_A, note: "Kent Smith, House 21" },
      { lpid: "rep_gone_pat_1", candidate_id: null, note: "not a Nov-2026 candidate" },
    ],
  };

  it("reads a good file and normalizes ids", () => {
    const crosswalk = parseOhioCrosswalkFile(good);
    expect(crosswalk.generalAssembly).toBe(136);
    expect(crosswalk.byLpid.get("rep_smith_kent_1")?.candidateId).toBe(UUID_A);
    expect(crosswalk.byLpid.get("rep_gone_pat_1")).toMatchObject({ candidateId: null, note: "not a Nov-2026 candidate" });
  });

  it("rejects duplicates and malformed entries, allows one candidate under two lpids", () => {
    expect(() => parseOhioCrosswalkFile({ ...good, jurisdiction: "US" })).toThrow('jurisdiction must be "OH"');
    expect(() =>
      parseOhioCrosswalkFile({ ...good, entries: [good.entries[0], good.entries[0]] })
    ).toThrow("twice");
    expect(() =>
      parseOhioCrosswalkFile({ ...good, entries: [{ lpid: "a_1", candidate_id: "not-a-uuid" }] })
    ).toThrow("UUID or null");
    // A representative appointed to the Senate mid-term carries two lpids.
    const twoSeats = parseOhioCrosswalkFile({
      ...good,
      entries: [
        { lpid: "rep_smith_kent_1", candidate_id: UUID_A },
        { lpid: "sen_smith_kent_1", candidate_id: UUID_A },
      ],
    });
    expect(twoSeats.byLpid.size).toBe(2);
  });
});

describe("resolveOhioMembers", () => {
  const crosswalk = parseOhioCrosswalkFile({
    jurisdiction: "OH",
    general_assembly: 136,
    entries: [
      { lpid: "rep_smith_kent_1", candidate_id: UUID_A },
      { lpid: "rep_gone_pat_1", candidate_id: null },
      { lpid: "rep_lost_lee_1", candidate_id: UUID_B },
    ],
  });
  const roster = new Map([["rep_smith_kent_1", legislator({})]]);
  const candidates = new Map([
    [UUID_A, { candidateId: UUID_A, name: "Kent Smith", inScope: true }],
    [UUID_B, { candidateId: UUID_B, name: "Lee Lost", inScope: false }],
  ]);

  it("covers every outcome and keeps sides", () => {
    const resolutions = resolveOhioMembers(
      { yeas: ["rep_smith_kent_1", "rep_new_ana_1"], nays: ["rep_gone_pat_1", "rep_lost_lee_1"] },
      crosswalk,
      roster,
      candidates
    );
    expect(resolutions.map((r) => [r.lpid, r.side, r.outcome])).toEqual([
      ["rep_smith_kent_1", "yea", "matched"],
      ["rep_new_ana_1", "yea", "no_crosswalk"],
      ["rep_gone_pat_1", "nay", "unmatched_reviewed"],
      ["rep_lost_lee_1", "nay", "out_of_scope"],
    ]);
    expect(resolutions[0]?.candidate?.name).toBe("Kent Smith");
    // A member no longer on the current roster still resolves through the
    // crosswalk (old journal actions name resigned members).
    expect(resolutions[3]?.legislator).toBeNull();
  });

  it("throws on a crosswalk candidate the loader did not return", () => {
    expect(() => resolveOhioMembers({ yeas: ["rep_lost_lee_1"], nays: [] }, crosswalk, roster, new Map())).toThrow(
      "unknown candidate"
    );
  });
});

describe("proposeOhioCrosswalk", () => {
  const candidates = [
    { candidateId: UUID_A, name: "Kent Smith", scope: "state_lower", districtName: "State House District 21 (2024); Ohio" },
    { candidateId: UUID_B, name: "Alessandro Cutrona", scope: "state_upper", districtName: "State Senate District 33 (2024); Ohio" },
  ];

  it("proposes exact and prefix first-name matches, tagged by confidence", () => {
    const { proposals } = proposeOhioCrosswalk(
      [legislator({}), legislator({ lpid: "sen_cutrona_al_1", firstName: "Al", lastName: "Cutrona", displayName: "Al Cutrona", chamber: "senate", district: "33" })],
      candidates
    );
    expect(proposals).toHaveLength(2);
    expect(proposals.find((p) => p.lpid === "rep_smith_kent_1")).toMatchObject({ candidateId: UUID_A, confidence: "first_and_last" });
    expect(proposals.find((p) => p.lpid === "sen_cutrona_al_1")).toMatchObject({ candidateId: UUID_B, confidence: "first_prefix" });
  });

  it("never proposes on last name alone, ambiguity, or a last-name-as-first-name collision", () => {
    // "Stacie Baker" the candidate vs "Rachel Baker" the member: last name
    // alone must propose nothing.
    const baker = proposeOhioCrosswalk(
      [legislator({ lpid: "rep_baker_rachel_1", firstName: "Rachel", lastName: "Baker", displayName: "Rachel Baker" })],
      [{ candidateId: UUID_A, name: "Stacie Baker", scope: "state_upper", districtName: "State Senate District 3 (2024); Ohio" }]
    );
    expect(baker.proposals).toHaveLength(0);
    expect(baker.unmatchedRoster).toHaveLength(1);
    expect(baker.unmatchedCandidates).toHaveLength(1);
    // "Craig Riedel" the candidate contains "Craig" only as a FIRST token;
    // members surnamed Craig must not match (the tail rule).
    const craig = proposeOhioCrosswalk(
      [legislator({ lpid: "sen_craig_hearcel_1", firstName: "Hearcel", lastName: "Craig", displayName: "Hearcel Craig", chamber: "senate" })],
      [{ candidateId: UUID_A, name: "Craig Riedel", scope: "state_upper", districtName: "State Senate District 1 (2024); Ohio" }]
    );
    expect(craig.proposals).toHaveLength(0);
    // Two members plausibly matching one candidate → nothing proposed.
    const twins = proposeOhioCrosswalk(
      [
        legislator({ lpid: "rep_smith_kent_1" }),
        legislator({ lpid: "rep_smith_kenton_1", firstName: "Kenton", displayName: "Kenton Smith", district: "40" }),
      ],
      [candidates[0]!]
    );
    expect(twins.proposals).toHaveLength(0);
  });

  it("matches hyphenated last names as token tails", () => {
    const { proposals } = proposeOhioCrosswalk(
      [
        legislator({
          lpid: "sen_hicks_hudson_paula_1",
          firstName: "Paula",
          lastName: "Hicks-Hudson",
          displayName: "Paula Hicks-Hudson",
          chamber: "senate",
          district: "11",
        }),
      ],
      [{ candidateId: UUID_A, name: "Paula Hicks-Hudson", scope: "state_upper", districtName: "State Senate District 11 (2024); Ohio" }]
    );
    expect(proposals).toHaveLength(1);
    expect(proposals[0]).toMatchObject({ candidateId: UUID_A, confidence: "first_and_last" });
  });
});
