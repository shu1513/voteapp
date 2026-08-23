import { describe, expect, it, vi } from "vitest";

import { indexLegislators, type Legislator } from "../../../src/pipeline/rollcall/congressLegislators.js";
import {
  loadCandidateFecIndex,
  resolveFederalMember,
  resolveFederalMembers,
  type CandidateFecIndex,
} from "../../../src/pipeline/rollcall/federalMemberResolver.js";
import type { FederalMemberVote } from "../../../src/pipeline/rollcall/federalRollCallMembers.js";
import { migrationTableColumns } from "../../helpers/migrationTableColumns.js";

const ADAMS: Legislator = {
  bioguide: "A000370",
  lis: null,
  fecIds: ["H4NC12100"],
  name: "Alma S. Adams",
  terms: [
    { type: "rep", start: "2023-01-03", end: "2025-01-03", state: "NC", district: 12 },
    { type: "rep", start: "2025-01-03", end: "2027-01-03", state: "NC", district: 12 },
  ],
};
// Left the Senate on 2025-01-03; two FEC ids (House run, then Senate).
const BROWN: Legislator = {
  bioguide: "B000944",
  lis: "S307",
  fecIds: ["H2OH13033", "S6OH00163"],
  name: "Sherrod Brown",
  terms: [
    { type: "rep", start: "1993-01-05", end: "2007-01-03", state: "OH", district: 13 },
    { type: "sen", start: "2007-01-04", end: "2025-01-03", state: "OH", district: null },
  ],
};
const NO_FEC: Legislator = {
  bioguide: "N000000",
  lis: null,
  fecIds: [],
  name: "No Fec",
  terms: [{ type: "rep", start: "2025-01-03", end: "2027-01-03", state: "PR", district: 0 }],
};
const LEGISLATORS = indexLegislators([ADAMS, BROWN, NO_FEC]);

const ADAMS_CANDIDATE = { candidateId: "cand-adams", name: "Alma Adams", inScope: true };
const BROWN_CANDIDATE = { candidateId: "cand-brown", name: "Sherrod Brown", inScope: true };

function index(entries: Record<string, { candidateId: string; name: string; inScope: boolean }[]>): CandidateFecIndex {
  return new Map(Object.entries(entries));
}

const ADAMS_VOTE: FederalMemberVote = {
  chamber: "house",
  memberId: "A000370",
  name: "Adams",
  state: "NC",
  party: "D",
  vote: "Nay",
};
const BROWN_VOTE: FederalMemberVote = {
  chamber: "senate",
  memberId: "S307",
  name: "Brown (D-OH)",
  state: "OH",
  party: "D",
  vote: "Yea",
};

describe("resolveFederalMember", () => {
  it("matches a House member by bioguide → FEC id → in-scope candidate", () => {
    const resolution = resolveFederalMember(ADAMS_VOTE, "2025-05-22", LEGISLATORS, index({ H4NC12100: [ADAMS_CANDIDATE] }));
    expect(resolution.outcome).toBe("matched");
    expect(resolution.candidate).toEqual(ADAMS_CANDIDATE);
    expect(resolution.legislator).toEqual({ bioguide: "A000370", name: "Alma S. Adams", fecIds: ["H4NC12100"] });
  });

  it("matches a senator by LIS id through any of the person's FEC ids", () => {
    const resolution = resolveFederalMember(BROWN_VOTE, "2024-12-18", LEGISLATORS, index({ S6OH00163: [BROWN_CANDIDATE] }));
    expect(resolution.outcome).toBe("matched");
    expect(resolution.candidate?.candidateId).toBe("cand-brown");
  });

  it("reports an id the crosswalk does not know", () => {
    const resolution = resolveFederalMember({ ...ADAMS_VOTE, memberId: "Z999999" }, "2025-05-22", LEGISLATORS, index({}));
    expect(resolution.outcome).toBe("unknown_member");
    expect(resolution.legislator).toBeNull();
    expect(resolution.detail).toBe("Z999999 is not in congress-legislators");
  });

  it("does not look up a House bioguide in the Senate LIS index or vice versa", () => {
    const asSenator = resolveFederalMember({ ...ADAMS_VOTE, chamber: "senate" }, "2025-05-22", LEGISLATORS, index({}));
    expect(asSenator.outcome).toBe("unknown_member");
  });

  it("requires a term of that chamber and state covering the vote date (inclusive)", () => {
    const candidates = index({ S6OH00163: [BROWN_CANDIDATE], H4NC12100: [ADAMS_CANDIDATE] });
    // Brown's last day in the Senate still counts; the day after does not.
    expect(resolveFederalMember(BROWN_VOTE, "2025-01-03", LEGISLATORS, candidates).outcome).toBe("matched");
    const after = resolveFederalMember(BROWN_VOTE, "2025-01-04", LEGISLATORS, candidates);
    expect(after.outcome).toBe("term_mismatch");
    expect(after.detail).toBe("no sen term in OH covers 2025-01-04");
    expect(after.legislator?.name).toBe("Sherrod Brown");
    // Adams' two consecutive terms share 2025-01-03.
    expect(resolveFederalMember(ADAMS_VOTE, "2025-01-03", LEGISLATORS, candidates).outcome).toBe("matched");
    // Same person, wrong state in the XML.
    expect(resolveFederalMember({ ...ADAMS_VOTE, state: "SC" }, "2025-05-22", LEGISLATORS, candidates).outcome).toBe(
      "term_mismatch"
    );
    // A House bioguide voting in the Senate file would need a sen term.
    expect(
      resolveFederalMember({ ...BROWN_VOTE, chamber: "house", memberId: "B000944" }, "2024-12-18", LEGISLATORS, candidates)
        .outcome
    ).toBe("term_mismatch");
  });

  it("reports a person without FEC ids, and a person no candidate carries", () => {
    const noFec = resolveFederalMember(
      { ...ADAMS_VOTE, memberId: "N000000", state: "PR" },
      "2025-05-22",
      LEGISLATORS,
      index({})
    );
    expect(noFec.outcome).toBe("no_fec_id");
    const noCandidate = resolveFederalMember(ADAMS_VOTE, "2025-05-22", LEGISLATORS, index({}));
    expect(noCandidate.outcome).toBe("no_candidate");
    expect(noCandidate.detail).toBe("no candidate carries H4NC12100");
    expect(noCandidate.candidates).toEqual([]);
  });

  it("reports a single out-of-scope candidate rather than matching it", () => {
    const stale = { ...ADAMS_CANDIDATE, inScope: false };
    const resolution = resolveFederalMember(ADAMS_VOTE, "2025-05-22", LEGISLATORS, index({ H4NC12100: [stale] }));
    expect(resolution.outcome).toBe("out_of_scope");
    expect(resolution.candidate).toBeNull();
    expect(resolution.candidates).toEqual([stale]);
  });

  it("reports ambiguity when the FEC ids land on more than one candidate row, whatever their scope", () => {
    const duplicate = { candidateId: "cand-brown-dupe", name: "Sherrod Brown (dupe)", inScope: false };
    const resolution = resolveFederalMember(
      BROWN_VOTE,
      "2024-12-18",
      LEGISLATORS,
      index({ H2OH13033: [duplicate], S6OH00163: [BROWN_CANDIDATE] })
    );
    expect(resolution.outcome).toBe("ambiguous");
    expect(resolution.candidate).toBeNull();
    expect(resolution.candidates.map((candidate) => candidate.candidateId)).toEqual(["cand-brown-dupe", "cand-brown"]);
    expect(resolution.detail).toBe("H2OH13033, S6OH00163 land on 2 candidates");
  });

  it("counts one candidate once even when several of the person's FEC ids point at it", () => {
    const resolution = resolveFederalMember(
      BROWN_VOTE,
      "2024-12-18",
      LEGISLATORS,
      index({ H2OH13033: [BROWN_CANDIDATE], S6OH00163: [BROWN_CANDIDATE] })
    );
    expect(resolution.outcome).toBe("matched");
    expect(resolution.candidates).toHaveLength(1);
  });
});

describe("resolveFederalMembers", () => {
  it("resolves every row in order", () => {
    const resolutions = resolveFederalMembers(
      [ADAMS_VOTE, { ...ADAMS_VOTE, memberId: "Z999999" }],
      "2025-05-22",
      LEGISLATORS,
      index({ H4NC12100: [ADAMS_CANDIDATE] })
    );
    expect(resolutions.map((resolution) => resolution.outcome)).toEqual(["matched", "unknown_member"]);
  });
});

describe("loadCandidateFecIndex", () => {
  it("builds the FEC → candidates map from one query, upper-casing and de-duplicating ids", async () => {
    const query = vi.fn().mockResolvedValue({
      rows: [
        { candidate_id: "c1", name: "Alma Adams", fec_id: "H4NC12100", in_scope: true },
        { candidate_id: "c1", name: "Alma Adams", fec_id: "H4NC12100", in_scope: true },
        { candidate_id: "c2", name: "Sherrod Brown", fec_id: "S6OH00163", in_scope: false },
        { candidate_id: "c3", name: "Someone Else", fec_id: "S6OH00163", in_scope: true },
        { candidate_id: "c4", name: "Blank", fec_id: "  ", in_scope: true },
      ],
    });
    const loaded = await loadCandidateFecIndex({ query } as never, "2026-11-01");
    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[1]).toEqual(["2026-11-01"]);
    expect(loaded.get("H4NC12100")).toEqual([{ candidateId: "c1", name: "Alma Adams", inScope: true }]);
    expect(loaded.get("S6OH00163")?.map((candidate) => candidate.candidateId)).toEqual(["c2", "c3"]);
    expect(loaded.size).toBe(2);
  });

  it("rejects a scope date that is not ISO", async () => {
    const query = vi.fn();
    await expect(loadCandidateFecIndex({ query } as never, "Nov 2026")).rejects.toThrow(/scopeFrom must be an ISO date/);
    expect(query).not.toHaveBeenCalled();
  });

  it("only names columns the migrations build", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [] });
    await loadCandidateFecIndex({ query } as never, "2026-11-01");
    const sql = query.mock.calls[0]?.[0] as string;
    const tables: Record<string, Set<string>> = {
      c: migrationTableColumns("candidates"),
      ce: migrationTableColumns("candidate_elections"),
      e: migrationTableColumns("elections"),
    };
    const references = [...sql.matchAll(/\b(c|ce|e)\.([a-z_]+)/g)];
    expect(references.length).toBeGreaterThan(5);
    for (const [, alias, column] of references) {
      expect(tables[alias!]!.has(column!), `${alias}.${column}`).toBe(true);
    }
  });
});
