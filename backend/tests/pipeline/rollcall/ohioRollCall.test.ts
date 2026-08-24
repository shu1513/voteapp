import { describe, expect, it } from "vitest";

import {
  classifyOhioVoteAction,
  ohioActionChamber,
  ohioActionHasVotes,
  ohioActionMemberVotes,
  ohioActionSha256,
  ohioActionsUrl,
  ohioActionVoteDate,
  ohioDisplayUrl,
  ohioEvidenceFileName,
  ohioKeptFloorDayCollisions,
  ohioMeasureId,
  ohioRollNumber,
  parseOhioBillNumber,
  parseOhioSessionDay,
  parseOhioVoteEvidence,
} from "../../../src/pipeline/rollcall/ohioRollCall.js";

describe("parseOhioBillNumber / ohioMeasureId", () => {
  it("reads every type and rejects everything else", () => {
    expect(parseOhioBillNumber("hb96")).toEqual({ type: "hb", number: "96" });
    expect(parseOhioBillNumber("SJR3")).toEqual({ type: "sjr", number: "3" });
    // hr must not swallow the leading of hjr/hcr (longest first).
    expect(parseOhioBillNumber("hjr2")).toEqual({ type: "hjr", number: "2" });
    expect(parseOhioBillNumber("hcr47")).toEqual({ type: "hcr", number: "47" });
    expect(parseOhioBillNumber("hr521")).toEqual({ type: "hr", number: "521" });
    expect(parseOhioBillNumber("am1234")).toBeNull();
    expect(parseOhioBillNumber("hb")).toBeNull();
    expect(ohioMeasureId({ type: "hb", number: "96" })).toBe("HB 96");
    expect(ohioMeasureId({ type: "sjr", number: "3" })).toBe("SJR 3");
  });
});

describe("URLs", () => {
  it("builds the probed shapes", () => {
    expect(ohioActionsUrl(136, "hb96")).toBe(
      "https://search-prod.lis.state.oh.us/api/v2/general_assembly_136/legislation/hb96/actions/"
    );
    expect(ohioDisplayUrl(136, "hb96")).toBe("https://www.legislature.ohio.gov/legislation/136/hb96");
  });
});

describe("action parsing", () => {
  it("maps chambers and rejects others", () => {
    expect(ohioActionChamber({ chamber: "House" })).toBe("house");
    expect(ohioActionChamber({ chamber: "Senate" })).toBe("senate");
    expect(() => ohioActionChamber({ chamber: "Joint" })).toThrow("not House or Senate");
  });

  it("takes the vote date from the journal session day first", () => {
    expect(parseOhioSessionDay("day045_s_20250611")).toBe("2025-06-11");
    expect(() => parseOhioSessionDay("2025-06-11")).toThrow("session_day");
    expect(ohioActionVoteDate({ session_day: "day063_h_20250721" })).toBe("2025-07-21");
    expect(ohioActionVoteDate({ session_day: "", date: "2025-07-01" })).toBe("2025-07-01");
    expect(() => ohioActionVoteDate({})).toThrow("neither");
  });

  it("derives the surrogate roll number from occurred, in range", () => {
    // 2025-06-11T13:05:12-04:00 = 2025-06-11T17:05:12Z.
    expect(ohioRollNumber({ occurred: "2025-06-11T13:05:12-04:00" })).toBe(Date.parse("2025-06-11T17:05:12Z") / 1000);
    expect(() => ohioRollNumber({ occurred: "2025-06-11" })).toThrow("offset timestamp");
    expect(() => ohioRollNumber({})).toThrow("offset timestamp");
    expect(() => ohioRollNumber({ occurred: "2039-01-01T00:00:00+00:00" })).toThrow("storable range");
  });

  it("checks the member lists", () => {
    expect(ohioActionHasVotes({ yeas: ["a"], nays: [] })).toBe(true);
    expect(ohioActionHasVotes({ yeas: [], nays: [] })).toBe(false);
    expect(ohioActionHasVotes({})).toBe(false);
    expect(ohioActionMemberVotes({ yeas: ["a", "b"], nays: ["c"] })).toEqual({ yeas: ["a", "b"], nays: ["c"] });
    expect(ohioActionMemberVotes({ yeas: ["a"] })).toEqual({ yeas: ["a"], nays: [] });
    expect(() => ohioActionMemberVotes({ yeas: ["a"], nays: ["a"] })).toThrow("both yea and nay");
    expect(() => ohioActionMemberVotes({ yeas: ["a", "a"], nays: [] })).toThrow("twice");
    expect(() => ohioActionMemberVotes({ yeas: [1], nays: [] })).toThrow("array of lpids");
  });
});

describe("classifyOhioVoteAction", () => {
  const hb = { type: "hb", number: "96" } as const;

  it("keeps the four floor classes on bills and joint resolutions", () => {
    expect(classifyOhioVoteAction({ actionCode: "pass_300", measure: hb })).toEqual({
      isFloorVote: true,
      questionClass: "passage",
      reason: "kept:passage",
    });
    expect(classifyOhioVoteAction({ actionCode: "msg_507", measure: hb }).questionClass).toBe("concurrence");
    expect(classifyOhioVoteAction({ actionCode: "concur_606", measure: hb }).questionClass).toBe("concurrence");
    expect(classifyOhioVoteAction({ actionCode: "confer_713", measure: hb }).questionClass).toBe("conference_report");
    expect(classifyOhioVoteAction({ actionCode: "govern_858", measure: { type: "sjr", number: "3" } })).toEqual({
      isFloorVote: true,
      questionClass: "veto_override",
      reason: "kept:veto_override",
    });
  });

  it("excludes committee reports, refusals, simple/concurrent resolutions, and unknown codes", () => {
    // A conference-report floor vote can carry the conference committee's
    // name, so committee-ness comes from the code, never cmte_name.
    expect(classifyOhioVoteAction({ actionCode: "crpt_301", measure: hb })).toEqual({
      isFloorVote: false,
      questionClass: null,
      reason: "committee:crpt_301",
    });
    expect(classifyOhioVoteAction({ actionCode: "msg_506", measure: hb }).isFloorVote).toBe(false);
    expect(classifyOhioVoteAction({ actionCode: "concur_608", measure: hb }).isFloorVote).toBe(false);
    expect(classifyOhioVoteAction({ actionCode: "pass_300", measure: { type: "hr", number: "1" } })).toEqual({
      isFloorVote: false,
      questionClass: "passage",
      reason: "excluded_measure:hr",
    });
    expect(classifyOhioVoteAction({ actionCode: "emerg_999", measure: hb })).toEqual({
      isFloorVote: null,
      questionClass: null,
      reason: "unknown_action:emerg_999",
    });
  });
});

describe("evidence", () => {
  it("hashes the action element deterministically across a parse round-trip", () => {
    const action = { action_code: "pass_300", yeas: ["rep_a_1"], nays: [], occurred: "2025-06-11T13:05:12-04:00" };
    const roundTripped = JSON.parse(JSON.stringify(action)) as unknown;
    expect(ohioActionSha256(action)).toBe(ohioActionSha256(roundTripped));
    expect(ohioActionSha256(action)).not.toBe(ohioActionSha256({ ...action, yeas: ["rep_b_1"] }));
  });

  it("names and re-reads evidence files against their names", () => {
    expect(ohioEvidenceFileName("house", 136, 1749661512)).toBe("oh-house-136-roll1749661512.json");
    const evidence = {
      jurisdiction: "OH",
      generalAssembly: 136,
      chamber: "house",
      rollNumber: 1749661512,
      bill: "hb96",
      measureId: "HB 96",
      machineUrl: "https://search-prod.lis.state.oh.us/api/v2/general_assembly_136/legislation/hb96/actions/",
      fetchedAt: "2026-08-23T00:00:00.000Z",
      action: { action_code: "pass_300" },
    };
    const expected = { chamber: "house", generalAssembly: 136, rollNumber: 1749661512 } as const;
    expect(parseOhioVoteEvidence(evidence, expected).bill).toBe("hb96");
    expect(() => parseOhioVoteEvidence({ ...evidence, chamber: "senate" }, expected)).toThrow("file name says");
    expect(() => parseOhioVoteEvidence({ ...evidence, action: null }, expected)).toThrow("action is missing");
    expect(() => parseOhioVoteEvidence({ ...evidence, jurisdiction: "US" }, expected)).toThrow("jurisdiction");
  });
});

describe("ohioKeptFloorDayCollisions", () => {
  const hb = { type: "hb", number: "96" } as const;
  const kept = (chamber: string, sessionDay: string, code = "pass_300") => ({
    action_code: code,
    chamber,
    session_day: sessionDay,
    yeas: ["rep_a_1"],
    nays: [],
  });

  it("flags only a day holding two KEPT floor votes of one chamber", () => {
    expect(
      ohioKeptFloorDayCollisions(
        [kept("House", "day010_h_20250301"), kept("House", "day011_h_20250302"), kept("Senate", "day010_s_20250301")],
        hb
      )
    ).toEqual(new Set());
    expect(
      ohioKeptFloorDayCollisions([kept("House", "day010_h_20250301"), kept("House", "day010_h_20250301", "msg_507")], hb)
    ).toEqual(new Set(["house:2025-03-01"]));
    // Committee and excluded floor votes never collide with a kept one.
    expect(
      ohioKeptFloorDayCollisions(
        [kept("House", "day010_h_20250301"), kept("House", "day010_h_20250301", "crpt_301"), kept("House", "day010_h_20250301", "msg_506")],
        hb
      )
    ).toEqual(new Set());
    // An unreadable action is the per-action loop's problem, not a collision.
    expect(
      ohioKeptFloorDayCollisions([kept("House", "day010_h_20250301"), { action_code: "pass_300", chamber: "Joint" }], hb)
    ).toEqual(new Set());
  });
});
