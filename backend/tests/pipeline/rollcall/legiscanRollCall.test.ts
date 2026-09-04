import { describe, expect, it } from "vitest";

import {
  classifyLegiscanDatasetFile,
  classifyLegiscanRollCall,
  formatLegiscanMeasureId,
  legiscanEvidenceFileName,
  legiscanMemberVotes,
  legiscanRollCallPageUrl,
  isLegiscanCommitteeChamberRollCall,
  legiscanRollCallSha256,
  parseLegiscanBill,
  parseLegiscanRollCall,
  parseLegiscanVoteEvidence,
  LEGISCAN_EVIDENCE_FILE_PATTERN,
} from "../../../src/pipeline/rollcall/legiscanRollCall.js";
import {
  getLegiscanStateConfig,
  LEGISCAN_RECORD_JURISDICTIONS,
  LEGISCAN_STATE_CONFIGS,
  type LegiscanStateConfig,
} from "../../../src/pipeline/rollcall/legiscanStateConfigs.js";

// A synthetic state: 100-seat house, 30-seat senate, one kept and one
// excluded pattern each. Real configs are written per state from survey
// data; the classifier's behavior is what these tests pin.
const CONFIG: LegiscanStateConfig = {
  jurisdiction: "XX",
  sessionId: 2172,
  chamberSizes: { house: 100, senate: 30 },
  keptQuestions: [
    { pattern: /^third reading/, questionClass: "passage" },
    { pattern: /concurred in senate amendments/, questionClass: "concurrence" },
  ],
  excludedQuestions: [/refused to concur/],
};

function voteList(yeas: number, nays: number, nv = 0, absent = 0): { people_id: number; vote_id: number }[] {
  const votes: { people_id: number; vote_id: number }[] = [];
  let id = 1;
  for (const [voteId, n] of [
    [1, yeas],
    [2, nays],
    [3, nv],
    [4, absent],
  ] as const) {
    for (let i = 0; i < n; i += 1) {
      votes.push({ people_id: id, vote_id: voteId });
      id += 1;
    }
  }
  return votes;
}

function rollCallElement(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    roll_call_id: 1523456,
    bill_id: 460445,
    date: "2025-04-09",
    desc: "Third Reading",
    yea: 60,
    nay: 35,
    nv: 3,
    absent: 2,
    total: 100,
    passed: 1,
    chamber: "H",
    chamber_id: 79,
    votes: voteList(60, 35, 3, 2),
    ...overrides,
  };
}

describe("formatLegiscanMeasureId", () => {
  it("normalizes the compact feed spelling", () => {
    expect(formatLegiscanMeasureId("HB1")).toBe("HB 1");
    expect(formatLegiscanMeasureId("SB0544")).toBe("SB 544");
    expect(formatLegiscanMeasureId("sjr10")).toBe("SJR 10");
    expect(() => formatLegiscanMeasureId("RV#105")).toThrow("not <letters><digits>");
  });
});

describe("classifyLegiscanDatasetFile", () => {
  it("routes by envelope key, not directory", () => {
    expect(classifyLegiscanDatasetFile({ status: "OK", bill: { bill_id: 1 } }).kind).toBe("bill");
    expect(classifyLegiscanDatasetFile({ status: "OK", roll_call: { roll_call_id: 1 } }).kind).toBe("vote");
    expect(classifyLegiscanDatasetFile({ status: "OK", person: { people_id: 1 } }).kind).toBe("person");
    expect(classifyLegiscanDatasetFile({ status: "OK" }).kind).toBe("other");
    expect(classifyLegiscanDatasetFile([1, 2]).kind).toBe("other");
  });
});

describe("parseLegiscanBill", () => {
  const bill = {
    bill_id: 460445,
    bill_number: "SB0544",
    bill_type: "B",
    session: { session_id: 2172, session_name: "2025-2026 Regular Session" },
    state: "xx",
    title: "An act",
    url: "https://legiscan.com/XX/bill/SB544/2025",
    state_link: "https://legis.example.gov/SB544",
    votes: [{ roll_call_id: 1523456, url: "https://legiscan.com/XX/rollcall/SB0544/id/1523456" }],
  };

  it("reads the fields the fetcher needs", () => {
    const parsed = parseLegiscanBill(bill);
    expect(parsed.measureId).toBe("SB 544");
    expect(parsed.state).toBe("XX");
    expect(parsed.sessionId).toBe(2172);
    expect(parsed.voteUrlsByRollCallId.get(1523456)).toBe("https://legiscan.com/XX/rollcall/SB0544/id/1523456");
  });

  it("tolerates a missing votes array and missing urls", () => {
    expect(parseLegiscanBill({ ...bill, votes: undefined }).voteUrlsByRollCallId.size).toBe(0);
    expect(parseLegiscanBill({ ...bill, votes: [{ roll_call_id: 9, url: "" }] }).voteUrlsByRollCallId.size).toBe(0);
  });

  it("rejects a bill without a session id", () => {
    expect(() => parseLegiscanBill({ ...bill, session: {} })).toThrow("session_id");
  });
});

describe("parseLegiscanRollCall", () => {
  it("round-trips a well-formed roll call", () => {
    const rollCall = parseLegiscanRollCall(rollCallElement());
    expect(rollCall.chamber).toBe("house");
    expect(rollCall.passed).toBe(true);
    expect(rollCall.votes).toHaveLength(100);
  });

  it("rejects tally mismatches between the summary and the member list", () => {
    expect(() => parseLegiscanRollCall(rollCallElement({ yea: 61 }))).toThrow("yea says 61 but the member list holds 60");
    expect(() => parseLegiscanRollCall(rollCallElement({ total: 99 }))).toThrow("total says 99 but the member list holds 100");
  });

  it("rejects a member listed twice and unknown vote_id values", () => {
    const votes = voteList(2, 1);
    votes[1] = { ...votes[1]!, people_id: votes[0]!.people_id };
    expect(() =>
      parseLegiscanRollCall(rollCallElement({ yea: 2, nay: 1, nv: 0, absent: 0, total: 3, votes }))
    ).toThrow("lists people_id 1 twice");
    expect(() =>
      parseLegiscanRollCall(
        rollCallElement({ yea: 0, nay: 0, nv: 0, absent: 0, total: 1, votes: [{ people_id: 1, vote_id: 9 }] })
      )
    ).toThrow("unknown vote_id 9");
  });

  it("accepts an unrecorded roll call (summary tallies, empty member list)", () => {
    // A Texas Senate non-record vote: real tallies, no positions. Not a
    // feed defect — the summary stands alone with nothing to cross-check.
    const rollCall = parseLegiscanRollCall(rollCallElement({ yea: 31, nay: 0, nv: 0, absent: 0, total: 31, votes: [] }));
    expect(rollCall.votes).toHaveLength(0);
    expect(rollCall.yea).toBe(31);
    const votes = legiscanMemberVotes(rollCall);
    expect(votes.yeas).toHaveLength(0);
    expect(votes.nays).toHaveLength(0);
  });

  it("still rejects an internally inconsistent summary, member list or not", () => {
    // total must equal yea+nay+nv+absent even when no positions are
    // published — the floor-vs-committee cut keys on total.
    expect(() =>
      parseLegiscanRollCall(rollCallElement({ yea: 31, nay: 0, nv: 0, absent: 0, total: 30, votes: [] }))
    ).toThrow("total says 30 but yea+nay+nv+absent is 31");
  });

  it("reads an Assembly chamber as the lower chamber", () => {
    // California (and NV/NJ/NY/WI) call the lower chamber the Assembly, and
    // LegiScan prints the state's own abbreviation: all 9,948 lower-chamber
    // rolls of CA 2172 carry `A`.
    expect(parseLegiscanRollCall(rollCallElement({ chamber: "A" })).chamber).toBe("house");
    expect(parseLegiscanRollCall(rollCallElement({ chamber: "S" })).chamber).toBe("senate");
  });

  it("rejects a roll_call_id outside the int4 range and a bad chamber", () => {
    expect(() => parseLegiscanRollCall(rollCallElement({ roll_call_id: 2_200_000_000 }))).toThrow("storable range");
    expect(() => parseLegiscanRollCall(rollCallElement({ chamber: "J" }))).toThrow("chamber is not H, A or S");
  });
});

describe("legiscanMemberVotes", () => {
  it("splits yeas and nays and counts the no-position members", () => {
    const votes = legiscanMemberVotes(parseLegiscanRollCall(rollCallElement()));
    expect(votes.yeas).toHaveLength(60);
    expect(votes.nays).toHaveLength(35);
    expect(votes.notVoting).toBe(3);
    expect(votes.absent).toBe(2);
  });
});

describe("classifyLegiscanRollCall", () => {
  const base = { chamber: "house" as const, billType: "B", config: CONFIG };

  it("keeps a kept desc at floor size and surfaces it below", () => {
    expect(classifyLegiscanRollCall({ ...base, desc: "Third  Reading", total: 100 })).toEqual({
      isFloorVote: true,
      questionClass: "passage",
      reason: "kept:passage",
    });
    // 60% of 100 is the floor line.
    expect(classifyLegiscanRollCall({ ...base, desc: "Third Reading", total: 60 }).isFloorVote).toBe(true);
    expect(classifyLegiscanRollCall({ ...base, desc: "Third Reading", total: 12 })).toEqual({
      isFloorVote: null,
      questionClass: "passage",
      reason: "kept_small_tally:12/100",
    });
  });

  it("checks excluded patterns before kept ones", () => {
    expect(classifyLegiscanRollCall({ ...base, desc: "Refused to concurred in Senate amendments", total: 95 })).toEqual({
      isFloorVote: false,
      questionClass: null,
      reason: "excluded_question",
    });
  });

  it("rejects an unknown desc with a committee-sized tally, surfaces the rest", () => {
    expect(classifyLegiscanRollCall({ ...base, desc: "DO PASS", total: 12 })).toEqual({
      isFloorVote: false,
      questionClass: null,
      reason: "committee_tally:12/100",
    });
    // The 50-60% gray zone and everything above it stays visible.
    expect(classifyLegiscanRollCall({ ...base, desc: "DO PASS", total: 55 })).toEqual({
      isFloorVote: null,
      questionClass: null,
      reason: "unknown_question",
    });
    expect(classifyLegiscanRollCall({ ...base, desc: "DO PASS", total: 98 }).reason).toBe("unknown_question");
  });

  it("excludes non-kept instrument types and sizes the senate separately", () => {
    expect(classifyLegiscanRollCall({ ...base, desc: "Third Reading", total: 95, billType: "R" })).toEqual({
      isFloorVote: false,
      questionClass: null,
      reason: "excluded_measure:R",
    });
    expect(classifyLegiscanRollCall({ ...base, chamber: "senate", desc: "Third Reading", total: 28 }).isFloorVote).toBe(true);
    expect(classifyLegiscanRollCall({ ...base, chamber: "senate", desc: "Third Reading", total: 12 }).reason).toBe(
      "kept_small_tally:12/30"
    );
  });

  it("surfaces a kept desc in a chamber the config does not size", () => {
    const config = { ...CONFIG, chamberSizes: { house: 100 } };
    expect(classifyLegiscanRollCall({ ...base, config, chamber: "senate", desc: "Third Reading", total: 28 })).toEqual({
      isFloorVote: null,
      questionClass: "passage",
      reason: "unknown_chamber_size:senate",
    });
  });
});

describe("evidence files", () => {
  it("names and recognizes evidence files", () => {
    const name = legiscanEvidenceFileName("TX", "house", 2172, 1523456);
    expect(name).toBe("ls-tx-house-2172-roll1523456.json");
    expect(LEGISCAN_EVIDENCE_FILE_PATTERN.exec(name)?.slice(1)).toEqual(["tx", "house", "2172", "1523456"]);
    expect(LEGISCAN_EVIDENCE_FILE_PATTERN.test("oh-house-136-roll1740000000.json")).toBe(false);
  });

  it("round-trips evidence and rejects a file that contradicts its name", () => {
    const element = rollCallElement();
    const evidence = {
      jurisdiction: "TX",
      sessionId: 2172,
      chamber: "house",
      rollNumber: 1523456,
      bill: "HB1",
      measureId: "HB 1",
      machineUrl: "https://legiscan.com/TX/rollcall/HB1/id/1523456",
      fetchedAt: "2026-08-24T00:00:00.000Z",
      rollCall: element,
    };
    const expected = { jurisdiction: "TX", chamber: "house" as const, sessionId: 2172, rollNumber: 1523456 };
    expect(parseLegiscanVoteEvidence(evidence, expected).bill).toBe("HB1");
    expect(() => parseLegiscanVoteEvidence({ ...evidence, sessionId: 2173 }, expected)).toThrow("sessionId");
    expect(() => parseLegiscanVoteEvidence({ ...evidence, rollCall: null }, expected)).toThrow("rollCall is missing");
  });

  it("pins the sha to the roll_call element bytes", () => {
    const element = rollCallElement();
    expect(legiscanRollCallSha256(element)).toBe(legiscanRollCallSha256(JSON.parse(JSON.stringify(element))));
    expect(legiscanRollCallSha256(element)).not.toBe(legiscanRollCallSha256(rollCallElement({ yea: 61, total: 101 })));
  });
});

describe("legiscanRollCallPageUrl", () => {
  it("builds the documented page shape", () => {
    expect(legiscanRollCallPageUrl("tx", "HB 1", 1523456)).toBe("https://legiscan.com/TX/rollcall/HB1/id/1523456");
  });
});


describe("Maryland's measured desc vocabulary", () => {
  const config = LEGISCAN_STATE_CONFIGS.MD!;
  const md = (desc: string, total: number, chamber: "house" | "senate" = "house") =>
    classifyLegiscanRollCall({ desc, total, chamber, billType: "B", config });

  it("keeps the three third-reading spellings and the session's one conference report", () => {
    // 2,295 of the session's 2,494 rolls are this exact string.
    expect(md("Third Reading Passed", 141)).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    expect(md("Third Reading Passed", 47, "senate")).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    // The same question, spelled singular in the House and plural in the Senate.
    expect(md("Third Reading Passed with Amendments", 141)).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
    expect(md("Third Readings Passed with Amendments", 47, "senate")).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
    expect(md("Conference Committee Report 903525/1 Adopted", 47, "senate")).toMatchObject({
      isFloorVote: true,
      questionClass: "conference_report",
    });
  });

  it("excludes the floor-sized procedural families by rule, never surfacing them", () => {
    for (const desc of [
      "Floor Amendment 273422/1 (Delegate Hornberger) Rejected",
      "Floor Amendment (Senator Carozza) Rejected",
      "Committee Amendment (Senator Beidle) Adopted",
      "Committee Amendment 123456/1 Adopted",
      "Motion Vote Previous Question (Delegate Wilkins) Adopted",
      "Motion Rules Suspend for Late Introduction (Delegate Barnes) Adopted",
      "Motion Special Order until Later This Session (Delegate Kipke) Rejected",
      "Motion Rules Suspend to Refer (Senator Ferguson) Rejected",
    ]) {
      expect(classifyLegiscanRollCall({ desc, total: 141, chamber: "house", billType: "B", config })).toMatchObject({
        isFloorVote: false,
      });
    }
  });
});

describe("Maryland 2026's measured desc vocabulary", () => {
  const config = LEGISCAN_STATE_CONFIGS["MD-2240"]!;
  const md = (desc: string, total: number, chamber: "house" | "senate" = "house") =>
    classifyLegiscanRollCall({ desc, total, chamber, billType: "B", config });

  it("keeps the measured final-passage spellings", () => {
    expect(md("Third Reading Passed", 141)).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    expect(md("Third Readings Passed with Amendments", 47, "senate")).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
    expect(md("Conference Committee Report Adopted", 47, "senate").questionClass).toBe("conference_report");
    expect(md("Conference Committee Report 583123/1 Adopted", 47, "senate").questionClass).toBe("conference_report");
    expect(md("Overridden", 141).questionClass).toBe("veto_override");
  });

  it("excludes each surveyed procedural family", () => {
    for (const desc of [
      "Decision of the Chair upheld",
      "Motion Limit Debate (Senator King) Adopted",
      "Committee Amendment (#69) Adopted",
      "Favorable with Amendments 923921/1 Adopted",
      "Floor Amendment (Delegate Buckel) Rejected",
    ]) {
      expect(md(desc, 141)).toMatchObject({ isFloorVote: false });
    }
  });
});

describe("Kentucky's two sessions read the same words differently", () => {
  const ky = (key: string, desc: string, chamber: "house" | "senate", billType = "B") =>
    classifyLegiscanRollCall({
      desc,
      total: chamber === "house" ? 100 : 38,
      chamber,
      billType,
      config: LEGISCAN_STATE_CONFIGS[key]!,
    });

  it("keeps the 2025 floor votes and excludes its procedural rolls", () => {
    expect(ky("KY", "House: Third Reading RCS# 157", "house")).toMatchObject({ isFloorVote: true });
    expect(ky("KY", "House: Adopt  RCS# 37", "house", "JR")).toMatchObject({ isFloorVote: true });
    expect(ky("KY", "Senate: Third Reading RSN# 3362", "senate")).toMatchObject({ isFloorVote: true });
    // The Senate names the adopted substitute and amendments in the desc.
    expect(ky("KY", "Senate: Third Reading W/scs1 sfa1 scta1 RSN# 3501", "senate")).toMatchObject({ isFloorVote: true });
    expect(ky("KY", "Senate: Third Reading W/sfta 19 RSN# 3503", "senate")).toMatchObject({ isFloorVote: true });
    for (const desc of [
      "House: Adopt HFA 1 RCS# 155",
      "House: Adopt HCS 1 RCS# 200",
      "House: Adopt SCS 1 RCS# 201",
      "Senate: Adopt SFA 5 RSN# 3501",
      "House: Suspend the Rules RCS# 148",
      "House: Table RCS# 86",
      "House: Lay on the Clerks Desk RCS# 90",
      // NOT an override: Kentucky's real 2025 overrides are worded
      // `Third Reading`, and all seven rolls wearing this label are a
      // previous question, a reconsideration, a motion to strike the
      // enacting clause, or a floor amendment.
      "House: Veto Override RCS# 333",
    ]) {
      expect(ky("KY", desc, desc.startsWith("Senate") ? "senate" : "house")).toMatchObject({ isFloorVote: false });
    }
  });

  it("reverses the two House labels for 2026, which is why each session is surveyed", () => {
    // The identical string is a kept floor vote in one session and an
    // excluded duplicate in the other.
    expect(ky("KY-2247", "House: Veto Override RCS# 46", "house")).toMatchObject({ isFloorVote: true });
    expect(ky("KY", "House: Veto Override RCS# 46", "house")).toMatchObject({ isFloorVote: false });
    expect(ky("KY-2247", "House: Third Reading RCS# 46", "house")).toMatchObject({ isFloorVote: false });
    expect(ky("KY", "House: Third Reading RCS# 46", "house")).toMatchObject({ isFloorVote: true });

    expect(ky("KY-2247", "Senate: Third Reading RSN# 4026", "senate")).toMatchObject({ isFloorVote: true });
    expect(ky("KY-2247", "Senate: Veto Override RSN# 4161", "senate")).toMatchObject({
      isFloorVote: true,
      questionClass: "veto_override",
    });
    expect(ky("KY-2247", "Senate: Adopt W/ SCS 1, SCA 1 (T) RSN# 4000", "senate", "JR")).toMatchObject({
      isFloorVote: true,
    });
    expect(ky("KY-2247", "House: Co-Sponsor RCS# 12", "house")).toMatchObject({ isFloorVote: false });
    expect(ky("KY-2247", "House: Adopt HFA 16 RCS# 169", "house")).toMatchObject({ isFloorVote: false });
    // HB 84's RCS# 40 arrives twice, once as an amendment and once wearing the
    // broad House label. Kentucky's record says it is the amendment vote, so
    // BOTH copies must be excluded; the neighbouring RCS# 41 (a real passage)
    // must still be kept.
    expect(ky("KY-2247", "House: Adopt HFA 1 RCS# 40", "house")).toMatchObject({ isFloorVote: false });
    expect(ky("KY-2247", "House: Veto Override RCS# 40", "house")).toMatchObject({ isFloorVote: false });
    expect(ky("KY-2247", "House: Veto Override RCS# 41", "house")).toMatchObject({ isFloorVote: true });
    expect(ky("KY-2247", "House: Veto Override RCS# 400", "house")).toMatchObject({ isFloorVote: true });
  });
});

describe("Indiana 2025's measured desc vocabulary", () => {
  const config = LEGISCAN_STATE_CONFIGS.IN!;
  const inRoll = (desc: string, total: number, chamber: "house" | "senate" = "house") =>
    classifyLegiscanRollCall({ desc, total, chamber, billType: "B", config });

  it("keeps the measured final-action spellings, scheduling prefix and all", () => {
    expect(inRoll("House - Third reading", 100)).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    expect(inRoll("Senate - Third reading", 50, "senate")).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
    expect(inRoll("House - House concurred with Senate amendments", 100).questionClass).toBe("concurrence");
    expect(inRoll("House - Rules Suspended. House concurred with Senate amendments", 100).questionClass).toBe(
      "concurrence"
    );
    expect(inRoll("Senate - Concurrence failed for lack of constitutional majority", 50, "senate").questionClass).toBe(
      "concurrence"
    );
    expect(inRoll("Senate - Conference Committee Report 1", 50, "senate").questionClass).toBe("conference_report");
    expect(inRoll("House - Rules Suspended. Conference Committee Report 2", 100).questionClass).toBe(
      "conference_report"
    );
  });

  it("excludes each surveyed procedural family", () => {
    for (const desc of [
      "House - Amendment #1 (Burton) failed",
      "Senate - Amendment #12 (Ford Jon) prevailed",
      "House - Appeal the ruling of the chair (Pryor)",
      "House - Second reading",
      "House - Referred to Committee on Education pursuant to House Rule 126.4",
    ]) {
      expect(inRoll(desc, 100)).toMatchObject({ isFloorVote: false });
    }
  });

  // Ten House rolls carry the literal desc `House -` with no question after
  // the dash. The bill histories show they are five third readings, two
  // concurrences, one failed amendment and two appeals of the chair, so no
  // pattern can recover the question from the desc — they must surface for a
  // human rather than be guessed at. See the config comment and
  // evidence/rollcall/legiscan-in-2143/CODE-FINDINGS.md.
  it("surfaces the blank-question rolls instead of guessing them", () => {
    expect(inRoll("House -", 100)).toMatchObject({
      isFloorVote: null,
      questionClass: null,
      reason: "unknown_question",
    });
  });
});

describe("Indiana 2026's measured desc vocabulary", () => {
  const config = LEGISCAN_STATE_CONFIGS["IN-2234"]!;
  const inRoll = (desc: string, total: number, chamber: "house" | "senate" = "house") =>
    classifyLegiscanRollCall({ desc, total, chamber, billType: "B", config });

  it("serves the second Indiana session under its own key, writing the same jurisdiction", () => {
    expect(config.jurisdiction).toBe("IN");
    expect(config.sessionId).toBe(2234);
    // The 2025 entry must be untouched, or its batches stop being re-runnable.
    expect(LEGISCAN_STATE_CONFIGS.IN!.sessionId).toBe(2143);
    expect(getLegiscanStateConfig("in-2234")).toBe(config);
    // Indiana is named once in the jurisdiction list despite the two entries.
    expect(LEGISCAN_RECORD_JURISDICTIONS.filter((j) => j === "IN")).toHaveLength(1);
  });

  it("keeps the final-action spellings, which do carry across from 2025", () => {
    expect(inRoll("House - Third reading", 100)).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    expect(inRoll("Senate - Third reading", 49, "senate")).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
    expect(inRoll("House - House concurred with Senate amendments", 100).questionClass).toBe("concurrence");
    expect(inRoll("Senate - Rules Suspended. Senate concurred with House amendments", 50, "senate").questionClass).toBe(
      "concurrence"
    );
    expect(inRoll("House - Conference Committee Report 1", 100).questionClass).toBe("conference_report");
    expect(inRoll("Senate - Rules Suspended. Conference Committee Report 1", 50, "senate").questionClass).toBe(
      "conference_report"
    );
  });

  // HB 1368 roll 399: the House defeated the motion 48-42, short of the
  // constitutional majority of 51, but LegiScan reports `passed: 1` because
  // its flag is a bare-majority check, and the fetcher writes `result`
  // straight from that flag. Keeping the desc would store a defeated vote as
  // "Passed". 2025's longer spelling stays kept because its flag is correct.
  it("excludes the defeated concurrence whose passed flag is wrong, but keeps 2025's", () => {
    expect(inRoll("House - Concurrence defeated", 100)).toMatchObject({
      isFloorVote: false,
      reason: "excluded_question",
    });
    expect(
      classifyLegiscanRollCall({
        desc: "Senate - Concurrence failed for lack of constitutional majority",
        total: 50,
        chamber: "senate",
        billType: "B",
        config: LEGISCAN_STATE_CONFIGS.IN!,
      }).questionClass
    ).toBe("concurrence");
  });

  it("excludes each surveyed procedural family, including the four new spellings", () => {
    for (const desc of [
      "House - Amendment #24 (DeLaney) failed",
      "House - Amendment #5 (Bauer) prevailed",
      "House - Appeal the ruling of the chair (Bartlett)",
      "House - Second reading",
      "House - Committee report",
      "House - Rules Suspended. Committee report, adopted",
      "House - Motion to postpone indefinitely, failed",
      "House - Recommitted to Committee on Veterans Affairs and Public Safety pursuant to House Rule 126.4",
      "Senate - First reading",
    ]) {
      expect(inRoll(desc, 100)).toMatchObject({ isFloorVote: false, reason: "excluded_question" });
    }
    // 2025's verb for the same motion still has to be excluded here too.
    expect(inRoll("House - Referred to Committee on Education pursuant to House Rule 126.4", 100).isFloorVote).toBe(
      false
    );
  });

  // The blank-question defect recurs in 2026 on HB 1002 and SB 0076.
  it("surfaces the blank-question rolls instead of guessing them", () => {
    expect(inRoll("House -", 100)).toMatchObject({
      isFloorVote: null,
      questionClass: null,
      reason: "unknown_question",
    });
  });
});

describe("Montana 2025's measured desc vocabulary", () => {
  const config = LEGISCAN_STATE_CONFIGS.MT!;
  const mt = (desc: string, total: number, chamber: "house" | "senate" = "house") =>
    classifyLegiscanRollCall({ desc, total, chamber, billType: "B", config });

  it("keeps every third-reading spelling the survey measured", () => {
    expect(mt("(H) 3rd Reading Passed", 100)).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    expect(mt("(S) 3rd Reading Concurred", 50, "senate")).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
    expect(mt("(S) 3rd Reading Failed", 50, "senate")).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    expect(mt("(H) 3rd Reading Failed; 2nd House Vote Required", 100).questionClass).toBe("passage");
    expect(mt("(H) 3rd Reading Passed as Amended by Senate", 100).questionClass).toBe("concurrence");
    expect(mt("(H) 3rd Reading Not Passed as Amended by Senate", 100).questionClass).toBe("concurrence");
    expect(mt("(S) 3rd Reading Free Conference Committee Report Adopted", 50, "senate").questionClass).toBe(
      "conference_report"
    );
    expect(mt("(S) 3rd Reading Conference Committee Report Rejected", 50, "senate").questionClass).toBe(
      "conference_report"
    );
    expect(mt("(H) 3rd Reading Governor's Proposed Amendments Adopted", 100).questionClass).toBe("concurrence");
  });

  it("excludes second reading, the stage where Montana takes floor amendments", () => {
    for (const desc of [
      "(H) 2nd Reading Passed",
      "(S) 2nd Reading Concurred",
      "(H) 2nd Reading Motion to Amend Carried",
      "(S) 2nd Reading Indefinitely Postponed",
      "(H) 2nd Reading Senate Amendments Concurred",
      "(S) 2nd Reading Conference Committee Report Adopted",
    ]) {
      expect(mt(desc, 100)).toMatchObject({ isFloorVote: false });
    }
  });

  it("excludes the surveyed motion, scheduling and resolution families", () => {
    for (const desc of [
      "(H) Motion Failed",
      "(S) Motion to Reconsider Failed",
      "(H) Taken from Committee; Placed on 2nd Reading",
      "(S) Reconsidered Previous Action; Placed on 2nd Reading",
      "(H) Reconsidered Previous Action; Remains in 3rd Reading Process",
      "(S) Resolution Adopted",
    ]) {
      expect(mt(desc, 100)).toMatchObject({ isFloorVote: false });
    }
  });

  it("rejects every committee roll on its tally, without naming a committee", () => {
    // Montana committee descriptions always join the committee name to its
    // question with a double hyphen and never report more than 23 votes,
    // while every floor roll reports the whole chamber.
    expect(mt("(H) Judiciary--Do Pass", 20)).toMatchObject({ isFloorVote: false });
    expect(mt("(S) Finance and Claims--Be Concurred In", 22, "senate")).toMatchObject({ isFloorVote: false });
  });
});

describe("North Carolina's measured desc vocabulary", () => {
  const config = LEGISCAN_STATE_CONFIGS.NC!;
  const nc = (desc: string, total: number, chamber: "house" | "senate" = "house") =>
    classifyLegiscanRollCall({ desc, total, chamber, billType: "B", config });

  it("keeps the readings, both concurrence spellings, conference reports and overrides", () => {
    // The recorded floor vote is taken on SECOND reading; a third reading
    // roll only exists on the days a member objected.
    expect(nc("Second Reading", 120)).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    expect(nc("Third Reading", 50, "senate")).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    // House concurrence, bare, with the amendment named, and under a
    // materiality ruling (LegiScan leaves the apostrophe HTML-escaped).
    expect(nc("M11 Concur", 120).questionClass).toBe("concurrence");
    expect(nc("M11 Concur Sen. Amd. 1", 120).questionClass).toBe("concurrence");
    expect(nc("R2 Ruled Mat&#x27;l M11 Concur", 120).questionClass).toBe("concurrence");
    // Senate concurrence, with and without a reading in front.
    expect(nc("Motion 9 To Concur House Amend", 50, "senate").questionClass).toBe("concurrence");
    expect(nc("Second Reading Motion 9 To Concur", 50, "senate").questionClass).toBe("concurrence");
    // Conference reports, one shape per chamber plus the Senate's readings
    // on the report.
    expect(nc("C RPT Adoption", 120).questionClass).toBe("conference_report");
    expect(nc("R3 Ruled Mat&#x27;l C RPT Adoption", 120).questionClass).toBe("conference_report");
    expect(nc("Conference Report Motion 8 To Adopt", 50, "senate").questionClass).toBe("conference_report");
    expect(nc("Conference Rpt Third Reading", 50, "senate").questionClass).toBe("conference_report");
    // The override votes, one spelling per chamber.
    expect(nc("Veto Override", 120)).toMatchObject({ isFloorVote: true, questionClass: "veto_override" });
    expect(nc("Motion 11 Veto Override", 50, "senate").questionClass).toBe("veto_override");
  });

  it("excludes the suffix traps that carry the passage wording", () => {
    // The question can be a SUFFIX: amendments, previous question and table
    // motions all end in the reading words, so exclusions must win.
    for (const desc of [
      "A1 Blackwell Second Reading",
      "A1 Smith, Carson Second Reading",
      "A1 Morey Second Reading M3 To Lay On The Table",
      "Amendment 3",
      "Amendment 3 Motion 1 To Table",
      "Second Reading M4 Previous Question",
      "Veto Override M4 Previous Question",
      "M11 Not Concur",
    ]) {
      expect(nc(desc, 120)).toMatchObject({ isFloorVote: false, questionClass: null });
    }
  });
});

describe("Alabama's measured desc vocabulary", () => {
  const config = LEGISCAN_STATE_CONFIGS.AL!;
  const al = (desc: string, total: number, chamber: "house" | "senate" = "house", billType = "B") =>
    classifyLegiscanRollCall({ desc, total, chamber, billType, config });

  it("keeps passage with or without a sponsor prefix or an ` as Amended` tail", () => {
    expect(al("Motion to Read a Third Time and Pass - Roll Call 376", 105)).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
    expect(al("Motion to Read a Third Time and Pass as Amended - Roll Call 130", 34, "senate")).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
    // SB 54 roll 164: a real passage vote printed under its sponsor's name,
    // which is why the passage pattern cannot anchor at the start.
    expect(al("Roberts motion to Read a Third Time and Pass as Amended - Roll Call 164", 34, "senate")).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
  });

  it("classes the concurrence spellings, and the conference report ahead of them", () => {
    expect(al("Garrett Concur In and Adopt - Roll Call 1188", 103).questionClass).toBe("concurrence");
    expect(al("Concur In and Senate Amendment", 103).questionClass).toBe("concurrence");
    expect(al("Senate Concurs In House Amendment -", 34, "senate").questionClass).toBe("concurrence");
    expect(al("Hassell Motion to Concur In and Adopt", 104).questionClass).toBe("concurrence");
    expect(al("Concur In and Adopt Conference Committee Report YMYZ96N-1", 103).questionClass).toBe(
      "conference_report"
    );
  });

  it("keeps the 2026 session's hyphenated executive-amendment concurrence", () => {
    // The Governor can return a bill with an executive amendment; the
    // chamber votes to accept it. Alabama prints that question with a hyphen
    // (`Concur-In`), which a space-only pattern would miss.
    expect(al("Reynolds Concur-In and Adopt Executive Amendment Roll Call 159", 104).questionClass).toBe(
      "concurrence"
    );
  });

  it("excludes the Budget Isolation Resolution under BOTH of its captions", () => {
    // Alabama votes a Budget Isolation Resolution before taking up most
    // bills, and LegiScan files that one vote twice. The second caption
    // reads like passage but is the same tally and the same member list, so
    // keeping it would double every Alabama record.
    for (const desc of [
      "HBIR: Passed by House of Origin",
      "HBIR: Passed by Second House",
      "SBIR: Passed by House of Origin",
      "SBIR: Passed by Second House",
      "Third Reading in House of Origin",
      "Third Reading in Second House",
      "Motion to Adopt BIR- Failed",
      // A FAILED Budget Isolation Resolution in the 2026 session. The desc
      // reads like a failed passage vote; only the bill history says
      // `BIR Lost in House of Origin` (HB 583, 47-37, three fifths needed).
      "Lost in House of Origin",
      "Lost in Second House",
    ]) {
      expect(al(desc, 105)).toMatchObject({ isFloorVote: false, questionClass: null });
    }
  });

  it("excludes each surveyed procedural family", () => {
    for (const desc of [
      "Albritton motion to Adopt - Roll Call 27 F2Z4DCC-1",
      "Carns motion to Table - Roll Call 1143 SLMKI78-1",
      "Waggoner Petition to Cease Debate",
      "Petition to Close Debate",
      "Stadthagen Previous Question",
      "Motion to Add Cosponsor",
      "LocalCertificationResolutionAdopted",
      "Orr Local Certification Resolution",
      "In Conference Committee",
      "Chambliss Reconsider",
      "Underwood motion to Non-Concur and Appoint Conference Committee",
    ]) {
      expect(al(desc, 105)).toMatchObject({ isFloorVote: false, questionClass: null });
    }
  });

  it("keeps a county delegation's local-bill vote, because its roll is still chamber-sized", () => {
    // SB 314 (Shelby County) passed the House 10-3 with 90 members recorded
    // as not voting, so `total` is 103 and no tally rule separates a local
    // act from a statewide one. Selection has to drop these by subject.
    expect(al("Motion to Read a Third Time and Pass - Roll Call 978", 103)).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
  });
});

describe("South Carolina's measured desc vocabulary", () => {
  const config = LEGISCAN_STATE_CONFIGS.SC!;
  const sc = (desc: string, total: number, chamber: "house" | "senate" = "house", billType = "B") =>
    classifyLegiscanRollCall({ desc, total, chamber, billType, config });

  it("keeps each chamber's own final-passage wording", () => {
    // The House names the instrument it is passing.
    expect(sc("House: Passage Of Bill", 124)).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    expect(sc("House: Passage Of Joint Resolution", 124, "house", "JR")).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
    // The Senate's substantive vote is SECOND reading; it records a third
    // reading as well, so both are kept and the judge's superseded-stage
    // gate picks the chamber's last one.
    expect(sc("Senate: 2nd Reading", 45, "senate")).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    expect(sc("Senate: 3rd Reading", 45, "senate")).toMatchObject({ isFloorVote: true, questionClass: "passage" });
  });

  it("classes concurrence, conference reports and veto overrides", () => {
    expect(sc("House: Concur In Senate Amendments", 124).questionClass).toBe("concurrence");
    expect(sc("Senate: To Concur", 45, "senate").questionClass).toBe("concurrence");
    expect(sc("House: Adopt Conference Report", 124).questionClass).toBe("conference_report");
    expect(sc("House: Adopt Free Conference Report", 123).questionClass).toBe("conference_report");
    expect(sc("Senate: To Adopt The Conference Report", 45, "senate").questionClass).toBe("conference_report");
    expect(sc("Senate: To Adopt The Free Conference Report", 45, "senate").questionClass).toBe("conference_report");
    expect(sc("House: Override Veto By The Governor", 124).questionClass).toBe("veto_override");
    expect(sc("Senate: To Override The Veto", 45, "senate").questionClass).toBe("veto_override");
  });

  it("excludes the appropriations act's section-by-section votes in both chambers", () => {
    // South Carolina votes its budget one part or agency at a time. None of
    // these is a vote on the measure, and `House: Passage Of Section 33,
    // Part 1A` would otherwise read as passage.
    for (const [desc, chamber] of [
      ["House: Adopt Section 5, Part 1B", "house"],
      ["House: Adopt Section", "house"],
      ["House: Passage Of Section 33, Part 1A", "house"],
      ["House: Proviso To 117.9", "house"],
      ["Senate: To Adopt Section 22 - Corrections, Department Of", "senate"],
    ] as const) {
      expect(sc(desc, chamber === "house" ? 124 : 46, chamber)).toMatchObject({
        isFloorVote: false,
        questionClass: null,
      });
    }
  });

  it("excludes each surveyed amendment and procedural family", () => {
    for (const [desc, chamber] of [
      ["House: Adopt Amendment 1 Amendment Number 1", "house"],
      ["House: Table Amendment 6 Amendment Number 6", "house"],
      ["House: Table Motion To Reconsider", "house"],
      ["House: Table Motion To Adjourn Debate", "house"],
      ["House: Commit", "house"],
      ["House: Recommit Bill", "house"],
      ["House: Adjourn For The Day", "house"],
      ["House: Invoke The Previous Question (cloture)", "house"],
      ["House: Waive Rule 5.15 Printing", "house"],
      ["House: Grant Free Conference Powers", "house"],
      ["House: Adopt House Resolution", "house"],
      ["House: Adopt Concurrent Resolution", "house"],
      ["Senate: To Lay On The Table Amendment Number 3", "senate"],
      ["Senate: To Lay On The Table Amendment No. 3", "senate"],
      ["Senate: To Adopt Amendment Number Rfh-1", "senate"],
      ["Senate: To Adopt Agriculture & Natural Resources Committee Amendment", "senate"],
      ["Senate: To Take Up Amendment 2 On Third Reading", "senate"],
      ["Senate: To Grant Free Conference Powers", "senate"],
      ["Senate: Motion To Suspend Rule 32a", "senate"],
      ["Senate: To Set For Special Order", "senate"],
      ["Senate: To Continue The Bill", "senate"],
      ["Senate: To Adopt The Resolution", "senate"],
    ] as const) {
      expect(sc(desc, chamber === "house" ? 124 : 46, chamber)).toMatchObject({
        isFloorVote: false,
        questionClass: null,
      });
    }
  });

  it("rejects the session's one under-sized Senate roll on the tally cut", () => {
    // Roll 1528748: a second reading that failed 0-8 with only eight
    // senators recorded. It wears a kept desc, so only the chamber-size cut
    // keeps it out of the queue.
    expect(sc("Senate: 2nd Reading", 8, "senate").isFloorVote).not.toBe(true);
  });
});

describe("Alabama's 2023 desc vocabulary", () => {
  const config = LEGISCAN_STATE_CONFIGS["AL-2014"]!;
  const al23 = (desc: string, total: number, chamber: "house" | "senate" = "house", billType = "B") =>
    classifyLegiscanRollCall({ desc, total, chamber, billType, config });

  it("keeps passage, which in 2023 carries no `Motion to` prefix", () => {
    // The whole reason this session needs its own definition: applying the
    // modern patterns here matches almost none of the 891 passage rolls and
    // reports a false empty divided pool.
    for (const desc of [
      "Read a Third Time and Pass",
      "Read A Third Time And Passed As Amended",
      "Read a Third Time and Pass as Amended",
      "Read Again a Third Time and Pass as Amended",
      "READ A THIRD TIME AND PASSED",
    ]) {
      expect(al23(desc, 105)).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    }
  });

  it("keeps the three concurrence spellings", () => {
    expect(al23("Concur In and Adopt", 105)).toMatchObject({ isFloorVote: true, questionClass: "concurrence" });
    expect(al23("House Concur and Adopt", 105)).toMatchObject({ isFloorVote: true, questionClass: "concurrence" });
    expect(al23("Concur", 34, "senate")).toMatchObject({ isFloorVote: true, questionClass: "concurrence" });
  });

  it("excludes a SPECIAL ORDER CALENDAR adoption, which is the only `Passed by` roll in 2023", () => {
    // 26 rolls, every one on a chamber resolution setting its own order of
    // business. Nothing in 2023 is a Budget Isolation Resolution roll call.
    expect(al23("Passed by House of Origin", 34, "senate")).toMatchObject({
      isFloorVote: false,
      questionClass: null,
    });
    expect(al23("Passed by Second House", 105)).toMatchObject({ isFloorVote: false, questionClass: null });
  });

  it("excludes each surveyed procedural family", () => {
    for (const desc of [
      "Adopt",
      "Adopt 4XDG33-1",
      "Table 8T91F2-1",
      "Non Concur and Appoint Conference Committee",
      "Accede",
      "Add Cosponsor",
      "LOCAL CERTIFICATION RESOLUTION",
      "Previous Question",
      "Petition to Cease Debate",
      "Carry Over to the Call of the Chair",
      "Reconsider",
    ]) {
      expect(al23(desc, 105)).toMatchObject({ isFloorVote: false, questionClass: null });
    }
  });

  it("shares its definition with the 2023 second special session", () => {
    expect(LEGISCAN_STATE_CONFIGS["AL-2060"]!.keptQuestions).toBe(config.keptQuestions);
    expect(LEGISCAN_STATE_CONFIGS["AL-2060"]!.excludedQuestions).toBe(config.excludedQuestions);
  });
});

describe("Alabama's 2024 desc vocabulary, which prints two caption systems at once", () => {
  const config = LEGISCAN_STATE_CONFIGS["AL-2103"]!;
  const al24 = (desc: string, total: number, chamber: "house" | "senate" = "house", billType = "B") =>
    classifyLegiscanRollCall({ desc, total, chamber, billType, config });

  it("keeps passage under BOTH captions", () => {
    // System B, the modern spelling.
    expect(al24("Motion to Read a Third Time and Pass - Roll Call 246", 35, "senate")).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
    expect(al24("Motion to Read Again a Third Time and Pass as Amended - Roll Call 7", 105)).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
    // System A. SB 47 stores its Roll Call 108 passage vote under this
    // caption and no other; reading it as a Budget Isolation Resolution
    // loses 176 real passage votes.
    expect(al24("Passed House Of Origin", 35, "senate")).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
    expect(al24("Passed Second House", 105)).toMatchObject({ isFloorVote: true, questionClass: "passage" });
  });

  it("excludes the Budget Isolation Resolution under both captions, which differ by one word", () => {
    for (const desc of [
      "Third Reading in House of Origin",
      "Third Reading in Second House",
      "Third Reading House of Origin",
      "Third Reading Second House",
    ]) {
      expect(al24(desc, 105)).toMatchObject({ isFloorVote: false, questionClass: null });
    }
  });

  it("keeps concurrence and conference-report votes, and ranks conference first", () => {
    expect(al24("Reed Concur In and Adopt House Amendment", 35, "senate")).toMatchObject({
      questionClass: "concurrence",
    });
    expect(al24("Chesnutt Motion to Concur in Executive Amendment - Roll Call 1157", 105)).toMatchObject({
      questionClass: "concurrence",
    });
    for (const desc of [
      "Blackshear Concur In and Adopt Conference Committee Report - Roll Call 941",
      "Albritton to Concur In and Adopt Conf Rpt",
      "Garrett - Concur in and Adopt Conference Committee Report",
      "Stadthagen Concur In and Adopt Concurrence Request for Conference Committee",
    ]) {
      expect(al24(desc, 105)).toMatchObject({ questionClass: "conference_report" });
    }
  });

  it("excludes amendment work under both spellings", () => {
    for (const desc of [
      "Motion to Adopt - Roll Call 259 KI7T55U-1",
      "Givhan motion to Adopt - Roll Call 1133 NQKCJTJ-1",
      "Baker motion to Table - Roll Call 1008 DRRQHTT-1",
      "Smitherman amendment C4NRQWW-1",
      "Coleman-Madison amendment",
      "Boyd substitution KI7T55U-1",
      "Instrument Change CURHQJQ-1",
      "Instrument Change Tabled RZPDMNM-1",
      "LocalCertificationResolutionAdopted",
      "LOCAL_CERTIFICATION_RESOLUTION_ADOPTED",
      "In Conference Committee",
      "Reed Non Concur and Appoint Conference Committee",
      "Orr - Suspend Rule 21",
    ]) {
      expect(al24(desc, 105)).toMatchObject({ isFloorVote: false, questionClass: null });
    }
  });

  it("does not let the amendment rule swallow a concurrence naming an amendment", () => {
    // `<sponsor> amendment <code>` is start-anchored on purpose: two kept
    // questions also carry the word `amendment`.
    expect(al24("Reed Concur In and Adopt House Amendment", 35, "senate").isFloorVote).toBe(true);
    expect(al24("Reed Motion to Concur in Executive Amendment", 35, "senate").isFloorVote).toBe(true);
  });
});

describe("getLegiscanStateConfig", () => {
  it("serves only surveyed states; an unsurveyed state is refused by name", () => {
    expect(Object.keys(LEGISCAN_STATE_CONFIGS)).toEqual([
      "GA",
      "CT",
      "IL",
      "TN",
      "TX",
      "FL",
      "CA",
      "PA",
      "ME",
      "MO",
      "MO-2226",
      "MD",
      "MD-2240",
      "KY",
      "KY-2247",
      "IN",
      "IN-2234",
      "MT",
      "NC",
      "AL",
      "AL-2218",
      "AL-2262",
      "SC",
      "NV",
      "AL-2014",
      "AL-2060",
      "AL-2103",
      "NY",
      "NM",
    ]);
    // A key is not a jurisdiction: Missouri and Maryland each have two
    // sessions in scope and write both under their postal jurisdiction, so a
    // judgment may never name `MO-2226` or `MD-2240`.
    expect(LEGISCAN_RECORD_JURISDICTIONS).toEqual([
      "GA",
      "CT",
      "IL",
      "TN",
      "TX",
      "FL",
      "CA",
      "PA",
      "ME",
      "MO",
      "MD",
      "KY",
      "IN",
      "MT",
      "NC",
      "AL",
      "SC",
      "NV",
      "NY",
      "NM",
    ]);
    expect(getLegiscanStateConfig("TX").sessionId).toBe(2160);
    expect(getLegiscanStateConfig("TN").sessionId).toBe(2161);
    expect(getLegiscanStateConfig("GA").sessionId).toBe(2167);
    expect(getLegiscanStateConfig("IL").sessionId).toBe(2176);
    expect(getLegiscanStateConfig("FL").sessionId).toBe(2135);
    expect(getLegiscanStateConfig("CA").sessionId).toBe(2172);
    expect(getLegiscanStateConfig("PA").sessionId).toBe(2192);
    expect(getLegiscanStateConfig("ME").sessionId).toBe(2181);
    expect(getLegiscanStateConfig("CT").sessionId).toBe(2174);
    expect(getLegiscanStateConfig("MO").sessionId).toBe(2169);
    expect(getLegiscanStateConfig("MO-2226")).toMatchObject({ jurisdiction: "MO", sessionId: 2226 });
    expect(getLegiscanStateConfig("MD").sessionId).toBe(2164);
    expect(getLegiscanStateConfig("NC").sessionId).toBe(2189);
    expect(getLegiscanStateConfig("md-2240")).toMatchObject({ jurisdiction: "MD", sessionId: 2240 });
    expect(getLegiscanStateConfig("KY").sessionId).toBe(2179);
    expect(getLegiscanStateConfig("KY-2247")).toMatchObject({ jurisdiction: "KY", sessionId: 2247 });
    expect(getLegiscanStateConfig("MT").sessionId).toBe(2159);
    expect(getLegiscanStateConfig("AL").sessionId).toBe(2148);
    expect(getLegiscanStateConfig("AL-2218")).toMatchObject({ jurisdiction: "AL", sessionId: 2218 });
    expect(getLegiscanStateConfig("AL-2262")).toMatchObject({ jurisdiction: "AL", sessionId: 2262 });
    expect(getLegiscanStateConfig("SC").sessionId).toBe(2194);
    expect(getLegiscanStateConfig("NV").sessionId).toBe(2144);
    expect(getLegiscanStateConfig("AL-2014")).toMatchObject({ jurisdiction: "AL", sessionId: 2014 });
    expect(getLegiscanStateConfig("AL-2060")).toMatchObject({ jurisdiction: "AL", sessionId: 2060 });
    expect(getLegiscanStateConfig("AL-2103")).toMatchObject({ jurisdiction: "AL", sessionId: 2103 });
    expect(getLegiscanStateConfig("NY").sessionId).toBe(2188);
    expect(getLegiscanStateConfig("NM").sessionId).toBe(2187);
    expect(getLegiscanStateConfig(" tx ").jurisdiction).toBe("TX");
    expect(() => getLegiscanStateConfig("WY")).toThrow("no LegiScan state config for WY");
  });

  it("classifies New York's real desc vocabulary as surveyed", () => {
    const config = LEGISCAN_STATE_CONFIGS.NY!;
    const ny = (desc: string, total: number, chamber: "house" | "senate", billType = "B") =>
      classifyLegiscanRollCall({ desc, total, chamber, billType, config });
    // The only two floor questions New York prints, both saying so in words.
    expect(ny("Assembly Floor Vote - Final Passage", 149, "house")).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
    expect(ny("Senate Floor Vote - Final Passage", 62, "senate")).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
    // Every other family names a committee and is far below the floor size,
    // so no exclusion rule is needed to keep them out of the queue.
    expect(ny("Assembly Rules Committee: Favorable", 31, "house")).toMatchObject({ isFloorVote: false });
    expect(ny("Senate Rules Committee Vote", 21, "senate")).toMatchObject({ isFloorVote: false });
    expect(ny("Assembly Codes Committee: Held for Consideration", 22, "house")).toMatchObject({
      isFloorVote: false,
    });
    // Electing Regents rides a concurrent resolution, which is not a kept
    // bill type, so it never reaches the desc rules.
    expect(ny("Assembly Floor Vote - Final Passage", 150, "house", "CR")).toMatchObject({
      isFloorVote: false,
      reason: "excluded_measure:CR",
    });
  });

  it("classifies Nevada's real desc vocabulary as surveyed", () => {
    const config = LEGISCAN_STATE_CONFIGS.NV!;
    const nv = (desc: string, total: number, chamber: "house" | "senate", billType = "B") =>
      classifyLegiscanRollCall({ desc, total, chamber, billType, config });
    // Nevada prints exactly two floor descriptions and nothing else. Both are
    // final passage, and a full chamber votes on every one of them.
    expect(nv("Assembly Final Passage", 42, "house")).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    expect(nv("Senate Final Passage", 21, "senate")).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    // The one short Senate roll in the session (SB 26, recorded 2-0) must
    // surface for a human rather than queue as a floor vote.
    expect(nv("Senate Final Passage", 2, "senate")).toMatchObject({ isFloorVote: null });
    // Nevada joint resolutions are kept by bill type; concurrent resolutions
    // are not, so the Joint Standing Rules vote never enters the queue.
    expect(nv("Senate Final Passage", 21, "senate", "JR")).toMatchObject({ isFloorVote: true });
    expect(nv("Senate Final Passage", 21, "senate", "CR")).toMatchObject({
      isFloorVote: false,
      reason: "excluded_measure:CR",
    });
    // Nevada has no exclusion rules, so an unseen description surfaces
    // instead of being guessed at.
    expect(nv("Senate Concurred in Assembly Amendment", 21, "senate")).toMatchObject({
      isFloorVote: null,
      reason: "unknown_question",
    });
  });

  it("classifies Missouri's real desc vocabulary as surveyed", () => {
    const config = LEGISCAN_STATE_CONFIGS.MO!;
    const mo = (desc: string, total: number, chamber: "house" | "senate" = "house", billType = "B") =>
      classifyLegiscanRollCall({ desc, total, chamber, billType, config });
    // The House prints its calendar heading and lets the substitute chain trail.
    expect(mo("House: HBs FOR THIRD READING HCS#2 HBS 567, 546, 758 & 958, E.C.", 162)).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
    expect(mo("House: SBs FOR THIRD READING HCS SS SB 160, A.A., E.C.", 161).questionClass).toBe("passage");
    expect(mo("House: HBs 3rd READ - INFORMAL HB 563", 162).questionClass).toBe("passage");
    expect(mo("House: HBs 3rd READING - CONSENT HCS HB 339, E.C.", 162).questionClass).toBe("passage");
    expect(mo("House: HJRs FOR THIRD READING HCS HJR 73", 162, "house", "JR").questionClass).toBe("passage");
    expect(mo("House: HBs WITH SENATE AMENDMENTS SS SCS HB 225, A.A., E.C.", 161).questionClass).toBe("concurrence");
    expect(mo("House: BILLS IN CONFERENCE CCS HCS SS SCS SBS 81 & 174, E.C", 161).questionClass).toBe(
      "conference_report"
    );
    // The Senate prints the bare question; `Third Reading` is also its
    // Truly Agreed To And Finally Passed vote.
    expect(mo("Senate: Third Reading", 33, "senate")).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    expect(mo("Senate: Conference Committee Report Adoption", 33, "senate").questionClass).toBe("conference_report");
    // Perfection is the amend-and-engross stage, not passage; the previous
    // question, substitute adoption and emergency clause are not the measure.
    for (const [desc, chamber] of [
      ["House: HBs FOR PERFECTION HB 660, A.A.", "house"],
      ["House: HBs PERFECTION - INFORMAL *HCS HB 970, A.A.", "house"],
      ["House: HJRs FOR PERFECTION *HCS HJR 73", "house"],
      ["House: General PQ", "house"],
      ["Senate: Adopt Substitute", "senate"],
      ["Senate: Emergency Clause", "senate"],
    ] as const) {
      expect(mo(desc, chamber === "house" ? 162 : 33, chamber)).toMatchObject({
        isFloorVote: false,
        questionClass: null,
      });
    }
    // `Senate: Adoption` is one desc over two questions (ceremonial
    // resolutions and the HB 595 conference report), so it surfaces.
    expect(mo("Senate: Adoption", 33, "senate")).toMatchObject({ isFloorVote: null, questionClass: null });
    // A heading this session never printed surfaces rather than being guessed.
    expect(mo("House: SJRs FOR THIRD READING SS SJR 78", 162, "house", "JR").isFloorVote).toBeNull();
  });

  it("serves Missouri's second 2025 session under its own key, writing the same jurisdiction", () => {
    const regular = LEGISCAN_STATE_CONFIGS.MO!;
    const special = LEGISCAN_STATE_CONFIGS["MO-2226"]!;
    // Same vocabulary object, so the two sessions cannot drift apart.
    expect(special.keptQuestions).toBe(regular.keptQuestions);
    expect(special.excludedQuestions).toBe(regular.excludedQuestions);
    expect(special.jurisdiction).toBe(regular.jurisdiction);
    expect(special.sessionId).not.toBe(regular.sessionId);
    const mo = (desc: string, total: number, chamber: "house" | "senate" = "house", billType = "B") =>
      classifyLegiscanRollCall({ desc, total, chamber, billType, config: special });
    // The special session's five measured descs, from its own survey.
    expect(mo("House: HJRs FOR THIRD READING HCS HJR 3", 159, "house", "JR").questionClass).toBe("passage");
    expect(mo("House: HBs FOR THIRD READING HB 1", 159).questionClass).toBe("passage");
    expect(mo("Senate: Third Reading", 34, "senate").questionClass).toBe("passage");
    for (const desc of ["House: HJRs FOR PERFECTION *HCS HJR 3, A.A.", "House: HBs FOR PERFECTION *HB 1"]) {
      expect(mo(desc, 159, "house", "JR")).toMatchObject({ isFloorVote: false, questionClass: null });
    }
  });

  it("classifies Texas's real desc vocabulary as surveyed", () => {
    const config = LEGISCAN_STATE_CONFIGS.TX!;
    const tx = (desc: string, total: number, chamber: "house" | "senate" = "house") =>
      classifyLegiscanRollCall({ desc, total, chamber, billType: "B", config });
    // House stamps every desc with a unique roll id.
    expect(tx("Read 3rd time RV#3832", 150)).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    expect(tx("Read 3rd time", 31, "senate")).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    // The House-only constitutional-amendment passage wording.
    expect(tx("Adopted RV#712", 150)).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    expect(tx("Adopted as amended RV#3001", 150)).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    expect(tx("Senate concurs in House amendment(s)", 31, "senate").questionClass).toBe("concurrence");
    expect(tx("Senate adopts conference committee report", 31, "senate").questionClass).toBe("conference_report");
    // Measured floor-sized procedural families are excluded, not surfaced.
    for (const desc of [
      "Read 2nd time RV#88",
      "Read 2nd time & passed to engrossment",
      "Rules suspended-Regular order of business",
      "Three day rule suspended",
      "Amendment fails of adoption RV#77",
      "Statement(s) of vote recorded in journal RV#123",
      "Laid out as postponed business RV#5",
    ]) {
      expect(tx(desc, 150).reason, desc).toBe("excluded_question");
    }
    // A bare roll id could be anything: surfaced, never dropped.
    expect(tx("RV#105", 150)).toMatchObject({ isFloorVote: null, reason: "unknown_question" });
    // Committee-sized unknowns are still cut by tally.
    expect(tx("Reported favorably", 9).reason).toBe("committee_tally:9/150");
  });

  it("classifies Pennsylvania's real desc vocabulary as surveyed", () => {
    const config = LEGISCAN_STATE_CONFIGS.PA!;
    const pa = (desc: string, total: number, chamber: "house" | "senate" = "house") =>
      classifyLegiscanRollCall({ desc, total, chamber, billType: "B", config });
    // Pennsylvania names the venue, then the measure and its printer's
    // number, then the question as the comma-delimited tail.
    expect(pa("House Floor: HB 1431 PN 1746, FINAL PASSAGE", 203)).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
    expect(pa("Senate Floor: PN1936 A02188, Final Passage", 50, "senate").questionClass).toBe("passage");
    // The four reconsidered-passage spellings and the two-thirds vote.
    for (const desc of [
      "Senate Floor: SB 101 PN 986, Final Passage - Reconsideration",
      "Senate Floor: PN1836, Final Passage-Reconsidered",
      "Senate Floor: SB 114 PN 751, Final Passage Reconsidered",
      "Senate Floor: SB 467 PN 1057, Reconsideration - Final Passage",
      "Senate Floor: PN3074, Final Passage Constitutional 2/3 Vote",
    ]) {
      expect(pa(desc, 50, "senate").questionClass, desc).toBe("passage");
    }
    expect(pa("House Floor: HB 103 PN 1999, CONCURRENCE", 203).questionClass).toBe("concurrence");
    for (const desc of [
      "Senate Floor: SB 95 PN 1019, Concur in House Amendments",
      "Senate Floor: PN1258, Concurrence in House Amendments as Amended",
      "Senate Floor: HB 640 PN 2052, Concur in House Amendments to Senate Amendments",
      // Four Senate rolls are captioned with the WRONG chamber word; no
      // pattern reads it.
      "House Floor: PN1030, Concur in House Amendments",
    ]) {
      expect(pa(desc, 50, "senate").questionClass, desc).toBe("concurrence");
    }
    // Measured floor-sized procedural families are excluded, not surfaced —
    // including the motion that ends in the passage pattern's own words.
    for (const desc of [
      "Senate Floor: PN1805, Motion to Reconsider bill on final passage",
      "House Floor: HB 1058 PN 1488, 2025 A594",
      "House Floor: PN1936 A02188",
      "Senate Floor: SB 25 PN 1122, A01234, Brooks Amendment No. A-1422",
      "House Floor: HB 1200 PN 1641, CONSTITUTIONALITY",
      "House Floor: UNCONTESTED CALENDAR",
      "Senate Floor: PN1122, Motion to consider bill on Second Consideration",
      "Senate Floor: PN1122, Re-referred to the Committee on Appropriations",
      "House Floor: HB 12 PN 34, Motion to Recommit Commerce",
      "Senate Floor: HB 257 PN 203, Third Consideration as Amended",
    ]) {
      expect(pa(desc, 203).reason, desc).toBe("excluded_question");
    }
    // A committee vote names the committee where the floor names the floor.
    expect(pa("House Judiciary: Report Bill As Committed", 26).reason).toBe("committee_tally:26/203");
  });

  it("classifies Tennessee's real desc vocabulary as surveyed", () => {
    const config = LEGISCAN_STATE_CONFIGS.TN!;
    const tn = (desc: string, total: number, chamber: "house" | "senate" = "house") =>
      classifyLegiscanRollCall({ desc, total, chamber, billType: "B", config });
    // Tennessee labels its own floor votes; the House prints the calendar
    // into the desc, the Senate prints the bare question.
    expect(tn("FLOOR VOTE: REGULAR CALENDAR PASSAGE ON THIRD CONSIDERATION", 96)).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
    expect(tn("FLOOR VOTE: CONSENT CALENDAR 3 PASSAGE ON THIRD CONSIDERATION", 93).questionClass).toBe("passage");
    expect(tn("FLOOR VOTE: APPROPRIATIONS CALENDAR AS AMENDED PASSAGE ON THIRD CONSIDERATION", 95).questionClass).toBe(
      "passage"
    );
    expect(tn("FLOOR VOTE: as Amended Third Consideration", 33, "senate").questionClass).toBe("passage");
    expect(tn("FLOOR VOTE: Third Consideration", 32, "senate").questionClass).toBe("passage");
    // The Senate takes its consent calendar and every resolution as a bare
    // `Motion to Adopt`; the House spells the resolution version the same way.
    expect(tn("FLOOR VOTE: Motion to Adopt", 32, "senate").questionClass).toBe("passage");
    expect(tn("FLOOR VOTE: Motion to Adopt Third and Final Reading", 32, "senate").questionClass).toBe("passage");
    expect(tn("FLOOR VOTE: REGULAR CALENDAR MOTION TO ADOPT", 93).questionClass).toBe("passage");
    expect(tn("FLOOR VOTE: Motion to Concur House Amendment # 1", 33, "senate").questionClass).toBe("concurrence");
    expect(tn("FLOOR VOTE: MESSAGE CALENDAR CONCUR IN SENATE AMENDMENT # 2", 95).questionClass).toBe("concurrence");
    expect(tn("FLOOR VOTE: MOTION TO ADOPT CONFERENCE COMMITTEE REPORT", 90).questionClass).toBe("conference_report");
    expect(tn("FLOOR VOTE: MOTION TO ADOPT CONFERENCE COMMITTEE REPORT 2", 90).questionClass).toBe("conference_report");
    // The trailing `PASSAGE ON THIRD CONSIDERATION` is the calendar item's
    // text, not the question: amendment and previous-question rolls carry it
    // too, and each one sits beside a plain passage roll on the same day.
    for (const desc of [
      "FLOOR VOTE: REGULAR CALENDAR PREVIOUS QUESTION AS AMENDED PASSAGE ON THIRD CONSIDERATION",
      "FLOOR VOTE: APPROPRIATIONS CALENDAR MOTION TO ADOPT AMENDMENT # 12 BY WILLIAMS PASSAGE ON THIRD CONSIDERATION",
      "FLOOR VOTE: APPROPRIATIONS CALENDAR MOTION TO CONSIDER AMENDMENT # 10 BY HARDAWAY PASSAGE ON THIRD CONSIDERATION",
      "FLOOR VOTE: REGULAR CALENDAR AMENDMENT # 2 BY JONES J AS AMENDED PASSAGE ON THIRD CONSIDERATION",
      "FLOOR VOTE: Motion to Adopt Amend# 2 by Senator Oliver",
      "FLOOR VOTE: REGULAR CALENDAR LAY ON THE TABLE MOTION TO ADOPT AMENDMENT # 3 BY JOHNSON PASSAGE ON THIRD CONSIDERATION",
      "FLOOR VOTE: MOTION TO ADOPT MINORITY CONFERENCE COMMITTEE REPORT",
      "FLOOR VOTE: Motion to Suspend the Rules",
      "FLOOR VOTE: REGULAR CALENDAR MOTION TO DEFER",
      "FLOOR VOTE: REFER TO COMMITTEE",
      "FLOOR VOTE: Motion to Adopt Appoint Conference Committee",
    ]) {
      expect(tn(desc, 95).reason, desc).toBe("excluded_question");
    }
    // Oddities stay surfaced rather than guessed at, and committee descs are
    // cut by tally — Tennessee names the committee in the desc.
    expect(tn("FLOOR VOTE: REGULAR CALENDAR 2", 94)).toMatchObject({ isFloorVote: null, reason: "unknown_question" });
    // The dataset's one prefix-only desc (`FLOOR VOTE:`, roll 1698192) is a
    // real non-empty string: it parses, matches nothing, and surfaces.
    expect(tn("FLOOR VOTE:", 33, "senate")).toMatchObject({ isFloorVote: null, reason: "unknown_question" });
    expect(tn("HOUSE JUDICIARY COMMITTEE: Rec. for pass; ref to Calendar & Rules Committee", 22).reason).toBe(
      "committee_tally:22/99"
    );
  });

  it("classifies Connecticut's real desc vocabulary as surveyed", () => {
    const config = LEGISCAN_STATE_CONFIGS.CT!;
    const ct = (desc: string, total: number, chamber: "house" | "senate" = "house") =>
      classifyLegiscanRollCall({ desc, total, chamber, billType: "B", config });
    // Every desc carries the chamber's sequential vote number; the House
    // names its question after it, the Senate names nothing at all (the
    // trailing space on the Senate spelling is real feed text).
    expect(ct("House Roll Call Vote 126", 151)).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    expect(ct("House Roll Call Vote 54 AS AMENDED", 151).questionClass).toBe("passage");
    expect(ct("House Roll Call Vote 32 CONSENT CALENDAR", 150).questionClass).toBe("passage");
    expect(ct("House Roll Call Vote 12 EMERGENCY CERTIFICATION", 148).questionClass).toBe("passage");
    expect(ct("Senate Roll Call Vote 284 ", 36, "senate")).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
    // Floor amendment votes, including the two concatenated spellings whose
    // question is STILL the amendment (SB 7 roll 225 rejected Amendment
    // Schedule E 48-99). Exclusions run first, so these never reach the
    // kept patterns above.
    for (const desc of [
      "House Roll Call Vote 53 HOUSE AMD B",
      "House Roll Call Vote 225 AS AMENDED HOUSE AMD E",
      "House Roll Call Vote 61 EMERGENCY CERTIFICATION HOUSE AMD A",
    ]) {
      expect(ct(desc, 151).reason, desc).toBe("excluded_question");
    }
    // Five Senate rolls list only the members present (21, 21, 22, 25, 27);
    // the two under the floor ratio surface rather than being kept unseen.
    expect(ct("Senate Roll Call Vote 302 ", 21, "senate")).toMatchObject({
      isFloorVote: null,
      reason: "kept_small_tally:21/36",
    });
    expect(ct("Senate Roll Call Vote 136 ", 25, "senate").isFloorVote).toBe(true);
  });

  it("rejects Connecticut's joint-committee tallies on the chamber code", () => {
    // Connecticut's standing committees seat both chambers, so LegiScan
    // files their tallies under chamber `J` — no chamber, and so no tally
    // denominator. They are recognized before parsing, which would (rightly)
    // refuse a roll call with no chamber.
    expect(isLegiscanCommitteeChamberRollCall({ chamber: "J" })).toBe(true);
    expect(isLegiscanCommitteeChamberRollCall({ chamber: " j " })).toBe(true);
    expect(isLegiscanCommitteeChamberRollCall({ chamber: "H" })).toBe(false);
    expect(isLegiscanCommitteeChamberRollCall({ chamber: "A" })).toBe(false);
    expect(isLegiscanCommitteeChamberRollCall({ chamber: "S" })).toBe(false);
    expect(isLegiscanCommitteeChamberRollCall({})).toBe(false);
    expect(() => parseLegiscanRollCall(rollCallElement({ chamber: "J" }))).toThrow("chamber is not H, A or S: J");
  });

  it("classifies Georgia's real desc vocabulary as surveyed", () => {
    const config = LEGISCAN_STATE_CONFIGS.GA!;
    const ga = (desc: string, total: number, chamber: "house" | "senate" = "house") =>
      classifyLegiscanRollCall({ desc, total, chamber, billType: "B", config });
    // Every Georgia desc carries a per-chamber vote number; the House
    // spelling puts a space before the colon.
    expect(ga("Passage: House Vote #804", 176)).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    expect(ga("Passage By Substitute: Senate Vote #221", 54, "senate")).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
    expect(ga("Passage As Amended: Senate Vote #98", 56, "senate").questionClass).toBe("passage");
    // Concurrence is matched on the `Agree To` stem: the chambers print 23
    // spellings of it, several abbreviated past readability.
    for (const desc of [
      "Agree To Senate Substitute: House Vote #612",
      "Agree To Senate Sub As Am: House Vote #77",
      "Agree To Sam To Hsub: House Vote #401",
      "Agree To House Amendment To Senate Substitute: Senate Vote #700",
    ]) {
      expect(ga(desc, 176).questionClass, desc).toBe("concurrence");
    }
    expect(ga("Adopt Conference Committee Report: Senate Vote #820", 56, "senate").questionClass).toBe(
      "conference_report"
    );
    expect(ga("Adopt CCR: House Vote #876", 180).questionClass).toBe("conference_report");
    // En-bloc local calendars (one roll attached to up to ten bills) and the
    // measured motion families are excluded, not surfaced.
    for (const desc of [
      "Local Calendar : House Vote #270",
      "Local Consent Calendar: Senate Vote #873",
      "Supplemental Local Consent Calendar: Senate Vote #200",
      "Uncontested House Resolutions: House Vote #61",
      "Motion To Engross: Hb 52, Hb 248, Hb 963: Senate Vote #876",
      "Motion For The Previous Question: Senate Vote #310",
      "Adoption Of Amendment #1 By The Senator From The 38th: Senate Vote #145",
      "Adoption Of The Amendment By The Sen From The 41st As Amended: Senate Vote #563",
      "Reconsider: House Vote #602",
      "Immediately Transmit: House Vote #90",
      "Shall The Ruling Of The Chair Be Sustained: Senate Vote #250",
    ]) {
      expect(ga(desc, 176).reason, desc).toBe("excluded_question");
    }
    // Oddities stay surfaced for a human rather than being guessed at.
    expect(ga("Recede From Senate Amendment: Senate Vote #335", 56, "senate")).toMatchObject({
      isFloorVote: null,
      reason: "unknown_question",
    });
    expect(ga("Sbs 234, 235, & 336 Passage: House Vote #436", 180)).toMatchObject({ reason: "unknown_question" });
    // Georgia's constitutional amendments ride on resolutions, which the
    // shared kept-types list drops before this config is consulted
    // (evidence/rollcall/legiscan-ga-2167/CODE-FINDINGS.md).
    expect(
      classifyLegiscanRollCall({
        desc: "Adoption Of Constitutional Amendment: Senate Vote #749",
        total: 56,
        chamber: "senate",
        billType: "R",
        config,
      }).reason
    ).toBe("excluded_measure:R");
  });

  it("classifies Florida's real desc vocabulary as surveyed", () => {
    const config = LEGISCAN_STATE_CONFIGS.FL!;
    const fl = (desc: string, total: number, chamber: "house" | "senate" = "house", billType = "B") =>
      classifyLegiscanRollCall({ desc, total, chamber, billType, config });
    // The only two floor shapes Florida prints: the House numbers every
    // vote, the Senate recycles its numbers across days.
    expect(fl("House: Third Reading RCS#1204", 120)).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    expect(fl("Senate: Third Reading RCS#3", 39, "senate")).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
    // Constitutional amendments ride joint resolutions, not a second desc.
    expect(fl("House: Third Reading RCS#900", 119, "house", "JR")).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
    // Senate Rules seats 25 of 40 — above the committee cut, so it is
    // excluded by name rather than surfaced 154 times.
    expect(fl("Senate Rules", 25, "senate").reason).toBe("excluded_question");
    // Every other committee is cut by tally in both chambers.
    expect(fl("House Budget Committee", 30).reason).toBe("committee_tally:30/120");
    expect(fl("Senate Fiscal Policy", 19, "senate").reason).toBe("committee_tally:19/40");
  });

  it("classifies Illinois's real desc vocabulary as surveyed", () => {
    const config = LEGISCAN_STATE_CONFIGS.IL!;
    const il = (desc: string, total: number, chamber: "house" | "senate" = "house") =>
      classifyLegiscanRollCall({ desc, total, chamber, billType: "B", config });
    // Both spellings of each floor family: LegiScan reworded them mid-dataset
    // (2025 spring = "<question> in <Chamber>", 2025 fall on = "<Chamber> <question>").
    expect(il("Third Reading in House", 117)).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    expect(il("House Third Reading", 114)).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    expect(il("Third Reading in Senate", 59, "senate")).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    expect(il("Senate Third Reading", 59, "senate")).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    expect(il("Concurrence in House", 115).questionClass).toBe("concurrence");
    expect(il("House Concurrence", 108).questionClass).toBe("concurrence");
    expect(il("Concurrence in Senate", 59, "senate").questionClass).toBe("concurrence");
    expect(il("Senate Concurrence", 59, "senate").questionClass).toBe("concurrence");
    // Every committee desc ends in the literal word — including the doubled
    // one the clerk actually prints — so committees are excluded by rule.
    for (const desc of [
      "House Executive Committee",
      "Senate Judiciary Committee",
      "House Police &amp; Fire Committee Committee",
      "House Tax Policy: Sales Tax Subcommittee Committee",
    ]) {
      expect(il(desc, 12).reason, desc).toBe("excluded_question");
    }
    // Resolution adoptions can never clear the became-law filter.
    expect(il("Senate Motion To Adopt", 59, "senate").reason).toBe("excluded_question");
    expect(il("Motion To Adopt in Senate", 59, "senate").reason).toBe("excluded_question");
    // The Motion bucket mixes real passages, procedural motions and the
    // amendatory-veto votes: surfaced for a human, never auto-kept.
    for (const desc of ["Motion in Senate", "Motion in House", "House Motion", "Senate Motion"]) {
      expect(il(desc, 117), desc).toMatchObject({ isFloorVote: null, reason: "unknown_question" });
    }
    // The only JRCA floor roll in the dataset, and the consent calendars.
    expect(il("House Amendments", 115)).toMatchObject({ isFloorVote: null, reason: "unknown_question" });
    expect(il("Agreed Bill List in Senate", 59, "senate")).toMatchObject({ isFloorVote: null });
  });

  it("classifies California's real desc vocabulary as surveyed", () => {
    const config = LEGISCAN_STATE_CONFIGS.CA!;
    const ca = (desc: string, total: number, chamber: "house" | "senate" = "house") =>
      classifyLegiscanRollCall({ desc, total, chamber, billType: "B", config });
    // California prints the measure and its author inside the desc, so the
    // question is a phrase, not the whole string.
    expect(ca("AB 111 Gabriel Assembly Third Reading", 80)).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    expect(ca("SB 586 Jones Senate Third Reading By Jeff Gonzalez", 79).questionClass).toBe("passage");
    expect(ca("AB 40 Bonta Third Reading Urgency", 80).questionClass).toBe("passage");
    expect(ca("AB 992 Irwin Concurrence in Senate Amendments", 80).questionClass).toBe("concurrence");
    expect(ca("AB 260 Aguiar-Curry Concurrence - Urgency Added", 80).questionClass).toBe("concurrence");
    expect(ca("AB 808 Addis Consent Calendar Second Day Regular Session", 78).questionClass).toBe("passage");
    // The Senate leads with the question and spells it differently.
    expect(ca("Senate 3rd Reading SB680 Rubio", 40, "senate").questionClass).toBe("passage");
    expect(ca("Assembly 3rd Reading AB123 BUDGET (Gabriel) By Wiener", 40, "senate").questionClass).toBe("passage");
    expect(ca("Unfinished Business SB524 Arreguín et al. Concurrence", 40, "senate").questionClass).toBe("concurrence");
    expect(ca("Consent Calendar 2nd AB1781 Michelle Rodriguez", 40, "senate").questionClass).toBe("passage");
    expect(ca("Special Consent ACR51 Haney et al", 40, "senate").questionClass).toBe("passage");
    // The file-section label on a substantive vote taken up without
    // reference to file; the waiver itself is by unanimous consent with no
    // roll call. SB 48's 27-5 here IS the Senate's concurrence in Assembly
    // amendments (Ayes 27, Noes 5 in the official history).
    expect(ca("W/O Ref. To File SB48 Gonzalez", 40, "senate").questionClass).toBe("concurrence");
    // Committee votes are worded as recommendations naming the same
    // destinations — the chamber word and the `^` anchor keep them out.
    for (const [desc, total] of [
      ["Do pass. To consent calendar", 11],
      ["Be adopted. To third reading", 10],
      ["Do pass, but first be re-referred to the Committee on [Appropriations]", 7],
      ["Placed on suspense file", 7],
    ] as const) {
      expect(classifyLegiscanRollCall({ desc, total, chamber: "house", billType: "B", config }).reason, desc).toBe(
        `committee_tally:${total}/80`
      );
    }
    // Second-reading urgency votes and procedural motions are excluded.
    for (const desc of [
      "Assembly 2nd Reading AB1207 Irwin et al. By Limón Urgency Clause",
      "Senate 3rd Reading SB274 Cervantes Motion To Reconsider",
      // The vote GRANTING reconsideration, not the question itself.
      "Unfinished Business SB627 Wiener et al. Concurrence Reconsider",
      "HR 4 Essayli Assembly Third Reading Motion To Lay On The Table By Aguiar-Curry",
    ]) {
      expect(ca(desc, 80).reason, desc).toBe("excluded_question");
    }
  });

  it("classifies Maine's real desc vocabulary as surveyed", () => {
    const config = LEGISCAN_STATE_CONFIGS.ME!;
    const me = (desc: string, total = 151, chamber: "house" | "senate" = "house") =>
      classifyLegiscanRollCall({ desc, total, chamber, billType: "B", config });
    // Maine passes a bill by accepting its ought-to-pass committee report,
    // and every desc ends with the clerk's roll number.
    expect(me("Acc Maj Otp As Amended Rep RC #214")).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    expect(me("Accept Majority Ought To Pass As Amended Report RC #58", 35, "senate")).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
    for (const desc of [
      'Acc Report "a" Otp-am By Ca "c" RC #12',
      "Acc Otp-am Report RC #3",
      "Acceptance Of The Otp-am Report RC #9",
      "Otp-am By Ca \"a\" RC #77",
      "Acc Min Otp As Amended Rep RC #40",
      "Enactment - Emer RC #101",
      "Enactment - Bond Issue RC #5",
      "Final Passage - Con Res RC #2",
      "Passage To Be Engrossed RC #61",
      "Passage Of Emergency Measure RC #8",
      "Adoption RC #17",
    ]) {
      expect(me(desc), desc).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    }
    expect(me("Recede And Concur RC #144").questionClass).toBe("concurrence");
    // A bare Recede is the chamber undoing its OWN earlier action, which is
    // not always a step toward the other chamber's text — surfaced, not kept.
    expect(me("Recede RC #9")).toMatchObject({ isFloorVote: null, reason: "unknown_question" });
    expect(me("Veto Override (2/3) RC #4", 35, "senate").questionClass).toBe("veto_override");
    expect(me("Reconsideration - Veto RC #66").questionClass).toBe("veto_override");
    // A vote to accept an ought-NOT-to-pass report kills the bill: excluded
    // by rule so the ought-to-pass token test can never invert a question.
    for (const desc of [
      "Acc Maj Ought Not To Pass Rep RC #31",
      "Accept Majority Ought Not To Pass Report RC #12",
      'Acc Report "b" Ontp RC #7',
      "Indefinitely Postpone RC #2",
      "Indef Pp Hbh-3 To Cah-1 RC #19",
      "Ipp Hah-489 RC #40",
      'Ha "a" Be Indef Pp RC #5',
      "Adopt Hah-963 To Cah-959 RC #22",
      "Adopt Senate Amendment (s-292) To Ld 1519 RC #3",
      "Reconsider RC #6",
      "Recon Of Maj Rep Otp-am By Ca-a RC #1",
      "Table Until Later RC #14",
      "Commit RC #2",
      "Reference To Judiciary RC #8",
      "Insist RC #4",
      "Suspend Rules (2/3) RC #6",
      "1st Reading Without Reference RC #1",
      "Accept Majority To Refer To Committee RC #3",
      "Accept To Reject Report And Refer Bill To Committee RC #2",
      "Substitute Joint Res For Committee Rpt RC #1",
    ]) {
      expect(me(desc).reason, desc).toBe("excluded_question");
    }
    // The report-kind-unstated families: a yea might pass or kill the bill,
    // so they surface for a human instead of being guessed either way.
    for (const desc of ["Accept Report RC #21", "Acceptance Of Report RC #2", "Acc Majority Report RC #1"]) {
      expect(me(desc), desc).toMatchObject({ isFloorVote: null, reason: "unknown_question" });
    }
  });

  it("classifies New Mexico's real desc vocabulary as surveyed", () => {
    const config = LEGISCAN_STATE_CONFIGS.NM!;
    const nm = (desc: string, total: number, chamber: "house" | "senate" = "house", billType = "B") =>
      classifyLegiscanRollCall({ desc, total, chamber, billType, config });
    // The whole session speaks two sentences. Nothing else exists.
    expect(nm("House Final Passage", 70)).toMatchObject({ isFloorVote: true, questionClass: "passage" });
    expect(nm("Senate Final Passage", 42, "senate")).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
    // Constitutional amendments ride joint resolutions and print the same
    // two descriptions, so they need no rule of their own.
    expect(nm("House Final Passage", 70, "house", "JR")).toMatchObject({
      isFloorVote: true,
      questionClass: "passage",
    });
    // Both patterns are anchored at both ends on purpose: a spelling this
    // survey never saw must surface for a human, not classify quietly.
    expect(nm("House Final Passage RC#12", 70).isFloorVote).toBeNull();
    expect(nm("House Concurrence", 70).isFloorVote).toBeNull();
  });

  it("refuses a state that has its own pipeline, whatever the spelling", () => {
    // Ohio's 1,330 live records cite its actions feed; the folded URL keys
    // (`oh:136:sb56` vs `ls:<id>`) would never match, so a LegiScan import
    // would duplicate every one of them instead of deduping.
    for (const spelling of ["OH", "oh", " Oh "]) {
      expect(() => getLegiscanStateConfig(spelling)).toThrow("served by its own roll-call pipeline");
    }
  });
});
