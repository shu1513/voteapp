import { describe, expect, it } from "vitest";

import {
  classifyLegiscanDatasetFile,
  classifyLegiscanRollCall,
  formatLegiscanMeasureId,
  legiscanEvidenceFileName,
  legiscanMemberVotes,
  legiscanRollCallPageUrl,
  legiscanRollCallSha256,
  parseLegiscanBill,
  parseLegiscanRollCall,
  parseLegiscanVoteEvidence,
  LEGISCAN_EVIDENCE_FILE_PATTERN,
} from "../../../src/pipeline/rollcall/legiscanRollCall.js";
import {
  getLegiscanStateConfig,
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

  it("rejects a roll_call_id outside the int4 range and a bad chamber", () => {
    expect(() => parseLegiscanRollCall(rollCallElement({ roll_call_id: 2_200_000_000 }))).toThrow("storable range");
    expect(() => parseLegiscanRollCall(rollCallElement({ chamber: "J" }))).toThrow("chamber is not H or S");
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

describe("getLegiscanStateConfig", () => {
  it("serves only surveyed states; an unsurveyed state is refused by name", () => {
    expect(Object.keys(LEGISCAN_STATE_CONFIGS)).toEqual(["GA", "IL", "TN", "TX", "FL"]);
    expect(getLegiscanStateConfig("TX").sessionId).toBe(2160);
    expect(getLegiscanStateConfig("TN").sessionId).toBe(2161);
    expect(getLegiscanStateConfig("GA").sessionId).toBe(2167);
    expect(getLegiscanStateConfig("IL").sessionId).toBe(2176);
    expect(getLegiscanStateConfig("FL").sessionId).toBe(2135);
    expect(getLegiscanStateConfig(" tx ").jurisdiction).toBe("TX");
    expect(() => getLegiscanStateConfig("PA")).toThrow("no LegiScan state config for PA");
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

  it("refuses a state that has its own pipeline, whatever the spelling", () => {
    // Ohio's 1,330 live records cite its actions feed; the folded URL keys
    // (`oh:136:sb56` vs `ls:<id>`) would never match, so a LegiScan import
    // would duplicate every one of them instead of deduping.
    for (const spelling of ["OH", "oh", " Oh "]) {
      expect(() => getLegiscanStateConfig(spelling)).toThrow("served by its own roll-call pipeline");
    }
  });
});
