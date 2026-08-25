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
  it("ships with no state registered, so no state can be fetched unsurveyed", () => {
    expect(Object.keys(LEGISCAN_STATE_CONFIGS)).toEqual([]);
    expect(() => getLegiscanStateConfig("TX")).toThrow("no LegiScan state config for TX");
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
