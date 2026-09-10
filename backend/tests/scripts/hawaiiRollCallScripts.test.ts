import { describe, expect, it } from "vitest";

import {
  classifyHawaiiAction,
  decodeHawaiiRollNumber,
  hawaiiEvidenceFileName,
  hawaiiKeptFloorDayCollisions,
  hawaiiRollNumber,
  hawaiiSittingMembers,
  hawaiiVotedDraft,
  parseHawaiiFloorVoteText,
  parseHawaiiHistoryRow,
  parseHawaiiSeatFile,
  reconstructHawaiiMembers,
  resolveHawaiiMemberName,
  splitHawaiiNameList,
  HAWAII_EVIDENCE_FILE_PATTERN,
} from "../../src/pipeline/rollcall/hawaiiRollCall.js";
import type { LegiscanPerson } from "../../src/pipeline/rollcall/legiscanMemberResolver.js";
import { rollCallUrlKey } from "../../src/pipeline/rollcall/rollCallRecordUrls.js";
import { getLegiscanStateConfig } from "../../src/pipeline/rollcall/legiscanStateConfigs.js";
import { parseHawaiiBillList } from "../../src/scripts/fetchHawaiiRollCallVotes.js";

// Real action strings from the 2025 Regular Session dataset (2175), verbatim.
const SENATE_FINAL_NAMED =
  "Passed Final Reading, as amended (CD 1). Ayes, 23; Aye(s) with reservations: Senator(s) Rhoads. Noes, 1 (Senator(s) DeCorte). Excused, 1 (Senator(s) McKelvey).";
const SENATE_FINAL_CLEAN =
  "Passed Final Reading, as amended (CD 1). Ayes, 25; Aye(s) with reservations: none . 0 No(es): none. 0 Excused: none.";
const SENATE_THIRD =
  "Report adopted; Passed Third Reading, as amended (SD 1). Ayes, 24; Aye(s) with reservations: none . Noes, 1 (Senator(s) Awa). Excused, 0 (none). Transmitted to House.";
const HOUSE_FINAL_NAMED =
  "Passed Final Reading as amended in CD 1 with Representative(s) Souza voting aye with reservations; Representative(s) Garcia, Pierick voting no (2) and Representative(s) Cochran excused (1).";
const HOUSE_THIRD_CLEAN =
  "Passed Third Reading with none voting aye with reservations; none voting no (0) and Representative(s) Cochran, Pierick excused (2). Transmitted to Senate.";

function person(peopleId: number, firstName: string, lastName: string, district: string, name?: string): LegiscanPerson {
  return {
    peopleId,
    name: name ?? `${firstName} ${lastName}`,
    firstName,
    lastName,
    party: "D",
    chamber: district.startsWith("HD") ? "house" : "senate",
    district,
  };
}

// A five-seat Senate and a seven-seat House stand in for 25 and 51 in the
// reconstruction tests; the seat counts are overridden per test through a
// sitting list of exactly that size.
const SENATE = [
  person(1, "Karl", "Rhoads", "SD-13"),
  person(2, "Brenton", "Awa", "SD-23"),
  person(3, "Samantha", "DeCorte", "SD-22"),
  person(4, "Mike", "Gabbard", "SD-21"),
  person(5, "Angus", "McKelvey", "SD-06"),
];

describe("history rows", () => {
  it("reads a row and maps H/S to the chamber", () => {
    const row = parseHawaiiHistoryRow({ date: "2025-04-30", action: SENATE_FINAL_NAMED, chamber: "S", chamber_id: 2 }, 41);
    expect(row).toEqual({ index: 41, date: "2025-04-30", chamber: "senate", action: SENATE_FINAL_NAMED });
    expect(parseHawaiiHistoryRow({ date: "2025-04-30", action: HOUSE_FINAL_NAMED, chamber: "H" }, 0).chamber).toBe("house");
  });

  it("refuses a row without a chamber or a real date", () => {
    expect(() => parseHawaiiHistoryRow({ date: "2025-04-30", action: "x", chamber: "" }, 0)).toThrow(/chamber/);
    expect(() => parseHawaiiHistoryRow({ date: "4/30/2025", action: "x", chamber: "S" }, 0)).toThrow(/date/);
  });
});

describe("classification", () => {
  it("keeps third and final readings on bills only", () => {
    expect(classifyHawaiiAction(SENATE_THIRD, "B")).toEqual({ isFloorVote: true, questionClass: "third_reading", reason: "kept:third_reading" });
    expect(classifyHawaiiAction(HOUSE_FINAL_NAMED, "B").questionClass).toBe("final_reading");
    expect(classifyHawaiiAction("Report Adopted; Passed Third Reading. Ayes, 25; ...", "B").questionClass).toBe("third_reading");
    expect(classifyHawaiiAction(SENATE_FINAL_NAMED, "CR")).toEqual({ isFloorVote: false, questionClass: "final_reading", reason: "excluded_measure:CR" });
  });

  it("never mistakes a second reading, a committee report, or the other chamber's notice for passage", () => {
    for (const action of [
      "Passed Second Reading as amended in HD 1; placed on the calendar for Third Reading with none voting aye with reservations; none voting no (0) and Representative(s) Cochran excused (1).",
      "Reported from FIN (Stand. Com. Rep. No. 1200), recommending passage on Third Reading.",
      "Received notice of passage on Final Reading in House (Hse. Com. No. 821).",
      "Received notice of Final Reading (Sen. Com. No. 888).",
      "Senate agrees with House amendments. Placed on the calendar for Final Reading on 04-30-25.",
    ]) {
      expect(classifyHawaiiAction(action, "B").questionClass).toBeNull();
    }
  });

  it("reads the voted draft off the passage line", () => {
    expect(hawaiiVotedDraft(SENATE_FINAL_NAMED)).toBe("CD 1");
    expect(hawaiiVotedDraft(HOUSE_FINAL_NAMED)).toBe("CD 1");
    expect(hawaiiVotedDraft(SENATE_THIRD)).toBe("SD 1");
    expect(hawaiiVotedDraft("Passed Third Reading as amended in HD 2 with none voting ...")).toBe("HD 2");
    expect(hawaiiVotedDraft(HOUSE_THIRD_CLEAN)).toBeNull();
  });
});

describe("name lists", () => {
  it("splits on commas and keeps a lone initial with its surname", () => {
    expect(splitHawaiiNameList("Representative(s) Garcia, Pierick")).toEqual(["Garcia", "Pierick"]);
    expect(splitHawaiiNameList("Representative(s) Lee, M., Garcia")).toEqual(["Lee, M.", "Garcia"]);
    expect(splitHawaiiNameList("Senator(s) Lee, C.")).toEqual(["Lee, C."]);
    expect(splitHawaiiNameList("none")).toEqual([]);
    expect(splitHawaiiNameList("Senator(s) Rhoads.")).toEqual(["Rhoads"]);
  });
});

describe("vote text", () => {
  it("reads the Senate form with named members", () => {
    expect(parseHawaiiFloorVoteText(SENATE_FINAL_NAMED, "senate")).toEqual({
      form: "senate",
      reading: "final_reading",
      draft: "CD 1",
      ayes: 23,
      reservations: ["Rhoads"],
      noes: ["DeCorte"],
      excused: ["McKelvey"],
    });
  });

  it("reads the Senate zero spellings", () => {
    expect(parseHawaiiFloorVoteText(SENATE_FINAL_CLEAN, "senate")).toEqual({
      form: "senate",
      reading: "final_reading",
      draft: "CD 1",
      ayes: 25,
      reservations: [],
      noes: [],
      excused: [],
    });
    expect(parseHawaiiFloorVoteText(SENATE_THIRD, "senate")).toMatchObject({ ayes: 24, noes: ["Awa"], excused: [], reading: "third_reading" });
  });

  it("reads the House form, which prints no aye count", () => {
    expect(parseHawaiiFloorVoteText(HOUSE_FINAL_NAMED, "house")).toEqual({
      form: "house",
      reading: "final_reading",
      draft: "CD 1",
      ayes: null,
      reservations: ["Souza"],
      noes: ["Garcia", "Pierick"],
      excused: ["Cochran"],
    });
    expect(parseHawaiiFloorVoteText(HOUSE_THIRD_CLEAN, "house")).toMatchObject({ noes: [], excused: ["Cochran", "Pierick"], draft: null });
  });

  it("refuses a form on the wrong chamber and a count that disagrees with its names", () => {
    expect(() => parseHawaiiFloorVoteText(HOUSE_FINAL_NAMED, "senate")).toThrow(/House-form/);
    expect(() => parseHawaiiFloorVoteText(SENATE_FINAL_NAMED, "house")).toThrow(/Senate-form/);
    expect(() =>
      parseHawaiiFloorVoteText(
        "Passed Final Reading as amended in CD 1 with none voting aye with reservations; Representative(s) Garcia voting no (2) and none excused (0).",
        "house"
      )
    ).toThrow(/names 1 no votes but counts 2/);
  });
});

describe("member resolution", () => {
  const house = [
    person(10, "Della", "au Belatti", "HD-26", "Della au Belatti"),
    person(11, "Rachele", "Fernandez Lamosao", "HD-36", "Rachele Fernandez Lamosao"),
    person(12, "Michael", "Lee", "HD-50", "Mike Lee"),
    person(13, "Sue", "Keohokapu-Lee Loy", "HD-02"),
    person(14, "Diamond", "Garcia", "HD-42"),
  ];

  it("matches the whole surname first, then its last token", () => {
    expect(resolveHawaiiMemberName("Garcia", house).peopleId).toBe(14);
    expect(resolveHawaiiMemberName("Belatti", house).peopleId).toBe(10);
    expect(resolveHawaiiMemberName("Lamosao", house).peopleId).toBe(11);
    expect(resolveHawaiiMemberName("Keohokapu-Lee Loy", house).peopleId).toBe(13);
  });

  it("matches a printed surname longer than LegiScan's by its last token", () => {
    const senate = [person(20, "Donovan", "Cruz", "SD-17"), person(21, "Chris", "Lee", "SD-25")];
    expect(resolveHawaiiMemberName("Dela Cruz", senate).peopleId).toBe(20);
  });

  it("uses a printed initial to pick between members of one surname", () => {
    const withTwoLees = [...house, person(15, "Cory", "Lee", "HD-99")];
    expect(resolveHawaiiMemberName("Lee, M.", withTwoLees).peopleId).toBe(12);
    expect(resolveHawaiiMemberName("Lee, C.", withTwoLees).peopleId).toBe(15);
    expect(() => resolveHawaiiMemberName("Lee", withTwoLees)).toThrow(/resolves to 2 members/);
  });

  it("does not let a bare surname reach a multi-part one whose LAST token differs", () => {
    // "Lee" must not reach Keohokapu-Lee Loy: her last token is Loy.
    expect(resolveHawaiiMemberName("Lee", house).peopleId).toBe(12);
    expect(() => resolveHawaiiMemberName("Smith", house)).toThrow(/resolves to 0/);
  });
});

describe("reconstruction", () => {
  it("makes every unnamed sitting member an aye and checks the printed count", () => {
    const text = parseHawaiiFloorVoteText(
      "Passed Final Reading, as amended (CD 1). Ayes, 3; Aye(s) with reservations: Senator(s) Rhoads. Noes, 1 (Senator(s) DeCorte). Excused, 1 (Senator(s) McKelvey).",
      "senate"
    );
    // Five sitting members stand in for the 25-seat Senate by overriding the seat count through the sitting list.
    const lists = reconstructWithSeats(text, SENATE, 5);
    expect(lists).toEqual({ yeas: [1, 2, 4], nays: [3], excused: [5], reservations: [1] });
  });

  it("refuses a reconstruction that does not fill the chamber, or that contradicts the printed ayes", () => {
    const text = parseHawaiiFloorVoteText(
      "Passed Final Reading, as amended (CD 1). Ayes, 4; Aye(s) with reservations: none . Noes, 1 (Senator(s) DeCorte). Excused, 0 (none).",
      "senate"
    );
    expect(() => reconstructWithSeats(text, SENATE.slice(0, 4), 5)).toThrow(/had 5 seats filled/);
    expect(() => reconstructWithSeats(text, [...SENATE, person(6, "Extra", "Member", "SD-99")], 5)).toThrow(/had 5 seats filled/);
    const wrongCount = parseHawaiiFloorVoteText(
      "Passed Final Reading, as amended (CD 1). Ayes, 3; Aye(s) with reservations: none . Noes, 1 (Senator(s) DeCorte). Excused, 0 (none).",
      "senate"
    );
    expect(() => reconstructWithSeats(wrongCount, SENATE, 5)).toThrow(/prints Ayes 3 but the reconstruction has 4/);
  });

  it("refuses a member named on two sides", () => {
    const text = parseHawaiiFloorVoteText(
      "Passed Final Reading, as amended (CD 1). Ayes, 3; Aye(s) with reservations: none . Noes, 1 (Senator(s) DeCorte). Excused, 1 (Senator(s) DeCorte).",
      "senate"
    );
    expect(() => reconstructWithSeats(text, SENATE, 5)).toThrow(/both as a no vote and as excused/);
  });
});

describe("seat file and sitting members", () => {
  it("drops a member outside their window and keeps everyone unlisted", () => {
    const seats = parseHawaiiSeatFile(
      { jurisdiction: "HI", session_id: 2245, windows: [{ people_id: 5, from: null, to: "2026-03-01", note: "resigned" }, { people_id: 6, from: "2026-03-02", to: null }] },
      2245
    );
    const people = [...SENATE, person(6, "New", "Senator", "SD-06")];
    expect(hawaiiSittingMembers(people, "senate", "2026-02-01", seats).map((p) => p.peopleId)).toEqual([1, 2, 3, 4, 5]);
    expect(hawaiiSittingMembers(people, "senate", "2026-04-01", seats).map((p) => p.peopleId)).toEqual([1, 2, 3, 4, 6]);
    expect(hawaiiSittingMembers(people, "house", "2026-04-01", seats)).toEqual([]);
  });

  it("refuses a seat file for another session or with a bad date", () => {
    expect(() => parseHawaiiSeatFile({ jurisdiction: "HI", session_id: 2175, windows: [] }, 2245)).toThrow(/session_id/);
    expect(() => parseHawaiiSeatFile({ jurisdiction: "HI", session_id: 2245, windows: [{ people_id: 5, from: "March" }] }, 2245)).toThrow(/from must be/);
  });
});

describe("roll numbers, files, URLs", () => {
  it("encodes bill_id and history index reversibly and refuses the overflow cases", () => {
    expect(hawaiiRollNumber(1980123, 41)).toBe(396024641);
    expect(decodeHawaiiRollNumber(396024641)).toEqual({ billId: 1980123, historyIndex: 41 });
    expect(() => hawaiiRollNumber(1980123, 200)).toThrow(/outside 0..199/);
    expect(() => hawaiiRollNumber(20_000_000, 0)).toThrow(/storable range/);
  });

  it("names evidence files so the LegiScan pattern never claims them", () => {
    const file = hawaiiEvidenceFileName("house", 2175, 396024641);
    expect(file).toBe("hi-house-2175-roll396024641.json");
    expect(HAWAII_EVIDENCE_FILE_PATTERN.exec(file)?.slice(1)).toEqual(["house", "2175", "396024641"]);
  });

  it("folds the capitol.hawaii.gov bill page to a per-bill key", () => {
    expect(rollCallUrlKey("https://www.capitol.hawaii.gov/session/measure_indiv.aspx?billtype=HB&billnumber=957&year=2025")).toEqual({
      chamber: null,
      key: "hi:2025:hb957",
    });
    expect(rollCallUrlKey("https://www.capitol.hawaii.gov/session/measure_indiv.aspx?billtype=SB&billnumber=0897&year=2025")?.key).toBe("hi:2025:sb897");
    expect(rollCallUrlKey("https://www.capitol.hawaii.gov/session/archives/")).toBeNull();
  });

  it("flags two kept floor votes of one chamber on one day", () => {
    expect(
      hawaiiKeptFloorDayCollisions([
        { chamber: "house", date: "2025-04-30" },
        { chamber: "senate", date: "2025-04-30" },
        { chamber: "house", date: "2025-04-30" },
      ])
    ).toEqual(new Set(["house:2025-04-30"]));
  });

  it("parses --bills into stored measure spellings", () => {
    expect([...parseHawaiiBillList("hb957, SB0897,HB 1194")]).toEqual(["HB 957", "SB 897", "HB 1194"]);
    expect(() => parseHawaiiBillList("hb")).toThrow(/not a bill number/);
  });
});

describe("registry guard", () => {
  it("refuses to serve Hawaii from the LegiScan registry", () => {
    expect(() => getLegiscanStateConfig("HI")).toThrow(/rollcall:hi:\*/);
    expect(() => getLegiscanStateConfig("OH")).toThrow(/rollcall:oh:\*/);
  });
});

// reconstructHawaiiMembers reads the chamber's seat count from
// HAWAII_CHAMBER_SEATS; the tests above pass a five-member Senate, so the
// helper below rebinds the count for the duration of one call.
function reconstructWithSeats(text: ReturnType<typeof parseHawaiiFloorVoteText>, sitting: LegiscanPerson[], seats: number) {
  return reconstructHawaiiMembers(text, "senate", sitting, seats);
}
