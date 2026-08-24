import { describe, expect, it } from "vitest";

import { parseJudgmentsFile } from "../../src/scripts/judgeRollCallVotes.js";

const SLUGS = new Set(["general", "integrity_and_ethics", "immigration", "gun_control"]);

const ENTRY = {
  chamber: "house",
  congress: 119,
  session: 1,
  roll: 145,
  measure_id: "H R 1",
  vote_date: "2025-05-22",
  review_status: "approved",
  yea_description: "Voted to pass H.R. 1. It passed the House 215-214.",
  nay_description: "Voted against passing H.R. 1. It passed the House 215-214.",
  labels: [{ slug: "immigration", yea: "for" }, { slug: "general" }],
};

describe("parseJudgmentsFile", () => {
  it("accepts null for a vote with no measure", () => {
    expect(parseJudgmentsFile({ judgments: [{ ...ENTRY, measure_id: null, review_status: "pending" }] }, SLUGS)[0]?.measureId).toBeNull();
  });

  it("turns a file entry into the store's judgment shape", () => {
    expect(parseJudgmentsFile({ judgments: [ENTRY] }, SLUGS)).toEqual([
      {
        jurisdiction: "US",
        chamber: "house",
        session: "119-1",
        rollNumber: 145,
        measureId: "H R 1",
        voteDate: "2025-05-22",
        yeaDescription: ENTRY.yea_description,
        nayDescription: ENTRY.nay_description,
        labels: [
          { slug: "immigration", yea: "for" },
          { slug: "general", yea: null },
        ],
        reviewStatus: "approved",
      },
    ]);
  });

  it("rejects every malformed entry with its index, before anything is written", () => {
    const cases: [Record<string, unknown> | unknown, RegExp][] = [
      [[], /must be an object with a judgments array/],
      [{ judgments: [] }, /non-empty array/],
      [{ judgments: ["x"] }, /judgments\[0\]: must be an object/],
      [{ judgments: [{ ...ENTRY, chamber: "assembly" }] }, /chamber must be one of house, senate/],
      [{ judgments: [{ ...ENTRY, congress: "119" }] }, /congress must be a positive integer/],
      [{ judgments: [{ ...ENTRY, session: 3 }] }, /session must be 1 or 2/],
      [{ judgments: [{ ...ENTRY, roll: 0 }] }, /roll must be a positive integer/],
      [{ judgments: [{ ...ENTRY, measure_id: undefined }] }, /measure_id must be a non-empty string, or null/],
      [{ judgments: [{ ...ENTRY, measure_id: " " }] }, /measure_id must be a non-empty string, or null/],
      [{ judgments: [{ ...ENTRY, vote_date: "May 22, 2025" }] }, /vote_date must be an ISO date/],
      [{ judgments: [{ ...ENTRY, review_status: "rejected" }] }, /review_status must be one of pending, approved/],
      [{ judgments: [{ ...ENTRY, nay_description: " " }] }, /nay_description must be a non-empty string/],
      [{ judgments: [{ ...ENTRY, nay_description: ENTRY.yea_description.toUpperCase() }] }, /same sentence/],
      [{ judgments: [{ ...ENTRY, labels: [] }] }, /judgments\[0\]: labels is not a non-empty array/],
      [{ judgments: [{ ...ENTRY, labels: [{ slug: "housing", yea: "for" }] }] }, /'housing' is not allowed/],
      [{ judgments: [{ ...ENTRY, labels: [{ slug: "general", yea: "for" }] }] }, /must not include stance/],
      [{ judgments: [ENTRY, { ...ENTRY, review_status: "pending" }] }, /judgments\[1\]: US:house:119-1:145 appears more than once/],
    ];
    for (const [raw, pattern] of cases) {
      expect(() => parseJudgmentsFile(raw, SLUGS), JSON.stringify(raw).slice(0, 80)).toThrow(pattern);
    }
  });
});

const OHIO_ENTRY = {
  jurisdiction: "OH",
  chamber: "house",
  session: "136",
  roll: 1744207254,
  measure_id: "HB 96",
  vote_date: "2025-04-09",
  review_status: "approved",
  yea_description: "Voted to pass H.B. 96. It passed the Ohio House 60-39.",
  nay_description: "Voted against passing H.B. 96. It passed the Ohio House 60-39.",
  labels: [{ slug: "general" }],
};

describe("parseJudgmentsFile (state entries)", () => {
  it("turns an Ohio entry into the store's judgment shape", () => {
    expect(parseJudgmentsFile({ judgments: [OHIO_ENTRY] }, SLUGS)[0]).toMatchObject({
      jurisdiction: "OH",
      chamber: "house",
      session: "136",
      rollNumber: 1744207254,
      measureId: "HB 96",
      voteDate: "2025-04-09",
      reviewStatus: "approved",
    });
  });

  it("mixes federal and state entries in one file", () => {
    const entries = parseJudgmentsFile({ judgments: [ENTRY, OHIO_ENTRY] }, SLUGS);
    expect(entries.map((entry) => `${entry.jurisdiction}:${entry.session}`)).toEqual(["US:119-1", "OH:136"]);
  });

  it("rejects malformed state entries", () => {
    const cases: [unknown, RegExp][] = [
      [{ judgments: [{ ...OHIO_ENTRY, jurisdiction: "ZZ" }] }, /jurisdiction must be omitted \(federal\) or one of OH/],
      [{ judgments: [{ ...OHIO_ENTRY, congress: 119 }] }, /a state entry names session, not congress/],
      [{ judgments: [{ ...OHIO_ENTRY, session: 136 }] }, /session must be the source's session key/],
      [{ judgments: [{ ...OHIO_ENTRY, session: " " }] }, /session must be the source's session key/],
      [{ judgments: [OHIO_ENTRY, { ...OHIO_ENTRY, review_status: "pending" }] }, /OH:house:136:1744207254 appears more than once/],
    ];
    for (const [file, pattern] of cases) {
      expect(() => parseJudgmentsFile(file, SLUGS), JSON.stringify(file).slice(0, 80)).toThrow(pattern);
    }
  });
});
