import { describe, expect, it } from "vitest";

import { parseJudgmentsFile } from "../../src/scripts/judgeRollCallVotes.js";

const SLUGS = new Set(["general", "integrity_and_ethics", "immigration", "gun_control"]);

const ENTRY = {
  chamber: "house",
  congress: 119,
  session: 1,
  roll: 145,
  review_status: "approved",
  yea_description: "Voted to pass H.R. 1. It passed the House 215-214.",
  nay_description: "Voted against passing H.R. 1. It passed the House 215-214.",
  labels: [{ slug: "immigration", yea: "for" }, { slug: "general" }],
};

describe("parseJudgmentsFile", () => {
  it("turns a file entry into the store's judgment shape", () => {
    expect(parseJudgmentsFile({ judgments: [ENTRY] }, SLUGS)).toEqual([
      {
        jurisdiction: "US",
        chamber: "house",
        session: "119-1",
        rollNumber: 145,
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
      [{ judgments: [{ ...ENTRY, review_status: "rejected" }] }, /review_status must be one of pending, approved/],
      [{ judgments: [{ ...ENTRY, nay_description: " " }] }, /nay_description must be a non-empty string/],
      [{ judgments: [{ ...ENTRY, nay_description: ENTRY.yea_description.toUpperCase() }] }, /same sentence/],
      [{ judgments: [{ ...ENTRY, labels: [] }] }, /judgments\[0\]: labels is not a non-empty array/],
      [{ judgments: [{ ...ENTRY, labels: [{ slug: "housing", yea: "for" }] }] }, /'housing' is not allowed/],
      [{ judgments: [{ ...ENTRY, labels: [{ slug: "general", yea: "for" }] }] }, /must not include stance/],
      [{ judgments: [ENTRY, { ...ENTRY, review_status: "pending" }] }, /judgments\[1\]: house:119-1:145 appears more than once/],
    ];
    for (const [raw, pattern] of cases) {
      expect(() => parseJudgmentsFile(raw, SLUGS), JSON.stringify(raw).slice(0, 80)).toThrow(pattern);
    }
  });
});
