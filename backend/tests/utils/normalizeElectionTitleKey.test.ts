import { describe, expect, it } from "vitest";

import { normalizeElectionTitleKey } from "../../src/utils/normalizeElectionTitleKey.js";

describe("normalizeElectionTitleKey", () => {
  it("normalizes punctuation and spacing", () => {
    expect(normalizeElectionTitleKey("Judge of the Superior Court, Office No. 2")).toBe(
      "judge of the superior court office no 2"
    );
    expect(normalizeElectionTitleKey("JUDGE OF THE SUPERIOR COURT Office No 2")).toBe(
      "judge of the superior court office no 2"
    );
    expect(normalizeElectionTitleKey("Member - Board of Supervisors, 1st District")).toBe(
      "member board of supervisors 1st district"
    );
  });

  it("normalizes leading zeros in office number", () => {
    expect(normalizeElectionTitleKey("Judge, Office No. 002")).toBe("judge office no 2");
  });

  it("does not rewrite no as a word prefix (Office North)", () => {
    expect(normalizeElectionTitleKey("Office North District")).toBe("office north district");
  });
});
