import { describe, expect, it } from "vitest";
import { isJudicialRetentionTitle, isRetentionRace } from "./retention";

describe("isJudicialRetentionTitle", () => {
  it("matches the retention phrasings states print", () => {
    expect(isJudicialRetentionTitle("Shall Judge Pat Example be retained in office?")).toBe(true);
    expect(isJudicialRetentionTitle("Retention of 4th Judicial District Court Judge Denise M. Porter")).toBe(true);
    expect(isJudicialRetentionTitle("Supreme Court Justice - Retain Jane Doe?")).toBe(true);
  });

  it("leaves ordinary races alone", () => {
    expect(isJudicialRetentionTitle("District Court Judge, Division 3")).toBe(false);
    expect(isJudicialRetentionTitle("State Senator, District 4")).toBe(false);
    expect(isJudicialRetentionTitle("Proposition 1: Road and Bridge Bond")).toBe(false);
  });

  it("needs a judicial noun, not just the retention verb", () => {
    expect(isJudicialRetentionTitle("Water Retention District Director")).toBe(false);
    expect(isJudicialRetentionTitle("Shall Pat Example be retained as Fire Chief?")).toBe(false);
  });
});

describe("isRetentionRace", () => {
  it("needs an office race and a retention title", () => {
    expect(isRetentionRace({ race_type: "office", official_ballot_title: "Shall Judge X be retained?" })).toBe(true);
    expect(isRetentionRace({ race_type: "ballot_measure", official_ballot_title: "Shall Judge X be retained?" })).toBe(
      false
    );
    expect(isRetentionRace({ race_type: "office", official_ballot_title: "Mayor" })).toBe(false);
  });
});
