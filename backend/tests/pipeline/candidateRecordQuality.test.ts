import { describe, expect, it } from "vitest";

import {
  classifyCandidateRecordQuality,
  isDisallowedThinCandidateRecord,
} from "../../src/pipeline/candidates/candidateRecordQuality.js";

describe("candidate record quality", () => {
  it("classifies pure candidacy and ballot-listing facts as disallowed thin records", () => {
    expect(
      classifyCandidateRecordQuality({
        description: "The Vermont Secretary of State lists Aly Richards as a candidate for Governor.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "pure_candidacy" });

    expect(
      classifyCandidateRecordQuality({
        description: "Amanda Janoo filed paperwork to run for Governor in the 2026 primary.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "pure_candidacy" });

    expect(
      isDisallowedThinCandidateRecord({
        description: "Phil Scott appears on the ballot for Governor.",
      })
    ).toBe(true);
  });

  it("classifies campaign promises and future plans as disallowed thin records", () => {
    expect(
      classifyCandidateRecordQuality({
        description: "The campaign website promises to cut taxes after the election.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "future_promise" });

    expect(
      classifyCandidateRecordQuality({
        description: "The candidate says she will expand health care access if elected.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "future_promise" });
  });

  it("classifies past-tense promissory phrasing as a future promise", () => {
    // Live escape: this exact phrasing was accepted and written as a
    // canonical record because only present-tense "promises to" matched.
    expect(
      classifyCandidateRecordQuality({
        description:
          "Promised as a judicial candidate to uphold impartiality and legal competence on the bench.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "future_promise" });

    expect(
      classifyCandidateRecordQuality({
        description: "She pledged during the forum that she would recuse herself from conflicts.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "future_promise" });

    // A completed action alongside promissory language is still substantive.
    expect(
      classifyCandidateRecordQuality({
        description: "As promised during the campaign, she sponsored the disclosure bill in 2025.",
      })
    ).toEqual({ classification: "substantive", reason: "actual_record_action" });
  });

  it("classifies completed public actions and service as substantive records", () => {
    expect(
      classifyCandidateRecordQuality({
        description: "Phil Scott signed H.289 into law in 2023.",
      })
    ).toEqual({ classification: "substantive", reason: "actual_record_action" });

    expect(
      classifyCandidateRecordQuality({
        description: "Sarah Copeland Hanzas serves as Vermont Secretary of State.",
      })
    ).toEqual({ classification: "substantive", reason: "actual_record_action" });

    expect(
      isDisallowedThinCandidateRecord({
        description: "Mike Pieciak oversaw Vermont Treasury investment policy.",
      })
    ).toBe(false);
  });

  it("keeps mixed candidacy or future-tense descriptions when they contain completed actions", () => {
    expect(
      classifyCandidateRecordQuality({
        description: "Jane Doe, who is running for reelection, signed H.289 into law.",
      })
    ).toEqual({ classification: "substantive", reason: "actual_record_action" });

    expect(
      classifyCandidateRecordQuality({
        description: "Phil Scott vetoed a bill that would raise taxes.",
      })
    ).toEqual({ classification: "substantive", reason: "actual_record_action" });

    expect(
      isDisallowedThinCandidateRecord({
        description: "Tom Jones ran for Senate in 2016 before serving as Secretary of State.",
      })
    ).toBe(false);
  });

  it("keeps biography and occupation facts as neutral fallback context", () => {
    expect(
      classifyCandidateRecordQuality({
        description: "The candidate biography says she earned a degree from the University of Vermont.",
      })
    ).toEqual({ classification: "neutral_context", reason: "fallback_context" });

    expect(
      classifyCandidateRecordQuality({
        description: "The profile says the candidate worked as a public school teacher.",
      })
    ).toEqual({ classification: "neutral_context", reason: "fallback_context" });
  });

  it("defaults unclear records to neutral context instead of rejecting them", () => {
    expect(
      classifyCandidateRecordQuality({
        description: "The source describes the candidate's community background.",
      })
    ).toEqual({ classification: "neutral_context", reason: "unclassified_context" });
  });

  it("rejects empty descriptions defensively", () => {
    expect(
      classifyCandidateRecordQuality({
        description: "   ",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "unclassified_context" });
  });
});
