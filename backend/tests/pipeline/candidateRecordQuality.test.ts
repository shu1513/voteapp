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

  it("classifies routine candidacy-machinery filings as disallowed thin records", () => {
    // Live escape (38 Orange County FL candidates): periodic finance-report
    // filings were accepted as neutral records and made unresearched
    // candidates look complete.
    expect(
      classifyCandidateRecordQuality({
        description: "Asima Azam filed a P2 campaign-finance report covering June 13 through June 26, 2026.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "pure_candidacy" });

    expect(
      classifyCandidateRecordQuality({
        description:
          "Her campaign reported a $7,639.20 payment to the Supervisor of Elections for her candidate qualifying fee.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "pure_candidacy" });

    expect(
      classifyCandidateRecordQuality({
        description:
          "Florida election records show he qualified as a Republican candidate for Governor after paying the qualifying fee.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "pure_candidacy" });

    // Article/pronoun boundary forms of the filing pattern, and the plural
    // fee form — ordinary wording must not resurrect the original escape.
    expect(
      classifyCandidateRecordQuality({
        description: "She filed the required P2 campaign-finance report on time.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "pure_candidacy" });

    expect(
      classifyCandidateRecordQuality({
        description: "He filed his year-end campaign finance disclosure with the county.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "pure_candidacy" });

    expect(
      classifyCandidateRecordQuality({
        description: "Paid the qualifying fees for both county offices in June 2026.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "pure_candidacy" });
  });

  it("rescues campaign-finance misconduct from the candidacy-machinery patterns", () => {
    // The machinery patterns match these sentences, but the misconduct verb
    // is the record: past-tense enforcement/misconduct verbs are substantive
    // and are checked first.
    expect(
      classifyCandidateRecordQuality({
        description:
          "She filed a false campaign-finance report that concealed a $50,000 contribution.",
      })
    ).toEqual({ classification: "substantive", reason: "actual_record_action" });

    expect(
      classifyCandidateRecordQuality({
        description:
          "An ethics audit found that her campaign illegally reimbursed the candidate qualifying fee.",
      })
    ).toEqual({ classification: "substantive", reason: "actual_record_action" });

    expect(
      classifyCandidateRecordQuality({
        description: "He was fined $2,500 for filing his campaign-finance reports late.",
      })
    ).toEqual({ classification: "substantive", reason: "actual_record_action" });
  });

  it("keeps the misconduct verbs from rescuing future promises", () => {
    // Tenseless wrongdoing adjectives ("illegal", "false") are deliberately
    // NOT substantive: they would pull promises like this one out of
    // future_promise.
    expect(
      classifyCandidateRecordQuality({
        description: "The candidate says she will fight illegal dumping in the harbor.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "future_promise" });

    expect(
      classifyCandidateRecordQuality({
        description: "Promised to hold anyone who concealed contributions accountable.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "future_promise" });
  });

  it("keeps records where a finance filing is the source, not the event", () => {
    // Live canonical integrity record: the finding is a sitting judge's
    // contribution; the filing is only where it surfaced. The filing pattern
    // is article-anchored ("filed a/his ... report") so this participle noun
    // phrase does not match.
    expect(
      classifyCandidateRecordQuality({
        description:
          "A filed campaign-finance statement for Karen McDonald for Prosecutor reported a $100 direct contribution from Christopher Dingell, identified as a State of Michigan judge.",
      })
    ).toEqual({ classification: "neutral_context", reason: "unclassified_context" });
  });

  it("keeps legislation about fees, reports, or qualifying programs substantive", () => {
    expect(
      classifyCandidateRecordQuality({
        description: "Sponsored a bill waiving the qualifying fee for veteran candidates.",
      })
    ).toEqual({ classification: "substantive", reason: "actual_record_action" });

    expect(
      classifyCandidateRecordQuality({
        description:
          "Sponsored Senate Bill 589, requiring qualifying health plans to cover a 12-month contraceptive refill.",
      })
    ).toEqual({ classification: "substantive", reason: "actual_record_action" });
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

  it("does not let a substantive verb inside a promise's content rescue the promise", () => {
    // "passed" belongs to the promise's object, not to anything the
    // candidate did; the promissory infinitive complement is blanked before
    // the substantive check.
    expect(
      classifyCandidateRecordQuality({
        description: "She promised to veto any tax increase passed by the legislature.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "future_promise" });

    expect(
      classifyCandidateRecordQuality({
        description: "Vowed to sponsor legislation expanding the homestead exemption.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "future_promise" });

    // A real action in a separate clause survives the blanking.
    expect(
      classifyCandidateRecordQuality({
        description: "He pledged to reform the agency, and in 2024 he signed the reorganization order.",
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
