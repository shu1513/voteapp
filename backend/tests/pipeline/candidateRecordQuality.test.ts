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

  it("classifies primary results and election-office qualification listings as disallowed thin records", () => {
    // Live escape (November state-leg repair pass): 22 primary-result rows
    // across 22 candidates — for nine, the candidate's ONLY row. All of these
    // are verbatim shapes from the retired rows.
    expect(
      classifyCandidateRecordQuality({
        description: "Won the Democratic primary for Georgia State Senate District 31 with 8,555 votes.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "pure_candidacy" });

    expect(
      classifyCandidateRecordQuality({
        description:
          "Penman won the March 17, 2026 Republican primary for Senate District 33 and advanced to the general election.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "pure_candidacy" });

    expect(
      classifyCandidateRecordQuality({
        description: "Willoughby lost the 2018 Republican primary for Arizona House District 17.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "pure_candidacy" });

    expect(
      classifyCandidateRecordQuality({
        description: "Michele Clark filed as the Democratic candidate for Illinois Senate District 33.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "pure_candidacy" });

    expect(
      classifyCandidateRecordQuality({
        description:
          "Florida's Division of Elections recorded Woodson as the incumbent Democratic candidate for House District 105, qualified by fee on June 8, 2026, and unopposed for the 2026 general election.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "pure_candidacy" });

    expect(
      classifyCandidateRecordQuality({
        description:
          "Tom Lally filed as the Republican candidate for Illinois Senate District 9 and remains active on the November 3, 2026 general-election candidate list.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "pure_candidacy" });
  });

  it("keeps general/special election wins and legislative-act language out of the candidacy patterns", () => {
    // Winning the FINAL election confers office — a service fact, not roster
    // evidence. The patterns anchor on primary/runoff-stage words on purpose.
    expect(
      classifyCandidateRecordQuality({
        description:
          "Won the December 9, 2025 special election for Florida House District 90, succeeding the late Rep. Joe Casello.",
      }).reason
    ).not.toBe("pure_candidacy");

    // "was elected to" is substantive and checked first.
    expect(
      classifyCandidateRecordQuality({
        description: "Was elected to the Florida House for District 115 in the 2024 general election and has served since.",
      })
    ).toEqual({ classification: "substantive", reason: "actual_record_action" });

    // Secretary-of-State ACT recordings must not trip the election-office
    // "recorded … as … candidate" pattern.
    expect(
      classifyCandidateRecordQuality({
        description:
          "Authored HB 180; the Governor signed it and the Secretary of State recorded it as Act 2024-419 with Shirey named first among its authors.",
      })
    ).toEqual({ classification: "substantive", reason: "actual_record_action" });

    // A primary win alongside a real completed action is rescued by the
    // substantive-verbs-first ordering.
    expect(
      classifyCandidateRecordQuality({
        description: "Won the 2026 Republican primary and sponsored HB 1 establishing the state audit office.",
      })
    ).toEqual({ classification: "substantive", reason: "actual_record_action" });
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

  it("does not let routine reimbursements rescue candidacy-machinery rows", () => {
    // Bare "reimbursed" is campaign bookkeeping, not misconduct — only the
    // adverb-anchored wrongdoing form is substantive.
    expect(
      classifyCandidateRecordQuality({
        description: "The campaign reimbursed the candidate qualifying fee in July 2026.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "pure_candidacy" });

    expect(
      classifyCandidateRecordQuality({
        description: "Her committee reimbursed her for campaign travel expenses.",
      })
    ).toEqual({ classification: "neutral_context", reason: "unclassified_context" });
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

  it("rejects a promise that takes a noun object instead of an infinitive", () => {
    // Live leak: this is 100% prospective, yet it escaped every promise
    // pattern. "pledges TO" needs the infinitive; the campaign/platform rule
    // needs that word BEFORE the verb (here "campaign" trails it); and the
    // past-tense rule does not match "pledges". It then matched "profile" in
    // FALLBACK_CONTEXT_PATTERNS and became a writable neutral_context row.
    expect(
      classifyCandidateRecordQuality({
        description:
          "Heather-Marie Wilson's 2026 independent-candidate profile pledges a people-powered campaign, rejection of special-interest and big-money influence, transparent policy agendas, and reporting of attempts to unduly influence her.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "future_promise" });

    expect(
      classifyCandidateRecordQuality({
        description: "Promises a full audit of the county budget.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "future_promise" });
  });

  it("rejects promises whose object is a bare or quantified noun phrase", () => {
    // The first version of the noun-object rule was determiner-anchored and
    // only caught "pledges A/THE/HIS ...", so every bare noun phrase walked
    // straight through into the writable bucket.
    for (const description of [
      "The candidate profile promises transparency and accountable government.",
      "She pledges lower taxes.",
      "He vows reform.",
      "She pledges $1 million for schools.",
      "Little pledges fiscal responsibility and conservative economic principles for Florida families.",
    ]) {
      expect(classifyCandidateRecordQuality({ description }), description).toEqual({
        classification: "disallowed_thin",
        reason: "future_promise",
      });
    }
  });

  it("does not let a verb inside a promised THING count as a completed action", () => {
    // The contractors were convicted; the candidate only promised a ban. The
    // commission has not been led by anyone — it does not exist yet. Both read
    // as substantive purely because a completed-action verb sat inside the
    // promised object, which the infinitive mask never covered.
    for (const description of [
      "The candidate profile pledges a ban on contractors convicted of fraud.",
      "The candidate profile promises a commission led by an independent chair.",
    ]) {
      expect(classifyCandidateRecordQuality({ description }), description).toEqual({
        classification: "disallowed_thin",
        reason: "future_promise",
      });
    }
  });

  it("keeps promise OUTCOMES, which are completed actions", () => {
    // Breaking a pledge is an integrity record and among the more
    // voter-relevant things we store. The promissory noun makes these look
    // like promises to a naive pattern; they are the opposite.
    for (const description of [
      "She kept a promise a year after taking office.",
      "She broke a pledge a month later.",
      "He fulfilled promises his campaign made.",
      "The governor abandoned his promises a year later.",
      "He reneged on a campaign pledge on housing.",
    ]) {
      expect(classifyCandidateRecordQuality({ description }), description).toEqual({
        classification: "substantive",
        reason: "actual_record_action",
      });
    }
  });

  it("does not let an outcome inside a PROMISE vouch for the record", () => {
    // The governor broke the pledge; the candidate only promised to look into
    // it. An outcome check that ran against the raw text scored these as
    // completed actions on someone else's conduct.
    for (const description of [
      "She promised to investigate whether the governor broke his campaign pledge.",
      "She pledges an audit of whether the mayor violated a commitment.",
    ]) {
      expect(classifyCandidateRecordQuality({ description }), description).toEqual({
        classification: "disallowed_thin",
        reason: "future_promise",
      });
    }
  });

  it("masks a whole promised LIST, not just its first item", () => {
    // Campaign copy lists promised programs. Stopping at the first comma left
    // "a commission led by independent experts" exposed, and "led" scored the
    // entire prospective sentence as substantive.
    expect(
      classifyCandidateRecordQuality({
        description:
          "The candidate profile promises a ban on convicted contractors, a commission led by independent experts.",
      })
    ).toEqual({ classification: "disallowed_thin", reason: "future_promise" });
  });

  it("treats the noun sense as a noun however it is introduced", () => {
    // Each of these was REJECTED as a future promise while saying the
    // opposite: Diaz opposes such pledges, and the other two describe someone
    // else's promises after the fact. A blacklist of preceding words missed
    // all three, which is why the rule now requires a plausible subject.
    for (const description of [
      "Diaz said judges should not make pledges to decide pending cases.",
      "The profile criticized Wilson's promises from the 2022 campaign.",
      "The report reviewed a series of pledges made during the race.",
    ]) {
      expect(classifyCandidateRecordQuality({ description }).reason, description).not.toBe(
        "future_promise"
      );
    }
  });

  it("leaves the NOUN sense alone when it follows a verb or a modifier", () => {
    // Live corpus rows: "should not MAKE pledges about cases" and "opposition
    // to ... UNFUNDED promises" are noun uses with no promise being made by
    // the candidate. A determiner-only lookbehind missed both.
    expect(
      classifyCandidateRecordQuality({
        description:
          "Diaz said judges should not make pledges about cases and must set aside personal views.",
      }).reason
    ).not.toBe("future_promise");

    expect(
      classifyCandidateRecordQuality({
        description:
          "Drew advocates fiscal discipline through opposition to unchecked spending, unfunded promises, and accounting gimmicks.",
      }).reason
    ).not.toBe("future_promise");
  });

  it("leaves the ATTRIBUTIVE participle alone when the promise is provably someone else's", () => {
    // Live wave-18 false positive: a timeshare-fraud suit described as
    // "without delivering the promised service" was rejected as the
    // candidate's own future promise — the COMPANY promised the service.
    // Only article-preceded, "its"-preceded, and hyphen-compound participles
    // are excluded; none of them can be a verb use or the candidate's own
    // promise.
    for (const description of [
      "Filed a consumer-fraud lawsuit against a timeshare exit company that took payments without delivering the promised service.",
      "Criticized the utility for failing to deliver its promised service upgrades.",
      "Praised the long-promised reforms after decades of delay.",
    ]) {
      expect(classifyCandidateRecordQuality({ description }).reason, description).not.toBe(
        "future_promise"
      );
    }

    // Verb uses and PERSONAL-possessive participles must still reject: in a
    // third-person description "his promised tax cuts" is normally the
    // candidate's own promise, and "their" includes singular-they candidates.
    for (const description of [
      "She promised lower taxes.",
      "Promised as a judicial candidate to uphold the law.",
      "Outlined his promised tax cuts.",
      "Their promised audit of the borough budget.",
    ]) {
      expect(classifyCandidateRecordQuality({ description }), description).toEqual({
        classification: "disallowed_thin",
        reason: "future_promise",
      });
    }
  });

  it("still keeps completed actions that carry the NOUN 'pledge'", () => {
    // Why the new pattern is determiner-anchored rather than a bare
    // /\bpledges?\b/: signing a pledge IS a completed action, and these must
    // survive. Substantive verbs are matched first, which is what rescues them.
    expect(
      classifyCandidateRecordQuality({
        description: "Cloud signed the U.S. Term Limits convention pledge.",
      })
    ).toEqual({ classification: "substantive", reason: "actual_record_action" });

    expect(
      classifyCandidateRecordQuality({
        description: "Voted for the state budget after signing the taxpayer protection pledge.",
      })
    ).toEqual({ classification: "substantive", reason: "actual_record_action" });
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
