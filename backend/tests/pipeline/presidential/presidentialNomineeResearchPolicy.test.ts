import { describe, expect, it } from "vitest";

import {
  addPresidentialNomineeResearchDelay,
  evaluatePresidentialNomineeResearchEligibility,
  getPresidentialNomineeResearchStartAt,
  getPresidentialNomineeResearchStopAt,
  PRESIDENTIAL_NOMINEE_RESEARCH_INTERVAL_DAYS,
} from "../../../src/pipeline/presidential/presidentialNomineeResearchPolicy.js";

describe("presidentialNomineeResearchPolicy", () => {
  it("opens 9 months before and closes 5 months before presidential Election Day", () => {
    expect(getPresidentialNomineeResearchStartAt(2028).toISOString()).toBe(
      "2028-02-07T00:00:00.000Z"
    );
    expect(getPresidentialNomineeResearchStopAt(2028).toISOString()).toBe(
      "2028-06-07T00:00:00.000Z"
    );
  });

  it("uses the nominee research cadence inside the research window", () => {
    expect(PRESIDENTIAL_NOMINEE_RESEARCH_INTERVAL_DAYS).toBe(2);
    expect(addPresidentialNomineeResearchDelay(new Date("2028-02-07T12:00:00.000Z"), 2028)?.toISOString()).toBe(
      "2028-02-09T12:00:00.000Z"
    );
    expect(addPresidentialNomineeResearchDelay(new Date("2028-06-06T12:00:00.000Z"), 2028)).toBeNull();
  });

  it("allows active cycles when they are due", () => {
    const result = evaluatePresidentialNomineeResearchEligibility({
      electionYear: 2028,
      cycleStatus: "active",
      now: new Date("2028-02-07T00:00:00.000Z"),
    });

    expect(result).toEqual({
      eligible: true,
      reason: "due",
      researchStartAt: new Date("2028-02-07T00:00:00.000Z"),
      researchStopAt: new Date("2028-06-07T00:00:00.000Z"),
    });
  });

  it("blocks before window, completed cycles, future due times, and after window", () => {
    expect(
      evaluatePresidentialNomineeResearchEligibility({
        electionYear: 2028,
        cycleStatus: "active",
        now: new Date("2028-02-06T23:59:59.999Z"),
      })
    ).toMatchObject({ eligible: false, reason: "before_research_window" });

    expect(
      evaluatePresidentialNomineeResearchEligibility({
        electionYear: 2028,
        cycleStatus: "completed",
        now: new Date("2028-02-07T00:00:00.000Z"),
      })
    ).toMatchObject({ eligible: false, reason: "cycle_completed", nextEligibleAt: null });

    expect(
      evaluatePresidentialNomineeResearchEligibility({
        electionYear: 2028,
        cycleStatus: "active",
        nextResearchAt: "2028-02-09T00:00:00.000Z",
        now: new Date("2028-02-08T23:59:59.999Z"),
      })
    ).toMatchObject({
      eligible: false,
      reason: "not_due",
      nextEligibleAt: new Date("2028-02-09T00:00:00.000Z"),
    });

    expect(
      evaluatePresidentialNomineeResearchEligibility({
        electionYear: 2028,
        cycleStatus: "active",
        now: new Date("2028-06-07T00:00:00.000Z"),
      })
    ).toMatchObject({ eligible: false, reason: "after_research_window", nextEligibleAt: null });
  });
});
