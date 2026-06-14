import { describe, expect, it } from "vitest";

import {
  addPresidentialRosterResearchDelay,
  evaluatePresidentialRosterResearchEligibility,
  getPresidentialRosterResearchPhase,
  getPresidentialRosterResearchStartAt,
  getPresidentialRosterResearchStopAt,
  PRESIDENTIAL_ROSTER_RESEARCH_ACTIVE_PRE_PRIMARY_DAYS,
  PRESIDENTIAL_ROSTER_RESEARCH_BALLOT_QUALIFICATION_DAYS,
  PRESIDENTIAL_ROSTER_RESEARCH_PRIMARY_SEASON_DAYS,
  PRESIDENTIAL_ROSTER_RESEARCH_WEEKLY_DAYS,
} from "../../../src/pipeline/presidential/presidentialRosterResearchPolicy.js";

describe("presidentialRosterResearchPolicy", () => {
  it("opens 20 months before and closes 5 months before presidential Election Day", () => {
    expect(getPresidentialRosterResearchStartAt(2028).toISOString()).toBe(
      "2027-03-07T00:00:00.000Z"
    );
    expect(getPresidentialRosterResearchStopAt(2028).toISOString()).toBe(
      "2028-06-07T00:00:00.000Z"
    );
  });

  it("maps the requested roster research phases", () => {
    expect(getPresidentialRosterResearchPhase(new Date("2027-03-07T00:00:00.000Z"), 2028)).toBe(
      "early_announcement"
    );
    expect(getPresidentialRosterResearchPhase(new Date("2027-07-07T00:00:00.000Z"), 2028)).toBe(
      "active_pre_primary"
    );
    expect(getPresidentialRosterResearchPhase(new Date("2027-11-07T00:00:00.000Z"), 2028)).toBe(
      "ballot_qualification"
    );
    expect(getPresidentialRosterResearchPhase(new Date("2028-01-07T00:00:00.000Z"), 2028)).toBe(
      "primary_season"
    );
    expect(getPresidentialRosterResearchPhase(new Date("2028-06-07T00:00:00.000Z"), 2028)).toBeNull();
  });

  it("computes the requested cadence in each phase", () => {
    expect(PRESIDENTIAL_ROSTER_RESEARCH_WEEKLY_DAYS).toBe(7);
    expect(PRESIDENTIAL_ROSTER_RESEARCH_ACTIVE_PRE_PRIMARY_DAYS).toBe(3);
    expect(PRESIDENTIAL_ROSTER_RESEARCH_BALLOT_QUALIFICATION_DAYS).toBe(2);
    expect(PRESIDENTIAL_ROSTER_RESEARCH_PRIMARY_SEASON_DAYS).toBe(2);

    expect(addPresidentialRosterResearchDelay(new Date("2027-03-07T12:00:00.000Z"), 2028)?.toISOString()).toBe(
      "2027-03-14T12:00:00.000Z"
    );
    expect(addPresidentialRosterResearchDelay(new Date("2027-07-07T12:00:00.000Z"), 2028)?.toISOString()).toBe(
      "2027-07-10T12:00:00.000Z"
    );
    expect(addPresidentialRosterResearchDelay(new Date("2027-11-07T12:00:00.000Z"), 2028)?.toISOString()).toBe(
      "2027-11-09T12:00:00.000Z"
    );
    expect(addPresidentialRosterResearchDelay(new Date("2028-01-07T12:00:00.000Z"), 2028)?.toISOString()).toBe(
      "2028-01-09T12:00:00.000Z"
    );
  });

  it("allows active cycles when they are due", () => {
    const result = evaluatePresidentialRosterResearchEligibility({
      electionYear: 2028,
      cycleStatus: "active",
      now: new Date("2027-03-07T00:00:00.000Z"),
    });

    expect(result).toEqual({
      eligible: true,
      reason: "due",
      phase: "early_announcement",
      researchStartAt: new Date("2027-03-07T00:00:00.000Z"),
      researchStopAt: new Date("2028-06-07T00:00:00.000Z"),
    });
  });

  it("blocks before window, completed cycles, future due times, and after window", () => {
    expect(
      evaluatePresidentialRosterResearchEligibility({
        electionYear: 2028,
        cycleStatus: "active",
        now: new Date("2027-03-06T23:59:59.999Z"),
      })
    ).toMatchObject({ eligible: false, reason: "before_research_window" });

    expect(
      evaluatePresidentialRosterResearchEligibility({
        electionYear: 2028,
        cycleStatus: "completed",
        now: new Date("2027-03-07T00:00:00.000Z"),
      })
    ).toMatchObject({ eligible: false, reason: "cycle_completed", nextEligibleAt: null });

    expect(
      evaluatePresidentialRosterResearchEligibility({
        electionYear: 2028,
        cycleStatus: "active",
        nextResearchAt: "2027-03-14T00:00:00.000Z",
        now: new Date("2027-03-13T23:59:59.999Z"),
      })
    ).toMatchObject({
      eligible: false,
      reason: "not_due",
      nextEligibleAt: new Date("2027-03-14T00:00:00.000Z"),
    });

    expect(
      evaluatePresidentialRosterResearchEligibility({
        electionYear: 2028,
        cycleStatus: "active",
        now: new Date("2028-06-07T00:00:00.000Z"),
      })
    ).toMatchObject({ eligible: false, reason: "after_research_window", nextEligibleAt: null });
  });
});
