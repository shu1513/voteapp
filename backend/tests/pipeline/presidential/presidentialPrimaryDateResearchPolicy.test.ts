import { describe, expect, it } from "vitest";

import {
  addPresidentialPrimaryDateResearchRetryDelay,
  evaluatePresidentialPrimaryDateResearchEligibility,
  getPresidentialPrimaryDateResearchStartAt,
  PRESIDENTIAL_PRIMARY_DATE_RESEARCH_RETRY_DAYS,
  type PresidentialPrimaryDateResearchStatus,
} from "../../../src/pipeline/presidential/presidentialPrimaryDateResearchPolicy.js";

describe("presidentialPrimaryDateResearchPolicy", () => {
  it("starts primary-date research 20 months before presidential Election Day", () => {
    expect(getPresidentialPrimaryDateResearchStartAt(2028).toISOString()).toBe(
      "2027-03-07T00:00:00.000Z"
    );
    expect(getPresidentialPrimaryDateResearchStartAt(2032).toISOString()).toBe(
      "2031-03-02T00:00:00.000Z"
    );
  });

  it("blocks rows before the research window opens", () => {
    const result = evaluatePresidentialPrimaryDateResearchEligibility({
      electionYear: 2028,
      dateResearchStatus: "pending",
      now: new Date("2027-03-06T23:59:59.999Z"),
    });

    expect(result).toEqual({
      eligible: false,
      reason: "before_research_window",
      researchStartAt: new Date("2027-03-07T00:00:00.000Z"),
      nextEligibleAt: new Date("2027-03-07T00:00:00.000Z"),
    });
  });

  it("allows pending rows once the research window opens", () => {
    const result = evaluatePresidentialPrimaryDateResearchEligibility({
      electionYear: 2028,
      dateResearchStatus: "pending",
      now: new Date("2027-03-07T00:00:00.000Z"),
    });

    expect(result).toEqual({
      eligible: true,
      reason: "due",
      researchStartAt: new Date("2027-03-07T00:00:00.000Z"),
    });
  });

  it("skips rows that already have an official date", () => {
    const result = evaluatePresidentialPrimaryDateResearchEligibility({
      electionYear: 2028,
      dateResearchStatus: "official_found",
      nextResearchAt: "2027-03-08T00:00:00.000Z",
      now: new Date("2027-03-09T00:00:00.000Z"),
    });

    expect(result).toEqual({
      eligible: false,
      reason: "already_official",
      researchStartAt: new Date("2027-03-07T00:00:00.000Z"),
      nextEligibleAt: null,
    });
  });

  it("respects future next_research_at values", () => {
    const result = evaluatePresidentialPrimaryDateResearchEligibility({
      electionYear: 2028,
      dateResearchStatus: "not_official_yet",
      nextResearchAt: "2027-03-14T00:00:00.000Z",
      now: new Date("2027-03-13T23:59:59.999Z"),
    });

    expect(result).toEqual({
      eligible: false,
      reason: "not_due",
      researchStartAt: new Date("2027-03-07T00:00:00.000Z"),
      nextEligibleAt: new Date("2027-03-14T00:00:00.000Z"),
    });
  });

  it("allows not-official-yet and error rows when the retry time has arrived", () => {
    for (const status of ["not_official_yet", "error"] as const) {
      const result = evaluatePresidentialPrimaryDateResearchEligibility({
        electionYear: 2028,
        dateResearchStatus: status,
        nextResearchAt: "2027-03-14T00:00:00.000Z",
        now: new Date("2027-03-14T00:00:00.000Z"),
      });

      expect(result).toEqual({
        eligible: true,
        reason: "due",
        researchStartAt: new Date("2027-03-07T00:00:00.000Z"),
      });
    }
  });

  it("computes the retry delay from the provided reference time", () => {
    expect(
      addPresidentialPrimaryDateResearchRetryDelay(new Date("2027-03-07T15:30:00.000Z")).toISOString()
    ).toBe("2027-03-14T15:30:00.000Z");
    expect(PRESIDENTIAL_PRIMARY_DATE_RESEARCH_RETRY_DAYS).toBe(7);
  });

  it("rejects invalid dates, years, and runtime statuses", () => {
    expect(() => getPresidentialPrimaryDateResearchStartAt(2026)).toThrow(
      "Year is not a presidential election year"
    );
    expect(() =>
      evaluatePresidentialPrimaryDateResearchEligibility({
        electionYear: 2028,
        dateResearchStatus: "pending",
        now: new Date("not-a-date"),
      })
    ).toThrow("Invalid presidential primary date research now");
    expect(() =>
      evaluatePresidentialPrimaryDateResearchEligibility({
        electionYear: 2028,
        dateResearchStatus: "mystery" as PresidentialPrimaryDateResearchStatus,
        now: new Date("2027-03-07T00:00:00.000Z"),
      })
    ).toThrow("Invalid presidential primary date research status: mystery");
  });
});
