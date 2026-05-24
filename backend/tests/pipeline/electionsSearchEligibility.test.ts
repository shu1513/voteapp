import { describe, expect, it } from "vitest";

import {
  classifyDistrictElectionSearchEligibility,
  type DistrictElectionSearchFacts,
} from "../../src/pipeline/elections/electionsSearchEligibility.js";

const baseFacts: DistrictElectionSearchFacts = {
  district_id: "d1",
  district_name: "District 1",
  district_type: "county",
  state: "CA",
  last_elections_searched_at: "2025-01-01T00:00:00.000Z",
  max_known_election_date: "2026-06-01",
  has_upcoming: true,
};

describe("classifyDistrictElectionSearchEligibility", () => {
  it("returns never_searched when last searched is null", () => {
    const reason = classifyDistrictElectionSearchEligibility(
      { ...baseFacts, last_elections_searched_at: null },
      { asOfDate: "2026-05-23", cooldownDays: 180 }
    );
    expect(reason).toBe("never_searched");
  });

  it("returns cooldown_not_elapsed when last searched is within cooldown", () => {
    const reason = classifyDistrictElectionSearchEligibility(
      { ...baseFacts, last_elections_searched_at: "2026-04-01T00:00:00.000Z" },
      { asOfDate: "2026-05-23", cooldownDays: 180 }
    );
    expect(reason).toBe("cooldown_not_elapsed");
  });

  it("returns due_no_upcoming when cooldown elapsed and no upcoming is known", () => {
    const reason = classifyDistrictElectionSearchEligibility(
      {
        ...baseFacts,
        last_elections_searched_at: "2025-01-01T00:00:00.000Z",
        has_upcoming: false,
        max_known_election_date: "2025-11-05",
      },
      { asOfDate: "2026-05-23", cooldownDays: 180 }
    );
    expect(reason).toBe("due_no_upcoming");
  });

  it("returns not_due when cooldown elapsed and an upcoming election exists", () => {
    const reason = classifyDistrictElectionSearchEligibility(
      {
        ...baseFacts,
        last_elections_searched_at: "2025-01-01T00:00:00.000Z",
        has_upcoming: true,
        max_known_election_date: "2026-06-01",
      },
      { asOfDate: "2026-05-23", cooldownDays: 180 }
    );
    expect(reason).toBe("not_due");
  });
});
