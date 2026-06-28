import { describe, expect, it, vi } from "vitest";

import { resolveArizonaCandidateCommittee } from "../../../src/pipeline/arizonaFinance/arizonaCandidateCommitteeResolver.js";

describe("arizonaCandidateCommitteeResolver", () => {
  it("matches exactly one Spotlight committee", async () => {
    const searchCandidateCommittees = vi.fn(async () => [
      {
        committeeId: "AZ100",
        committeeName: "Katie Hobbs for Governor",
        amount: 1000,
        rowCount: 2,
        sourceUrl: "https://seethemoney.az.gov/Reporting/Explore",
      },
    ]);

    await expect(
      resolveArizonaCandidateCommittee(
        {
          candidateName: "Katie Hobbs",
          officeScope: "statewide",
          officeName: "Governor",
          electionYear: 2024,
        },
        { timeoutMs: 1000 },
        { searchCandidateCommittees }
      )
    ).resolves.toEqual({
      status: "matched",
      committeeId: "AZ100",
      committeeName: "Katie Hobbs for Governor",
      confidence: "single_committee",
      source: "spotlight",
      sourceUrl: "https://seethemoney.az.gov/Reporting/Explore",
      matchedIncomeRowCount: 2,
      totalIncomeAmount: 1000,
    });
  });

  it("skips ambiguous committee matches", async () => {
    const searchCandidateCommittees = vi.fn(async () => [
      {
        committeeId: "AZ100",
        committeeName: "Katie Hobbs for Governor",
        amount: 1000,
        rowCount: 2,
        sourceUrl: null,
      },
      {
        committeeId: "AZ200",
        committeeName: "Katie Hobbs Exploratory",
        amount: 500,
        rowCount: 1,
        sourceUrl: null,
      },
    ]);

    await expect(
      resolveArizonaCandidateCommittee(
        {
          candidateName: "Katie Hobbs",
          officeScope: "statewide",
          officeName: "Governor",
          electionYear: 2024,
        },
        {},
        { searchCandidateCommittees }
      )
    ).resolves.toMatchObject({
      status: "ambiguous",
      reason: "multiple_matching_committees",
      matches: [{ committeeId: "AZ100" }, { committeeId: "AZ200" }],
    });
  });

  it("rejects unsupported offices before querying Spotlight", async () => {
    const searchCandidateCommittees = vi.fn();

    await expect(
      resolveArizonaCandidateCommittee(
        {
          candidateName: "Katie Hobbs",
          officeScope: "county",
          officeName: "Sheriff",
          electionYear: 2024,
        },
        {},
        { searchCandidateCommittees }
      )
    ).resolves.toMatchObject({
      status: "unmatched",
      reason: "unsupported_office",
    });
    expect(searchCandidateCommittees).not.toHaveBeenCalled();
  });
});
