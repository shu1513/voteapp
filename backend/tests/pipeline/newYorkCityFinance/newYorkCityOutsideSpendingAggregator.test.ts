import { describe, expect, it } from "vitest";

import { aggregateNewYorkCityOutsideSpending } from "../../../src/pipeline/newYorkCityFinance/newYorkCityOutsideSpendingAggregator.js";

describe("newYorkCityOutsideSpendingAggregator", () => {
  it("groups exact candidate allocations by spender and direction using cent arithmetic", () => {
    const result = aggregateNewYorkCityOutsideSpending({
      electionYear: 2025,
      electionCycle: "2025",
      candidateId: "A1",
      sourceUrl: "https://example.test/source",
      rows: [
        { electionYear: 2025, electionCycle: "2025", spenderId: "Z1", spenderName: "Group One", communicationId: "1", candidateId: "A1", candidateName: "Candidate", allocation: 10.005, supportOppose: "support" },
        { electionYear: 2025, electionCycle: "2025", spenderId: "Z1", spenderName: "Group One", communicationId: "2", candidateId: "A1", candidateName: "Candidate", allocation: 5.004, supportOppose: "support" },
        { electionYear: 2025, electionCycle: "2025", spenderId: "Z2", spenderName: "Group Two", communicationId: "3", candidateId: "A1", candidateName: "Candidate", allocation: 7, supportOppose: "oppose" },
        { electionYear: 2025, electionCycle: "2025", spenderId: "Z3", spenderName: "Other Candidate", communicationId: "4", candidateId: "A2", candidateName: "Other", allocation: 99, supportOppose: "support" },
      ],
    });
    expect(result).toEqual({
      supportTotal: 15.01,
      opposeTotal: 7,
      groups: [
        { spenderId: "Z1", spenderName: "Group One", supportOppose: "support", amount: 15.01, expenditureCount: 2, sourceUrl: "https://example.test/source" },
        { spenderId: "Z2", spenderName: "Group Two", supportOppose: "oppose", amount: 7, expenditureCount: 1, sourceUrl: "https://example.test/source" },
      ],
    });
  });
});
