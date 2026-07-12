import { describe, expect, it, vi } from "vitest";

import { syncNewYorkCityCandidateFinance } from "../../../src/pipeline/newYorkCityFinance/newYorkCityCandidateFinanceSync.js";

describe("newYorkCityCandidateFinanceSync", () => {
  it("dry-runs without DB writes", async () => {
    const db = { query: vi.fn(), connect: vi.fn() };
    const result = await syncNewYorkCityCandidateFinance({
      db: db as never,
      candidateId: "candidate-1", electionId: "election-1", candidateName: "Jane Doe", electionYear: 2025,
      resolution: {
        status: "matched", cfbCandidateId: "A1", cfbCandidateName: "DOE, JANE", officeCode: "1", boroughCode: null,
        summary: {
          electionYear: 2025, fromStatement: 1, toStatement: 16, officeCode: "1", candidateName: "DOE, JANE",
          candidateId: "A1", boroughCode: null, privateContributions: 100, publicFunds: 20,
          netExpenditures: 50, outstandingBills: 5,
        },
      },
      contributionRows: [{
        electionYear: 2025, officeCode: "1", candidateId: "A1", candidateName: "DOE, JANE", filing: 10,
        schedule: "ABC", referenceNumber: "R1", contributorName: "Alex Smith", contributorType: "IND",
        occupation: "Teacher", employer: "NYC DOE", amount: 100, adjustmentType: null,
      }],
      outsideSpendingRows: [{
        electionYear: 2025, electionCycle: "2025", spenderId: "Z1", spenderName: "Outside Group", communicationId: "C1",
        candidateId: "A1", candidateName: "DOE, JANE", allocation: 25, supportOppose: "support",
      }],
      outsideFunderRows: [{
        electionYear: 2025, electionCycle: "2025", spenderId: "Z1", transactionId: "ICONT:R1",
        funderName: "Business Group", funderType: "CORP", amount: 50,
      }],
      outsideElectionCycle: "2025",
      dryRun: true,
    });
    expect(result).toEqual({
      dryRun: true,
      breakdownsWritten: 0,
      outsideGroupsWritten: 0,
      outsideUpdated: true,
      acceptedContributionRows: 1,
    });
    expect(db.query).not.toHaveBeenCalled();
    expect(db.connect).not.toHaveBeenCalled();
  });
});
