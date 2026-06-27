import { describe, expect, it } from "vitest";

import { resolveOregonCandidateCommitteeFromSearchRows } from "../../../src/pipeline/oregonFinance/oregonCandidateCommitteeResolver.js";
import type { OregonOrestarTransactionSearchResultRow } from "../../../src/pipeline/oregonFinance/oregonOrestarParser.js";

function row(overrides: Partial<OregonOrestarTransactionSearchResultRow> = {}): OregonOrestarTransactionSearchResultRow {
  return {
    transactionId: "4458653",
    transactionDate: "10/12/2022",
    status: "Original",
    filerCommitteeName: "Friends of Tina Kotek",
    filerCommitteeId: "4792",
    contributorPayeeName: "John Ramsbacher",
    contributorPayeeOutOfState: false,
    subType: "Cash Contribution",
    amount: 10_000,
    isInKindExpenditure: false,
    detailUrl: "https://secure.sos.state.or.us/orestar/gotoPublicTransactionDetail.do?tranRsn=4458653",
    committeeUrl: "https://secure.sos.state.or.us/orestar/sooDetail.do?cneCommitteeId=4792",
    ...overrides,
  };
}

describe("oregonCandidateCommitteeResolver", () => {
  it("resolves a unique ORESTAR committee from parsed search rows", () => {
    expect(
      resolveOregonCandidateCommitteeFromSearchRows({
        candidateName: "Tina Kotek",
        searchRows: [row(), row({ transactionId: "4458654" })],
      })
    ).toEqual({
      status: "matched",
      committeeId: "4792",
      committeeName: "Friends of Tina Kotek",
      confidence: "exact",
      source: "orestar_public",
      sourceUrl: "https://secure.sos.state.or.us/orestar/sooDetail.do?cneCommitteeId=4792",
      matchedCommitteeRowCount: 2,
    });
  });

  it("returns no_match when committee names do not contain the candidate name", () => {
    expect(
      resolveOregonCandidateCommitteeFromSearchRows({
        candidateName: "Tina Kotek",
        searchRows: [row({ filerCommitteeName: "2022 Our Oregon Voter Guide", filerCommitteeId: "22333" })],
      })
    ).toMatchObject({
      status: "no_match",
      matchedCommitteeRowCount: 0,
    });
  });

  it("returns ambiguous when multiple committees match", () => {
    expect(
      resolveOregonCandidateCommitteeFromSearchRows({
        candidateName: "Tina Kotek",
        searchRows: [
          row(),
          row({ filerCommitteeName: "Tina Kotek Victory Fund", filerCommitteeId: "9999" }),
        ],
      })
    ).toMatchObject({
      status: "ambiguous",
      matchedCommitteeRowCount: 2,
      matches: expect.arrayContaining([
        expect.objectContaining({ committeeId: "4792" }),
        expect.objectContaining({ committeeId: "9999" }),
      ]),
    });
  });
});
