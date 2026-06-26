import { describe, expect, it } from "vitest";

import {
  floridaOutsideGroupCommitteeIdFromName,
  resolveFloridaOutsideGroupSupport,
  supportOpposeFromFloridaCommitteeText,
} from "../../../src/pipeline/floridaFinance/floridaOutsideGroupSupportResolver.js";
import type { FloridaContributionRow } from "../../../src/pipeline/floridaFinance/floridaCampaignFinanceRows.js";

function contribution(overrides: Partial<FloridaContributionRow> = {}): FloridaContributionRow {
  return {
    recipientName: "Floridians for Jane Doe",
    contributionDate: "9/15/2026",
    amount: "25000.00",
    transactionType: "CHE",
    contributorName: "Energy Transfer LLC",
    address: "1 Main St",
    city: "Tallahassee",
    state: "FL",
    zip: "32301",
    occupation: "",
    inKindDescription: "",
    electionCode: "20261103-GEN",
    sourceUrl: "https://dos.elections.myflorida.com/cgi-bin/contrib.exe",
    ...overrides,
  };
}

describe("floridaOutsideGroupSupportResolver", () => {
  it("uses trusted groups first and preserves their amount over duplicate evidence", () => {
    const result = resolveFloridaOutsideGroupSupport({
      candidateName: "Jane Doe",
      trustedOutsideGroups: [
        {
          committeeId: "FLORIDIANS_FOR_JANE_DOE",
          committeeName: "Floridians for Jane Doe",
          supportOppose: "support",
          amount: 1200,
          sourceUrl: "https://example.test/trusted",
        },
      ],
      supportEvidence: [
        {
          committeeName: "Floridians for Jane Doe",
          supportOppose: "support",
          amount: 5000,
          evidenceUrl: "https://example.test/evidence",
          evidenceNote: "Newswire says the PAC supports Jane Doe.",
        },
      ],
    });

    expect(result).toMatchObject({
      trustedGroupCount: 1,
      evidenceLinkCount: 0,
      heuristicGroupCount: 0,
    });
    expect(result.outsideGroups).toEqual([
      expect.objectContaining({
        committeeId: "FLORIDIANS_FOR_JANE_DOE",
        committeeName: "Floridians for Jane Doe",
        supportOppose: "support",
        amount: 1200,
        discoverySource: "trusted_group",
        confidence: "high",
      }),
    ]);
  });

  it("converts support evidence into outside groups with deterministic committee ids", () => {
    const result = resolveFloridaOutsideGroupSupport({
      candidateName: "Jane Doe",
      supportEvidence: [
        {
          committeeName: "Floridians for Jane Doe",
          supportOppose: "support",
          confidence: "medium",
          amount: 0,
          evidenceUrl: "https://example.test/evidence",
          evidenceNote: "Trusted manual link.",
        },
      ],
    });

    expect(result).toMatchObject({
      trustedGroupCount: 0,
      evidenceLinkCount: 1,
      heuristicGroupCount: 0,
    });
    expect(result.outsideGroups[0]).toMatchObject({
      committeeId: "FLORIDIANS_FOR_JANE_DOE",
      committeeName: "Floridians for Jane Doe",
      supportOppose: "support",
      amount: 0,
      sourceUrl: "https://example.test/evidence",
      confidence: "medium",
      discoverySource: "manual",
      evidenceNote: "Trusted manual link.",
    });
    expect(floridaOutsideGroupCommitteeIdFromName("Floridians for Jane Doe")).toBe("FLORIDIANS_FOR_JANE_DOE");
  });

  it("optionally discovers support and oppose groups from PAC recipient names containing the candidate name", () => {
    const result = resolveFloridaOutsideGroupSupport({
      candidateName: "Jane Doe",
      outsideContributionRows: [
        contribution({ recipientName: "Floridians for Jane Doe" }),
        contribution({ recipientName: "Stop Jane Doe Now" }),
        contribution({ recipientName: "Floridians for Good Schools" }),
      ],
      includeNameHeuristics: true,
      heuristicSourceUrl: "https://dos.elections.myflorida.com/cgi-bin/contrib.exe",
    });

    expect(result).toMatchObject({
      trustedGroupCount: 0,
      evidenceLinkCount: 0,
      heuristicGroupCount: 2,
    });
    expect(result.outsideGroups).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          committeeName: "Floridians for Jane Doe",
          supportOppose: "support",
          amount: 0,
          confidence: "low",
          discoverySource: "name_heuristic",
        }),
        expect.objectContaining({
          committeeName: "Stop Jane Doe Now",
          supportOppose: "oppose",
          amount: 0,
          confidence: "low",
          discoverySource: "name_heuristic",
        }),
      ])
    );
  });

  it("classifies committee text only when it mentions the candidate", () => {
    const candidateKeys = new Set(["JANE DOE"]);

    expect(supportOpposeFromFloridaCommitteeText("Floridians for Jane Doe", candidateKeys)).toBe("support");
    expect(supportOpposeFromFloridaCommitteeText("Stop Jane Doe Now", candidateKeys)).toBe("oppose");
    expect(supportOpposeFromFloridaCommitteeText("Floridians for Good Schools", candidateKeys)).toBeNull();
  });
});
