import { describe, expect, it } from "vitest";

import {
  normalizeAlaskaCandidateNameForStorage,
  normalizeAlaskaCandidateNameKeys,
  resolveAlaskaCandidateCommittee,
} from "../../../src/pipeline/alaskaFinance/alaskaCandidateCommitteeResolver.js";
import type { AlaskaApocCampaignIncomeRow } from "../../../src/pipeline/alaskaFinance/alaskaApocClient.js";

function income(overrides: Partial<AlaskaApocCampaignIncomeRow> = {}): AlaskaApocCampaignIncomeRow {
  return {
    reportYear: 2026,
    filerId: "1001",
    filerName: "Doe, Jane",
    filerType: "Candidate",
    name: "Doe, Jane",
    date: "10/01/2026",
    type: "Income",
    contributor: "Smith, Pat",
    address: "1 Main",
    city: "Juneau",
    state: "AK",
    zip: "99801",
    country: "USA",
    paymentType: "Check",
    paymentDetail: "1001",
    occupation: "Attorney",
    employer: "Law Firm",
    purpose: "Contribution",
    amount: 250,
    submitted: "10/02/2026",
    status: "Complete",
    sourceUrl: null,
    ...overrides,
  };
}

describe("alaskaCandidateCommitteeResolver", () => {
  it("normalizes candidate names for storage and matching", () => {
    expect(normalizeAlaskaCandidateNameForStorage(" Jane   Doe ")).toBe("JANE DOE");
    expect([...normalizeAlaskaCandidateNameKeys("Doe, Jane")]).toEqual(
      expect.arrayContaining(["DOE JANE", "JANE DOE"])
    );
  });

  it("resolves one clear APOC candidate filer from income rows", () => {
    expect(
      resolveAlaskaCandidateCommittee({
        candidateName: "Jane Doe",
        electionYear: 2026,
        sourceUrl: "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx",
        incomeRows: [
          income({ amount: 100 }),
          income({ amount: 200, contributor: "Roe, Alex" }),
          income({ filerId: "9999", filerName: "Other Candidate", name: "Other Candidate" }),
        ],
      })
    ).toEqual({
      status: "matched",
      candidateFilerId: "1001",
      candidateFilerName: "Doe, Jane",
      confidence: "exact",
      source: "apoc_csv",
      sourceUrl: "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx",
      matchedRowCount: 2,
    });
  });

  it("returns ambiguous when multiple candidate filers match", () => {
    expect(
      resolveAlaskaCandidateCommittee({
        candidateName: "Jane Doe",
        electionYear: 2026,
        incomeRows: [
          income({ filerId: "1001", filerName: "Doe, Jane" }),
          income({ filerId: "1002", filerName: "Jane Doe for Alaska" }),
        ],
      })
    ).toEqual({
      status: "ambiguous",
      reason: "multiple_matching_filers",
      candidateNameNormalized: "JANE DOE",
      candidateFilerIds: ["1001", "1002"],
    });
  });

  it("ignores out-of-cycle rows", () => {
    expect(
      resolveAlaskaCandidateCommittee({
        candidateName: "Jane Doe",
        electionYear: 2026,
        incomeRows: [income({ reportYear: 2024, date: "10/01/2024" })],
      })
    ).toEqual({
      status: "unmatched",
      reason: "no_candidate_filer_match",
      candidateNameNormalized: "JANE DOE",
    });
  });
});
