import { describe, expect, it } from "vitest";

import { syncMinnesotaCandidateFinance } from "../../../src/pipeline/minnesotaFinance/minnesotaCandidateFinanceSync.js";
import type { MinnesotaCampaignFinanceCsvRow } from "../../../src/pipeline/minnesotaFinance/minnesotaCampaignFinanceArtifactReader.js";

function candidateRow(overrides: Partial<MinnesotaCampaignFinanceCsvRow> = {}): MinnesotaCampaignFinanceCsvRow {
  return {
    "Committee ID": "1001",
    "Committee Name": "FRIENDS OF JANE DOE",
    Candidate: "Jane Doe",
    Office: "Governor",
    District: "",
    Status: "Active",
    Year: "2026",
    ...overrides,
  };
}

function outsideExpenditureRow(overrides: Partial<MinnesotaCampaignFinanceCsvRow> = {}): MinnesotaCampaignFinanceCsvRow {
  return {
    "Spender": "Better Minnesota",
    "Spender Reg Num": "SP123",
    "Spender sub-type": "Independent expenditure",
    "Affected Comte Name": "Friends of Jane Doe",
    "Affected Cmte Reg Num": "1001",
    "For /Against": "For",
    "Type": "Independent Expenditure",
    "Amount": "70000.00",
    "Purpose": "Support for Example",
    Year: "2026",
    ...overrides,
  };
}

function outsideContributionRow(overrides: Partial<MinnesotaCampaignFinanceCsvRow> = {}): MinnesotaCampaignFinanceCsvRow {
  return {
    "Recipient reg num": "SP123",
    "Recipient": "Better Minnesota",
    "Amount": "20000.00",
    "Receipt date": "2026-09-01",
    "Year": "2026",
    "Contributor": "Google LLC",
    "Contrib type": "Business",
    "Receipt type": "Contribution",
    ...overrides,
  };
}

describe("Minnesota candidate finance sync", () => {
  it("writes outside groups and backtrace rows while leaving direct totals blank", async () => {
    const queries: Array<{ text: string; values: readonly unknown[] }> = [];
    const db = {
      async query(text: string, values: readonly unknown[] = []) {
        queries.push({ text, values });
        if (text.includes("INSERT INTO public.mn_candidate_finance_links")) {
          return { rows: [{ id: "link-1" }] };
        }
        return { rows: [] };
      },
    };

    const result = await syncMinnesotaCandidateFinance({
      db,
      candidateId: "11111111-1111-1111-1111-111111111111",
      electionId: "22222222-2222-2222-2222-222222222222",
      candidateName: "Jane Doe",
      electionYear: 2026,
      officeScope: "statewide",
      officeName: "Governor",
      contributionRows: [candidateRow()],
      expenditureRows: [outsideExpenditureRow()],
      outsideContributionRows: [outsideContributionRow()],
      now: new Date("2026-10-01T00:00:00Z"),
    });

    expect(result.resolution).toMatchObject({
      status: "matched",
      committeeId: "1001",
      committeeName: "FRIENDS OF JANE DOE",
    });
    expect(result.linkWritten).toBe(true);
    expect(result.summaryWritten).toBe(true);
    expect(result.totalReceipts).toBeNull();
    expect(result.directContributionTotal).toBeNull();
    expect(result.outsideSupportTotal).toBe(70000);
    expect(result.outsideOpposeTotal).toBe(0);
    expect(result.outsideGroupsWritten).toBe(1);
    expect(result.outsideGroupBreakdownsWritten).toBeGreaterThan(0);
    expect(result.matchedOutsideExpenditureRowCount).toBe(1);
    expect(result.includedOutsideExpenditureRowCount).toBe(1);
    expect(result.skippedOutsideExpenditureRowCount).toBe(0);
    expect(result.matchedOutsideContributionRowCount).toBe(1);
    expect(result.includedOutsideContributionRowCount).toBe(1);
    expect(result.skippedOutsideContributionRowCount).toBe(0);

    expect(queries.some(({ text }) => text.includes("mn_candidate_finance_links"))).toBe(true);
    expect(queries.some(({ text }) => text.includes("mn_candidate_finance_summaries"))).toBe(true);
    expect(queries.some(({ text }) => text.includes("mn_candidate_finance_outside_groups"))).toBe(true);
    expect(queries.some(({ text }) => text.includes("mn_candidate_finance_outside_group_breakdowns"))).toBe(true);
  });
});
