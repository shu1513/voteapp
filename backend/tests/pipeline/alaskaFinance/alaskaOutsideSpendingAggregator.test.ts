import { describe, expect, it } from "vitest";

import {
  aggregateAlaskaOutsideSpending,
  supportOpposeFromAlaskaApocPosition,
} from "../../../src/pipeline/alaskaFinance/alaskaOutsideSpendingAggregator.js";
import type { AlaskaApocIndependentExpenditureRow } from "../../../src/pipeline/alaskaFinance/alaskaApocClient.js";

function expenditure(overrides: Partial<AlaskaApocIndependentExpenditureRow> = {}): AlaskaApocIndependentExpenditureRow {
  return {
    reportYear: 2026,
    filerId: "8001",
    filerName: "Alaska Future PAC",
    filerType: "Group",
    businessPhone: "907-555-0100",
    businessType: "Super PAC",
    type: "Expenditure",
    date: "09/15/2026",
    recipient: "Vendor",
    address: "1 Main",
    city: "Anchorage",
    state: "AK",
    zip: "99501",
    country: "USA",
    position: "Support",
    candidateProposition: "Jane Doe",
    description: "Mailers supporting Jane Doe",
    reportType: "24-hour",
    election: "General",
    paymentType: "Card",
    paymentDetail: "ad buy",
    amount: 25_000,
    submitted: "09/16/2026",
    status: "Complete",
    sourceUrl: "https://aws.state.ak.us/ApocReports/IndependentExpenditures/IEExpenditures.aspx",
    ...overrides,
  };
}

describe("alaskaOutsideSpendingAggregator", () => {
  it("aggregates APOC independent expenditures by supporting and opposing outside groups", () => {
    const sourceUrl = "https://aws.state.ak.us/ApocReports/IndependentExpenditures/IEExpenditures.aspx";
    const result = aggregateAlaskaOutsideSpending({
      candidateName: "Jane Doe",
      electionYear: 2026,
      sourceUrl,
      expenditureRows: [
        expenditure({ amount: 25_000 }),
        expenditure({ amount: 10_000, paymentDetail: "digital ad" }),
        expenditure({
          filerId: "8002",
          filerName: "Accountability PAC",
          position: "Oppose",
          amount: 5_000,
        }),
        expenditure({
          filerId: "9000",
          filerName: "Unrelated PAC",
          candidateProposition: "Other Candidate",
          description: "Mailers supporting Other Candidate",
          amount: 99_000,
        }),
      ],
    });

    expect(result).toEqual({
      summary: {
        supportTotal: 35000,
        opposeTotal: 5000,
        groups: [
          {
            committeeId: "8001",
            committeeName: "Alaska Future PAC",
            supportOppose: "support",
            amount: 35000,
            sourceUrl,
          },
          {
            committeeId: "8002",
            committeeName: "Accountability PAC",
            supportOppose: "oppose",
            amount: 5000,
            sourceUrl,
          },
        ],
        sourceUrl,
      },
      matchedExpenditureRowCount: 3,
      includedExpenditureRowCount: 3,
      skippedExpenditureRowCount: 0,
    });
  });

  it("skips invalid matching expenditure rows", () => {
    const result = aggregateAlaskaOutsideSpending({
      candidateName: "Jane Doe",
      electionYear: 2026,
      expenditureRows: [
        expenditure({ amount: 0 }),
        expenditure({ position: "Information" }),
        expenditure({ reportYear: 2024, date: "09/15/2024" }),
        expenditure({ status: "Rejected" }),
      ],
    });

    expect(result).toEqual({
      summary: null,
      matchedExpenditureRowCount: 4,
      includedExpenditureRowCount: 0,
      skippedExpenditureRowCount: 4,
    });
  });

  it("limits top outside groups separately for support and opposition", () => {
    const result = aggregateAlaskaOutsideSpending({
      candidateName: "Jane Doe",
      electionYear: 2026,
      maxGroups: 1,
      expenditureRows: [
        expenditure({ filerId: "8001", filerName: "Top Support PAC", position: "Support", amount: 100_000 }),
        expenditure({ filerId: "8002", filerName: "Second Support PAC", position: "Support", amount: 90_000 }),
        expenditure({ filerId: "9001", filerName: "Top Oppose PAC", position: "Oppose", amount: 5_000 }),
      ],
    });

    expect(result.summary?.supportTotal).toBe(190000);
    expect(result.summary?.opposeTotal).toBe(5000);
    expect(result.summary?.groups).toEqual([
      expect.objectContaining({ committeeId: "8001", supportOppose: "support", amount: 100000 }),
      expect.objectContaining({ committeeId: "9001", supportOppose: "oppose", amount: 5000 }),
    ]);
  });

  it("maps APOC position text to support or oppose", () => {
    expect(supportOpposeFromAlaskaApocPosition("Support")).toBe("support");
    expect(supportOpposeFromAlaskaApocPosition("Supports")).toBe("support");
    expect(supportOpposeFromAlaskaApocPosition("Opposed")).toBe("oppose");
    expect(supportOpposeFromAlaskaApocPosition("Opposes")).toBe("oppose");
    expect(supportOpposeFromAlaskaApocPosition("Against")).toBe("oppose");
    expect(supportOpposeFromAlaskaApocPosition("Information")).toBeNull();
  });
});
