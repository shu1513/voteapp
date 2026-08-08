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
      firstNameConflict: false,
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

  it("matches IE mentions through middle names and nicknames but not across fields", () => {
    // The VoteApp middle initial is absent from the IE mention text.
    expect(
      aggregateAlaskaOutsideSpending({
        candidateName: "Jane M. Doe",
        electionYear: 2026,
        expenditureRows: [expenditure()],
      })
    ).toMatchObject({ matchedExpenditureRowCount: 1 });

    // One-sided nickname: VoteApp campaign name, formal name in IE text.
    expect(
      aggregateAlaskaOutsideSpending({
        candidateName: "Becky Schwanke",
        electionYear: 2026,
        expenditureRows: [
          expenditure({
            candidateProposition: "Rebecca Schwanke",
            description: "Mailers supporting Rebecca Schwanke",
          }),
        ],
      })
    ).toMatchObject({ matchedExpenditureRowCount: 1 });

    // A key must not match across the field seam: recipient ends with the
    // surname, description starts with the first name.
    expect(
      aggregateAlaskaOutsideSpending({
        candidateName: "Jane Doe",
        electionYear: 2026,
        expenditureRows: [
          expenditure({
            candidateProposition: "Ballot Measure 1",
            recipient: "Committee to Elect Doe",
            description: "Jane Smith consulting",
          }),
        ],
      })
    ).toMatchObject({ matchedExpenditureRowCount: 0 });
  });

  it("refuses to aggregate when includable rows span conflicting formal families", () => {
    // "Pat Smith" expands to PATRICK SMITH and PATRICIA SMITH; rows for both
    // formal names are positive evidence of two people, so the aggregation
    // aborts rather than combining their money.
    const result = aggregateAlaskaOutsideSpending({
      candidateName: "Pat Smith",
      electionYear: 2026,
      expenditureRows: [
        expenditure({
          candidateProposition: "Patrick Smith",
          description: "Mailers supporting Patrick Smith",
          amount: 10_000,
        }),
        expenditure({
          filerId: "8002",
          filerName: "Accountability PAC",
          candidateProposition: "Patricia Smith",
          description: "Mailers supporting Patricia Smith",
          amount: 5_000,
        }),
      ],
    });
    expect(result).toEqual({
      firstNameConflict: true,
      summary: null,
      matchedExpenditureRowCount: 2,
      includedExpenditureRowCount: 0,
      skippedExpenditureRowCount: 2,
    });

    // The nickname appearing alongside the formal names must not launder the
    // family evidence away.
    expect(
      aggregateAlaskaOutsideSpending({
        candidateName: "Pat Smith",
        electionYear: 2026,
        expenditureRows: [
          expenditure({
            candidateProposition: "Patrick Smith",
            description: "Pat Smith mailers supporting Patrick Smith",
            amount: 10_000,
          }),
          expenditure({
            filerId: "8002",
            filerName: "Accountability PAC",
            candidateProposition: "Patricia Smith",
            description: "Pat Smith mailers supporting Patricia Smith",
            amount: 5_000,
          }),
        ],
      })
    ).toMatchObject({ firstNameConflict: true, includedExpenditureRowCount: 0 });

    // A single formal family is the deliberate one-sided nickname link.
    expect(
      aggregateAlaskaOutsideSpending({
        candidateName: "Pat Smith",
        electionYear: 2026,
        expenditureRows: [
          expenditure({
            candidateProposition: "Patrick Smith",
            description: "Mailers supporting Patrick Smith",
          }),
        ],
      })
    ).toMatchObject({ firstNameConflict: false, matchedExpenditureRowCount: 1, includedExpenditureRowCount: 1 });

    // Formal spellings of one name are not two people.
    expect(
      aggregateAlaskaOutsideSpending({
        candidateName: "Steve Weir",
        electionYear: 2026,
        expenditureRows: [
          expenditure({ candidateProposition: "Stephen Weir", description: "supporting Stephen Weir" }),
          expenditure({
            filerId: "8002",
            filerName: "Accountability PAC",
            candidateProposition: "Steven Weir",
            description: "supporting Steven Weir",
          }),
        ],
      })
    ).toMatchObject({ firstNameConflict: false, includedExpenditureRowCount: 2 });

    // A conflicting-name row that cannot contribute money (out of cycle)
    // must not zero out valid totals.
    expect(
      aggregateAlaskaOutsideSpending({
        candidateName: "Pat Smith",
        electionYear: 2026,
        expenditureRows: [
          expenditure({
            candidateProposition: "Patrick Smith",
            description: "Mailers supporting Patrick Smith",
            amount: 10_000,
          }),
          expenditure({
            filerId: "8002",
            filerName: "Accountability PAC",
            candidateProposition: "Patricia Smith",
            description: "Mailers supporting Patricia Smith",
            reportYear: 2023,
            date: "09/15/2023",
          }),
        ],
      })
    ).toMatchObject({ firstNameConflict: false, includedExpenditureRowCount: 1 });
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
      firstNameConflict: false,
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

  it("rejects an IE row whose named candidate carries a contradicting middle name", () => {
    // The reversed key "DOE JANE" matches "Doe, Jane B" contiguously, so
    // without middle-name evidence another Jane Doe's IE money lands here.
    const rows = [
      expenditure({
        candidateProposition: "Doe, Jane B",
        description: "Mailers supporting Doe, Jane B",
        amount: 25_000,
      }),
    ];

    expect(
      aggregateAlaskaOutsideSpending({
        candidateName: "Jane A. Doe",
        electionYear: 2026,
        expenditureRows: rows,
      })
    ).toMatchObject({ summary: null, matchedExpenditureRowCount: 0 });

    // A candidate with no middle of its own still falls back to first+last.
    expect(
      aggregateAlaskaOutsideSpending({
        candidateName: "Jane Doe",
        electionYear: 2026,
        expenditureRows: rows,
      }).summary?.supportTotal
    ).toBe(25000);

    // An initial corroborating the full middle keeps matching.
    expect(
      aggregateAlaskaOutsideSpending({
        candidateName: "Jane A. Doe",
        electionYear: 2026,
        expenditureRows: [expenditure({ candidateProposition: "Doe, Jane Ann", amount: 25_000 })],
      }).summary?.supportTotal
    ).toBe(25000);
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
