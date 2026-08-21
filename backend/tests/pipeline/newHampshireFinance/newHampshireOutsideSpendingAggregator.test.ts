import { describe, expect, it } from "vitest";

import type { NewHampshireIndependentExpenditureRow } from "../../../src/pipeline/newHampshireFinance/newHampshireCfsClient.js";
import {
  aggregateNewHampshireOutsideSpending,
  normalizeNewHampshireCandidateAlias,
} from "../../../src/pipeline/newHampshireFinance/newHampshireOutsideSpendingAggregator.js";

function expenditure(
  overrides: Partial<NewHampshireIndependentExpenditureRow> = {}
): NewHampshireIndependentExpenditureRow {
  return {
    transactionId: 1,
    transactionVersionId: 1,
    guid: "00000000-0000-4000-8000-000000000001",
    filerReportId: 10,
    filerReportVersionId: 1,
    filerEntityId: 31342,
    filerName: "Example IE Committee",
    transactionAmount: 100,
    transactionDate: "2026-07-31T00:00:00",
    reportName: "2026 R&E Report - 08/19/2026",
    reportVersion: false,
    reportVersionFilter: "RPTFLD",
    isAmended: false,
    transactionTypeCode: "TIE",
    transactionSubTypeCode: "TIE",
    candidateMeasure: "Candidate, Sample",
    stance: "Support",
    electionCycle: "2026 Election Cycle",
    transactionCategory: "Canvassing",
    ...overrides,
  };
}

describe("newHampshireOutsideSpendingAggregator", () => {
  it("selects current reports, retains reportless rows, sums cents, and groups by filer entity", () => {
    const sourceUrl =
      "https://cfsapi.sos.nh.gov/api/PublicTransactionDetails/GetPublicExpenditureDetails";
    const result = aggregateNewHampshireOutsideSpending({
      candidateAliases: ["Sample Candidate"],
      electionYear: 2026,
      sourceUrl,
      expenditureRows: [
        expenditure({ transactionId: 101, filerReportVersionId: 1, transactionAmount: 100 }),
        expenditure({
          transactionId: 201,
          filerReportVersionId: 2,
          transactionAmount: 150,
          reportVersion: true,
          reportVersionFilter: "RPTAMD",
        }),
        expenditure({
          transactionId: 202,
          filerReportVersionId: 2,
          transactionAmount: 100.25,
          stance: "Oppose",
          reportVersion: true,
          reportVersionFilter: "RPTAMD",
        }),
        expenditure({
          transactionId: 301,
          guid: "00000000-0000-4000-8000-000000000301",
          filerReportId: null,
          filerReportVersionId: null,
          reportName: null,
          reportVersionFilter: null,
          transactionAmount: 0.1,
        }),
        expenditure({
          transactionId: 401,
          filerReportId: 11,
          candidateMeasure: "Person, Other",
          transactionAmount: 9000,
        }),
      ],
    });

    expect(result).toEqual({
      summary: {
        supportTotal: 150.1,
        opposeTotal: 100.25,
        groups: [
          {
            filerEntityId: 31342,
            filerName: "Example IE Committee",
            supportOppose: "support",
            amount: 150.1,
            sourceUrl,
          },
          {
            filerEntityId: 31342,
            filerName: "Example IE Committee",
            supportOppose: "oppose",
            amount: 100.25,
            sourceUrl,
          },
        ],
        sourceUrl,
      },
      sourceRowCount: 5,
      currentVersionRowCount: 4,
      supersededRowCount: 1,
      matchedTargetRowCount: 3,
      includedRowCount: 3,
      blankTargetRowCount: 0,
      blankStanceRowCount: 0,
      nonPositiveRowCount: 0,
    });
  });

  it("matches only normalized trusted aliases, including official Last, First order", () => {
    expect(normalizeNewHampshireCandidateAlias("Candidaté, Sample Q.")).toBe(
      "SAMPLE Q CANDIDATE"
    );

    const result = aggregateNewHampshireOutsideSpending({
      candidateAliases: ["Sample Candidate", "Sample Q. Candidate"],
      electionYear: 2026,
      expenditureRows: [
        expenditure({ transactionId: 1, candidateMeasure: "Candidate, Sample" }),
        expenditure({ transactionId: 2, filerReportId: 11, candidateMeasure: "Candidate, Sample Q." }),
        expenditure({ transactionId: 3, filerReportId: 12, candidateMeasure: "Candidate, Sampl" }),
        expenditure({
          transactionId: 4,
          filerReportId: 13,
          candidateMeasure: "Person, Other",
          transactionCategory: "Sample Candidate",
        }),
      ],
    });

    expect(result.matchedTargetRowCount).toBe(2);
    expect(result.includedRowCount).toBe(2);
    expect(result.summary?.supportTotal).toBe(200);
  });

  it("excludes blank target, blank stance, and non-positive rows with diagnostics", () => {
    const result = aggregateNewHampshireOutsideSpending({
      candidateAliases: ["Sample Candidate"],
      electionYear: 2026,
      expenditureRows: [
        expenditure({ transactionId: 1, candidateMeasure: null, transactionAmount: 5000 }),
        expenditure({ transactionId: 2, filerReportId: 11, stance: null, transactionAmount: 4000 }),
        expenditure({
          transactionId: 3,
          filerReportId: 12,
          candidateMeasure: "Person, Other",
          stance: null,
          transactionAmount: 3000,
        }),
        expenditure({ transactionId: 4, filerReportId: 13, transactionAmount: 0 }),
        expenditure({ transactionId: 5, filerReportId: 14, transactionAmount: -10 }),
      ],
    });

    expect(result).toEqual({
      summary: null,
      sourceRowCount: 5,
      currentVersionRowCount: 5,
      supersededRowCount: 0,
      matchedTargetRowCount: 3,
      includedRowCount: 0,
      blankTargetRowCount: 1,
      blankStanceRowCount: 2,
      nonPositiveRowCount: 2,
    });
  });

  it("limits displayed groups per direction without changing totals", () => {
    const result = aggregateNewHampshireOutsideSpending({
      candidateAliases: ["Sample Candidate"],
      electionYear: 2026,
      maxGroups: 1,
      expenditureRows: [
        expenditure({ transactionAmount: 100 }),
        expenditure({
          transactionId: 2,
          filerReportId: 11,
          filerEntityId: 999,
          filerName: "Bigger Committee",
          transactionAmount: 250,
        }),
        expenditure({
          transactionId: 3,
          filerReportId: 12,
          filerEntityId: 777,
          filerName: "Opposing Committee",
          transactionAmount: 200,
          stance: "Oppose",
        }),
      ],
    });

    expect(result.summary).toMatchObject({
      supportTotal: 350,
      opposeTotal: 200,
      groups: [
        { filerEntityId: 999, supportOppose: "support", amount: 250 },
        { filerEntityId: 777, supportOppose: "oppose", amount: 200 },
      ],
    });
  });

  it("uses filer entity ID and the latest transaction name independent of row order", () => {
    const rows = [
      expenditure({
        filerName: "Legacy Committee Name",
        transactionAmount: 100,
        transactionDate: "2026-07-01T00:00:00",
      }),
      expenditure({
        transactionId: 2,
        filerReportId: 11,
        filerName: "Current Committee Name",
        transactionAmount: 50,
        transactionDate: "2026-08-01T00:00:00",
        stance: "Oppose",
      }),
      expenditure({
        transactionId: 3,
        filerReportId: 12,
        filerEntityId: 999,
        filerName: "Current Committee Name",
        transactionAmount: 25,
      }),
    ];
    const aggregate = (expenditureRows: readonly NewHampshireIndependentExpenditureRow[]) =>
      aggregateNewHampshireOutsideSpending({
        candidateAliases: ["Sample Candidate"],
        electionYear: 2026,
        expenditureRows,
      }).summary?.groups;

    const expected = [
      expect.objectContaining({
        filerEntityId: 31342,
        filerName: "Current Committee Name",
        supportOppose: "support",
        amount: 100,
      }),
      expect.objectContaining({
        filerEntityId: 31342,
        filerName: "Current Committee Name",
        supportOppose: "oppose",
        amount: 50,
      }),
      expect.objectContaining({
        filerEntityId: 999,
        filerName: "Current Committee Name",
        amount: 25,
      }),
    ];
    expect(aggregate(rows)).toEqual(expected);
    expect(aggregate([...rows].reverse())).toEqual(expected);
  });

  it("fails closed on inexact TIE contract, unknown stance, or duplicate identity", () => {
    const aggregate = (rows: readonly NewHampshireIndependentExpenditureRow[]) =>
      aggregateNewHampshireOutsideSpending({
        candidateAliases: ["Sample Candidate"],
        electionYear: 2026,
        expenditureRows: rows,
      });

    expect(() => aggregate([expenditure({ transactionTypeCode: "TEXP" })])).toThrow(
      "returned non-IE transaction"
    );
    expect(() => aggregate([expenditure({ transactionSubTypeCode: "EXP" })])).toThrow(
      "returned non-IE transaction"
    );
    expect(() => aggregate([expenditure({ electionCycle: "2024 Election Cycle" })])).toThrow(
      "expected 2026 Election Cycle"
    );
    expect(() => aggregate([expenditure({ stance: "Neutral" })])).toThrow("unknown stance");
    expect(() =>
      aggregate([expenditure(), expenditure({ filerReportId: 11 })])
    ).toThrow("duplicate identity");
    expect(() =>
      aggregate([expenditure({ filerReportId: null, filerReportVersionId: 1 })])
    ).toThrow("partial report identity");
  });

  it("rejects invalid inputs and unsafe amounts", () => {
    expect(() =>
      aggregateNewHampshireOutsideSpending({
        candidateAliases: [],
        electionYear: 2026,
        expenditureRows: [],
      })
    ).toThrow("candidateAliases must contain a name");
    expect(() =>
      aggregateNewHampshireOutsideSpending({
        candidateAliases: ["Sample Candidate"],
        electionYear: 2015,
        expenditureRows: [],
      })
    ).toThrow("Invalid New Hampshire outside spending election year");
    expect(() =>
      aggregateNewHampshireOutsideSpending({
        candidateAliases: ["Sample Candidate"],
        electionYear: 2026,
        maxGroups: 0,
        expenditureRows: [],
      })
    ).toThrow("Invalid New Hampshire outside spending maxGroups");
    expect(() =>
      aggregateNewHampshireOutsideSpending({
        candidateAliases: ["Sample Candidate"],
        electionYear: 2026,
        expenditureRows: [expenditure({ transactionAmount: Number.POSITIVE_INFINITY })],
      })
    ).toThrow("Invalid New Hampshire IE amount");
  });
});
