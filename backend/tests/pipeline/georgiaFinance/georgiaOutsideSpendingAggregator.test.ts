import { describe, expect, it } from "vitest";

import { aggregateGeorgiaOutsideSpending } from "../../../src/pipeline/georgiaFinance/georgiaOutsideSpendingAggregator.js";
import type {
  GeorgiaIndependentExpenditureRow,
  GeorgiaIndependentExpenditureTarget,
} from "../../../src/pipeline/georgiaFinance/georgiaEthicsClient.js";

const CANDIDATE_GUID = "d627fc6e-f324-4077-82f5-bec26f54aac7";
const OTHER_CANDIDATE_GUID = "39c3cba4-5a9d-404a-8d85-920887242d40";
const SPENDER_A_GUID = "639d6189-d718-40b3-ba3f-d4d7544a3451";
const SPENDER_B_GUID = "5f5eaf2d-4581-4b96-931c-59893d39d5ce";

function target(overrides: Partial<GeorgiaIndependentExpenditureTarget> = {}): GeorgiaIndependentExpenditureTarget {
  return {
    candidateMeasureTitle: "LeMario Brown for Georgia",
    stance: "Support",
    reasonTypeCode: "CAN",
    filerRegistrationGuid: CANDIDATE_GUID,
    ...overrides,
  };
}

function ieRow(overrides: Partial<GeorgiaIndependentExpenditureRow> = {}): GeorgiaIndependentExpenditureRow {
  return {
    guid: `g-${overrides.transactionId ?? 1}`,
    transactionId: 1,
    amountApplied: 1000,
    filerRegistrationGuid: SPENDER_A_GUID,
    filerName: "Georgia REALTORS IE Committee",
    filerReportGuid: "r-1",
    timedFiledReportGuid: null,
    filerReportVersionId: 1,
    transactionDate: "2026-01-15T00:00:00",
    transactionStatusCode: "TFIL",
    transactionTypeCode: "TIE",
    electionYear: 2026,
    candidateMeasures: [target()],
    ...overrides,
  };
}

describe("aggregateGeorgiaOutsideSpending", () => {
  it("attributes single-target rows by registration-guid ID join and builds per-spender support/oppose groups", () => {
    const result = aggregateGeorgiaOutsideSpending({
      host: "peachfile",
      candidateRegistrationGuid: CANDIDATE_GUID,
      sourceUrl: "https://ethics.ga.gov/records-search-all/",
      rows: [
        ieRow({ transactionId: 1, amountApplied: 1000 }),
        ieRow({ transactionId: 2, amountApplied: 250.5 }),
        // Same spender opposing — separate group, same committee id.
        ieRow({ transactionId: 3, amountApplied: 400, candidateMeasures: [target({ stance: "Oppose" })] }),
        // Second spender; timed-pending status counts (TPEN money is real).
        ieRow({
          transactionId: 4,
          amountApplied: 2000,
          filerRegistrationGuid: SPENDER_B_GUID,
          filerName: "Working Families Party PAC",
          filerReportGuid: null,
          timedFiledReportGuid: "t-1",
          transactionStatusCode: "TPEN",
        }),
        // Another candidate's IE — invisible to this aggregation.
        ieRow({
          transactionId: 5,
          amountApplied: 9999,
          candidateMeasures: [target({ filerRegistrationGuid: OTHER_CANDIDATE_GUID })],
        }),
      ],
    });

    expect(result.supportTotal).toBe(3250.5);
    expect(result.opposeTotal).toBe(400);
    expect(result.attributedRowCount).toBe(4);
    expect(result.attributedAmount).toBe(3650.5);
    expect(result.candidateTargetRowCount).toBe(4);
    expect(result.storeRowCount).toBe(5);
    expect(result.outsideGroups).toEqual([
      {
        committeeId: SPENDER_B_GUID,
        committeeName: "Working Families Party PAC",
        supportOppose: "support",
        amount: 2000,
        sourceUrl: "https://ethics.ga.gov/records-search-all/",
      },
      {
        committeeId: SPENDER_A_GUID,
        committeeName: "Georgia REALTORS IE Committee",
        supportOppose: "support",
        amount: 1250.5,
        sourceUrl: "https://ethics.ga.gov/records-search-all/",
      },
      {
        committeeId: SPENDER_A_GUID,
        committeeName: "Georgia REALTORS IE Committee",
        supportOppose: "oppose",
        amount: 400,
        sourceUrl: "https://ethics.ga.gov/records-search-all/",
      },
    ]);
  });

  it("quarantines every multi-target row referencing the candidate, including candidate-plus-ballot, as excluded dollars", () => {
    const result = aggregateGeorgiaOutsideSpending({
      host: "peachfile",
      candidateRegistrationGuid: CANDIDATE_GUID,
      rows: [
        // Two candidate targets.
        ieRow({
          transactionId: 1,
          amountApplied: 5000,
          candidateMeasures: [target(), target({ filerRegistrationGuid: OTHER_CANDIDATE_GUID })],
        }),
        // One candidate target plus one ballot target — still unallocatable (D6).
        ieRow({
          transactionId: 2,
          amountApplied: 700,
          candidateMeasures: [target(), target({ reasonTypeCode: "CIEBM", filerRegistrationGuid: null })],
        }),
        // The candidate duplicated across targets counts the row's dollars once.
        ieRow({ transactionId: 3, amountApplied: 300, candidateMeasures: [target(), target()] }),
      ],
    });
    expect(result.multiTargetRowCount).toBe(3);
    expect(result.multiTargetAmount).toBe(6000);
    expect(result.attributedRowCount).toBe(0);
    expect(result.supportTotal).toBe(0);
    expect(result.opposeTotal).toBe(0);
    expect(result.outsideGroups).toEqual([]);
  });

  it("quarantines malformed single-target rows: non-CAN reason, missing stance, missing spender identity, non-positive amount", () => {
    const result = aggregateGeorgiaOutsideSpending({
      host: "peachfile",
      candidateRegistrationGuid: CANDIDATE_GUID,
      rows: [
        ieRow({ transactionId: 1, candidateMeasures: [target({ reasonTypeCode: null })] }),
        ieRow({ transactionId: 2, candidateMeasures: [target({ stance: null })] }),
        ieRow({ transactionId: 3, candidateMeasures: [target({ stance: "Unknown" })] }),
        ieRow({ transactionId: 4, filerRegistrationGuid: null }),
        ieRow({ transactionId: 5, filerName: null }),
        // The outside-group schema requires amount >= 0; nothing non-positive
        // may enter a group.
        ieRow({ transactionId: 6, amountApplied: 0 }),
        ieRow({ transactionId: 7, amountApplied: -50 }),
      ],
    });
    expect(result.malformedRowCount).toBe(7);
    // Five $1,000 rows plus the $0 row plus the −$50 row, summed as signed.
    expect(result.malformedAmount).toBe(4950);
    expect(result.attributedRowCount).toBe(0);
    expect(result.outsideGroups).toEqual([]);
  });

  it("excludes rows outside the host's pinned status vocabulary and counts their dollars", () => {
    const result = aggregateGeorgiaOutsideSpending({
      host: "peachfile",
      candidateRegistrationGuid: CANDIDATE_GUID,
      rows: [
        ieRow({ transactionId: 1, transactionStatusCode: "F", amountApplied: 800 }),
        ieRow({ transactionId: 2, transactionStatusCode: null, amountApplied: 100 }),
        ieRow({ transactionId: 3, transactionStatusCode: "TAMD", amountApplied: 60 }),
      ],
    });
    expect(result.unrecognizedStatusRowCount).toBe(2);
    expect(result.unrecognizedStatusAmount).toBe(900);
    expect(result.attributedRowCount).toBe(1);
    expect(result.supportTotal).toBe(60);
  });

  it("matches the candidate guid case-insensitively and caps the group list without capping the totals", () => {
    const rows = Array.from({ length: 3 }, (_, index) =>
      ieRow({
        transactionId: index + 1,
        amountApplied: (index + 1) * 100,
        filerRegistrationGuid: `spender-${index}`,
        filerName: `Spender ${index}`,
        candidateMeasures: [target({ filerRegistrationGuid: CANDIDATE_GUID.toUpperCase() })],
      })
    );
    const result = aggregateGeorgiaOutsideSpending({
      host: "peachfile",
      candidateRegistrationGuid: ` ${CANDIDATE_GUID} `,
      rows,
      maxGroups: 2,
    });
    expect(result.supportTotal).toBe(600);
    expect(result.outsideGroups.map((group) => group.committeeName)).toEqual(["Spender 2", "Spender 1"]);
  });

  it("rejects a blank candidate registration guid", () => {
    expect(() =>
      aggregateGeorgiaOutsideSpending({ host: "peachfile", candidateRegistrationGuid: "  ", rows: [] })
    ).toThrow(/candidate registration guid/);
  });
});
