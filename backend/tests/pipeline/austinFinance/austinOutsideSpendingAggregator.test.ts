import { describe, expect, it } from "vitest";
import {
  aggregateAustinOutsideSpending,
  austinCommitteeDirections,
  swapAustinCommaName,
  type AustinReportFacts,
} from "../../../src/pipeline/austinFinance/austinOutsideSpendingAggregator.js";
import type {
  AustinCommitteePurposeRow,
  AustinDirectCampaignExpenditureRow,
} from "../../../src/pipeline/austinFinance/austinSocrataClient.js";

/** Report facts behind the fixture rows: R1 = a PAC monthly inside the 2024 window. */
function reportFacts(): Map<string, AustinReportFacts> {
  return new Map([
    ["R1", { formTypeCode: "MPAC", periodFrom: "2024-09-26", periodTo: "2024-10-25", dateFiled: "2024-10-28" }],
    ["R2022", { formTypeCode: "MPAC", periodFrom: "2022-11-26", periodTo: "2022-12-25", dateFiled: "2022-12-28" }],
  ]);
}

const WATSON = {
  candidateDisplayName: "Kirk Watson",
  filerName: "Watson, Kirk P.",
  officeCode: "MAYOR" as const,
  electionDate: "2024-11-05",
  windowFrom: "2023-07-01",
  windowTo: "2024-12-31",
  reportsById: reportFacts(),
};

let nextDce = 1;
function dce(
  overrides: Partial<AustinDirectCampaignExpenditureRow> & { reportId: string },
): AustinDirectCampaignExpenditureRow {
  const suffix = String(nextDce++).padStart(5, "0");
  return {
    dceId: `${overrides.reportId}-D${suffix}`,
    parentTransaction: overrides.parentTransaction ?? `${overrides.reportId}-F00001`,
    paidBy: "Austin Leadership PAC",
    payee: "Mailer Co",
    paymentDate: "2024-10-01",
    amountCents: 100_00,
    candidateOrMeasure: "Watson, Kirk",
    officeSoughtInfo: "MAYOR",
    officeHeldInfo: null,
    correction: false,
    reportUrl: null,
    ...overrides,
  };
}

let nextPurpose = 1;
function purpose(
  overrides: Partial<AustinCommitteePurposeRow> = {},
): AustinCommitteePurposeRow {
  return {
    committeePurposeId: `R1-C${String(nextPurpose++).padStart(5, "0")}`,
    reportId: "R1",
    filerName: "Austin Leadership PAC",
    committeeActivity: "SUPPORT",
    purposeType: "CANDIDATE",
    recipient: "Kirk,Watson",
    officeSought: "MAYOR",
    officeHeld: null,
    electionDate: "2024-11-05",
    measureDescription: null,
    correction: false,
    reportUrl: null,
    ...overrides,
  };
}

/** RECA's $71,000 mailer: five targets, listed on two reports (Phase 0 gate 6). */
function recaRows(): AustinDirectCampaignExpenditureRow[] {
  const targets = [
    ["Watson, Kirk", "MAYOR"],
    ["Fuentes, Vanessa", "COUNCIL_MBR_DISTRICT_02"],
    ["Vela, Chito", "COUNCIL_MBR_DISTRICT_04"],
    ["Laine, Krista", "COUNCIL_MBR_DISTRICT_06"],
    ["Ganguly, Ashika", "COUNCIL_MBR_DISTRICT_10"],
  ] as const;
  const rows: AustinDirectCampaignExpenditureRow[] = [];
  for (const reportId of ["R20240926", "R20241021"])
    for (const [target, office] of targets)
      rows.push(
        dce({
          reportId,
          paidBy: "The Real Estate Council of Austin, Inc. Advancing Democracy PAC",
          payee: "CounterPoint Messaging",
          paymentDate: "2024-09-20",
          amountCents: 7_100_000,
          candidateOrMeasure: target,
          officeSoughtInfo: office,
        }),
      );
  return rows;
}

describe("swapAustinCommaName", () => {
  it("swaps the two sides of the first comma", () => {
    expect(swapAustinCommaName("Kirk,Watson")).toBe("Watson, Kirk");
    expect(swapAustinCommaName("Erin ,Zwiener")).toBe("Zwiener, Erin");
    expect(swapAustinCommaName("Carmen,Llanes Pulido")).toBe("Llanes Pulido, Carmen");
    expect(swapAustinCommaName("Austin Leadership PAC")).toBeNull();
    expect(swapAustinCommaName(",Watson")).toBeNull();
  });
});

describe("austinCommitteeDirections", () => {
  it("reads SUPPORT/OPPOSE per filer for the candidate, ignoring ASSIST and other people", () => {
    const directions = austinCommitteeDirections({
      purposeRows: [
        purpose(),
        purpose({ filerName: "Vibrant Austin PAC", committeeActivity: "OPPOSE" }),
        purpose({ filerName: "Austin Fire Fighters PAC", committeeActivity: "ASSIST", purposeType: "OFFICE" }),
        purpose({ filerName: "Austin United PAC", recipient: "Carmen,Llanes Pulido" }),
        purpose({ filerName: null }),
        purpose({ filerName: "Equity Action", recipient: null }),
        purpose({ filerName: "Austinites for Equity", purposeType: "MEASURE", recipient: null }),
      ],
      ...WATSON,
    });
    expect([...directions]).toEqual([
      ["AUSTIN LEADERSHIP PAC", "support"],
      ["VIBRANT AUSTIN PAC", "oppose"],
    ]);
  });

  it("treats an OTHER office as silence and a different election date or office as a conflict", () => {
    const directions = austinCommitteeDirections({
      purposeRows: [
        purpose({ filerName: "Austin Apartment Association PAC", officeSought: "OTHER" }),
        purpose({ filerName: "Old PAC", electionDate: "2022-11-08" }),
        purpose({ filerName: "Council PAC", officeSought: "COUNCIL_MBR_DISTRICT_09" }),
      ],
      ...WATSON,
    });
    expect([...directions]).toEqual([["AUSTIN APARTMENT ASSOCIATION PAC", "support"]]);
  });

  it("accepts a blank-dated row only when its report period overlaps the cycle window", () => {
    const directions = austinCommitteeDirections({
      purposeRows: [
        // Filed on a 2024 monthly inside the window: this cycle's declaration.
        purpose({ filerName: "Austin Board of Realtors PAC", electionDate: null, reportId: "R1" }),
        // Filed on a December-2022 monthly: the 2022 cycle's stance, not evidence for 2024.
        purpose({ filerName: "The Real Estate Council of Austin, Inc. Advancing Democracy PAC", electionDate: null, reportId: "R2022" }),
        // No report at all / unknown report: cannot be placed in a cycle.
        purpose({ filerName: "Ghost PAC", electionDate: null, reportId: null }),
        purpose({ filerName: "Unknown Report PAC", electionDate: null, reportId: "R404" }),
      ],
      ...WATSON,
    });
    expect([...directions]).toEqual([["AUSTIN BOARD OF REALTORS PAC", "support"]]);
  });

  it("marks a filer with both directions ambiguous and reads call names and typos through the shared gates", () => {
    const directions = austinCommitteeDirections({
      purposeRows: [
        purpose({ filerName: "Flip PAC", committeeActivity: "SUPPORT" }),
        purpose({ filerName: "Flip PAC", committeeActivity: "OPPOSE" }),
        purpose({ filerName: "City Accountability Project", recipient: "Zo,Qadri", officeSought: "COUNCIL_MBR_DISTRICT_09", committeeActivity: "OPPOSE" }),
        // A typo'd recipient never matches — undirected rather than guessed.
        purpose({ filerName: "Free Zilker Coalition", recipient: "Kurt,Watson" }),
      ],
      ...WATSON,
    });
    expect([...directions]).toEqual([["FLIP PAC", "ambiguous"]]);
    const qadri = austinCommitteeDirections({
      purposeRows: [
        purpose({ filerName: "City Accountability Project", recipient: "Zo,Qadri", officeSought: "COUNCIL_MBR_DISTRICT_09", committeeActivity: "OPPOSE" }),
      ],
      reportsById: reportFacts(),
      candidateDisplayName: 'Zohaib "Zo" Qadri',
      officeCode: "COUNCIL_MBR_DISTRICT_09",
      electionDate: "2024-11-05",
      windowFrom: "2023-07-01",
      windowTo: "2024-12-31",
    });
    expect([...qadri]).toEqual([["CITY ACCOUNTABILITY PROJECT", "oppose"]]);
  });
});

describe("aggregateAustinOutsideSpending", () => {
  it("dedupes payments across reports, allocates single-target payments with direction, and quarantines multi-target payments (D6)", () => {
    const rows = [
      // Austin Leadership PAC: three payments, one listed twice (original + correction).
      dce({ reportId: "R1", amountCents: 10_000_00, paymentDate: "2024-09-01" }),
      dce({ reportId: "R1C", amountCents: 10_000_00, paymentDate: "2024-09-01", correction: true }),
      dce({ reportId: "R1", amountCents: 5_000_00, paymentDate: "2024-09-02", payee: "TV Co" }),
      dce({ reportId: "R1", amountCents: 5_000_00, paymentDate: "2024-09-02", payee: "Radio Co" }),
      // Vibrant Austin PAC opposes.
      dce({ reportId: "R2", paidBy: "Vibrant Austin PAC", amountCents: 2_500_00, paymentDate: "2024-10-10" }),
      // Same spender, target spelled "FIRST, LAST" on another payment.
      dce({ reportId: "R2", paidBy: "Vibrant Austin PAC", amountCents: 1_000_00, paymentDate: "2024-10-11", candidateOrMeasure: "KIRK, WATSON" }),
      ...recaRows(),
      // Outside the window: never counted.
      dce({ reportId: "R0", amountCents: 999_00, paymentDate: "2022-11-01" }),
      // Another candidate entirely.
      dce({ reportId: "R3", amountCents: 700_00, candidateOrMeasure: "Greco, Doug", officeSoughtInfo: "MAYOR" }),
      // Same person string but a different seat: not this race.
      dce({ reportId: "R3", amountCents: 600_00, officeSoughtInfo: "COUNCIL_MBR_DISTRICT_09" }),
      // No office info at all: cannot name a candidate (fail closed).
      dce({ reportId: "R3", amountCents: 500_00, officeSoughtInfo: null }),
      // Spender missing: quarantined.
      dce({ reportId: "R3", amountCents: 400_00, paidBy: null }),
    ];
    const result = aggregateAustinOutsideSpending({
      dceRows: rows,
      purposeRows: [
        purpose(),
        purpose({ filerName: "Vibrant Austin PAC", committeeActivity: "OPPOSE" }),
        purpose({ filerName: "The Real Estate Council of Austin, Inc. Advancing Democracy PAC" }),
      ],
      ...WATSON,
    });
    expect(result.groups).toEqual([
      { spenderName: "Austin Leadership PAC", supportOppose: "support", amountCents: 20_000_00 },
      { spenderName: "Vibrant Austin PAC", supportOppose: "oppose", amountCents: 3_500_00 },
    ]);
    expect(result).toMatchObject({
      supportTotalCents: 20_000_00,
      opposeTotalCents: 3_500_00,
      windowRowCount: rows.length - 1,
      rowsWithoutSpender: 1,
      attributedPaymentCount: 5,
      selfPaymentCount: 0,
      selfCents: 0,
      multiTargetPaymentCount: 1,
      multiTargetCents: 7_100_000,
      undirectedCents: 0,
      undirectedSpenders: [],
      ambiguousDirectionCents: 0,
    });
  });

  it("lets a correction of a regular PAC report supersede the spender's rows in its period (changed dates count once)", () => {
    const reportsById = new Map<string, AustinReportFacts>([
      ...reportFacts(),
      ["RATX1", { formTypeCode: "ATX1", periodFrom: "2024-10-28", periodTo: "2024-10-30", dateFiled: "2024-10-30" }],
      ["RGPAC", { formTypeCode: "GPAC", periodFrom: "2024-11-05", periodTo: "2024-12-04", dateFiled: "2024-12-06" }],
      ["RCOR1", { formTypeCode: "CORPAC", periodFrom: "2024-10-28", periodTo: "2024-12-04", dateFiled: "2024-12-05" }],
      ["RCOR2", { formTypeCode: "CORPAC", periodFrom: "2024-10-28", periodTo: "2024-12-04", dateFiled: "2024-12-06" }],
    ]);
    const vibrant = (over: Partial<AustinDirectCampaignExpenditureRow> & { reportId: string }) =>
      dce({ paidBy: "Vibrant Austin PAC", candidateOrMeasure: "Watson, Kirk", officeSoughtInfo: "MAYOR", ...over });
    const rows = [
      // Special report: two payments.
      vibrant({ reportId: "RATX1", paymentDate: "2024-10-29", amountCents: 9_400, payee: "Meta" }),
      vibrant({ reportId: "RATX1", paymentDate: "2024-10-29", amountCents: 102_344, payee: "Scale to Win" }),
      // Regular report re-lists a later payment.
      vibrant({ reportId: "RGPAC", paymentDate: "2024-11-14", amountCents: 790_742, payee: "DSPolitical" }),
      // An earlier correction of the same period (superseded by the later one).
      vibrant({ reportId: "RCOR1", paymentDate: "2024-10-29", amountCents: 9_400, payee: "Meta" }),
      // The latest correction: full re-list, one payment re-dated.
      vibrant({ reportId: "RCOR2", paymentDate: "2024-10-29", amountCents: 9_400, payee: "Meta" }),
      vibrant({ reportId: "RCOR2", paymentDate: "2024-10-30", amountCents: 102_344, payee: "Scale to Win" }),
      vibrant({ reportId: "RCOR2", paymentDate: "2024-11-14", amountCents: 790_742, payee: "DSPolitical" }),
      // Another spender's rows in the same dates are untouched by Vibrant's correction.
      dce({ reportId: "RGPAC", paymentDate: "2024-11-14", amountCents: 5_000, payee: "Other Co" }),
    ];
    const result = aggregateAustinOutsideSpending({
      dceRows: rows,
      purposeRows: [purpose(), purpose({ filerName: "Vibrant Austin PAC", committeeActivity: "OPPOSE" })],
      ...WATSON,
      reportsById,
    });
    expect(result.groups).toEqual([
      { spenderName: "Vibrant Austin PAC", supportOppose: "oppose", amountCents: 9_400 + 102_344 + 790_742 },
      { spenderName: "Austin Leadership PAC", supportOppose: "support", amountCents: 5_000 },
    ]);
    // Dropped: both RATX1 rows, the RGPAC re-list, the older correction's row.
    expect(result.supersededRowCount).toBe(4);
  });

  it("reports undirected and ambiguous-direction dollars instead of guessing", () => {
    const result = aggregateAustinOutsideSpending({
      dceRows: [
        dce({ reportId: "R1", paidBy: "Austin Firefighters Public Safety Fund", amountCents: 5_800_000 }),
        dce({ reportId: "R1", paidBy: "Flip PAC", amountCents: 100_00 }),
        dce({ reportId: "R1", amountCents: 50_00 }),
      ],
      purposeRows: [
        purpose(),
        purpose({ filerName: "Flip PAC", committeeActivity: "SUPPORT" }),
        purpose({ filerName: "Flip PAC", committeeActivity: "OPPOSE" }),
      ],
      ...WATSON,
    });
    expect(result.groups).toEqual([
      { spenderName: "Austin Leadership PAC", supportOppose: "support", amountCents: 50_00 },
    ]);
    expect(result).toMatchObject({
      undirectedCents: 5_800_000,
      undirectedSpenders: ["Austin Firefighters Public Safety Fund"],
      ambiguousDirectionCents: 100_00,
      ambiguousDirectionSpenders: ["Flip PAC"],
    });
  });

  it("drops the candidate's own DCE rows as self-spending (any spelling)", () => {
    const result = aggregateAustinOutsideSpending({
      dceRows: [
        dce({ reportId: "R1", paidBy: "Watson, Kirk P.", candidateOrMeasure: "Watson, Kirk", amountCents: 300_00 }),
        dce({ reportId: "R1", paidBy: "Watson, Kirk", candidateOrMeasure: "WATSON, KIRK", amountCents: 200_00 }),
        dce({ reportId: "R1", amountCents: 100_00 }),
      ],
      purposeRows: [purpose()],
      ...WATSON,
    });
    expect(result.groups).toEqual([
      { spenderName: "Austin Leadership PAC", supportOppose: "support", amountCents: 100_00 },
    ]);
    expect(result).toMatchObject({ selfPaymentCount: 2, selfCents: 500_00 });
  });

  it("rejects an inverted window", () => {
    expect(() =>
      aggregateAustinOutsideSpending({
        dceRows: [],
        purposeRows: [],
        ...WATSON,
        windowFrom: "2025-01-01",
        windowTo: "2024-01-01",
      }),
    ).toThrow(/window is inverted/);
  });
});
