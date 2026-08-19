import { describe, expect, it } from "vitest";
import {
  aggregateAustinOutsideSpending,
  austinCommitteeDirections,
  swapAustinCommaName,
} from "../../../src/pipeline/austinFinance/austinOutsideSpendingAggregator.js";
import type {
  AustinCommitteePurposeRow,
  AustinDirectCampaignExpenditureRow,
} from "../../../src/pipeline/austinFinance/austinSocrataClient.js";

const WATSON = {
  candidateDisplayName: "Kirk Watson",
  filerName: "Watson, Kirk P.",
  officeCode: "MAYOR" as const,
  electionDate: "2024-11-05",
  windowFrom: "2023-07-01",
  windowTo: "2024-12-31",
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

  it("treats a blank election date or an OTHER office as silence, a different one as a conflict", () => {
    const directions = austinCommitteeDirections({
      purposeRows: [
        purpose({ filerName: "Austin Board of Realtors PAC", electionDate: null }),
        purpose({ filerName: "Austin Apartment Association PAC", officeSought: "OTHER", electionDate: null }),
        purpose({ filerName: "Old PAC", electionDate: "2022-11-08" }),
        purpose({ filerName: "Council PAC", officeSought: "COUNCIL_MBR_DISTRICT_09" }),
      ],
      ...WATSON,
    });
    expect([...directions]).toEqual([
      ["AUSTIN BOARD OF REALTORS PAC", "support"],
      ["AUSTIN APARTMENT ASSOCIATION PAC", "support"],
    ]);
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
      candidateDisplayName: 'Zohaib "Zo" Qadri',
      officeCode: "COUNCIL_MBR_DISTRICT_09",
      electionDate: "2024-11-05",
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
