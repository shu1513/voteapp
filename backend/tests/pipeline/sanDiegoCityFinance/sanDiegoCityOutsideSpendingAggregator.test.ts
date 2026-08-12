import { describe, expect, it } from "vitest";

import type {
  EfileCalS496Row,
  EfileCalScheduleDRow,
} from "../../../src/pipeline/efileCalFinance/efileCalWorkbookParser.js";
import { aggregateSanDiegoCityOutsideSpending } from "../../../src/pipeline/sanDiegoCityFinance/sanDiegoCityOutsideSpendingAggregator.js";
import type { SanDiegoCityPaper496Supplement } from "../../../src/pipeline/sanDiegoCityFinance/sanDiegoCityPaperFilingSupplements.js";

// Every scenario reproduces the live 2025+2026 San Diego export (Phase 0
// probe, hand-derived 2026-08-10..12): the blank-Filer_ID 496 / id-carrying
// Schedule D twin (EDT2), the respelled-committee split (WFSGR), the
// cross-source PDT1 amount conflict, the COU council office-code variant,
// and the mislabeled-district Crosby rows.

const WFOB_SHORT = "Working Families Opposing Richard Bailey";
const WFOB_LONG =
  "Working Families Opposing Richard Bailey for City Council 2026, sponsored by labor organizations";

function s496(overrides: Partial<EfileCalS496Row> = {}): EfileCalS496Row {
  return {
    filerId: "1490001",
    filerName: WFOB_SHORT,
    reportNum: "000",
    eFilingId: "500",
    origEFilingId: "500",
    cmtteType: null,
    rptDate: "2026-04-15",
    fromDate: null,
    thruDate: null,
    electDate: null,
    formType: "S496",
    tranId: "T1",
    amountCents: 100_00,
    expDate: "2026-04-11",
    candidateLastName: "Richard Bailey",
    candidateFirstName: null,
    officeCd: "CCM",
    officeDscr: null,
    jurisCd: "CIT",
    jurisDscr: "City of San Diego",
    distNo: "2",
    suppOppCd: "OPPOSE",
    memo: false,
    ...overrides,
  };
}

function dRow(overrides: Partial<EfileCalScheduleDRow> = {}): EfileCalScheduleDRow {
  return {
    filerId: "1490001",
    filerName: WFOB_SHORT,
    reportNum: "000",
    eFilingId: "600",
    origEFilingId: "600",
    cmtteType: null,
    rptDate: "2026-05-01",
    fromDate: "2026-01-01",
    thruDate: "2026-04-18",
    electDate: null,
    formType: "D",
    tranId: "T1",
    entityCd: "OTH",
    payeeLastName: null,
    expnCode: "IND",
    expnDate: "2026-04-11",
    amountCents: 100_00,
    candidateLastName: "Richard Bailey",
    candidateFirstName: null,
    officeCd: "CCM",
    officeDscr: null,
    jurisCd: "CIT",
    jurisDscr: "City of San Diego",
    distNo: "2",
    suppOppCd: "OPPOSE",
    memo: false,
    ...overrides,
  };
}

const BAILEY = { displayName: "Richard Bailey", officeName: "City Council Member" as const, seatNumber: 2 };

function aggregate(input: {
  candidate?: typeof BAILEY;
  s496?: EfileCalS496Row[];
  scheduleD?: EfileCalScheduleDRow[];
  paperSupplements?: SanDiegoCityPaper496Supplement[];
}) {
  return aggregateSanDiegoCityOutsideSpending({
    candidate: input.candidate ?? BAILEY,
    s496: input.s496 ?? [],
    scheduleD: input.scheduleD ?? [],
    paperSupplements: input.paperSupplements,
  });
}

function paperEntry(
  overrides: Partial<SanDiegoCityPaper496Supplement> = {},
): SanDiegoCityPaper496Supplement {
  return {
    electionYear: 2026,
    spenderFilerId: "1490001",
    spenderName: WFOB_SHORT,
    candidateLastName: "Bailey",
    candidateFirstName: "Richard",
    officeCd: "CCM",
    jurisDscr: "City of San Diego",
    distNo: "2",
    direction: "OPPOSE",
    amountCents: 5270_27,
    expenditureDate: "2026-05-11",
    eFilingId: "24823",
    sourceNote: "test",
    ...overrides,
  };
}

describe("aggregateSanDiegoCityOutsideSpending dual-identity dedup", () => {
  it("counts the blank-id 496 / id-carrying Schedule D twin ONCE (live EDT2, $22,165)", () => {
    // The 496 row's Filer_ID cell is blank (normalized to "Pending"); its
    // Schedule D twin carries the real id. Same Tran_ID + target, shared
    // NAME edge — one expenditure. Id-only matching double-counts $22,165.
    const rows = {
      s496: [
        s496({
          eFilingId: "700",
          rptDate: "2026-06-01",
          tranId: "EDT2",
          filerId: "Pending",
          amountCents: 22_165_00,
        }),
      ],
      scheduleD: [
        dRow({ eFilingId: "800", rptDate: "2026-07-01", tranId: "EDT2", amountCents: 22_165_00 }),
      ],
    };
    const result = aggregate(rows);
    expect(result.opposeTotalCents).toBe(22_165_00);
    expect(result.diagnostics.duplicateReportRowsExcluded).toBe(1);
    // One group, under the REAL id — the blank-id row is the same spender.
    expect(result.groups).toEqual([
      expect.objectContaining({ spenderFilerId: "1490001", amountCents: 22_165_00, expenditureCount: 1 }),
    ]);
    // Same outcome when the blank-id 496 is the LATER (surviving) report.
    const flipped = aggregate({
      s496: [{ ...rows.s496[0]!, rptDate: "2026-08-01" }],
      scheduleD: rows.scheduleD,
    });
    expect(flipped.opposeTotalCents).toBe(22_165_00);
    expect(flipped.groups).toEqual([
      expect.objectContaining({ spenderFilerId: "1490001", expenditureCount: 1 }),
    ]);
  });

  it("merges respelled committee rows by shared id (live WFSGR, $146k regression)", () => {
    // Committee 1490398 files 496s under a short spelling and Schedule D rows
    // under the long sponsored spelling. Same id — one expenditure, and the
    // group shows the fullest disclosed name.
    const result = aggregate({
      s496: [
        s496({ eFilingId: "1", rptDate: "2026-06-01", tranId: "W1", filerName: WFOB_SHORT, amountCents: 73_000_00 }),
      ],
      scheduleD: [
        dRow({ eFilingId: "2", rptDate: "2026-05-01", tranId: "W1", filerName: WFOB_LONG, amountCents: 73_000_00 }),
      ],
    });
    expect(result.opposeTotalCents).toBe(73_000_00);
    expect(result.diagnostics.duplicateReportRowsExcluded).toBe(1);
    expect(result.groups).toEqual([
      expect.objectContaining({ spenderName: WFOB_LONG, expenditureCount: 1 }),
    ]);
  });

  it("the latest report wins CROSS-source (live PDT1: the 496 amendment's $45,000 beats the older 460's $50,000)", () => {
    const result = aggregate({
      s496: [
        s496({ eFilingId: "900", rptDate: "2026-07-06", tranId: "PDT1", amountCents: 45_000_00 }),
      ],
      scheduleD: [
        dRow({ eFilingId: "850", rptDate: "2026-05-20", tranId: "PDT1", amountCents: 50_000_00 }),
      ],
    });
    expect(result.opposeTotalCents).toBe(45_000_00);
    expect(result.diagnostics.duplicateReportRowsExcluded).toBe(1);
  });

  it("reproduces the Working Families Opposing Bailey probe fixture: $140,564.19 across 5, one group", () => {
    // EDT1 $32,197 (amended 496) + EDT2 $22,165 (blank-id 496 = id'd D twin)
    // + EDT3 $22,165 + EDT19 $19,037.19 + PDT1 $45,000 (496 beats older 460).
    const result = aggregate({
      s496: [
        s496({ eFilingId: "1", rptDate: "2026-05-01", tranId: "EDT1", amountCents: 30_000_00 }),
        s496({ eFilingId: "2", rptDate: "2026-07-06", tranId: "EDT1", amountCents: 32_197_00 }),
        s496({ eFilingId: "3", rptDate: "2026-06-01", tranId: "EDT2", filerId: "Pending", amountCents: 22_165_00 }),
        s496({ eFilingId: "4", rptDate: "2026-06-01", tranId: "EDT3", amountCents: 22_165_00 }),
        s496({ eFilingId: "5", rptDate: "2026-06-15", tranId: "EDT19", amountCents: 19_037_19 }),
        s496({ eFilingId: "6", rptDate: "2026-07-06", tranId: "PDT1", amountCents: 45_000_00 }),
      ],
      scheduleD: [
        dRow({ eFilingId: "7", rptDate: "2026-07-01", tranId: "EDT2", amountCents: 22_165_00 }),
        dRow({ eFilingId: "8", rptDate: "2026-05-20", tranId: "PDT1", amountCents: 50_000_00 }),
      ],
    });
    expect(result.opposeTotalCents).toBe(14_056_419);
    expect(result.groups).toHaveLength(1);
    expect(result.groups[0]).toMatchObject({
      spenderFilerId: "1490001",
      direction: "oppose",
      amountCents: 14_056_419,
      expenditureCount: 5,
    });
  });

  it("two Pending committees sharing a Tran_ID are different spenders, never deduped against each other", () => {
    // Tran_IDs are committee-local; "Pending" is not an identity, and blank
    // ids never form an id edge.
    const result = aggregate({
      s496: [
        s496({ eFilingId: "1", tranId: "T1", filerId: "Pending", filerName: "First Pending Committee" }),
        s496({ eFilingId: "2", tranId: "T1", filerId: "Pending", filerName: "Second Pending Committee" }),
      ],
    });
    expect(result.opposeTotalCents).toBe(200_00);
    expect(result.diagnostics.duplicateReportRowsExcluded).toBe(0);
    expect(result.diagnostics.sharedTranIdRowsKept).toBe(2);
  });

  it("a correcting re-report replaces the original — amount, direction, and name-layout corrections never add", () => {
    const amountFix = aggregate({
      s496: [
        s496({ eFilingId: "1", rptDate: "2026-04-01", tranId: "C1", amountCents: 100_00 }),
        s496({ eFilingId: "2", rptDate: "2026-05-01", tranId: "C1", amountCents: 120_00 }),
      ],
    });
    expect(amountFix.opposeTotalCents).toBe(120_00);
    const directionFix = aggregate({
      s496: [
        s496({ eFilingId: "1", rptDate: "2026-04-01", tranId: "C2", suppOppCd: "OPPOSE" }),
        s496({ eFilingId: "2", rptDate: "2026-05-01", tranId: "C2", suppOppCd: "SUPPORT" }),
      ],
    });
    expect(directionFix.opposeTotalCents).toBe(0);
    expect(directionFix.supportTotalCents).toBe(100_00);
    const layoutFix = aggregate({
      s496: [
        s496({ eFilingId: "1", rptDate: "2026-04-01", tranId: "C3", candidateLastName: "Richard Bailey" }),
        s496({
          eFilingId: "2",
          rptDate: "2026-05-01",
          tranId: "C3",
          candidateLastName: "Bailey",
          candidateFirstName: "Richard",
        }),
      ],
    });
    expect(layoutFix.opposeTotalCents).toBe(100_00);
    expect(layoutFix.diagnostics.duplicateReportRowsExcluded).toBe(1);
  });

  it("keeps same-Tran_ID rows that name different targets", () => {
    // A same-key row naming a DIFFERENT candidate is information, not a
    // duplicate — different (Tran_ID, target) buckets.
    const rows = [
      s496({ tranId: "M1", amountCents: 11_87 }),
      s496({ tranId: "M1", amountCents: 11_87, candidateLastName: "Kent Lee", distNo: "6" }),
    ];
    const result = aggregate({ s496: rows });
    expect(result.opposeTotalCents).toBe(11_87);
    expect(result.diagnostics.duplicateReportRowsExcluded).toBe(0);
    const lee = aggregate({
      candidate: { displayName: "Kent Lee", officeName: "City Council Member", seatNumber: 6 },
      s496: rows,
    });
    expect(lee.opposeTotalCents).toBe(11_87);
  });

  it("Schedule D MON/IKD rows never touch outside totals", () => {
    const result = aggregate({
      scheduleD: [
        dRow({ tranId: "T9", amountCents: 40_00 }),
        dRow({ tranId: "T10", amountCents: 500_00, expnCode: "MON" }),
        dRow({ tranId: "T11", amountCents: 500_00, expnCode: "IKD" }),
      ],
    });
    expect(result.opposeTotalCents).toBe(40_00);
    expect(result.diagnostics.scheduleDIndRows).toBe(1);
  });
});

describe("aggregateSanDiegoCityOutsideSpending gates", () => {
  it("accepts BOTH council office codes (CCM and the vendor's COU variant)", () => {
    // Live 2026 file: 10 of 158 S496 council rows carry COU instead of CCM.
    const result = aggregate({
      s496: [
        s496({ tranId: "O1", officeCd: "CCM", amountCents: 10_00 }),
        s496({ tranId: "O2", officeCd: "COU", amountCents: 5_00 }),
      ],
    });
    expect(result.opposeTotalCents).toBe(15_00);
    expect(result.diagnostics.officeGateExcludedRows).toBe(0);
  });

  it("excludes measure rows and vetoes office, jurisdiction, district, and direction fails", () => {
    const rows = [
      // Ballot-measure spending: no candidate named.
      s496({ tranId: "B1", candidateLastName: null, officeCd: null, jurisDscr: null, distNo: null }),
      // Same name, county-supervisor race: office veto (blank fails too).
      s496({ tranId: "B2", officeCd: "CSU", jurisDscr: "San Diego County", distNo: "1" }),
      s496({ tranId: "B2X", officeCd: null }),
      // Same name, another city: jurisdiction veto.
      s496({ tranId: "B3", jurisDscr: "City of Chula Vista" }),
      // Same name, wrong district: district veto.
      s496({ tranId: "B4", distNo: "6" }),
      // Direction missing: never guessed.
      s496({ tranId: "B5", suppOppCd: null }),
      // Memo rows are excluded from totals.
      s496({ tranId: "B6", memo: true }),
      s496({ tranId: "OK", amountCents: 55_00 }),
    ];
    const result = aggregate({ s496: rows });
    expect(result.opposeTotalCents).toBe(55_00);
    expect(result.diagnostics).toMatchObject({
      nonCandidateTargetRows: 1,
      officeGateExcludedRows: 2,
      jurisdictionGateExcludedRows: 1,
      districtGateExcludedRows: 1,
      unknownDirectionRows: 1,
      unknownDirectionCents: 100_00,
      memoRowsExcluded: 1,
    });
  });

  it("fails the mislabeled Crosby rows closed (live: 7 CWFP rows tagged Dist_No=6 for the D2 candidate)", () => {
    // The veto is right and the data is wrong — $8,194.67 stays excluded
    // until the Phase 4 PDF check settles the correction.
    const result = aggregate({
      candidate: { displayName: "Nicole Crosby", officeName: "City Council Member", seatNumber: 2 },
      s496: [
        s496({
          tranId: "X1",
          filerId: "Pending",
          filerName: "California Working Families Party",
          candidateLastName: "Nicole Crosby",
          suppOppCd: "SUPPORT",
          distNo: "6",
          amountCents: 8_194_67,
        }),
      ],
    });
    expect(result.supportTotalCents).toBe(0);
    expect(result.diagnostics.districtGateExcludedRows).toBe(1);
  });

  it("matches dirty target names by token, never substring", () => {
    const rows = [
      s496({ tranId: "N1", candidateLastName: "RICHARD BAILEY", jurisDscr: "CITY OF SAN DIEGO" }),
      s496({ tranId: "N2", candidateLastName: "Bailey", candidateFirstName: "Richard", jurisDscr: null, distNo: null }),
    ];
    const result = aggregate({ s496: rows });
    expect(result.opposeTotalCents).toBe(200_00);
    const lee = aggregate({
      candidate: { displayName: "Kent Lee", officeName: "City Council Member", seatNumber: 6 },
      s496: [
        s496({ tranId: "L1", candidateLastName: "Lee Ramirez", distNo: "6" }),
        s496({ tranId: "L2", candidateLastName: "Point Loma Electrical", distNo: "6" }),
      ],
    });
    expect(lee.opposeTotalCents).toBe(0);
    expect(lee.diagnostics.otherCandidateRows).toBe(2);
  });

  it("fails closed on a council candidate without a seat number and on mayor rows disclosing a district", () => {
    expect(() =>
      aggregate({ candidate: { displayName: "Richard Bailey", officeName: "City Council Member", seatNumber: null as unknown as number } }),
    ).toThrow(/seat number/);
    const mayor = aggregateSanDiegoCityOutsideSpending({
      candidate: { displayName: "Todd Gloria", officeName: "Mayor", seatNumber: null },
      s496: [
        s496({ tranId: "M1", candidateLastName: "Todd Gloria", officeCd: "MAY", distNo: null }),
        s496({ tranId: "M2", candidateLastName: "Todd Gloria", officeCd: "MAY", distNo: "3" }),
      ],
      scheduleD: [],
    });
    expect(mayor.opposeTotalCents).toBe(100_00);
    expect(mayor.diagnostics.districtGateExcludedRows).toBe(1);
  });

  it("splits support and oppose and groups Pending spenders by normalized name", () => {
    const rows = [
      s496({ tranId: "P1", filerId: "Pending", filerName: "California Working Families Party", suppOppCd: "SUPPORT", amountCents: 10_00 }),
      s496({ tranId: "P2", filerId: "Pending", filerName: "CALIFORNIA WORKING FAMILIES  PARTY", suppOppCd: "SUPPORT", amountCents: 5_00 }),
      s496({ tranId: "P3", filerId: "Pending", filerName: "Some Other Committee", suppOppCd: "SUPPORT", amountCents: 1_00 }),
      s496({ tranId: "P4", amountCents: 7_00 }),
    ];
    const result = aggregate({ s496: rows });
    expect(result.supportTotalCents).toBe(16_00);
    expect(result.opposeTotalCents).toBe(7_00);
    expect(
      result.groups
        .filter((group) => group.direction === "support")
        .map((group) => [group.spenderName, group.amountCents]),
    ).toEqual([
      ["CALIFORNIA WORKING FAMILIES  PARTY", 15_00],
      ["Some Other Committee", 1_00],
    ]);
  });
});

describe("aggregateSanDiegoCityOutsideSpending paper supplements", () => {
  it("merges a paper-496 supplement into the same spender's e-filed group", () => {
    const result = aggregate({
      s496: [s496({ tranId: "E1", eFilingId: "24950", amountCents: 5270_18 })],
      paperSupplements: [paperEntry()],
    });
    expect(result.opposeTotalCents).toBe(10540_45);
    expect(result.groups).toHaveLength(1);
    expect(result.groups[0]).toMatchObject({
      spenderFilerId: "1490001",
      direction: "oppose",
      amountCents: 10540_45,
      expenditureCount: 2,
    });
    expect(result.diagnostics.paperSupplementRows).toBe(1);
  });

  it("runs supplements through the normal target and veto gates, including the COU code", () => {
    const otherCandidate = aggregate({
      paperSupplements: [
        paperEntry({ candidateLastName: "Martinez", candidateFirstName: "Antonio", eFilingId: "24824" }),
      ],
    });
    expect(otherCandidate.opposeTotalCents).toBe(0);
    expect(otherCandidate.diagnostics.otherCandidateRows).toBe(1);
    const wrongDistrict = aggregate({
      paperSupplements: [paperEntry({ distNo: "7" })],
    });
    expect(wrongDistrict.opposeTotalCents).toBe(0);
    expect(wrongDistrict.diagnostics.districtGateExcludedRows).toBe(1);
    const couEntry = aggregate({
      paperSupplements: [paperEntry({ officeCd: "COU" })],
    });
    expect(couEntry.opposeTotalCents).toBe(5270_27);
  });

  it("rejects an invalid supplement list at aggregation time", () => {
    expect(() =>
      aggregate({ paperSupplements: [paperEntry({ amountCents: -1 })] }),
    ).toThrow(/positive integer/);
  });

  it("suppresses a supplement whose filing entered the export (no double count)", () => {
    const exported = s496({
      tranId: "E9",
      eFilingId: "25600",
      origEFilingId: "24823",
      amountCents: 5270_27,
    });
    const result = aggregate({
      s496: [exported],
      paperSupplements: [paperEntry()],
    });
    expect(result.opposeTotalCents).toBe(5270_27);
    expect(result.diagnostics.paperSupplementRows).toBe(0);
    expect(result.diagnostics.paperSupplementRowsSuppressed).toBe(1);
    const direct = aggregate({
      s496: [{ ...exported, eFilingId: "24823", origEFilingId: "24823" }],
      paperSupplements: [paperEntry()],
    });
    expect(direct.opposeTotalCents).toBe(5270_27);
    expect(direct.diagnostics.paperSupplementRowsSuppressed).toBe(1);
  });
});
