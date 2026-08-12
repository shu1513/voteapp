import { describe, expect, it } from "vitest";

import type {
  EfileCalS496Row,
  EfileCalScheduleDRow,
} from "../../../src/pipeline/efileCalFinance/efileCalWorkbookParser.js";
import { aggregateSanJoseOutsideSpending } from "../../../src/pipeline/sanJoseFinance/sanJoseOutsideSpendingAggregator.js";
import type { SanJosePaper496Supplement } from "../../../src/pipeline/sanJoseFinance/sanJosePaperFilingSupplements.js";

// Every scenario reproduces the live 2025+2026 export (plan "Outside
// spending" + the Phase 3 dry-run 2026-08-10): dirty target identity, a
// duplicate 496 report, multi-candidate mailers, measure rows, a Pending
// spender, and the Schedule D MON/IKD contribution rows.

function s496(overrides: Partial<EfileCalS496Row> = {}): EfileCalS496Row {
  return {
    filerId: "744711",
    filerName: "South Bay AFL-CIO Labor Council COPE",
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
    candidateLastName: "Peter Ortiz",
    candidateFirstName: null,
    officeCd: "CCM",
    officeDscr: null,
    jurisCd: "CIT",
    jurisDscr: "San Jose",
    distNo: "5",
    suppOppCd: "SUPPORT",
    memo: false,
    ...overrides,
  };
}

function dRow(overrides: Partial<EfileCalScheduleDRow> = {}): EfileCalScheduleDRow {
  return {
    filerId: "744711",
    filerName: "South Bay AFL-CIO Labor Council COPE",
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
    candidateLastName: "Peter Ortiz",
    candidateFirstName: null,
    officeCd: "CCM",
    officeDscr: null,
    jurisCd: "CIT",
    jurisDscr: "San Jose",
    distNo: "5",
    suppOppCd: "SUPPORT",
    memo: false,
    ...overrides,
  };
}

const ORTIZ = { displayName: "Peter Ortiz", officeName: "City Council Member" as const, seatNumber: 5 };

function aggregate(input: {
  candidate?: typeof ORTIZ;
  s496?: EfileCalS496Row[];
  scheduleD?: EfileCalScheduleDRow[];
  paperSupplements?: SanJosePaper496Supplement[];
}) {
  return aggregateSanJoseOutsideSpending({
    candidate: input.candidate ?? ORTIZ,
    s496: input.s496 ?? [],
    scheduleD: input.scheduleD ?? [],
    paperSupplements: input.paperSupplements,
  });
}

// The live paper anti-Campos 496 (e_filing_id 24823), as curated.
function paperEntry(
  overrides: Partial<SanJosePaper496Supplement> = {},
): SanJosePaper496Supplement {
  return {
    electionYear: 2026,
    spenderFilerId: "941786",
    spenderName: "Santa Clara County Government Attorneys' Association PAC",
    candidateLastName: "Campos",
    candidateFirstName: "Nora",
    officeCd: "CCM",
    jurisDscr: "City of San Jose",
    distNo: "5",
    direction: "OPPOSE",
    amountCents: 5270_27,
    expenditureDate: "2026-05-11",
    eFilingId: "24823",
    sourceNote: "test",
    ...overrides,
  };
}

const CAMPOS = {
  displayName: "Nora Campos",
  officeName: "City Council Member" as const,
  seatNumber: 5,
};

describe("aggregateSanJoseOutsideSpending", () => {
  it("collapses a duplicate 496 report of one expenditure to the latest report (spender 744711 case)", () => {
    // Live: Tran_ID PDT8743 appears under two e-filings with identical
    // candidate/amount/date — one expenditure, reported twice.
    const rows = [
      s496({ eFilingId: "24613", rptDate: "2026-04-12", tranId: "PDT8743", amountCents: 374_02 }),
      s496({ eFilingId: "24641", rptDate: "2026-04-15", tranId: "PDT8743", amountCents: 374_02 }),
    ];
    const result = aggregate({ s496: rows });
    expect(result.supportTotalCents).toBe(374_02);
    expect(result.diagnostics.duplicateReportRowsExcluded).toBe(1);
  });

  it("a correcting re-report replaces the original — amount, direction, and name-layout corrections never add", () => {
    // A later report of the same (spender, Tran_ID, canonical target) wins
    // regardless of the mutable fields: $100 corrected to $120 must total
    // $120, not $220; same for a SUPPORT→OPPOSE fix and a whole-name row
    // re-reported with the name split across NamL/NamF.
    const amountFix = aggregate({
      s496: [
        s496({ eFilingId: "1", rptDate: "2026-04-01", tranId: "C1", amountCents: 100_00 }),
        s496({ eFilingId: "2", rptDate: "2026-05-01", tranId: "C1", amountCents: 120_00 }),
      ],
    });
    expect(amountFix.supportTotalCents).toBe(120_00);
    expect(amountFix.diagnostics.duplicateReportRowsExcluded).toBe(1);
    const directionFix = aggregate({
      s496: [
        s496({ eFilingId: "1", rptDate: "2026-04-01", tranId: "C2", suppOppCd: "SUPPORT" }),
        s496({ eFilingId: "2", rptDate: "2026-05-01", tranId: "C2", suppOppCd: "OPPOSE" }),
      ],
    });
    expect(directionFix.supportTotalCents).toBe(0);
    expect(directionFix.opposeTotalCents).toBe(100_00);
    const layoutFix = aggregate({
      s496: [
        s496({ eFilingId: "1", rptDate: "2026-04-01", tranId: "C3", candidateLastName: "Peter Ortiz" }),
        s496({
          eFilingId: "2",
          rptDate: "2026-05-01",
          tranId: "C3",
          candidateLastName: "Ortiz",
          candidateFirstName: "Peter",
        }),
      ],
    });
    expect(layoutFix.supportTotalCents).toBe(100_00);
    expect(layoutFix.diagnostics.duplicateReportRowsExcluded).toBe(1);
  });

  it("two Pending committees sharing a Tran_ID are different spenders, never deduped against each other", () => {
    // Tran_IDs are committee-local; "Pending" is not an identity. Two ID-less
    // spenders both using T1 for a $100 Ortiz mailer must both count.
    const result = aggregate({
      s496: [
        s496({ eFilingId: "1", tranId: "T1", filerId: "Pending", filerName: "First Pending Committee" }),
        s496({ eFilingId: "2", tranId: "T1", filerId: "Pending", filerName: "Second Pending Committee" }),
      ],
    });
    expect(result.supportTotalCents).toBe(200_00);
    expect(result.diagnostics.duplicateReportRowsExcluded).toBe(0);
  });

  it("dedupes Schedule-D-only rows against each other (re-reported 460 filings)", () => {
    // The export re-reports whole 460 filings (duplicate-period chains), so a
    // 460-only IE can appear under two e-filings. One expenditure, once.
    const result = aggregate({
      scheduleD: [
        dRow({ eFilingId: "1", rptDate: "2026-04-01", tranId: "D1", amountCents: 100_00 }),
        dRow({ eFilingId: "2", rptDate: "2026-05-01", tranId: "D1", amountCents: 100_00 }),
      ],
    });
    expect(result.supportTotalCents).toBe(100_00);
    expect(result.diagnostics).toMatchObject({
      duplicateReportRowsExcluded: 1,
      scheduleDRowsAdded: 1,
    });
  });

  it("keeps same-Tran_ID rows that name different targets, and counts multi-candidate mailer rows once each", () => {
    // Multi-candidate mailers were verified to carry DISTINCT Tran_IDs per
    // candidate; a same-key row naming a DIFFERENT candidate is information,
    // not a duplicate — never silently dropped.
    const rows = [
      s496({ tranId: "M1", amountCents: 11_87 }),
      s496({ tranId: "M2", amountCents: 11_87, candidateLastName: "Bien Doan", distNo: "7" }),
      s496({ tranId: "M3", amountCents: 11_87, candidateLastName: "Rafael Garcia", distNo: "7" }),
      // Hypothetical vendor collision: same Tran_ID, different candidate.
      s496({ tranId: "M1", amountCents: 11_87, candidateLastName: "Bien Doan", distNo: "7" }),
    ];
    const result = aggregate({ s496: rows });
    expect(result.supportTotalCents).toBe(11_87);
    expect(result.diagnostics.sharedTranIdRowsKept).toBe(2);
    expect(result.diagnostics.duplicateReportRowsExcluded).toBe(0);
    const doan = aggregate({
      candidate: { displayName: "Bien Doan", officeName: "City Council Member", seatNumber: 7 },
      s496: rows,
    });
    expect(doan.supportTotalCents).toBe(23_74);
  });

  it("adds Schedule D IND rows only when the (Filer_ID, Tran_ID) key is absent from S496; MON/IKD never", () => {
    // Live: all 41 D IND rows matched an S496 row — the union adds nothing
    // today; MON/IKD are contributions TO committees.
    const result = aggregate({
      s496: [s496({ tranId: "T1", amountCents: 100_00 })],
      scheduleD: [
        dRow({ tranId: "T1", amountCents: 100_00 }), // already on the 496
        dRow({ tranId: "T9", amountCents: 40_00 }), // 460-only IE — added
        dRow({ tranId: "T10", amountCents: 500_00, expnCode: "MON" }),
        dRow({ tranId: "T11", amountCents: 500_00, expnCode: "IKD" }),
      ],
    });
    expect(result.supportTotalCents).toBe(140_00);
    expect(result.diagnostics).toMatchObject({ scheduleDIndRows: 2, scheduleDRowsAdded: 1 });
  });

  it("matches dirty target names: casing variants, split NamL/NamF, accents — by token, never substring", () => {
    const rows = [
      s496({ tranId: "N1", candidateLastName: "PETER ORTIZ", jurisDscr: "CITY OF SAN JOSE" }),
      s496({ tranId: "N2", candidateLastName: "Ortiz", candidateFirstName: "Peter", jurisDscr: null, distNo: null }),
      s496({ tranId: "N3", candidateLastName: "Peter Ortiz", jurisDscr: "City of San José" }),
    ];
    const result = aggregate({ s496: rows });
    expect(result.supportTotalCents).toBe(300_00);
    // "Le" must not substring-match unrelated names (live: IBEW "Electrical",
    // "Silicon Valley"); an unrelated target row is simply another candidate.
    const le = aggregate({
      candidate: { displayName: "Van Le", officeName: "City Council Member", seatNumber: 7 },
      s496: [
        s496({ tranId: "L1", candidateLastName: "Le Pham", distNo: "7" }),
        s496({ tranId: "L2", candidateLastName: "Valley Electrical", distNo: "7" }),
      ],
    });
    expect(le.supportTotalCents).toBe(0);
    expect(le.diagnostics.otherCandidateRows).toBe(2);
  });

  it("excludes measure rows (no candidate) and vetoes office, jurisdiction, and district conflicts", () => {
    const rows = [
      // Ballot-measure spending: no candidate named (live: 10 rows, $35k).
      s496({ tranId: "B1", candidateLastName: null, officeCd: null, jurisDscr: null, distNo: null }),
      // Same name, county-supervisor race (Sylvia Arenas shape): office veto.
      s496({ tranId: "B2", officeCd: "CSU", jurisDscr: "Santa Clara County", distNo: "1" }),
      // Same name, another city: jurisdiction veto.
      s496({ tranId: "B3", jurisDscr: "City of Santa Clara" }),
      // Same name, wrong district: district veto.
      s496({ tranId: "B4", distNo: "2" }),
      // Direction missing: never guessed.
      s496({ tranId: "B5", suppOppCd: null }),
      // Memo rows are excluded from totals.
      s496({ tranId: "B6", memo: true }),
      s496({ tranId: "OK", amountCents: 55_00 }),
    ];
    const result = aggregate({ s496: rows });
    expect(result.supportTotalCents).toBe(55_00);
    expect(result.diagnostics).toMatchObject({
      nonCandidateTargetRows: 1,
      officeGateExcludedRows: 1,
      jurisdictionGateExcludedRows: 1,
      districtGateExcludedRows: 1,
      unknownDirectionRows: 1,
      unknownDirectionCents: 100_00,
      memoRowsExcluded: 1,
    });
  });

  it("splits support and oppose and groups by spender with the fullest disclosed name", () => {
    const rows = [
      s496({ tranId: "S1", amountCents: 625_00 }),
      s496({
        tranId: "S2",
        amountCents: 158_10,
        suppOppCd: "OPPOSE",
        filerId: "941786",
        filerName: "Santa Clara County Government Attorneys' Association PAC",
      }),
      // Same spender, shorter name spelling on a second row: variants merge,
      // longest observed spelling wins.
      s496({ tranId: "S3", amountCents: 100_00, filerName: "South Bay AFL-CIO Labor Council Committee on Political Education" }),
    ];
    const result = aggregate({ s496: rows });
    expect(result.supportTotalCents).toBe(725_00);
    expect(result.opposeTotalCents).toBe(158_10);
    expect(result.groups).toEqual([
      {
        spenderFilerId: "744711",
        spenderName: "South Bay AFL-CIO Labor Council Committee on Political Education",
        direction: "support",
        amountCents: 725_00,
        expenditureCount: 2,
      },
      {
        spenderFilerId: "941786",
        spenderName: "Santa Clara County Government Attorneys' Association PAC",
        direction: "oppose",
        amountCents: 158_10,
        expenditureCount: 1,
      },
    ]);
  });

  it("groups Pending-ID spenders by normalized name so two ID-less spenders never collapse", () => {
    const rows = [
      s496({ tranId: "P1", filerId: "Pending", filerName: "California Working Families Party", amountCents: 10_00 }),
      s496({ tranId: "P2", filerId: "Pending", filerName: "CALIFORNIA WORKING FAMILIES  PARTY", amountCents: 5_00 }),
      s496({ tranId: "P3", filerId: "Pending", filerName: "Some Other Committee", amountCents: 1_00 }),
    ];
    const result = aggregate({ s496: rows });
    expect(result.groups.map((group) => [group.spenderName, group.amountCents])).toEqual([
      ["CALIFORNIA WORKING FAMILIES  PARTY", 15_00],
      ["Some Other Committee", 1_00],
    ]);
  });

  it("fails closed on a council candidate without a seat number and on mayor rows disclosing a district", () => {
    expect(() =>
      aggregate({ candidate: { displayName: "Peter Ortiz", officeName: "City Council Member", seatNumber: null as unknown as number } }),
    ).toThrow(/seat number/);
    // A mayor-targeted row disclosing a district is malformed — vetoed.
    const mayor = aggregateSanJoseOutsideSpending({
      candidate: { displayName: "Sam Liccardo", officeName: "Mayor", seatNumber: null },
      s496: [
        s496({ tranId: "M1", candidateLastName: "Sam Liccardo", officeCd: "MAY", distNo: null }),
        s496({ tranId: "M2", candidateLastName: "Sam Liccardo", officeCd: "MAY", distNo: "3" }),
      ],
      scheduleD: [],
    });
    expect(mayor.supportTotalCents).toBe(100_00);
    expect(mayor.diagnostics.districtGateExcludedRows).toBe(1);
  });

  it("merges a paper-496 supplement into the same spender's e-filed group (live Campos case)", () => {
    // Live: the PAC's e-filed 496 ($5,270.18) plus its paper twin ($5,270.27)
    // must reproduce the form's own cumulative-to-date, $10,540.45.
    const result = aggregate({
      candidate: CAMPOS,
      s496: [
        s496({
          filerId: "941786",
          filerName: "Santa Clara County Government Attorneys' Association PAC",
          tranId: "E1",
          eFilingId: "24950",
          candidateLastName: "Nora Campos",
          suppOppCd: "OPPOSE",
          amountCents: 5270_18,
        }),
      ],
      paperSupplements: [paperEntry()],
    });
    expect(result.opposeTotalCents).toBe(10540_45);
    expect(result.groups).toHaveLength(1);
    expect(result.groups[0]).toMatchObject({
      spenderFilerId: "941786",
      direction: "oppose",
      amountCents: 10540_45,
      expenditureCount: 2,
    });
    expect(result.diagnostics.paperSupplementRows).toBe(1);
  });

  it("runs supplements through the normal target and veto gates", () => {
    // Another candidate's supplement (the paper Karen Martinez 496) books
    // nothing for Campos; a wrong-district entry is vetoed, never guessed.
    const otherCandidate = aggregate({
      candidate: CAMPOS,
      paperSupplements: [
        paperEntry({ candidateLastName: "Martinez", candidateFirstName: "Karen", eFilingId: "24824" }),
      ],
    });
    expect(otherCandidate.opposeTotalCents).toBe(0);
    expect(otherCandidate.diagnostics.otherCandidateRows).toBe(1);
    const wrongDistrict = aggregate({
      candidate: CAMPOS,
      paperSupplements: [paperEntry({ distNo: "7" })],
    });
    expect(wrongDistrict.opposeTotalCents).toBe(0);
    expect(wrongDistrict.diagnostics.districtGateExcludedRows).toBe(1);
  });

  it("rejects an invalid supplement list at aggregation time", () => {
    expect(() =>
      aggregate({ paperSupplements: [paperEntry({ amountCents: -1 })] }),
    ).toThrow(/positive integer/);
  });

  it("suppresses a supplement whose filing entered the export (no double count)", () => {
    // If the paper filing is later e-filed — directly or as the origin of an
    // amendment chain — the export row is authoritative and the stale
    // supplement must NOT add on top of it.
    const exported = s496({
      filerId: "941786",
      filerName: "Santa Clara County Government Attorneys' Association PAC",
      tranId: "E9",
      eFilingId: "25600",
      origEFilingId: "24823",
      candidateLastName: "Nora Campos",
      suppOppCd: "OPPOSE",
      amountCents: 5270_27,
    });
    const result = aggregate({
      candidate: CAMPOS,
      s496: [exported],
      paperSupplements: [paperEntry()],
    });
    expect(result.opposeTotalCents).toBe(5270_27);
    expect(result.diagnostics.paperSupplementRows).toBe(0);
    expect(result.diagnostics.paperSupplementRowsSuppressed).toBe(1);
    // Direct id match (the filing itself re-exported) suppresses too.
    const direct = aggregate({
      candidate: CAMPOS,
      s496: [s496({ ...exported, eFilingId: "24823", origEFilingId: "24823" })],
      paperSupplements: [paperEntry()],
    });
    expect(direct.opposeTotalCents).toBe(5270_27);
    expect(direct.diagnostics.paperSupplementRowsSuppressed).toBe(1);
  });
});
