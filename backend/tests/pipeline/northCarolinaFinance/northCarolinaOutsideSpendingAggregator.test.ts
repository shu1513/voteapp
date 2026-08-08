import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

import {
  aggregateNorthCarolinaOutsideSpending,
  northCarolinaIeTargetMatchesCandidate,
  northCarolinaOutsideGroupCommitteeId,
  NCSBE_IE_REPORT_TYPE_UNREGISTERED,
} from "../../../src/pipeline/northCarolinaFinance/northCarolinaOutsideSpendingAggregator.js";
import {
  parseNcsbeDate,
  parseNcsbeDocumentListPage,
  parseNcsbeExpendituresPage,
  type NcsbeDocumentRow,
  type NcsbeExpenditureRow,
} from "../../../src/pipeline/northCarolinaFinance/northCarolinaNcsbeParsers.js";

function fixture(name: string): string {
  return readFileSync(
    fileURLToPath(new URL(`../../fixtures/northCarolinaFinance/${name}`, import.meta.url)),
    "utf8"
  );
}

const IE_INVENTORY = parseNcsbeDocumentListPage(fixture("ie-doc-type-inventory-2026.html"));
const ADVANCE_ROWS = parseNcsbeExpendituresPage(fixture("ie-expenditures-advance-232624-p0.json")).rows;
const CAROLINA_FEDERATION_ROWS = parseNcsbeExpendituresPage(
  fixture("ie-expenditures-carolina-federation-p0.json")
).rows;

const PIERCE = {
  candidateKey: "pierce|2026",
  candidateName: "Rodney Pierce",
  officeScope: "state_lower",
  district: "27",
};
const SADLER = {
  candidateKey: "sadler|2026",
  candidateName: "Rodney Sadler",
  officeScope: "state_lower",
  district: null,
};

function makeIeInventoryRow(overrides: Partial<NcsbeDocumentRow> = {}): NcsbeDocumentRow {
  return {
    committeeName: "SOME OUTSIDE GROUP",
    sboeId: null,
    reportYear: 2026,
    documentType: "Informational Report",
    reportType: NCSBE_IE_REPORT_TYPE_UNREGISTERED,
    isAmendment: false,
    imageReceiptDate: parseNcsbeDate("03/01/2026"),
    dataImportDate: parseNcsbeDate("03/01/2026"),
    periodStartDate: parseNcsbeDate("01/01/2026"),
    periodEndDate: parseNcsbeDate("02/14/2026"),
    dataLink: "900001",
    imageLink: null,
    ...overrides,
  };
}

function makeIeRow(overrides: Partial<NcsbeExpenditureRow> = {}): NcsbeExpenditureRow {
  return {
    occurDate: parseNcsbeDate("02/01/2026"),
    orgName: "MAILER VENDOR LLC",
    isOrg: true,
    amountCents: 100_00,
    ieAmountCents: 100_00,
    isAggregated: false,
    expenditureTypeDesc: "Independent Expenditure",
    purposeTypeCode: null,
    purpose: null,
    accountAbbr: null,
    formOfPaymentDesc: null,
    candidate: "PIERCE RODNEY",
    officeSought: "House",
    declaration: "Support",
    ...overrides,
  };
}

describe("aggregateNorthCarolinaOutsideSpending (real fixture bytes)", () => {
  const result = aggregateNorthCarolinaOutsideSpending({
    ieInventoryRows: IE_INVENTORY,
    reports: [
      // Advance NC's IRIEX informational: official total = the spike-verified
      // $29,306.30 IEAmount sum ($20K under the vendor-invoice Amount sum).
      { reportId: "232624", officialExpenditureTotalCents: 2_930_630, expenditureRows: ADVANCE_ROWS },
      // Carolina Federation Freedom PAC's registered-committee form: IEAmount
      // null, Amount carries the true $10,500.
      {
        reportId: "232613",
        officialExpenditureTotalCents: 1_050_000,
        expenditureRows: CAROLINA_FEDERATION_ROWS,
      },
    ],
    candidates: [PIERCE, SADLER],
    sourceUrl: "https://cf.ncsbe.gov/CFDocLkup/DocumentResult/?year=2026",
  });

  it("attributes both token orders and both forms to the right candidates", () => {
    expect(result.candidates.map((candidate) => candidate.candidateKey)).toEqual([
      "pierce|2026",
      "sadler|2026",
    ]);
    const pierce = result.candidates[0]!;
    // $320 + $980 from Advance NC ("PIERCE RODNEY", House) plus the $10,500
    // Pivot Group row ("RODNEY PIERCE", NC HOUSE 27) from the registered
    // form — three spellings, one person, no fuzzy matching.
    expect(pierce.supportTotal).toBe(11_800);
    expect(pierce.opposeTotal).toBe(0);
    expect(pierce.groups).toEqual([
      {
        committeeId: "STA-98J33C-C-001",
        committeeName: "CAROLINA FEDERATION FREEDOM PAC",
        supportOppose: "support",
        amount: 10_500,
        sourceUrl: "https://cf.ncsbe.gov/CFDocLkup/DocumentResult/?year=2026",
      },
      expect.objectContaining({
        committeeId: expect.stringMatching(/^NC-IE-FILER:[0-9a-f]{64}$/),
        committeeName: "ADVANCE NORTH CAROLINA",
        supportOppose: "support",
        amount: 1_300,
      }),
    ]);
    // Split vendor-invoice rows: $1,166.66 + $833.33.
    expect(result.candidates[1]!.supportTotal).toBe(1_999.99);
    expect(result.attributedRowCount).toBe(5);
    expect(result.attributedCents).toBe(1_379_999);
  });

  it("passes the decision-4 reconciliation gate on both forms", () => {
    const byId = new Map(result.reports.map((report) => [report.reportId, report]));
    expect(byId.get("232624")).toMatchObject({
      quarantined: false,
      chosenAmountSumCents: 2_930_630,
      // Six split rows earn six cents of rounding slack.
      toleranceCents: 6,
    });
    expect(byId.get("232613")).toMatchObject({
      quarantined: false,
      chosenAmountSumCents: 1_050_000,
      toleranceCents: 0,
    });
  });

  it("filters federal and county targets before matching, with their money counted", () => {
    // 33 Foushee rows across both federal spellings; DeBerry's two
    // County/Municipal rows must not reach a same-named state candidate.
    expect(result.federalTargetRowCount).toBe(33);
    expect(result.federalTargetCents).toBe(2_450_630);
    expect(result.countyMunicipalTargetRowCount).toBe(2);
    // Everything in the probed report is accounted for: attributed + federal
    // + county equals the official total.
    expect(result.attributedCents - 1_050_000 + result.federalTargetCents + 150_001).toBe(2_930_630);
  });

  it("quarantines every selected structured filing without supplied artifacts", () => {
    expect(result.missingReportIds).not.toContain("232624");
    expect(result.missingReportIds).not.toContain("232613");
    expect(result.missingReportIds.length).toBeGreaterThan(0);
    expect(result.quarantinedReportCount).toBe(result.missingReportIds.length);
  });

  it("surfaces image-only and ambiguous-lineage filings as coverage gaps", () => {
    // Carolina Federation Freedom PAC's IE Disclosure is image-only with a
    // live year-3026 period end.
    expect(result.coverageGaps).toContainEqual(
      expect.objectContaining({
        filerKey: "STA-98J33C-C-001",
        kind: "image_only_current",
        periodEndRaw: "06/01/3026",
      })
    );
    // Citizens for NC Jobs Action PAC: four blank-period image-only
    // originals share one selection key — unknowable lineage.
    expect(result.coverageGaps).toContainEqual(
      expect.objectContaining({
        filerKey: "STA-BF20V2-C-001",
        kind: "quarantined_lineage",
      })
    );
  });
});

describe("aggregateNorthCarolinaOutsideSpending (fail-closed paths)", () => {
  it("quarantines an unregistered-form report whose IEAmount is null", () => {
    const result = aggregateNorthCarolinaOutsideSpending({
      ieInventoryRows: [makeIeInventoryRow()],
      reports: [
        {
          reportId: "900001",
          officialExpenditureTotalCents: 10_000,
          expenditureRows: [makeIeRow({ ieAmountCents: null })],
        },
      ],
      candidates: [PIERCE],
    });
    expect(result.reports[0]).toMatchObject({
      quarantined: true,
      quarantineReason: "null_ie_amount_on_unregistered_form",
    });
    expect(result.candidates).toEqual([]);
  });

  it("quarantines on official-total mismatch and on a missing official total", () => {
    const mismatch = aggregateNorthCarolinaOutsideSpending({
      ieInventoryRows: [makeIeInventoryRow()],
      reports: [
        { reportId: "900001", officialExpenditureTotalCents: 99_999, expenditureRows: [makeIeRow()] },
      ],
      candidates: [PIERCE],
    });
    expect(mismatch.reports[0]).toMatchObject({ quarantineReason: "official_total_mismatch" });
    expect(mismatch.candidates).toEqual([]);

    const missing = aggregateNorthCarolinaOutsideSpending({
      ieInventoryRows: [makeIeInventoryRow()],
      reports: [{ reportId: "900001", officialExpenditureTotalCents: null, expenditureRows: [makeIeRow()] }],
      candidates: [PIERCE],
    });
    expect(missing.reports[0]).toMatchObject({ quarantineReason: "missing_official_total" });
  });

  it("excludes undeclared directions and the SPECIFIC NON CANDIDATE sentinel, counted", () => {
    const result = aggregateNorthCarolinaOutsideSpending({
      ieInventoryRows: [makeIeInventoryRow()],
      reports: [
        {
          reportId: "900001",
          officialExpenditureTotalCents: 300_00,
          expenditureRows: [
            makeIeRow(),
            makeIeRow({ declaration: null }),
            makeIeRow({ candidate: "SPECIFIC NON CANDIDATE" }),
          ],
        },
      ],
      candidates: [PIERCE],
    });
    expect(result.excludedDeclarationRowCount).toBe(1);
    expect(result.excludedDeclarationCents).toBe(100_00);
    expect(result.nonCandidateTargetRowCount).toBe(1);
    expect(result.attributedRowCount).toBe(1);
  });

  it("quarantines a district conflict but confirms a matching district", () => {
    const result = aggregateNorthCarolinaOutsideSpending({
      ieInventoryRows: [makeIeInventoryRow()],
      reports: [
        {
          reportId: "900001",
          officialExpenditureTotalCents: 200_00,
          expenditureRows: [
            makeIeRow({ officeSought: "NC HOUSE 27" }),
            makeIeRow({ officeSought: "NC HOUSE 99" }),
          ],
        },
      ],
      candidates: [PIERCE],
    });
    expect(result.attributedRowCount).toBe(1);
    expect(result.unmatchedTargets).toEqual([
      { value: "PIERCE RODNEY", rowCount: 1, amountCents: 100_00 },
    ]);
  });

  it("counts a chamber conflict as unmatched — money for a same-named senator stays out", () => {
    const result = aggregateNorthCarolinaOutsideSpending({
      ieInventoryRows: [makeIeInventoryRow()],
      reports: [
        {
          reportId: "900001",
          officialExpenditureTotalCents: 100_00,
          expenditureRows: [makeIeRow({ officeSought: "Senate" })],
        },
      ],
      candidates: [PIERCE],
    });
    expect(result.attributedRowCount).toBe(0);
    expect(result.unmatchedTargets).toHaveLength(1);
  });

  it("flags identical directional rows across a filer's overlapping reports, without dedup", () => {
    const early = makeIeInventoryRow();
    const late = makeIeInventoryRow({
      dataLink: "900002",
      periodStartDate: parseNcsbeDate("02/01/2026"),
      periodEndDate: parseNcsbeDate("06/30/2026"),
    });
    const identicalRow = makeIeRow();
    const result = aggregateNorthCarolinaOutsideSpending({
      ieInventoryRows: [early, late],
      reports: [
        { reportId: "900001", officialExpenditureTotalCents: 100_00, expenditureRows: [identicalRow] },
        { reportId: "900002", officialExpenditureTotalCents: 100_00, expenditureRows: [identicalRow] },
      ],
      candidates: [PIERCE],
    });
    expect(result.overlappingReportPairCount).toBe(1);
    expect(result.duplicateLookingRowCount).toBe(1);
    expect(result.duplicateLookingCents).toBe(100_00);
    // Both rows still counted (spike item 13: incremental, no dedup rule).
    expect(result.candidates[0]!.supportTotal).toBe(200);
  });

  it("counts inventory rows outside the two pinned IE report types", () => {
    const result = aggregateNorthCarolinaOutsideSpending({
      ieInventoryRows: [makeIeInventoryRow({ reportType: "48-Hour", dataLink: null, imageLink: "x.pdf" })],
      reports: [],
      candidates: [],
    });
    expect(result.unknownReportTypeRowCount).toBe(1);
    expect(result.reports).toEqual([]);
  });
});

describe("northCarolinaIeTargetMatchesCandidate", () => {
  it("matches both token orders, middle initials included", () => {
    expect(northCarolinaIeTargetMatchesCandidate("Rodney Pierce", "PIERCE RODNEY")).toBe(true);
    expect(northCarolinaIeTargetMatchesCandidate("Rodney Pierce", "PIERCE RODNEY D")).toBe(true);
    expect(northCarolinaIeTargetMatchesCandidate("Rodney Pierce", "RODNEY PIERCE")).toBe(true);
  });

  it("keeps the strict conflict guard on both readings", () => {
    expect(northCarolinaIeTargetMatchesCandidate("Rodney Blake Pierce", "PIERCE RODNEY D")).toBe(false);
    // Real misspelling across filings quarantines, as designed (spike item 4).
    expect(northCarolinaIeTargetMatchesCandidate("Satana Deberry", "DEWBERRY SANTANA")).toBe(false);
  });
});

describe("northCarolinaOutsideGroupCommitteeId", () => {
  it("uses the SBoEID when present, else a name-hash key — never the raw name", () => {
    expect(
      northCarolinaOutsideGroupCommitteeId({ sboeId: "sta-98j33c-c-001", committeeName: "X" })
    ).toBe("STA-98J33C-C-001");
    const key = northCarolinaOutsideGroupCommitteeId({
      sboeId: null,
      committeeName: "Advance North Carolina",
    });
    expect(key).toMatch(/^NC-IE-FILER:[0-9a-f]{64}$/);
    // Case/punctuation variants of one name yield one key.
    expect(
      northCarolinaOutsideGroupCommitteeId({ sboeId: null, committeeName: "ADVANCE  NORTH CAROLINA" })
    ).toBe(key);
  });
});
