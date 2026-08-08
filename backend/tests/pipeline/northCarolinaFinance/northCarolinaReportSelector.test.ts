import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

import {
  ncsbeFilerKeyForDocumentRow,
  selectNcsbeCurrentFilings,
} from "../../../src/pipeline/northCarolinaFinance/northCarolinaReportSelector.js";
import {
  parseNcsbeDate,
  parseNcsbeDocumentListPage,
  type NcsbeDocumentRow,
} from "../../../src/pipeline/northCarolinaFinance/northCarolinaNcsbeParsers.js";

function fixtureRows(name: string): NcsbeDocumentRow[] {
  return parseNcsbeDocumentListPage(
    readFileSync(
      fileURLToPath(new URL(`../../fixtures/northCarolinaFinance/${name}`, import.meta.url)),
      "utf8"
    )
  );
}

function documentRow(overrides: Partial<NcsbeDocumentRow> = {}): NcsbeDocumentRow {
  return {
    committeeName: "COMMITTEE TO ELECT JANE DOE",
    sboeId: "STA-AB12CD-C-001",
    reportYear: 2026,
    documentType: "Disclosure Report",
    reportType: "First Quarter",
    isAmendment: false,
    imageReceiptDate: parseNcsbeDate("02/24/2026"),
    dataImportDate: parseNcsbeDate("02/24/2026"),
    periodStartDate: parseNcsbeDate("01/01/2026"),
    periodEndDate: parseNcsbeDate("02/14/2026"),
    dataLink: "100001",
    imageLink: "image.pdf",
    ...overrides,
  };
}

describe("selectNcsbeCurrentFilings", () => {
  it("selects every Gadson disclosure report — no amendments, no quarantines", () => {
    const rows = fixtureRows("document-inventory-gadson.html").filter(
      (row) => row.documentType === "Disclosure Report"
    );
    const result = selectNcsbeCurrentFilings({ rows });
    // Ordered by period start: Year End Semi-Annual (07/01/2025) precedes
    // the overlapping Organizational report (10/31/2025).
    expect(result.selected.map((filing) => filing.reportId)).toEqual(["227042", "226297", "229931"]);
    expect(result.supersededUnavailable).toEqual([]);
    expect(result.quarantinedGroups).toEqual([]);
    expect(result.groupCount).toBe(3);
    expect(result.duplicateRowCount).toBe(0);
  });

  it("lets the real Carolina Federation amendments supersede their originals", () => {
    // Year End Semi-Annual 2025: original 227151 (image 01/21/2026) vs
    // amendment 230622 (image 04/09/2026). Third Quarter 2024: original
    // 220019 vs amendment 221937.
    const rows = fixtureRows("document-inventory-carolina-federation.html").filter(
      (row) => row.documentType === "Disclosure Report" && row.reportType !== "Independent Expenditure Report"
    );
    const result = selectNcsbeCurrentFilings({ rows });
    const byPeriod = new Map(result.selected.map((filing) => [filing.periodStartRaw, filing]));
    expect(byPeriod.get("07/01/2025")).toMatchObject({ reportId: "230622", isAmendment: true });
    expect(byPeriod.get("07/01/2024")).toMatchObject({ reportId: "221937", isAmendment: true });
    // Unamended periods keep their originals.
    expect(byPeriod.get("01/01/2026")).toMatchObject({ reportId: "229235", isAmendment: false });
    expect(result.quarantinedGroups).toEqual([]);
    expect(result.supersededUnavailable).toEqual([]);
  });

  it("marks an image-only current filing superseded-unavailable, never falls back", () => {
    // Real rows: Carolina Federation's IE Disclosure Report is image-only with
    // a live year-3026 period end — its own group, no structured fallback.
    const rows = fixtureRows("document-inventory-carolina-federation.html").filter(
      (row) => row.reportType === "Independent Expenditure Report"
    );
    const result = selectNcsbeCurrentFilings({ rows });
    expect(result.selected).toEqual([]);
    expect(result.supersededUnavailable).toHaveLength(1);
    expect(result.supersededUnavailable[0]).toMatchObject({
      reportId: null,
      periodEndIso: null,
      filerKey: "STA-98J33C-C-001",
    });
  });

  it("selects the image-only amendment over the image-only original (Conservation Votes shape)", () => {
    const original = documentRow({
      committeeName: "CONSERVATION VOTES PAC",
      sboeId: "STA-2301JS-C-001",
      reportType: "Independent Expenditure Report",
      periodStartDate: parseNcsbeDate("02/15/2026"),
      periodEndDate: parseNcsbeDate("03/03/2026"),
      dataLink: null,
      imageReceiptDate: parseNcsbeDate("02/20/2026"),
      dataImportDate: parseNcsbeDate(""),
    });
    const amendment = documentRow({
      ...original,
      isAmendment: true,
      imageReceiptDate: parseNcsbeDate("03/03/2026"),
    });
    const result = selectNcsbeCurrentFilings({ rows: [original, amendment] });
    expect(result.selected).toEqual([]);
    expect(result.supersededUnavailable).toHaveLength(1);
    expect(result.supersededUnavailable[0]).toMatchObject({ isAmendment: true, reportId: null });
  });

  it("merges a split DATA + IMAGE pair into one filing with the image date as chronology", () => {
    const dataRow = documentRow({ imageReceiptDate: parseNcsbeDate(""), imageLink: null });
    const imageRow = documentRow({
      dataLink: null,
      dataImportDate: parseNcsbeDate(""),
      imageReceiptDate: parseNcsbeDate("03/01/2026"),
    });
    const result = selectNcsbeCurrentFilings({ rows: [dataRow, imageRow] });
    expect(result.selected).toHaveLength(1);
    expect(result.selected[0]).toMatchObject({
      reportId: "100001",
      filedDateIso: "2026-03-01",
    });
    expect(result.selected[0]!.rows).toHaveLength(2);
  });

  it("keeps an all-DATA amendment chain as distinct filings and selects the newest (Berger shape)", () => {
    const original = documentRow({
      reportType: "Mid Year Semi-Annual",
      periodStartDate: parseNcsbeDate("01/01/2025"),
      periodEndDate: parseNcsbeDate("06/30/2025"),
      dataLink: "225000",
      imageReceiptDate: parseNcsbeDate("07/10/2025"),
      dataImportDate: parseNcsbeDate("07/10/2025"),
    });
    const firstAmendment = documentRow({
      ...original,
      isAmendment: true,
      dataLink: "225581",
      imageReceiptDate: parseNcsbeDate("08/01/2025"),
      dataImportDate: parseNcsbeDate("08/01/2025"),
    });
    const secondAmendment = documentRow({
      ...original,
      isAmendment: true,
      dataLink: "232191",
      imageReceiptDate: parseNcsbeDate("07/10/2026"),
      // Import lag: the newer amendment imported EARLIER must still win on
      // the image date.
      dataImportDate: parseNcsbeDate("07/09/2026"),
    });
    const result = selectNcsbeCurrentFilings({ rows: [original, firstAmendment, secondAmendment] });
    expect(result.selected).toHaveLength(1);
    expect(result.selected[0]).toMatchObject({ reportId: "232191", isAmendment: true });
  });

  it("quarantines a null amendment flag as unknowable lineage", () => {
    const result = selectNcsbeCurrentFilings({ rows: [documentRow({ isAmendment: null })] });
    expect(result.selected).toEqual([]);
    expect(result.quarantinedGroups).toEqual([
      expect.objectContaining({ reason: "null_amendment_flag", rowCount: 1 }),
    ]);
  });

  it("quarantines multiple non-amendment originals sharing a period", () => {
    const result = selectNcsbeCurrentFilings({
      rows: [documentRow(), documentRow({ dataLink: "100002" })],
    });
    expect(result.quarantinedGroups).toEqual([
      expect.objectContaining({ reason: "multiple_original_filings" }),
    ]);
  });

  it("quarantines an unmergeable DATA/IMAGE mix", () => {
    // One DATA row + two IMAGE rows: the extra image could be a newer
    // image-only amendment hiding behind the structured filing.
    const result = selectNcsbeCurrentFilings({
      rows: [
        documentRow({ imageLink: null }),
        documentRow({ dataLink: null, imageReceiptDate: parseNcsbeDate("02/25/2026") }),
        documentRow({ dataLink: null, imageReceiptDate: parseNcsbeDate("02/26/2026") }),
      ],
    });
    expect(result.quarantinedGroups).toEqual([
      expect.objectContaining({ reason: "ambiguous_row_merge" }),
    ]);
  });

  it("selects the amendment when it ties its original on every date — flag semantics need no chronology", () => {
    const original = documentRow();
    const amendment = documentRow({ isAmendment: true, dataLink: "100003" });
    const result = selectNcsbeCurrentFilings({ rows: [original, amendment] });
    expect(result.quarantinedGroups).toEqual([]);
    expect(result.selected).toHaveLength(1);
    expect(result.selected[0]).toMatchObject({ reportId: "100003", isAmendment: true });
  });

  it("quarantines two amendments tying on every chronology key — never ordered by report id", () => {
    const original = documentRow({
      imageReceiptDate: parseNcsbeDate("02/20/2026"),
      dataImportDate: parseNcsbeDate("02/20/2026"),
    });
    const firstAmendment = documentRow({ isAmendment: true, dataLink: "100009" });
    const secondAmendment = documentRow({ isAmendment: true, dataLink: "1000010" });
    const result = selectNcsbeCurrentFilings({ rows: [original, firstAmendment, secondAmendment] });
    expect(result.selected).toEqual([]);
    expect(result.quarantinedGroups).toEqual([
      expect.objectContaining({ reason: "ambiguous_filing_chronology" }),
    ]);
  });

  it("quarantines when chronology says the original is newer than an amendment", () => {
    const original = documentRow({ imageReceiptDate: parseNcsbeDate("05/01/2026") });
    const amendment = documentRow({
      isAmendment: true,
      dataLink: "100003",
      imageReceiptDate: parseNcsbeDate("03/01/2026"),
      dataImportDate: parseNcsbeDate("03/01/2026"),
    });
    const result = selectNcsbeCurrentFilings({ rows: [original, amendment] });
    expect(result.selected).toEqual([]);
    expect(result.quarantinedGroups).toEqual([
      expect.objectContaining({ reason: "original_newer_than_amendment" }),
    ]);
  });

  it("dedups the same structured filing listed by more than one inventory", () => {
    // 232613 appears in both the committee inventory and the IE doc-type
    // inventory — one report id is one filing.
    const committeeRows = fixtureRows("document-inventory-carolina-federation.html").filter(
      (row) => row.dataLink === "232613"
    );
    const ieRows = fixtureRows("ie-doc-type-inventory-2026.html").filter(
      (row) => row.dataLink === "232613"
    );
    expect(committeeRows).toHaveLength(1);
    expect(ieRows).toHaveLength(1);
    const result = selectNcsbeCurrentFilings({ rows: [...committeeRows, ...ieRows] });
    expect(result.selected.map((filing) => filing.reportId)).toEqual(["232613"]);
    expect(result.duplicateRowCount).toBe(1);
  });

  it("keys unregistered filers by committee name so distinct No-Id filers never collide", () => {
    expect(
      ncsbeFilerKeyForDocumentRow({ sboeId: null, committeeName: "Advance North Carolina" })
    ).toBe("NAME:ADVANCE NORTH CAROLINA");
    expect(ncsbeFilerKeyForDocumentRow({ sboeId: "sta-ab12cd-c-001", committeeName: "X" })).toBe(
      "STA-AB12CD-C-001"
    );
  });
});
