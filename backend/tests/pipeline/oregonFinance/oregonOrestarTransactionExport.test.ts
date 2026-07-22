import { describe, expect, it } from "vitest";

import {
  isOregonOrestarExportWorkbook,
  parseOregonOrestarTransactionExport,
} from "../../../src/pipeline/oregonFinance/oregonOrestarTransactionExport.js";
import { buildOregonOrestarExportWorkbook } from "./orestarExportFixture.js";

describe("oregonOrestarTransactionExport", () => {
  it("maps export rows onto transaction details, normalizing mixed cell types", () => {
    const data = buildOregonOrestarExportWorkbook([
      {
        "Tran Id": "5699168",
        "Tran Date": "06/22/2026",
        "Tran Status": "Original",
        Filer: "Friends of Andrea Valderrama",
        "Contributor/Payee": "Union Pacific Railroad",
        "Sub Type": "Cash Contribution",
        Amount: 500,
        "Aggregate Amount": 500,
        "Filer Id": "18462",
        "Filed Date": "06/24/2026",
        "Book Type": "Business Entity",
      },
      {
        // Numeric Tran Id / Filer Id cells and a $-formatted amount string
        // must normalize the same way as their text/number counterparts.
        "Tran Id": 5699169,
        "Tran Date": "03/05/2025",
        Filer: "Friends of Andrea Valderrama",
        "Contributor/Payee": "Jane Donor",
        "Sub Type": "Cash Contribution",
        Amount: "$1,250.00",
        "Filer Id": 18462,
        "Occptn Txt": "Teacher",
        "Emp Name": "Portland Public Schools",
      },
    ]);

    const details = parseOregonOrestarTransactionExport({
      data,
      transactionType: "Contribution",
      sourceUrl: "https://secure.sos.state.or.us/orestar/gotoPublicTransactionSearch.do",
    });

    expect(details).toHaveLength(2);
    expect(details[0]).toMatchObject({
      transactionId: "5699168",
      transactionDate: "06/22/2026",
      transactionType: "Contribution",
      transactionSubType: "Cash Contribution",
      filedDate: "06/24/2026",
      amount: 500,
      aggregate: 500,
      processStatus: "Original",
      filerCommitteeName: "Friends of Andrea Valderrama",
      filerCommitteeId: "18462",
      addressBookType: "Business Entity",
      contributorPayeeName: "Union Pacific Railroad",
      occupation: null,
      employerName: null,
      outsideAssociations: [],
      sourceUrl: "https://secure.sos.state.or.us/orestar/gotoPublicTransactionSearch.do",
    });
    expect(details[1]).toMatchObject({
      transactionId: "5699169",
      amount: 1_250,
      filerCommitteeId: "18462",
      occupation: "Teacher",
      employerName: "Portland Public Schools",
    });
  });

  it("returns an empty list for a workbook with only a header row", () => {
    expect(parseOregonOrestarTransactionExport({ data: buildOregonOrestarExportWorkbook([]) })).toEqual([]);
  });

  it("rejects workbooks missing required columns", () => {
    const data = buildOregonOrestarExportWorkbook(
      [{ "Tran Id": "1", "Tran Date": "01/01/2026" }],
      ["Tran Id", "Tran Date", "Filer"]
    );
    expect(() => parseOregonOrestarTransactionExport({ data })).toThrow(
      "ORESTAR export is missing required columns: Filer Id, Contributor/Payee, Sub Type, Amount"
    );
  });

  it("rejects bytes that are not an .xls workbook (blocked-page HTML)", () => {
    const html = new TextEncoder().encode("<html><body>blocked</body></html>");
    expect(isOregonOrestarExportWorkbook(html)).toBe(false);
    expect(() => parseOregonOrestarTransactionExport({ data: html })).toThrow(
      "ORESTAR export response is not an .xls workbook"
    );
  });
});
