import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";

import {
  CONNECTICUT_ECRIS_NO_DOCUMENTS_MESSAGE,
  parseConnecticutEcrisDate,
  parseConnecticutEcrisHiddenFields,
  parseConnecticutEcrisIndependentExpenditureSearchResults,
  parseConnecticutEcrisMoneyCents,
  splitConnecticutEcrisNameList,
} from "../../../src/pipeline/connecticutFinance/connecticutEcrisIndependentExpenditureParsers.js";

const FIXTURE_PATH = new URL("../../fixtures/connecticutFinance/independent-expenditure-search-results.html", import.meta.url);

async function fixture(): Promise<string> {
  return await readFile(FIXTURE_PATH, "utf8");
}

describe("connecticutEcrisIndependentExpenditureParsers", () => {
  it("reads every hidden WebForms field, decoding entities", async () => {
    const fields = parseConnecticutEcrisHiddenFields(await fixture());

    expect(fields).toEqual({
      __EVENTTARGET: "",
      __EVENTARGUMENT: "",
      __VIEWSTATE: "VIEWSTATE+RESULTS/==",
      __VIEWSTATEGENERATOR: "ABC123",
      __VIEWSTATEENCRYPTED: "",
    });
  });

  it("parses the results grid into typed rows", async () => {
    const parsed = parseConnecticutEcrisIndependentExpenditureSearchResults(await fixture());

    expect(parsed.status).toBe("rows");
    if (parsed.status !== "rows") return;
    expect(parsed.rows).toHaveLength(5);

    expect(parsed.rows[0]).toEqual({
      rootExpenditureId: "0",
      committeeName: "Nutmeg Forward",
      formTag: "SEEC40",
      documentUrl:
        "https://seec.ct.gov/eCrisReporting/Data/Attachment/Unassigned/SEEC40_24_Hour_Independent_Expenditure_Primary_1_900001.PDF",
      reportType: "24 Hour Independent Expenditure Primary 1",
      documentType: "Original",
      payee: "Harbor Media & Print",
      receivedDate: "2026-07-30",
      fileYear: 2026,
      periodStartDate: "2026-07-22",
      periodEndDate: "2026-08-04",
      amountCents: 125_000,
      formSection: "G. Expenses Paid by Committee",
      supportingCandidates: ["Jane Q Doe"],
      supportingOffices: ["State Representative"],
      opposingCandidates: [],
      opposingOffices: [],
      dataSource: "eFile",
    });

    expect(parsed.rows[1]).toMatchObject({
      documentType: "Amendment",
      amountCents: 50_000,
      formSection: "I. Expenses Incurred by Committee but Not Paid",
      supportingCandidates: [],
      opposingCandidates: ["John Roe"],
      opposingOffices: ["State Senator"],
    });

    expect(parsed.rows[2]).toMatchObject({
      supportingCandidates: ["Jane Q Doe", "Sam Poe"],
      supportingOffices: ["State Representative", "State Senator"],
      opposingCandidates: ["John Roe", "Ann Coe"],
      opposingOffices: ["State Senator"],
      amountCents: 500_000,
    });

    expect(parsed.rows[3]).toMatchObject({
      rootExpenditureId: "612345",
      committeeName: "Riverbend Town Committee",
      formTag: "SEEC20",
      formSection: "P. Expenses Paid by Committee",
      dataSource: "eFILE",
    });

    expect(parsed.rows[4]).toMatchObject({
      formTag: "SEEC8",
      reportType: "",
      payee: "",
      receivedDate: "2026-01-13",
      periodStartDate: null,
      periodEndDate: null,
      amountCents: null,
      formSection: "",
      supportingCandidates: [],
      dataSource: "Scan",
    });
  });

  it("recognizes the no-documents page", () => {
    const html = `<html><body><span id="ctl00_ContentPlaceHolder1_lblMessage">${CONNECTICUT_ECRIS_NO_DOCUMENTS_MESSAGE}</span></body></html>`;

    expect(parseConnecticutEcrisIndependentExpenditureSearchResults(html)).toEqual({ status: "no_documents" });
  });

  it("fails closed on an unrecognized page or a drifted grid", async () => {
    expect(() => parseConnecticutEcrisIndependentExpenditureSearchResults("<html><body>Runtime Error</body></html>")).toThrow(
      "neither results nor a no-documents message"
    );

    const renamedColumn = (await fixture()).replace(">Supporting Candidates<", ">Candidates Supported<");
    expect(() => parseConnecticutEcrisIndependentExpenditureSearchResults(renamedColumn)).toThrow(
      'column 12 is "Candidates Supported"; expected "Supporting Candidates"'
    );

    const droppedCell = (await fixture()).replace("<td>eFile</td>\n</tr>", "\n</tr>");
    expect(() => parseConnecticutEcrisIndependentExpenditureSearchResults(droppedCell)).toThrow(
      "row has 15 cells; expected 16"
    );

    const oddAmount = (await fixture()).replace("$1,250.00", "1250");
    expect(() => parseConnecticutEcrisIndependentExpenditureSearchResults(oddAmount)).toThrow("Unparseable Connecticut eCRIS amount");
  });

  it("parses money and dates strictly", () => {
    expect(parseConnecticutEcrisMoneyCents("$8,544.45")).toBe(854_445);
    expect(parseConnecticutEcrisMoneyCents("$0.00")).toBe(0);
    expect(parseConnecticutEcrisMoneyCents("($4,000.00)")).toBe(-400_000);
    expect(parseConnecticutEcrisMoneyCents("  ")).toBeNull();
    expect(() => parseConnecticutEcrisMoneyCents("$12")).toThrow("Unparseable");
    expect(() => parseConnecticutEcrisMoneyCents("($12.00")).toThrow("Unparseable");

    expect(parseConnecticutEcrisDate("07/17/2026")).toBe("2026-07-17");
    expect(parseConnecticutEcrisDate("")).toBeNull();
    expect(() => parseConnecticutEcrisDate("2026-07-17")).toThrow("Unparseable");
    expect(() => parseConnecticutEcrisDate("02/30/2026")).toThrow("Invalid");
  });

  it("splits comma lists without a space after the comma", () => {
    expect(splitConnecticutEcrisNameList("Doug McCrory,Ayana Taylor")).toEqual(["Doug McCrory", "Ayana Taylor"]);
    expect(splitConnecticutEcrisNameList(" Erin  E Stewart ")).toEqual(["Erin E Stewart"]);
    expect(splitConnecticutEcrisNameList("")).toEqual([]);
  });
});
