import { afterEach, describe, expect, it, vi } from "vitest";

import {
  UtahDisclosuresClientError,
  buildUtahAdvancedSearchUrl,
  buildUtahEntityReportListFormBody,
  buildUtahEntityReportListUrl,
  buildUtahGenerateReportUrl,
  downloadUtahGeneratedReportRows,
  fetchUtahEntityReportListHtml,
  parseUtahAdvancedSearchEntityRows,
  parseUtahDisclosuresCsvRows,
  parseUtahDisclosuresTransactionRows,
} from "../../../src/pipeline/utahFinance/utahDisclosuresClient.js";

function responseWithText(body: string, init: ResponseInit = {}): Response {
  return new Response(body, {
    status: 200,
    statusText: "OK",
    ...init,
  });
}

describe("utahDisclosuresClient", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("builds Utah disclosures endpoint URLs", () => {
    expect(buildUtahAdvancedSearchUrl()).toBe("https://disclosures.utah.gov/Search/AdvancedSearch");
    expect(buildUtahEntityReportListUrl()).toBe(
      "https://disclosures.utah.gov/Search/AdvancedSearch/GetEntityReportList"
    );

    const bulkUrl = new URL(buildUtahGenerateReportUrl({ reportYear: 2024, entityType: "PCC" }));
    expect(bulkUrl.origin + bulkUrl.pathname).toBe("https://disclosures.utah.gov/Search/AdvancedSearch/GenerateReport");
    expect(bulkUrl.searchParams.get("ReportYear")).toBe("2024");
    expect(bulkUrl.searchParams.get("EntityType")).toBe("PCC");

    const folderUrl = new URL(buildUtahGenerateReportUrl({ reportYear: 2024, folderId: "12345" }));
    expect(folderUrl.origin + folderUrl.pathname).toBe(
      "https://disclosures.utah.gov/Search/AdvancedSearch/GenerateReport/12345"
    );
    expect(folderUrl.searchParams.get("ReportYear")).toBe("2024");
    expect(folderUrl.searchParams.get("EntityType")).toBeNull();
  });

  it("builds strict advanced-search form bodies", () => {
    expect(
      buildUtahEntityReportListFormBody({
        search: "  Jane   ",
        entityType: "PCC",
        reportYear: 2024,
        hideContributions: true,
        hideExpenditures: false,
        pageNumber: 2,
      })
    ).toBe("Search=Jane&EntityType=PCC&ReportYear=2024&HideContributions=true&HideExpenditures=false&PageNumber=2");

    expect(() =>
      buildUtahEntityReportListFormBody({
        entityType: "BAD" as "PCC",
        reportYear: 2024,
      })
    ).toThrow(UtahDisclosuresClientError);
    expect(() =>
      buildUtahEntityReportListFormBody({
        entityType: "PCC",
        reportYear: 1997,
      })
    ).toThrow("report year");
  });

  it("parses advanced-search entity rows with folder ids and year downloads", () => {
    const html = `
      <table>
        <tr>
          <td><a href="/Search/AdvancedSearch/FolderDetails/98765">Jane &amp; Utah</a></td>
          <td>Ending Balance: $1,234.56</td>
          <td>
            <a href="/Search/AdvancedSearch/GenerateReport/98765?ReportYear=2024">2024</a>
            <a href="/Search/AdvancedSearch/GenerateReport/98765?ReportYear=2022">2022</a>
          </td>
        </tr>
        <tr><td>No folder here</td></tr>
      </table>
    `;

    expect(parseUtahAdvancedSearchEntityRows(html, undefined, "PCC")).toEqual([
      {
        folderId: "98765",
        entityName: "Jane & Utah",
        entityType: "PCC",
        endingBalance: 1234.56,
        reportYears: [2024, 2022],
        sourceUrl: "https://disclosures.utah.gov/Search/AdvancedSearch/FolderDetails/98765",
      },
    ]);
  });

  it("parses quoted CSV rows while preserving Utah header names", () => {
    expect(
      parseUtahDisclosuresCsvRows(
        '\uFEFFFILED,PCC,TRAN_ID,TRAN_AMT,NAME,PURPOSE\r\n"01/01/2024","Jane, Inc.","T1","$1,250.50","Doe, John","He said ""hello"""\r\n'
      )
    ).toEqual([
      {
        FILED: "01/01/2024",
        PCC: "Jane, Inc.",
        TRAN_ID: "T1",
        TRAN_AMT: "$1,250.50",
        NAME: "Doe, John",
        PURPOSE: 'He said "hello"',
      },
    ]);
  });

  it("maps Utah bulk generated-report CSV transactions", () => {
    const csv = [
      "FILED,PCC,REPORT,TRAN_ID,TRAN_TYPE,TRAN_DATE,TRAN_AMT,INKIND,LOAN,AMENDS,NAME,PURPOSE,ADDRESS1,ADDRESS2,CITY,STATE,ZIP,INKIND_COMMENTS",
      "01/05/2024,Jane for Utah,Year End,T100,Contribution,01/02/2024,\"$2,500.00\",False,False,,John Smith,Donation,1 Main,,Salt Lake City,UT,84111,",
    ].join("\n");

    expect(parseUtahDisclosuresTransactionRows(csv)).toEqual([
      {
        filed: "01/05/2024",
        entityType: "PCC",
        entityName: "Jane for Utah",
        report: "Year End",
        transactionId: "T100",
        transactionType: "Contribution",
        transactionDate: "01/02/2024",
        amount: 2500,
        name: "John Smith",
        purpose: "Donation",
        address1: "1 Main",
        city: "Salt Lake City",
        state: "UT",
        zip: "84111",
        inKind: false,
        loan: false,
      },
    ]);
  });

  it("maps Utah folder CSV transactions with shorter rows", () => {
    const csv = [
      "FILED,PCC,REPORT,TRAN_ID,TRAN_TYPE,TRAN_DATE,TRAN_AMT,NAME,PURPOSE,AMENDS,AMEND_COMMENTS,ADDRESS1,ADDRESS2,CITY,STATE,ZIP,INKIND,INKIND_COMMENTS,LOAN,COMMENT,PSA",
      "02/01/2024,Jane for Utah,January,T200,Expenditure,01/30/2024,125.00,Vendor,Printing,,,,,Provo,UT",
    ].join("\n");

    expect(parseUtahDisclosuresTransactionRows(csv)).toEqual([
      {
        filed: "02/01/2024",
        entityType: "PCC",
        entityName: "Jane for Utah",
        report: "January",
        transactionId: "T200",
        transactionType: "Expenditure",
        transactionDate: "01/30/2024",
        amount: 125,
        name: "Vendor",
        purpose: "Printing",
        city: "Provo",
        state: "UT",
        inKind: false,
        loan: false,
      },
    ]);
  });

  it("returns no rows for Utah's no-recorded-transactions response", () => {
    expect(parseUtahDisclosuresCsvRows("There are no recorded transactions for the selected filters.\n")).toEqual([]);
    expect(parseUtahDisclosuresTransactionRows("There are no recorded transactions for the selected filters.\n")).toEqual([]);
  });

  it("rejects unexpected 200 responses that are not transaction CSVs", () => {
    expect(() => parseUtahDisclosuresTransactionRows("<html><body>temporarily unavailable</body></html>")).toThrow(
      "TRAN_ID"
    );
    expect(() => parseUtahDisclosuresTransactionRows("FILED,PCC,NAME\n01/01/2024,Jane for Utah,John Smith")).toThrow(
      "TRAN_ID"
    );
  });

  it("rejects malformed CSV", () => {
    expect(() => parseUtahDisclosuresCsvRows('FILED,PCC\n"unterminated')).toThrow("unterminated");
  });

  it("fetches the advanced-search entity list with Utah's posted form fields", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(responseWithText("<table></table>")) as unknown as typeof fetch;

    await expect(
      fetchUtahEntityReportListHtml(
        { search: "Jane", entityType: "PCC", reportYear: 2024 },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toBe("<table></table>");

    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect(String(vi.mocked(fetchImpl).mock.calls[0]?.[0])).toBe(
      "https://disclosures.utah.gov/Search/AdvancedSearch/GetEntityReportList"
    );
    const init = vi.mocked(fetchImpl).mock.calls[0]?.[1];
    expect(init?.method).toBe("POST");
    expect(String(init?.body)).toBe(
      "Search=Jane&EntityType=PCC&ReportYear=2024&HideContributions=false&HideExpenditures=false&PageNumber=1"
    );
    expect((init?.headers as Headers).get("x-requested-with")).toBe("XMLHttpRequest");
  });

  it("downloads generated report CSV rows", async () => {
    const csv = [
      "FILED,PCC,REPORT,TRAN_ID,TRAN_TYPE,TRAN_DATE,TRAN_AMT,INKIND,LOAN,AMENDS,NAME,PURPOSE,ADDRESS1,ADDRESS2,CITY,STATE,ZIP,INKIND_COMMENTS",
      "01/05/2024,Jane for Utah,Year End,T100,Contribution,01/02/2024,10,True,False,,John Smith,Donation,,,,,,In-kind note",
    ].join("\n");
    const fetchImpl = vi.fn().mockResolvedValue(responseWithText(csv)) as unknown as typeof fetch;

    await expect(
      downloadUtahGeneratedReportRows({ reportYear: 2024, entityType: "PCC" }, { fetchImpl, timeoutMs: 1000 })
    ).resolves.toMatchObject([
      {
        transactionId: "T100",
        amount: 10,
        inKind: true,
        loan: false,
        inKindComments: "In-kind note",
      },
    ]);

    expect(String(vi.mocked(fetchImpl).mock.calls[0]?.[0])).toBe(
      "https://disclosures.utah.gov/Search/AdvancedSearch/GenerateReport?ReportYear=2024&EntityType=PCC"
    );
  });

  it("surfaces HTTP failures as Utah client errors", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      responseWithText("nope", {
        status: 500,
        statusText: "Server Error",
      })
    ) as unknown as typeof fetch;

    await expect(
      fetchUtahEntityReportListHtml(
        { entityType: "PCC", reportYear: 2024 },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).rejects.toMatchObject({ code: "http_error", status: 500 });
  });

  it("keeps the request timeout active while reading the response body", async () => {
    vi.useFakeTimers();
    const fetchImpl = vi.fn().mockResolvedValue({
      ok: true,
      text: () => new Promise<string>(() => undefined),
    } as Response) as unknown as typeof fetch;

    const request = fetchUtahEntityReportListHtml(
      { entityType: "PCC", reportYear: 2024 },
      { fetchImpl, timeoutMs: 100 }
    );
    const expectation = expect(request).rejects.toMatchObject({ code: "network_error" });
    await vi.advanceTimersByTimeAsync(100);

    await expectation;
  });
});
