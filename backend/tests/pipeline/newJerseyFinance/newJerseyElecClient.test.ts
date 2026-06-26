import { afterEach, describe, expect, it, vi } from "vitest";

import {
  NewJerseyElecClientError,
  buildNewJerseyElecContributionRowsForm,
  buildNewJerseyElecContributionRowsSourceUrl,
  buildNewJerseyElecEntityListUrl,
  buildNewJerseyElecEntityFilingsUrl,
  buildNewJerseyElecIndependentExpenditureEntitiesUrl,
  buildNewJerseyElecReportDownloadUrl,
  downloadNewJerseyElecReportUrl,
  getNewJerseyElecContributionRows,
  getNewJerseyElecEntityFilings,
  getNewJerseyElecFilingRows,
  getNewJerseyElecReportDownload,
  listNewJerseyElecIndependentExpenditureEntities,
  mapNewJerseyElecContributionRow,
  mapNewJerseyElecEntity,
  mapNewJerseyElecFiling,
  searchNewJerseyElecEntities,
} from "../../../src/pipeline/newJerseyFinance/newJerseyElecClient.js";

function jsonResponse(payload: unknown, init: ResponseInit = {}): Response {
  return new Response(JSON.stringify(payload), {
    status: 200,
    statusText: "OK",
    ...init,
  });
}

describe("newJerseyElecClient", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("builds ELEC entity search URLs with required query fields", () => {
    const url = new URL(
      buildNewJerseyElecEntityListUrl({
        firstName: " Mikie ",
        lastName: " Sherrill ",
        nonPacOnly: true,
      })
    );

    expect(url.origin + url.pathname).toBe("https://www.njelecefilesearch.com/api/VWEntity/GetEntityList");
    expect(url.searchParams.get("FirstName")).toBe("Mikie");
    expect(url.searchParams.get("LastName")).toBe("Sherrill");
    expect(url.searchParams.get("NONPACOnly")).toBe("true");
    expect(() => buildNewJerseyElecEntityListUrl({})).toThrow(NewJerseyElecClientError);

    const ieUrl = new URL(buildNewJerseyElecIndependentExpenditureEntitiesUrl());
    expect(ieUrl.origin + ieUrl.pathname).toBe("https://www.njelecefilesearch.com/api/VWEntity/GetEntityList");
    expect(ieUrl.searchParams.get("NONPACOnly")).toBe("false");
    expect(ieUrl.searchParams.get("LastName")).toBe("");
    expect(ieUrl.searchParams.get("FirstName")).toBe("");
    expect(ieUrl.searchParams.get("NonIndName")).toBe("");
    expect(ieUrl.searchParams.get("PACName")).toBe("");
  });

  it("builds contribution DataTables forms without inventing filters", () => {
    const body = buildNewJerseyElecContributionRowsForm({
      entityS: 473742,
      electionYear: 2025,
      officeCode: "GOV",
      partyCode: "D",
      electionTypeCode: "G",
      rowLimit: 500,
    });

    expect(body.get("ENTITY_S")).toBe("473742");
    expect(body.get("ElectionYears")).toBe("2025");
    expect(body.get("ElectionTypeCodes")).toBe("G");
    expect(body.get("length")).toBe("500");
    expect(body.get("columns[0][data]")).toBe("CONTRIBUTOR");
    expect(body.get("columns[10][data]")).toBe("CONTRIB_S");
    expect(buildNewJerseyElecContributionRowsSourceUrl(473742)).toBe(
      "https://www.njelecefilesearch.com/SearchContributionToEntity?eid=473742"
    );
  });

  it("builds filing metadata and report download URLs", () => {
    const filingUrl = new URL(buildNewJerseyElecEntityFilingsUrl({ entityS: 477267 }));
    expect(filingUrl.origin + filingUrl.pathname).toBe(
      "https://www.njelecefilesearch.com/api/VWEntity/GetEntityFilingData"
    );
    expect(filingUrl.searchParams.get("ENTITY_S")).toBe("477267");

    const downloadUrl = new URL(buildNewJerseyElecReportDownloadUrl(3909738));
    expect(downloadUrl.origin + downloadUrl.pathname).toBe("https://www.njelecefilesearch.com/SearchIndExpReports/");
    expect(downloadUrl.searchParams.get("handler")).toBe("DownloadReport");
    expect(downloadUrl.searchParams.get("DocId")).toBe("3909738");
    expect(() => buildNewJerseyElecReportDownloadUrl(0)).toThrow(NewJerseyElecClientError);
  });

  it("maps entity rows and skips malformed rows", () => {
    const sourceUrl = "https://www.njelecefilesearch.com/api/VWEntity/GetEntityList?LastName=Sherrill";

    expect(
      mapNewJerseyElecEntity(
        {
          ENTITY_S: "473742",
          ENTITYNAME: "SHERRILL, MIKIE",
          FIRST_NAME: "Mikie",
          LAST_NAME: "Sherrill",
          ELECTIONYEAR: "2025",
          OFFICE: "Governor",
          PARTY: "Democratic",
          LOCATION_CODE: "0",
          ELECTIONTYPECODE: "G",
          ELECTIONTYPE: "General",
        },
        sourceUrl
      )
    ).toMatchObject({
      entityS: 473742,
      entityName: "SHERRILL, MIKIE",
      firstName: "Mikie",
      lastName: "Sherrill",
      electionYear: 2025,
      office: "Governor",
      locationCode: 0,
      electionTypeCode: "G",
      sourceUrl,
    });

    expect(mapNewJerseyElecEntity({ ENTITYNAME: "Missing ID" }, sourceUrl)).toBeNull();
  });

  it("maps contribution rows and parses currency amounts", () => {
    const sourceUrl = "https://www.njelecefilesearch.com/SearchContributionToEntity?eid=473742";

    expect(
      mapNewJerseyElecContributionRow(
        {
          CONTRIB_S: "1001",
          ENTITY_S: "473742",
          ELECTIONYEAR: "2025",
          CAND_NAME: "SHERRILL, MIKIE",
          CONTRIBUTOR: "Jane Doe",
          FIRST_NAME: "Jane",
          LAST_NAME: "Doe",
          IsIndividual: "Y",
          ContributorType: "Individual",
          ContributionType: "Monetary",
          CONT_DATE: "06/01/2025",
          CONT_AMT: "$1,234.56",
          EMP_NAME: "Acme Law",
          OccupationName: "Attorney",
        },
        sourceUrl
      )
    ).toEqual({
      contribS: 1001,
      entityS: 473742,
      electionYear: 2025,
      recipientName: "SHERRILL, MIKIE",
      contributorName: "Jane Doe",
      contributorFirstName: "Jane",
      contributorLastName: "Doe",
      contributorNonIndividualName: null,
      isIndividual: true,
      contributorType: "Individual",
      contributionType: "Monetary",
      contributionDate: "06/01/2025",
      amount: 1234.56,
      employerName: "Acme Law",
      occupationCode: null,
      occupationName: "Attorney",
      sourceUrl,
    });

    expect(mapNewJerseyElecContributionRow({ CONTRIB_S: 1001 }, sourceUrl)).toBeNull();
  });

  it("maps filing metadata rows and skips malformed rows", () => {
    const sourceUrl = "https://www.njelecefilesearch.com/api/VWEntity/GetEntityFilingData?ENTITY_S=477267";

    expect(
      mapNewJerseyElecFiling(
        {
          ENTITY_S: "477267",
          PERIOD: "3",
          AMEND_NO: "1",
          DATE_RECEIVED: "2025-12-29T00:00:00",
          DOCID: "3909738",
          PUBLIC_ACCESS: 1,
          HOUR_48_NOTICE_PUBLIC_ACCESS: 0,
          AMT_REC: "6,301,050.00",
          AMT_DISB: "5084060.08",
          FilingStatusFormCode: "R",
          LinkTabFormType: "R",
          FormName: "R-1",
          SORT_SEQ: 300,
        },
        sourceUrl
      )
    ).toEqual({
      entityS: 477267,
      docId: 3909738,
      period: 3,
      amendmentNumber: 1,
      dateReceived: "2025-12-29T00:00:00",
      publicAccess: true,
      hour48NoticePublicAccess: false,
      amountReceived: 6301050,
      amountDisbursed: 5084060.08,
      filingStatusFormCode: "R",
      linkTabFormType: "R",
      formName: "R-1",
      sortSequence: 300,
      reportDownloadUrl: "https://www.njelecefilesearch.com/SearchIndExpReports/?handler=DownloadReport&DocId=3909738",
      sourceUrl,
    });

    expect(mapNewJerseyElecFiling({ ENTITY_S: 477267 }, sourceUrl)).toBeNull();
  });

  it("fetches and maps entity search payloads", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse([
        {
          ENTITY_S: 473742,
          ENTITYNAME: "SHERRILL, MIKIE",
          ELECTIONYEAR: 2025,
          OFFICE: "Governor",
        },
        { ENTITYNAME: "malformed" },
      ])
    ) as unknown as typeof fetch;

    await expect(
      searchNewJerseyElecEntities({ lastName: "Sherrill" }, { fetchImpl, timeoutMs: 1000 })
    ).resolves.toMatchObject([{ entityS: 473742, entityName: "SHERRILL, MIKIE" }]);

    expect(String(vi.mocked(fetchImpl).mock.calls[0]?.[0])).toContain("LastName=Sherrill");
  });

  it("lists independent expenditure entities from the complete ELEC entity set", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse([
        {
          ENTITY_S: 481918,
          ENTITYNAME: "CLEAN POWER VOTERS FOR SHERRILL",
          ELECTIONYEAR: 2025,
          OFFICECODE: "Z",
          OFFICE: "INDEPENDENT EXPENDITURE CMTE (Z)",
          ELECTIONTYPECODE: "G",
        },
        {
          ENTITY_S: 472129,
          ENTITYNAME: "ONE GIANT LEAP PAC OGL PAC",
          ELECTIONYEAR: 2025,
          OFFICECODE: "Z",
          ELECTIONTYPECODE: "P",
        },
        {
          ENTITY_S: 473742,
          ENTITYNAME: "SHERRILL, MIKIE",
          ELECTIONYEAR: 2025,
          OFFICECODE: "0",
          ELECTIONTYPECODE: "G",
        },
        {
          ENTITY_S: 481918,
          ENTITYNAME: "CLEAN POWER VOTERS FOR SHERRILL",
          ELECTIONYEAR: 2024,
          OFFICECODE: "Z",
          ELECTIONTYPECODE: "G",
        },
      ])
    ) as unknown as typeof fetch;

    await expect(
      listNewJerseyElecIndependentExpenditureEntities(
        { electionYear: 2025, electionTypeCode: "G" },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toEqual([
      expect.objectContaining({
        entityS: 481918,
        entityName: "CLEAN POWER VOTERS FOR SHERRILL",
        officeCode: "Z",
        electionTypeCode: "G",
      }),
    ]);

    const url = new URL(String(vi.mocked(fetchImpl).mock.calls[0]?.[0]));
    expect(url.searchParams.get("NONPACOnly")).toBe("false");
    expect(url.searchParams.get("LastName")).toBe("");
    expect(url.searchParams.get("PACName")).toBe("");
  });

  it("posts contribution search forms and maps returned rows", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse({
        recordsTotal: "2",
        recordsFiltered: "2",
        data: [
          {
            CONTRIB_S: 1001,
            ENTITY_S: 473742,
            ELECTIONYEAR: 2025,
            CONTRIBUTOR: "Jane Doe",
            IsIndividual: "Y",
            CONT_AMT: "250",
            OccupationName: "Attorney",
          },
        ],
      })
    ) as unknown as typeof fetch;

    const result = await getNewJerseyElecContributionRows(
      { entityS: 473742, electionYear: 2025, electionTypeCode: "G", rowLimit: 10 },
      { fetchImpl, timeoutMs: 1000 }
    );

    expect(result.recordsTotal).toBe(2);
    expect(result.rows).toEqual([expect.objectContaining({ contribS: 1001, amount: 250 })]);
    expect(String(vi.mocked(fetchImpl).mock.calls[0]?.[0])).toBe(
      "https://www.njelecefilesearch.com/api/VWContributionDetail/GetContBitsDataByObject"
    );
    expect(vi.mocked(fetchImpl).mock.calls[0]?.[1]).toMatchObject({ method: "POST" });
  });

  it("fetches entity filings and report download JSON", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse([
          {
            ENTITY_S: 477267,
            DOCID: 3909738,
            FormName: "R-1",
            AMT_REC: 6301050,
          },
          { ENTITY_S: 477267 },
        ])
      )
      .mockResolvedValueOnce(
        jsonResponse({
          FileNameWithSAS: "https://storage.example/3909738.pdf?sv=short-lived",
        })
      ) as unknown as typeof fetch;

    await expect(getNewJerseyElecEntityFilings({ entityS: 477267 }, { fetchImpl, timeoutMs: 1000 })).resolves.toEqual([
      expect.objectContaining({ entityS: 477267, docId: 3909738, formName: "R-1", amountReceived: 6301050 }),
    ]);
    await expect(getNewJerseyElecReportDownload(3909738, { fetchImpl, timeoutMs: 1000 })).resolves.toEqual({
      docId: 3909738,
      fileNameWithSas: "https://storage.example/3909738.pdf?sv=short-lived",
      sourceUrl: "https://www.njelecefilesearch.com/SearchIndExpReports/?handler=DownloadReport&DocId=3909738",
    });
  });

  it("exposes phase-plan aliases for filing rows and report URLs", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse([{ ENTITY_S: 477267, DOCID: 3909738 }]))
      .mockResolvedValueOnce(jsonResponse({ FileNameWithSAS: "https://storage.example/3909738.pdf?sv=short-lived" })) as unknown as typeof fetch;

    await expect(getNewJerseyElecFilingRows({ entityS: 477267 }, { fetchImpl, timeoutMs: 1000 })).resolves.toEqual([
      expect.objectContaining({ entityS: 477267, docId: 3909738 }),
    ]);
    await expect(downloadNewJerseyElecReportUrl(3909738, { fetchImpl, timeoutMs: 1000 })).resolves.toBe(
      "https://storage.example/3909738.pdf?sv=short-lived"
    );
  });
});
