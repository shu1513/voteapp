import { describe, expect, it } from "vitest";

import {
  NorthDakotaCfrsClientError,
  getAllNorthDakotaCommittees,
  getAllNorthDakotaTransactions,
  getNorthDakotaChartData,
  getNorthDakotaDataDownloadCatalog,
  getNorthDakotaDataDownloadFileUrl,
  redactPresignedUrl,
} from "../../../src/pipeline/northDakotaFinance/northDakotaCfrsClient.js";

function jsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

const COMMITTEE_ROW = {
  orgID: 1636,
  orgVersID: 1,
  entityId: "1010001636",
  orgName: "Doug Sharbono for ND",
  candidateName: "Mr. Sharbono, Doug",
  officerName: "Mr. Sharbono, Doug",
  orgTypeCode: "101",
  orgType: "Candidate/Candidate Committee",
  orgSubType: "Candidate Committee",
  orgSubTypeCode: "CNCM",
  orgAddress: "1708 9th St S, Fargo, ND, USA, 58103",
  election: "2026 Election - Statewide",
  office: "State Representative",
  stance: null,
  district: "District 11",
  party: "North Dakota Republican Party",
  orgStatus: "Active",
  totalCount: 1,
  registrationYear: "2026",
};

const TRANSACTION_ROW = {
  transactionID: 12460,
  entityID: "1040001626",
  orgID: 1626,
  committeeName: "StrongND Fund",
  candidateName: null,
  transactionAmount: 16857.14,
  transactionDate: "2026-06-04T00:00:00",
  filedDate: "2026-06-04T00:00:00",
  entityTypeDesc: "Business or Organization",
  transactionCategoryDesc: "Monetary",
  transactionTypeDesc: "Independent Expenditures",
  transactionPurpose: null,
  contributorPayeeName: "Edgerton Strategies",
  contributorPayeeID: 77,
  addressLine1: "1 Main St",
  employerName: null,
  employerOccupation: null,
  transactionTotalYTD: "153999.9800",
  amendedFlag: false,
  reportVersionID: "1",
  reportFileName: "IE Report",
  s3ReportFilePath: "nd-cfs/Reports/1626/x.pdf",
  stanceDescription: "Support",
  candidateNameAssocation: "Kringstad, Jill",
  electionYear: 2026,
  orgType: "Independent Expenditure Committee",
};

describe("redactPresignedUrl", () => {
  it("strips the credential-bearing query string", () => {
    const url = "https://bucket.s3.us-gov-west-1.amazonaws.com/nd-cfs/x.csv?X-Amz-Security-Token=SECRET&X-Amz-Expires=3600";
    expect(redactPresignedUrl(url)).toBe("https://bucket.s3.us-gov-west-1.amazonaws.com/nd-cfs/x.csv");
    expect(redactPresignedUrl(url)).not.toContain("SECRET");
  });
});

describe("getNorthDakotaDataDownloadCatalog", () => {
  it("sends the mandatory pagination body and parses rows", async () => {
    let capturedBody: unknown;
    const rows = [{ id: 1019, dataType: "Contributions", year: "2026", totalCount: 1, s3ReportFilePath: "nd-cfs/x.csv" }];
    const catalog = await getNorthDakotaDataDownloadCatalog({
      fetchImpl: async (_url, init) => {
        capturedBody = JSON.parse(String(init?.body));
        return jsonResponse({ isSuccess: true, responseData: { totalRecords: 1, data: rows }, message: null });
      },
    });
    expect(capturedBody).toEqual({ pageNumber: 1, pageSize: 500 });
    expect(catalog).toEqual([{ id: 1019, dataType: "Contributions", year: "2026", s3ReportFilePath: "nd-cfs/x.csv" }]);
  });

  it("fails closed on the WAF's HTML block page", async () => {
    await expect(
      getNorthDakotaDataDownloadCatalog({
        fetchImpl: async () =>
          new Response("<html>403 Forbidden</html>", { status: 200, headers: { "content-type": "text/html" } }),
      })
    ).rejects.toThrow(/unexpected content type/);
  });

  it("classifies a body-read failure as a typed network error", async () => {
    const brokenBody = {
      ok: true,
      status: 200,
      headers: new Headers({ "content-type": "application/json" }),
      arrayBuffer: async () => {
        throw new TypeError("terminated");
      },
    } as unknown as Response;
    const pending = getNorthDakotaDataDownloadCatalog({ fetchImpl: async () => brokenBody });
    await expect(pending).rejects.toBeInstanceOf(NorthDakotaCfrsClientError);
    await expect(pending).rejects.toThrow(/response read failed .*terminated/);
  });

  it("fails closed on isSuccess=false envelopes", async () => {
    await expect(
      getNorthDakotaDataDownloadCatalog({
        fetchImpl: async () => jsonResponse({ isSuccess: false, responseData: null, message: "Something went wrong." }),
      })
    ).rejects.toThrow(/Something went wrong/);
  });
});

describe("getNorthDakotaDataDownloadFileUrl", () => {
  it("puts the catalog id in the path and returns the https fileUrl", async () => {
    let capturedUrl = "";
    const url = await getNorthDakotaDataDownloadFileUrl(1013, {
      fetchImpl: async (input) => {
        capturedUrl = String(input);
        return jsonResponse({
          isSuccess: true,
          responseData: { cloudFrontPolicy: "p", fileName: "x.csv", fileUrl: "https://s3.example/x.csv?sig=1" },
        });
      },
    });
    expect(capturedUrl).toBe("https://cfrs.sos.nd.gov/api/Public-Service/AccessReport/getDataDownloadfile/1013");
    expect(url).toBe("https://s3.example/x.csv?sig=1");
  });
});

describe("getAllNorthDakotaCommittees", () => {
  it("paginates on totalRecords and drops address/officer fields", async () => {
    const bodies: unknown[] = [];
    const committees = await getAllNorthDakotaCommittees(
      { pageSize: 1 },
      {
        fetchImpl: async (_url, init) => {
          const body = JSON.parse(String(init?.body)) as { pageNumber: number };
          bodies.push(body);
          const row = { ...COMMITTEE_ROW, orgID: 1636 + body.pageNumber, entityId: `101000163${body.pageNumber}` };
          return jsonResponse({ isSuccess: true, responseData: { totalRecords: 2, data: [row] } });
        },
      }
    );
    expect(bodies).toEqual([
      { pageNumber: 1, pageSize: 1 },
      { pageNumber: 2, pageSize: 1 },
    ]);
    expect(committees).toHaveLength(2);
    expect(committees[0]).not.toHaveProperty("orgAddress");
    expect(committees[0]).not.toHaveProperty("officerName");
    expect(committees[0].office).toBe("State Representative");
  });

  it("detects mid-pagination drift", async () => {
    let call = 0;
    await expect(
      getAllNorthDakotaCommittees(
        { pageSize: 1 },
        {
          fetchImpl: async () => {
            call += 1;
            return jsonResponse({ isSuccess: true, responseData: { totalRecords: call === 1 ? 3 : 4, data: [COMMITTEE_ROW] } });
          },
        }
      )
    ).rejects.toThrow(/totalRecords changed/);
  });
});

describe("getAllNorthDakotaTransactions", () => {
  it("requires orgTypeCode for the IE dataset", async () => {
    await expect(getAllNorthDakotaTransactions({ transactionCategory: "IE" })).rejects.toThrow(/requires orgTypeCode/);
  });

  it("sends the pinned body shape and parses rows without address fields", async () => {
    let capturedBody: unknown;
    const rows = await getAllNorthDakotaTransactions(
      { transactionCategory: "IE", orgTypeCode: "104", transactionYear: 2026, pageSize: 50 },
      {
        fetchImpl: async (_url, init) => {
          capturedBody = JSON.parse(String(init?.body));
          return jsonResponse({ isSuccess: true, responseData: { totalRecords: 1, data: [TRANSACTION_ROW] } });
        },
      }
    );
    expect(capturedBody).toEqual({
      transactionCategory: "IE",
      orgTypeCode: "104",
      sortColumn: "transactionDate",
      sortDirection: "DESC",
      transactionYear: "2026",
      pageNumber: 1,
      pageSize: 50,
    });
    expect(rows[0].stanceDescription).toBe("Support");
    expect(rows[0].electionYear).toBe(2026);
    expect(rows[0].contributorPayeeID).toBe(77);
    expect(rows[0]).not.toHaveProperty("addressLine1");
  });

  it("sends an empty transactionYear when no year is given", async () => {
    let capturedBody: { transactionYear?: string } = {};
    await getAllNorthDakotaTransactions(
      { transactionCategory: "CON" },
      {
        fetchImpl: async (_url, init) => {
          capturedBody = JSON.parse(String(init?.body));
          return jsonResponse({ isSuccess: true, responseData: { totalRecords: 0, data: null } });
        },
      }
    );
    expect(capturedBody.transactionYear).toBe("");
    expect(capturedBody).not.toHaveProperty("orgTypeCode");
  });
});

describe("getNorthDakotaChartData", () => {
  it("issues a bare GET and parses the series", async () => {
    let capturedMethod: string | undefined;
    let capturedUrl = "";
    const series = await getNorthDakotaChartData("contributions", {
      fetchImpl: async (input, init) => {
        capturedUrl = String(input);
        capturedMethod = init?.method;
        return jsonResponse({
          isSuccess: true,
          responseData: [
            {
              name: "By Contributor Type",
              totalAmount: 8369740.67,
              data: [{ description: "Lumpsum", amount: 1369801.55, totalAmount: 8369740.67 }],
            },
          ],
        });
      },
    });
    expect(capturedMethod).toBe("GET");
    expect(capturedUrl).toBe("https://cfrs.sos.nd.gov/api/Public-Service/CommitteeTransactions/getContributionChartData");
    expect(series).toEqual([
      { name: "By Contributor Type", totalAmount: 8369740.67, data: [{ description: "Lumpsum", amount: 1369801.55 }] },
    ]);
  });
});
