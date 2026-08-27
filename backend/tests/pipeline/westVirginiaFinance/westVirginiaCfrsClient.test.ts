import { describe, expect, it } from "vitest";

import {
  WestVirginiaCfrsClientError,
  getAllWestVirginiaTransactions,
  getWestVirginiaDataDownloadCatalog,
  getWestVirginiaDocumentDownloadUrl,
  redactPresignedUrl,
} from "../../../src/pipeline/westVirginiaFinance/westVirginiaCfrsClient.js";

function jsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

const TRANSACTION_ROW = {
  transactionID: 1,
  entityID: "1010000001",
  orgID: 1,
  committeeName: "Test Committee",
  candidateName: "Test, Candidate",
  transactionAmount: 25.44,
  transactionDate: "2026-06-17T00:00:00",
  filedDate: "2026-07-07T00:00:00",
  entityTypeDesc: "Individual",
  transactionCategoryDesc: "Monetary",
  transactionTypeDesc: "Contributions",
  transactionPurpose: null,
  contributorPayeeName: "Jacob Hively",
  employerName: null,
  employerOccupation: null,
  transactionTotalYTD: "25.4400",
  amendedFlag: false,
  reportVersionID: "1",
  reportFileName: "2026 2nd Quarter Report",
  s3ReportFilePath: "prd/Reports/1/x.pdf",
  stanceDescription: null,
  candidateNameAssocation: null,
  ballotMeasureDescription: null,
  orgType: "State Candidate",
};

describe("redactPresignedUrl", () => {
  it("strips the credential-bearing query string", () => {
    const url = "https://bucket.s3.us-gov-east-1.amazonaws.com/prd/x.csv?X-Amz-Security-Token=SECRET&X-Amz-Expires=3600";
    expect(redactPresignedUrl(url)).toBe("https://bucket.s3.us-gov-east-1.amazonaws.com/prd/x.csv");
    expect(redactPresignedUrl(url)).not.toContain("SECRET");
  });
});

describe("getWestVirginiaDataDownloadCatalog", () => {
  it("sends the mandatory pagination body and parses rows", async () => {
    let capturedBody: unknown;
    const rows = [{ id: 1078, dataType: "Contributions", year: "2026", s3ReportFilePath: "prd/x.csv" }];
    const catalog = await getWestVirginiaDataDownloadCatalog(
      {},
      {
        fetchImpl: async (_url, init) => {
          capturedBody = JSON.parse(String(init?.body));
          return jsonResponse({ isSuccess: true, responseData: { totalRecords: 1, data: rows } });
        },
      }
    );
    expect(capturedBody).toEqual({ pageNumber: 1, pageSize: 5000 });
    expect(catalog).toEqual(rows);
  });

  it("fails closed on the WAF's HTML block page", async () => {
    await expect(
      getWestVirginiaDataDownloadCatalog(
        {},
        {
          fetchImpl: async () =>
            new Response("<html>403 Forbidden</html>", {
              status: 200,
              headers: { "content-type": "text/html" },
            }),
        }
      )
    ).rejects.toThrow(/unexpected content type/);
  });

  it("fails closed on isSuccess=false envelopes", async () => {
    await expect(
      getWestVirginiaDataDownloadCatalog(
        {},
        { fetchImpl: async () => jsonResponse({ isSuccess: false, responseData: null, message: "Something went wrong." }) }
      )
    ).rejects.toThrow(/Something went wrong/);
  });
});

describe("getAllWestVirginiaTransactions", () => {
  it("requires a 3-digit orgTypeCode", async () => {
    await expect(
      getAllWestVirginiaTransactions({ orgTypeCode: "" }, { fetchImpl: async () => jsonResponse({}) })
    ).rejects.toThrow(WestVirginiaCfrsClientError);
  });

  it("paginates on totalRecords and detects mid-pagination drift", async () => {
    let call = 0;
    await expect(
      getAllWestVirginiaTransactions(
        { orgTypeCode: "101", pageSize: 1 },
        {
          fetchImpl: async () => {
            call += 1;
            return jsonResponse({
              isSuccess: true,
              responseData: { totalRecords: call === 1 ? 2 : 5, data: [TRANSACTION_ROW] },
            });
          },
        }
      )
    ).rejects.toThrow(/totalRecords changed/);
  });

  it("returns typed rows across pages", async () => {
    let call = 0;
    const rows = await getAllWestVirginiaTransactions(
      { orgTypeCode: "101", transactionCategory: "CON", pageSize: 1 },
      {
        fetchImpl: async (_url, init) => {
          call += 1;
          const body = JSON.parse(String(init?.body));
          expect(body.orgTypeCode).toBe("101");
          expect(body.transactionCategory).toBe("CON");
          expect(body.pageNumber).toBe(call);
          return jsonResponse({ isSuccess: true, responseData: { totalRecords: 2, data: [TRANSACTION_ROW] } });
        },
      }
    );
    expect(rows).toHaveLength(2);
    expect(rows[0].entityID).toBe("1010000001");
    expect(rows[0].amendedFlag).toBe(false);
  });
});

describe("getWestVirginiaDocumentDownloadUrl", () => {
  it("posts s3FilePath to Common-Service and returns the https url", async () => {
    let capturedUrl = "";
    const url = await getWestVirginiaDocumentDownloadUrl("prd/OrgDocuments/3981/x.pdf", {
      fetchImpl: async (requestUrl) => {
        capturedUrl = String(requestUrl);
        return jsonResponse({ isSuccess: true, responseData: "https://bucket/x.pdf?token=1" });
      },
    });
    expect(capturedUrl).toContain("/api/Common-Service/AmazonCloudFront/getDownloadLinkWithoutCookies");
    expect(url).toBe("https://bucket/x.pdf?token=1");
  });

  it("rejects absolute urls passed as s3FilePath", async () => {
    await expect(
      getWestVirginiaDocumentDownloadUrl("https://evil.example/x.pdf", { fetchImpl: async () => jsonResponse({}) })
    ).rejects.toThrow(/Invalid West Virginia CFRS s3FilePath/);
  });
});
