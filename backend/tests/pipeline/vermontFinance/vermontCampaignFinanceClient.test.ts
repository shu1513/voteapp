import { afterEach, describe, expect, it, vi } from "vitest";

import {
  VermontCampaignFinanceClientError,
  buildVermontCampaignFinanceApiUrl,
  buildVermontTransactionSearchPayload,
  getVermontContributionCategoriesByFilerRegistrationGuid,
  getVermontContributionDetails,
  getVermontExpenditureDetails,
  getVermontOfficeSoughtLookup,
  getVermontTransactionDetailsByGuid,
} from "../../../src/pipeline/vermontFinance/vermontCampaignFinanceClient.js";

function jsonResponse(payload: unknown, init: ResponseInit = {}): Response {
  return new Response(JSON.stringify(payload), {
    status: 200,
    statusText: "OK",
    ...init,
  });
}

function requestBody(fetchImpl: typeof fetch): Record<string, unknown> {
  const body = vi.mocked(fetchImpl).mock.calls[0]?.[1]?.body;
  expect(typeof body).toBe("string");
  return JSON.parse(String(body)) as Record<string, unknown>;
}

describe("vermontCampaignFinanceClient", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("builds Vermont campaign finance API URLs", () => {
    expect(buildVermontCampaignFinanceApiUrl("PublicTransactionDetails/GetContributionsDetails")).toBe(
      "https://api.campaignfinance.vermont.gov/api/PublicTransactionDetails/GetContributionsDetails"
    );
    expect(() => buildVermontCampaignFinanceApiUrl("/bad")).toThrow(VermontCampaignFinanceClientError);
  });

  it("builds transaction search payloads with conservative defaults and optional filters", () => {
    expect(
      buildVermontTransactionSearchPayload({
        pageSize: 25,
        sortBy: "TransactionAmount",
        sortType: "DESC",
        transactionTypeCode: "TCON",
        filerRegistrationGuid: " filer-guid ",
        filerName: " Smith ",
        electionYear: 2024,
        electionId: 24,
        transactionAmountMin: 1000,
      })
    ).toEqual({
      pageNumber: 1,
      pageSize: 25,
      sortBy: "TransactionAmount",
      sortType: "DESC",
      transactionTypeCode: "TCON",
      filerRegistrationGuid: "filer-guid",
      filerName: "Smith",
      electionYear: 2024,
      electionId: 24,
      transactionAmountMin: 1000,
    });

    expect(() => buildVermontTransactionSearchPayload({ pageSize: 10_000 })).toThrow(VermontCampaignFinanceClientError);
  });

  it("posts contribution searches and parses contribution rows", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse({
        data: {
          items: [
            {
              transactionID: 90013,
              transactionVersionID: 1,
              guid: "4f68b713-d2a1-44b8-b0fc-12ed5a6ba2a9",
              filerRegistrationGuid: "a48617ea-15fa-4a9f-a2f9-75ef95f240b0",
              filerName: "SMITH, SHAP",
              transactionAmount: 1000,
              transactionDate: "05/24/2016",
              sourceName: "STRITZLER, BILL B",
              firstName: "BILL",
              lastName: "STRITZLER",
              middleName: "B",
              transactionSource: "Individual",
              transactionSourceTypeCode: "TIND",
              transactionSubTypeCode: "ITMY",
              transactionSubTypeDescription: "Monetary Contribution",
              filerTypeCode: "CAN",
              filerTypeDescription: "Candidate",
              electionYear: 2016,
              electionCycle: "2016  General",
              electionId: 3,
              officeID: 7,
              officeType: "OTREP",
              entityId: 66206,
              reportName: "07/15/2016 - GENERAL",
              candidateFirstName: "SHAP",
              candidateLastName: "SMITH",
              occupation: "Attorney",
              employer: "Acme Law",
              filingYear: 2016,
              addressLine1: "PO BOX 437",
              city: "JEFFERSONVILLE",
              stateCode: "VT",
              zipCode: "05464-0437",
            },
            { guid: "missing required fields" },
          ],
          totalItems: 10697,
        },
        succeeded: true,
        error: null,
      })
    ) as unknown as typeof fetch;

    await expect(
      getVermontContributionDetails(
        { pageNumber: 2, pageSize: 10, sortBy: "TransactionAmount", transactionAmountMin: 1000 },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toEqual({
      totalItems: 10697,
      items: [
        {
          transactionId: 90013,
          transactionVersionId: 1,
          guid: "4f68b713-d2a1-44b8-b0fc-12ed5a6ba2a9",
          filerRegistrationGuid: "a48617ea-15fa-4a9f-a2f9-75ef95f240b0",
          filerName: "SMITH, SHAP",
          transactionAmount: 1000,
          transactionDate: "05/24/2016",
          sourceName: "STRITZLER, BILL B",
          sourceFirstName: "BILL",
          sourceLastName: "STRITZLER",
          sourceMiddleName: "B",
          transactionSource: "Individual",
          transactionSourceTypeCode: "TIND",
          transactionSubTypeCode: "ITMY",
          transactionSubTypeDescription: "Monetary Contribution",
          filerTypeCode: "CAN",
          filerTypeDescription: "Candidate",
          electionYear: 2016,
          electionCycle: "2016  General",
          electionId: 3,
          officeId: 7,
          officeType: "OTREP",
          entityId: 66206,
          reportName: "07/15/2016 - GENERAL",
          candidateFirstName: "SHAP",
          candidateLastName: "SMITH",
          candidateMiddleName: null,
          occupation: "Attorney",
          employer: "Acme Law",
          filingYear: 2016,
          addressLine1: "PO BOX 437",
          addressLine2: null,
          city: "JEFFERSONVILLE",
          stateCode: "VT",
          zipCode: "05464-0437",
        },
      ],
    });

    const request = vi.mocked(fetchImpl).mock.calls[0];
    expect(String(request?.[0])).toBe(
      "https://api.campaignfinance.vermont.gov/api/PublicTransactionDetails/GetContributionsDetails"
    );
    expect(request?.[1]?.method).toBe("POST");
    expect(requestBody(fetchImpl)).toMatchObject({
      pageNumber: 2,
      pageSize: 10,
      sortBy: "TransactionAmount",
      sortType: "DESC",
      transactionTypeCode: "TCON",
      transactionAmountMin: 1000,
    });
  });

  it("posts expenditure searches and parses expenditure rows with Vermont's entityID casing", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse({
        data: {
          items: [
            {
              transactionID: 407243,
              transactionVersionID: 1,
              guid: "dbd93101-7907-4166-9178-b0648f79238c",
              filerRegistrationGuid: "c249ea88-a0e1-4925-9aea-98fd02f2917a",
              filerName: "RAM, KESHA",
              transactionAmount: 1000,
              transactionDate: "03/22/2022",
              transactionCategoryDescription: "Returned Contribution to Contributor",
              transactionCategoryCode: "PURC",
              isStanceSupport: "true",
              payeeType: "Individual",
              entityID: 183337,
              expenditurePurpose: "Returned Contribution to Contributor",
              candidateMentioned: "Candidate Name",
              description: "memo",
              sourceName: "WHALEN, JODI",
              transactionSource: "Individual",
              filerTypeCode: "CAN",
              filerTypeDescription: "Candidate",
              electionYear: 2022,
              electionCycle: "2022  General Election",
              electionId: 24,
              officeID: "6",
              officeType: "OTSEN",
              reportName: "07/01/2022 - GENERAL",
              candidateFirstName: "KESHA",
              candidateLastName: "RAM",
              sourceAddressLine1: "352 SOUTH COVE ROAD",
              sourceCity: "BURLINGTON",
              sourceState: "VT",
              sourceZipCode: "05401",
            },
          ],
          totalItems: 9441,
        },
        succeeded: true,
        error: null,
      })
    ) as unknown as typeof fetch;

    await expect(getVermontExpenditureDetails({ pageSize: 5 }, { fetchImpl, timeoutMs: 1000 })).resolves.toEqual({
      totalItems: 9441,
      items: [
        {
          transactionId: 407243,
          transactionVersionId: 1,
          guid: "dbd93101-7907-4166-9178-b0648f79238c",
          filerRegistrationGuid: "c249ea88-a0e1-4925-9aea-98fd02f2917a",
          filerName: "RAM, KESHA",
          transactionAmount: 1000,
          transactionDate: "03/22/2022",
          transactionCategoryCode: "PURC",
          transactionCategoryDescription: "Returned Contribution to Contributor",
          expenditurePurpose: "Returned Contribution to Contributor",
          description: "memo",
          isStanceSupport: true,
          payeeType: "Individual",
          sourceName: "WHALEN, JODI",
          transactionSource: "Individual",
          filerTypeCode: "CAN",
          filerTypeDescription: "Candidate",
          electionYear: 2022,
          electionCycle: "2022  General Election",
          electionId: 24,
          officeId: 6,
          officeType: "OTSEN",
          entityId: 183337,
          reportName: "07/01/2022 - GENERAL",
          candidateMentioned: "Candidate Name",
          candidateFirstName: "KESHA",
          candidateLastName: "RAM",
          candidateMiddleName: null,
          sourceAddressLine1: "352 SOUTH COVE ROAD",
          sourceAddressLine2: null,
          sourceCity: "BURLINGTON",
          sourceState: "VT",
          sourceZipCode: "05401",
        },
      ],
    });

    expect(String(vi.mocked(fetchImpl).mock.calls[0]?.[0])).toBe(
      "https://api.campaignfinance.vermont.gov/api/PublicTransactionDetails/GetExpenditureDetails"
    );
    expect(requestBody(fetchImpl)).toMatchObject({ transactionTypeCode: "TEXP" });
  });

  it("gets transaction details by guid", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse({
        data: {
          transactionID: 90013,
          transactionVersionID: 1,
          guid: "4f68b713-d2a1-44b8-b0fc-12ed5a6ba2a9",
          transactionTypeCode: "TCON",
          transactionTypeDescription: "Contribution",
          transactionSubTypeCode: "ITMY",
          transactionSubTypeDesc: "Monetary Contribution",
          transactionSourceTypeCode: "TIND",
          transactionSourceTypeDesc: "Individual",
          transactionSource: "Individual",
          transactionDate: "2016-05-24T00:00:00",
          filerName: "SMITH, SHAP",
          contributor: "STRITZLER, BILL B",
          valueOfNonMoneyItem: 1000,
          transactionCategoryDesc: null,
          electionYear: 2016,
          comments: null,
        },
        succeeded: true,
        error: null,
      })
    ) as unknown as typeof fetch;

    await expect(
      getVermontTransactionDetailsByGuid(" 4f68b713-d2a1-44b8-b0fc-12ed5a6ba2a9 ", { fetchImpl, timeoutMs: 1000 })
    ).resolves.toEqual({
      transactionId: 90013,
      transactionVersionId: 1,
      guid: "4f68b713-d2a1-44b8-b0fc-12ed5a6ba2a9",
      transactionTypeCode: "TCON",
      transactionTypeDescription: "Contribution",
      transactionSubTypeCode: "ITMY",
      transactionSubTypeDescription: "Monetary Contribution",
      transactionSourceTypeCode: "TIND",
      transactionSourceTypeDescription: "Individual",
      transactionSource: "Individual",
      transactionDate: "2016-05-24T00:00:00",
      filerName: "SMITH, SHAP",
      contributor: "STRITZLER, BILL B",
      valueOfNonMoneyItem: 1000,
      transactionCategoryDescription: null,
      electionYear: 2016,
      comments: null,
    });
    expect(requestBody(fetchImpl)).toEqual({ transactionGuid: "4f68b713-d2a1-44b8-b0fc-12ed5a6ba2a9" });
  });

  it("gets contribution category totals by filer registration guid", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse({
        data: [
          {
            amount: 39950,
            transactionSourceTypeCode: "TBSN",
            transactionSourceType: "Business/Group/Organization",
          },
          { amount: "bad" },
        ],
        succeeded: true,
        error: null,
      })
    ) as unknown as typeof fetch;

    await expect(
      getVermontContributionCategoriesByFilerRegistrationGuid(" filer-guid ", { fetchImpl, timeoutMs: 1000 })
    ).resolves.toEqual([
      {
        amount: 39950,
        transactionSourceTypeCode: "TBSN",
        transactionSourceType: "Business/Group/Organization",
      },
    ]);
    expect(String(vi.mocked(fetchImpl).mock.calls[0]?.[0])).toBe(
      "https://api.campaignfinance.vermont.gov/api/PublicFilerDetails/GetContributionsCategoriesDetails"
    );
    expect(requestBody(fetchImpl)).toEqual({ filerRegistrationGuid: "filer-guid" });
  });

  it("gets Vermont office sought lookup rows", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse({
        data: [
          { value: "19", name: "Governor", code: "GOV" },
          { value: "bad", name: "Missing office id" },
        ],
        succeeded: true,
        error: null,
      })
    ) as unknown as typeof fetch;

    await expect(getVermontOfficeSoughtLookup({ fetchImpl, timeoutMs: 1000 })).resolves.toEqual([
      {
        value: "19",
        officeId: 19,
        name: "Governor",
        code: "GOV",
      },
    ]);

    const request = vi.mocked(fetchImpl).mock.calls[0];
    expect(String(request?.[0])).toBe("https://api.campaignfinance.vermont.gov/api/PublicLookup/GetOfficeSoughtLookup");
    expect(request?.[1]?.method).toBe("GET");
    expect(request?.[1]?.body).toBeUndefined();
  });

  it("maps HTTP, API envelope, and malformed JSON failures", async () => {
    await expect(
      getVermontContributionDetails({}, {
        fetchImpl: vi.fn().mockResolvedValue(jsonResponse({}, { status: 500, statusText: "Server Error" })) as unknown as typeof fetch,
        timeoutMs: 1000,
      })
    ).rejects.toMatchObject({ code: "http_error", status: 500 });

    await expect(
      getVermontContributionDetails({}, {
        fetchImpl: vi.fn().mockResolvedValue(jsonResponse({ succeeded: false, error: "Nope" })) as unknown as typeof fetch,
        timeoutMs: 1000,
      })
    ).rejects.toMatchObject({ code: "bad_response", message: "Nope" });

    await expect(
      getVermontContributionDetails({}, {
        fetchImpl: vi.fn().mockResolvedValue(new Response("not-json", { status: 200 })) as unknown as typeof fetch,
        timeoutMs: 1000,
      })
    ).rejects.toMatchObject({ code: "bad_response" });
  });
});
