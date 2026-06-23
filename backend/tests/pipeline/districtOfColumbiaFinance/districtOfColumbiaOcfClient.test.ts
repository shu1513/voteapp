import { afterEach, describe, expect, it, vi } from "vitest";

import {
  DISTRICT_OF_COLUMBIA_OCF_FILER_TYPES,
  DistrictOfColumbiaOcfClientError,
  buildDistrictOfColumbiaOcfDataDownloadUrl,
  buildDistrictOfColumbiaOcfExportFormBody,
  buildDistrictOfColumbiaOcfExportUrl,
  buildDistrictOfColumbiaOcfSubmitSearchUrl,
  districtOfColumbiaOcfContributionRecordFromRow,
  districtOfColumbiaOcfExpenditureRecordFromRow,
  downloadIndependentExpenditureContributions,
  downloadIndependentExpenditureExpenditures,
  downloadPrincipalCampaignContributions,
  fetchDistrictOfColumbiaOcfContributionRecords,
  parseDistrictOfColumbiaOcfCsvRows,
} from "../../../src/pipeline/districtOfColumbiaFinance/districtOfColumbiaOcfClient.js";

function responseWithText(body: string, init: ResponseInit = {}): Response {
  return new Response(body, {
    status: 200,
    statusText: "OK",
    ...init,
  });
}

function responseWithBytes(bytes: Uint8Array, init: ResponseInit = {}): Response {
  return new Response(bytes, {
    status: 200,
    statusText: "OK",
    ...init,
  });
}

function utf16LeCsv(csv: string): Uint8Array {
  return Buffer.concat([Buffer.from([0xff, 0xfe]), Buffer.from(csv, "utf16le")]);
}

describe("districtOfColumbiaOcfClient", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("builds D.C. OCF endpoint URLs", () => {
    expect(buildDistrictOfColumbiaOcfDataDownloadUrl()).toBe("https://efiling.ocf.dc.gov/DataDownload");
    expect(buildDistrictOfColumbiaOcfSubmitSearchUrl()).toBe(
      "https://efiling.ocf.dc.gov/DataDownload/SubmitSearch"
    );
    const exportUrl = new URL(buildDistrictOfColumbiaOcfExportUrl());
    expect(exportUrl.origin + exportUrl.pathname).toBe("https://efiling.ocf.dc.gov/DataDownload/Export");
    expect(exportUrl.searchParams.get("exportType")).toBe("CSV");
  });

  it("builds strict export form bodies", () => {
    expect(
      buildDistrictOfColumbiaOcfExportFormBody({
        filerTypeId: DISTRICT_OF_COLUMBIA_OCF_FILER_TYPES.principalCampaignCommittee,
        searchType: "Contributions",
        fromDate: "01/01/2022",
        toDate: "12/31/2022",
      })
    ).toBe("FilerTypeId=2&SearchType=Contributions&FromDate=01%2F01%2F2022&ToDate=12%2F31%2F2022");

    expect(() =>
      buildDistrictOfColumbiaOcfExportFormBody({
        filerTypeId: 999 as typeof DISTRICT_OF_COLUMBIA_OCF_FILER_TYPES.principalCampaignCommittee,
        searchType: "Contributions",
      })
    ).toThrow(DistrictOfColumbiaOcfClientError);
    expect(() =>
      buildDistrictOfColumbiaOcfExportFormBody({
        filerTypeId: DISTRICT_OF_COLUMBIA_OCF_FILER_TYPES.principalCampaignCommittee,
        searchType: "Contributions",
        fromDate: "2022-01-01",
      })
    ).toThrow("MM/DD/YYYY");
  });

  it("parses quoted CSV rows and normalizes headers", () => {
    expect(
      parseDistrictOfColumbiaOcfCsvRows(
        'Committee Name,Contributor Name,Occupation,Amount\r\n"Friends, Inc.","Doe, Jane",Attorney,"$1,250.50"\r\n'
      )
    ).toEqual([
      {
        committee_name: "Friends, Inc.",
        contributor_name: "Doe, Jane",
        occupation: "Attorney",
        amount: "$1,250.50",
      },
    ]);
  });

  it("skips D.C. OCF report preamble rows before the real CSV header", () => {
    expect(
      parseDistrictOfColumbiaOcfCsvRows(
        'Principal Campaign Committee  Contributions Report\r\nCommittee Name,Contributor First Name,Contributor Last Name,Occupation,Amount\r\n"Jane 2022","John","Smith","Attorney","$100.00"\r\n'
      )
    ).toEqual([
      {
        committee_name: "Jane 2022",
        contributor_first_name: "John",
        contributor_last_name: "Smith",
        occupation: "Attorney",
        amount: "$100.00",
      },
    ]);
  });

  it("detects expenditure-style headers after preamble rows", () => {
    expect(
      parseDistrictOfColumbiaOcfCsvRows(
        'Independent Expenditure Committee Expenditures Report\r\nFiler Name,Payee Name,Purpose Of Expenditure,Expenditure Amount\r\n"DCCSA IEC","Vendor","Independent Expenditures","$500.00"\r\n'
      )
    ).toEqual([
      {
        filer_name: "DCCSA IEC",
        payee_name: "Vendor",
        purpose_of_expenditure: "Independent Expenditures",
        expenditure_amount: "$500.00",
      },
    ]);
  });

  it("maps contribution rows conservatively", () => {
    const record = districtOfColumbiaOcfContributionRecordFromRow({
      committee_name: "Committee To Elect Jane Doe",
      contributor_name: "John Smith",
      contributor_type: "Individual",
      employer: "Acme",
      occupation: "Attorney",
      amount: "$250.00",
      receipt_date: "03/01/2022",
    });

    expect(record).toEqual({
      committeeName: "Committee To Elect Jane Doe",
      committeeKey: "COMMITTEE TO ELECT JANE DOE",
      contributorName: "John Smith",
      contributorType: "Individual",
      employer: "Acme",
      occupation: "Attorney",
      amount: 250,
      date: "03/01/2022",
    });
    expect(districtOfColumbiaOcfContributionRecordFromRow({ amount: "$0.00" })).toBeNull();
  });

  it("maps split person and organization contributor names from live D.C. OCF exports", () => {
    expect(
      districtOfColumbiaOcfContributionRecordFromRow({
        committee_name: "Committee To Elect Jane Doe",
        contributor_first_name: "John",
        contributor_middle_name: "Q",
        contributor_last_name: "Smith",
        contributor_type: "Individual",
        occupation: "Attorney",
        amount: "$250.00",
      })?.contributorName
    ).toBe("John Q Smith");
    expect(
      districtOfColumbiaOcfContributionRecordFromRow({
        committee_name: "D.C. IE Committee",
        contributor_organization_name: "District Workers Union PAC",
        contributor_type: "Political Committee",
        amount: "$25,000.00",
      })?.contributorName
    ).toBe("District Workers Union PAC");
  });

  it("maps expenditure rows with further explanation text", () => {
    const record = districtOfColumbiaOcfExpenditureRecordFromRow({
      filer_name: "D.C. IE Committee",
      payee_name: "Vendor",
      purpose_of_expenditure: "Independent Expenditure",
      further_explanation: "Supports Jane Doe",
      amount: "1000",
      payment_date: "10/01/2022",
    });

    expect(record).toEqual({
      committeeName: "D.C. IE Committee",
      committeeKey: "D.C. IE COMMITTEE",
      payeeName: "Vendor",
      purpose: "Independent Expenditure",
      furtherExplanation: "Supports Jane Doe",
      amount: 1000,
      date: "10/01/2022",
    });
  });

  it("fetches export CSV through the D.C. OCF search flow", async () => {
    const csv = "Committee Name,Contributor Name,Occupation,Amount\nJane 2022,John Smith,Attorney,$100.00\n";
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        responseWithText("<html></html>", {
          headers: { "set-cookie": "ASP.NET_SessionId=abc; path=/; HttpOnly" },
        })
      )
      .mockResolvedValueOnce(responseWithText("<div>results</div>"))
      .mockResolvedValueOnce(responseWithBytes(utf16LeCsv(csv))) as unknown as typeof fetch;

    await expect(
      fetchDistrictOfColumbiaOcfContributionRecords(
        {
          filerTypeId: DISTRICT_OF_COLUMBIA_OCF_FILER_TYPES.principalCampaignCommittee,
          fromDate: "01/01/2022",
          toDate: "12/31/2022",
        },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toEqual([
      {
        committeeName: "Jane 2022",
        committeeKey: "JANE 2022",
        contributorName: "John Smith",
        occupation: "Attorney",
        amount: 100,
      },
    ]);

    expect(fetchImpl).toHaveBeenCalledTimes(3);
    expect(String(vi.mocked(fetchImpl).mock.calls[0]?.[0])).toBe("https://efiling.ocf.dc.gov/DataDownload");
    expect(String(vi.mocked(fetchImpl).mock.calls[1]?.[0])).toBe(
      "https://efiling.ocf.dc.gov/DataDownload/SubmitSearch"
    );
    const submitInit = vi.mocked(fetchImpl).mock.calls[1]?.[1];
    expect(submitInit?.method).toBe("POST");
    expect((submitInit?.headers as Headers).get("content-length")).toBe(String(String(submitInit?.body).length));
    expect((submitInit?.headers as Headers).get("cookie")).toContain("ASP.NET_SessionId=abc");
    expect(String(vi.mocked(fetchImpl).mock.calls[2]?.[0])).toBe(
      "https://efiling.ocf.dc.gov/DataDownload/Export?exportType=CSV"
    );
  });

  it("uses fixed D.C. OCF filer types for named download helpers", async () => {
    const fetchImpl = vi
      .fn()
      .mockImplementation(async () => responseWithText("<html></html>")) as unknown as typeof fetch;

    await downloadPrincipalCampaignContributions(
      { fromDate: "01/01/2022", toDate: "12/31/2022" },
      { fetchImpl, timeoutMs: 1000 }
    );
    await downloadIndependentExpenditureContributions(
      { fromDate: "01/01/2022", toDate: "12/31/2022" },
      { fetchImpl, timeoutMs: 1000 }
    );
    await downloadIndependentExpenditureExpenditures(
      { fromDate: "01/01/2022", toDate: "12/31/2022" },
      { fetchImpl, timeoutMs: 1000 }
    );

    const postedBodies = vi
      .mocked(fetchImpl)
      .mock.calls.filter((call) => String(call[0]).endsWith("/SubmitSearch"))
      .map((call) => String(call[1]?.body));
    expect(postedBodies).toEqual([
      "FilerTypeId=2&SearchType=Contributions&FromDate=01%2F01%2F2022&ToDate=12%2F31%2F2022",
      "FilerTypeId=14&SearchType=Contributions&FromDate=01%2F01%2F2022&ToDate=12%2F31%2F2022",
      "FilerTypeId=14&SearchType=Expenditures&FromDate=01%2F01%2F2022&ToDate=12%2F31%2F2022",
    ]);
  });

  it("surfaces HTTP errors", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(responseWithText("nope", { status: 500, statusText: "Server Error" }));

    await expect(
      fetchDistrictOfColumbiaOcfContributionRecords(
        { filerTypeId: DISTRICT_OF_COLUMBIA_OCF_FILER_TYPES.principalCampaignCommittee },
        { fetchImpl: fetchImpl as unknown as typeof fetch, timeoutMs: 1000 }
      )
    ).rejects.toMatchObject({ code: "http_error", status: 500 });
  });
});
