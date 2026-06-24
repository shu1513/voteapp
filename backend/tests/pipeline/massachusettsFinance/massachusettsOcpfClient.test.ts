import { afterEach, describe, expect, it, vi } from "vitest";

import {
  MassachusettsOcpfClientError,
  buildMassachusettsOcpfApiUrl,
  buildMassachusettsOcpfCandidateFilerSearchUrl,
  buildMassachusettsOcpfContributionItemsUrl,
  buildMassachusettsOcpfIepacReportSummariesUrl,
  buildMassachusettsOcpfLegislativeReportsUrl,
  buildMassachusettsOcpfReportDetailUrl,
  buildMassachusettsOcpfStatewideReportsUrl,
  getMassachusettsOcpfContributionItems,
  getMassachusettsOcpfIepacReportSummaries,
  getMassachusettsOcpfLegislativeCandidateReports,
  getMassachusettsOcpfReportDetail,
  getMassachusettsOcpfStatewideCandidateReports,
  searchMassachusettsOcpfCandidateFilers,
} from "../../../src/pipeline/massachusettsFinance/massachusettsOcpfClient.js";

function jsonResponse(payload: unknown, init: ResponseInit = {}): Response {
  return new Response(JSON.stringify(payload), {
    status: 200,
    statusText: "OK",
    ...init,
  });
}

describe("massachusettsOcpfClient", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("builds Massachusetts OCPF API URLs", () => {
    const url = new URL(buildMassachusettsOcpfApiUrl("/search/items", { cpfId: "15710", pageSize: 1000 }));
    expect(url.origin + url.pathname).toBe("https://api.ocpf.us/search/items");
    expect(url.searchParams.get("cpfId")).toBe("15710");
    expect(url.searchParams.get("pageSize")).toBe("1000");
    expect(() => buildMassachusettsOcpfApiUrl("search/items")).toThrow(MassachusettsOcpfClientError);
  });

  it("builds candidate filer search URLs", () => {
    const url = new URL(buildMassachusettsOcpfCandidateFilerSearchUrl({ searchPhrase: " Maura   Healey " }));
    expect(url.origin + url.pathname).toBe("https://api.ocpf.us/filers/listings/A");
    expect(url.searchParams.get("searchPhrase")).toBe("Maura Healey");
  });

  it("parses candidate filer search rows", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse([
        {
          cpfId: 15710,
          filerName: "Maura T. Healey",
          filerNameReverse: "Healey, Maura T.",
          committeeName: "Healey Committee",
          officeSought: "Statewide, Governor",
          accountTypeCode: "CC",
          accountTypeDescription: "Candidate Committee",
          isCandidate: "true",
          isActive: "yes",
        },
        { filerName: "missing cpf" },
      ])
    ) as unknown as typeof fetch;

    await expect(
      searchMassachusettsOcpfCandidateFilers({ searchPhrase: "Maura Healey" }, { fetchImpl, timeoutMs: 1000 })
    ).resolves.toEqual([
      {
        cpfId: "15710",
        filerName: "Maura T. Healey",
        filerNameReverse: "Healey, Maura T.",
        committeeName: "Healey Committee",
        officeSought: "Statewide, Governor",
        accountTypeCode: "CC",
        accountTypeDescription: "Candidate Committee",
        isCandidate: true,
        isActive: true,
      },
    ]);

    expect(String(vi.mocked(fetchImpl).mock.calls[0]?.[0])).toContain("/filers/listings/A");
  });

  it("builds and parses statewide candidate report rows", async () => {
    const url = new URL(buildMassachusettsOcpfStatewideReportsUrl({ electionYear: 2022, onBallot: true, limit: 500 }));
    expect(url.origin + url.pathname).toBe("https://api.ocpf.us/reports/statewide/ytd/2022");
    expect(url.searchParams.get("onBallot")).toBe("true");
    expect(url.searchParams.get("PageSize")).toBe("500");

    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse([
        {
          cpfId: "15710",
          filerName: "Maura T. Healey",
          officeSought: "Statewide, Governor",
          receiptsYtdNumeric: "$6,123.45",
          expendituresYtdNumeric: "1000",
          bankReportId: "858575",
          isWinner: true,
          ocpfUsReportLink: "https://www.ocpf.us/Reports/SearchItems?cpfId=15710",
        },
        { cpfId: "bad" },
      ])
    ) as unknown as typeof fetch;

    await expect(
      getMassachusettsOcpfStatewideCandidateReports({ electionYear: 2022 }, { fetchImpl, timeoutMs: 1000 })
    ).resolves.toEqual([
      {
        cpfId: "15710",
        filerName: "Maura T. Healey",
        officeSought: "Statewide, Governor",
        receiptsYtd: 6123.45,
        expendituresYtd: 1000,
        bankReportId: 858575,
        isWinner: true,
        sourceUrl: "https://www.ocpf.us/Reports/SearchItems?cpfId=15710",
      },
    ]);
  });

  it("builds and parses legislative candidate report rows", async () => {
    expect(buildMassachusettsOcpfLegislativeReportsUrl({ electionYear: 2022 })).toBe(
      "https://api.ocpf.us/reports/legislative/2022"
    );

    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse([
        {
          cpfId: "12345",
          filerName: "Example Candidate",
          officeSought: "Senate, 2nd Middlesex",
          receiptsYtdNumeric: "2500",
          expendituresYtdNumeric: "$1,000.50",
          bankReportId: "777",
          isWinner: "false",
        },
        { cpfId: "missing name" },
      ])
    ) as unknown as typeof fetch;

    await expect(
      getMassachusettsOcpfLegislativeCandidateReports({ electionYear: 2022 }, { fetchImpl, timeoutMs: 1000 })
    ).resolves.toEqual([
      {
        cpfId: "12345",
        filerName: "Example Candidate",
        officeSought: "Senate, 2nd Middlesex",
        receiptsYtd: 2500,
        expendituresYtd: 1000.5,
        bankReportId: 777,
        isWinner: false,
        sourceUrl: undefined,
      },
    ]);
  });

  it("builds and parses direct contribution item rows", async () => {
    const url = new URL(buildMassachusettsOcpfContributionItemsUrl({ candidateCpfId: "15710", electionYear: 2022 }));
    expect(url.origin + url.pathname).toBe("https://api.ocpf.us/search/items");
    expect(url.searchParams.get("cpfId")).toBe("15710");
    expect(url.searchParams.get("startDate")).toBe("1/1/2022");
    expect(url.searchParams.get("endDate")).toBe("12/31/2022");
    expect(url.searchParams.get("pageSize")).toBe("100000");
    expect(url.searchParams.get("sortField")).toBe("date");

    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse([
        {
          id: "abc",
          reportId: "858575",
          filerCpfId: 15710,
          filerFullNameReverse: "Healey, Maura T.",
          fullNameReverse: "Donor, Jane",
          contributorType: "Individual",
          occupation: "Attorney",
          employer: "Law Firm",
          recordTypeDescription: "Contribution",
          amountValue: "($250.50)",
          date: "11/01/2022",
          sourceLink: '<a href="https://www.ocpf.us/item/abc">View</a>',
        },
        { amountValue: "bad" },
      ])
    ) as unknown as typeof fetch;

    await expect(
      getMassachusettsOcpfContributionItems(
        { candidateCpfId: "15710", electionYear: 2022, limit: 10 },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toEqual([
      {
        itemId: "abc",
        reportId: 858575,
        cpfId: "15710",
        filerName: "Healey, Maura T.",
        contributorName: "Donor, Jane",
        contributorType: "Individual",
        occupation: "Attorney",
        employer: "Law Firm",
        recordTypeDescription: "Contribution",
        amount: -250.5,
        date: "11/01/2022",
        sourceUrl: "https://www.ocpf.us/item/abc",
      },
    ]);
  });

  it("refuses to return partial contribution items when the response hits the row cap", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(jsonResponse([{ amountValue: "1" }, { amountValue: "2" }])) as unknown as typeof fetch;

    await expect(
      getMassachusettsOcpfContributionItems(
        { candidateCpfId: "15710", electionYear: 2022, limit: 2 },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).rejects.toMatchObject({ code: "bad_response" });
  });

  it("builds and parses IE PAC report summaries", async () => {
    expect(buildMassachusettsOcpfIepacReportSummariesUrl(2022)).toBe(
      "https://api.ocpf.us/miscreports/iepacs/reports/2022"
    );

    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse([
        {
          reportId: "858575",
          cpfId: "16116",
          committeeName: "Local 103 IBEW Independent Expenditure PAC",
          reportYear: "2022",
          reportType: "7 Day Report",
          reportingPeriod: "10/20/2022-10/27/2022",
          candidateListing: "Maura T. Healey",
          candidateSpendingBreakdown: "Maura T. Healey (Supported) $32,420.00<br>",
          receiptTotalNumeric: "32420",
          expenditureTotalNumeric: "$32,420.00",
          ocpfUsReportLink: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
        },
        { cpfId: "missing report id" },
      ])
    ) as unknown as typeof fetch;

    await expect(getMassachusettsOcpfIepacReportSummaries(2022, { fetchImpl, timeoutMs: 1000 })).resolves.toEqual([
      {
        reportId: 858575,
        cpfId: "16116",
        committeeName: "Local 103 IBEW Independent Expenditure PAC",
        reportYear: 2022,
        reportType: "7 Day Report",
        reportingPeriod: "10/20/2022-10/27/2022",
        candidateListing: "Maura T. Healey",
        candidateSpendingBreakdown: "Maura T. Healey (Supported) $32,420.00<br>",
        receiptsTotal: 32420,
        expendituresTotal: 32420,
        sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
      },
    ]);
  });

  it("builds and parses per-report details with receipts and expenditures", async () => {
    expect(buildMassachusettsOcpfReportDetailUrl(858575)).toBe("https://api.ocpf.us/report/858575");

    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse({
        reportId: 858575,
        cpfId: "16116",
        committeeName: "Local 103 IBEW Independent Expenditure PAC",
        reportYear: 2022,
        candidateSpendingBreakdown: "Maura T. Healey (Supported) $32,420.00<br>",
        receipts: [
          {
            contributorName: "IBEW 103",
            contributorType: "Union/Association",
            recordTypeDescription: "Contribution",
            amountValue: "32420",
            date: "10/25/2022",
          },
          { amountValue: "bad" },
        ],
        expenditures: [
          {
            affectedCandidateName: "Maura T. Healey",
            relatedCpfId: "15710",
            isSupported: false,
            recordTypeDescription: "Independent Expenditure",
            ieInfo: "in opposition to Maura T. Healey (15710)",
            amountValue: "70000",
            date: "10/25/2022",
          },
        ],
      })
    ) as unknown as typeof fetch;

    await expect(getMassachusettsOcpfReportDetail({ reportId: 858575 }, { fetchImpl, timeoutMs: 1000 })).resolves.toEqual({
      reportId: 858575,
      cpfId: "16116",
      committeeName: "Local 103 IBEW Independent Expenditure PAC",
      reportYear: 2022,
      candidateSpendingBreakdown: "Maura T. Healey (Supported) $32,420.00<br>",
      receipts: [
        {
          contributorName: "IBEW 103",
          contributorType: "Union/Association",
          recordTypeDescription: "Contribution",
          amount: 32420,
          date: "10/25/2022",
        },
      ],
      expenditures: [
        {
          affectedCandidateName: "Maura T. Healey",
          relatedCpfId: "15710",
          isSupported: false,
          recordTypeDescription: "Independent Expenditure",
          ieInfo: "in opposition to Maura T. Healey (15710)",
          amount: 70000,
          date: "10/25/2022",
        },
      ],
    });
  });

  it("wraps HTTP and malformed JSON responses", async () => {
    const httpFetch = vi.fn().mockResolvedValue(new Response("no", { status: 500, statusText: "Server Error" })) as unknown as typeof fetch;
    await expect(
      searchMassachusettsOcpfCandidateFilers({ searchPhrase: "Maura Healey" }, { fetchImpl: httpFetch, timeoutMs: 1000 })
    ).rejects.toMatchObject({ code: "http_error", status: 500 });

    const malformedFetch = vi.fn().mockResolvedValue(new Response("not json", { status: 200 })) as unknown as typeof fetch;
    await expect(
      searchMassachusettsOcpfCandidateFilers({ searchPhrase: "Maura Healey" }, { fetchImpl: malformedFetch, timeoutMs: 1000 })
    ).rejects.toMatchObject({ code: "bad_response" });
  });
});
