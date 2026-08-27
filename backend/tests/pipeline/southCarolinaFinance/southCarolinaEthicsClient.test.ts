import { describe, expect, it, vi } from "vitest";

import {
  SOUTH_CAROLINA_ETHICS_API_BASE_URL,
  SouthCarolinaEthicsClientError,
  getSouthCarolinaCandidateReports,
  getSouthCarolinaReportDetails,
  searchSouthCarolinaContributions,
  searchSouthCarolinaFilersByName,
  southCarolinaReportDetailsUrl,
} from "../../../src/pipeline/southCarolinaFinance/southCarolinaEthicsClient.js";

function jsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "Content-Type": "application/json; charset=utf-8" },
  });
}

function htmlResponse(): Response {
  return new Response("<!DOCTYPE html><html><head><title>SC Ethics Filing</title></head></html>", {
    status: 200,
    headers: { "Content-Type": "text/html; charset=utf-8" },
  });
}

function mockFetch(response: Response | (() => Response)) {
  return vi
    .fn()
    .mockImplementation(async () => (typeof response === "function" ? response() : response)) as unknown as typeof fetch;
}

const reportRow = {
  reportId: 426376,
  report: "Quarter 2, 2026 Report",
  reportType: "Quarterly",
  electionDate: "6/9/2026",
  filingPeriod: "5/21/2026 - 6/30/2026",
  contributions: 4859328.27,
  expenses: 3998672.75,
  balance: 860655.52,
  dateSubmitted: "2026-07-10T14:30:34.567",
  lastAmendment: null,
  year: 2026,
  reportWithDates: {
    deadline: "2026-07-10T00:00:00",
    filingStartDate: "2026-05-21T04:00:00",
    filingEndDate: "2026-06-30T00:00:00",
    filingYear: 2026,
    quarter: 2,
    isInitial: false,
    isPreElection: false,
    isGeneral: false,
    isPrimary: true,
    isFinal: false,
    campaignId: 77574,
    candidateFilerId: 54344,
  },
};

const reportDetails = {
  filerName: "Wilson, Michael A",
  electionDate: "2026-06-09T04:00:00",
  electionType: "Primary",
  reportType: "Quarter 2, 2026 Report",
  filingPeriod: "5/21/2026 - 6/30/2026",
  isAmendment: false,
  isSystemCreated: false,
  accountCredits: [],
  loans: {},
  overview: {
    reportSequenceNumber: 10,
    submittedDate: "July 10, 2026",
    filingFeeAmount: 0,
    income: [
      { type: "Cash Contributions", filingPeriod: 1614778.24, electionCycleTotal: 4817978.1 },
      { type: "In-kind Contributions", filingPeriod: 13500.0, electionCycleTotal: 41350.17 },
      { type: "Loans", filingPeriod: 0.0, electionCycleTotal: 0.0 },
      { type: "Total", filingPeriod: 1628278.24, electionCycleTotal: 4859328.27 },
    ],
    expenditures: [
      { type: "Expenditures", filingPeriod: 1256018.24, electionCycleTotal: 3932004.53 },
      { type: "Returned Contributions", filingPeriod: 10500.0, electionCycleTotal: 25318.05 },
      { type: "Total", filingPeriod: 1280018.24, electionCycleTotal: 3998672.75 },
    ],
    totals: [{ totalType: "Campaign Funds", startingBalance: 512395.52, endingBalance: 860655.52 }],
  },
  contributions: { contributionsTotal: 1628278.24, details: [] },
  expenditures: { expendituresTotal: 1280018.24, details: [] },
  versions: [{ id: 426376, name: "Original Report", dateSubmitted: "0001-01-01T00:00:00", status: false }],
};

const contributionRow = {
  contributionId: 1968028,
  officeRunId: 77574,
  candidateId: 54344,
  date: "2025-06-23T04:00:00",
  amount: 500.0,
  candidateName: "Michael Wilson ",
  officeName: "4",
  electionDate: "2026-06-09T05:00:00",
  contributorName: "Test Person",
  contributorOccupation: "Attorney",
  group: "No",
  contributorAddress: "1 Test St Columbia, SC 29201",
  description: "",
};

describe("South Carolina ethics client", () => {
  it("posts the filer search text as a bare JSON string and parses rows", async () => {
    const fetchImpl = mockFetch(
      jsonResponse({
        result: [
          {
            candidate: "Wilson, Michael A.",
            candidateFilerId: 54344,
            officeName: "Attorney General",
            lastCampaignDisclosureReport: "07/10/2026",
          },
          { candidate: "Wilson, Amy F.", candidateFilerId: 0, officeName: "", lastCampaignDisclosureReport: "" },
        ],
        count: 2,
      })
    );

    const rows = await searchSouthCarolinaFilersByName("Wilson", { fetchImpl });

    expect(fetchImpl).toHaveBeenCalledWith(
      `${SOUTH_CAROLINA_ETHICS_API_BASE_URL}/Ethics/Get/Public/Search/By/Filer/Name/`,
      expect.objectContaining({ method: "POST", body: JSON.stringify("Wilson") })
    );
    expect(rows).toEqual([
      {
        candidate: "Wilson, Michael A.",
        candidateFilerId: 54344,
        officeName: "Attorney General",
        lastCampaignDisclosureReport: "07/10/2026",
      },
      { candidate: "Wilson, Amy F.", candidateFilerId: 0, officeName: null, lastCampaignDisclosureReport: null },
    ]);
  });

  it("rejects filer searches shorter than the portal minimum without fetching", async () => {
    const fetchImpl = mockFetch(jsonResponse({ result: [] }));
    await expect(searchSouthCarolinaFilersByName("  Wi ", { fetchImpl })).rejects.toMatchObject({
      code: "invalid_request",
    });
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it("parses candidate report rows including run identity from reportWithDates", async () => {
    const fetchImpl = mockFetch(jsonResponse({ results: [reportRow] }));

    const rows = await getSouthCarolinaCandidateReports(54344, { fetchImpl });

    expect(fetchImpl).toHaveBeenCalledWith(
      `${SOUTH_CAROLINA_ETHICS_API_BASE_URL}/Ethics/Get/Public/Candidate/Reports`,
      expect.objectContaining({ method: "POST", body: JSON.stringify({ candidateFilerId: 54344 }) })
    );
    expect(rows).toEqual([
      {
        reportId: 426376,
        reportName: "Quarter 2, 2026 Report",
        reportType: "Quarterly",
        electionDate: "6/9/2026",
        contributions: 4859328.27,
        expenses: 3998672.75,
        balance: 860655.52,
        dateSubmitted: "2026-07-10T14:30:34.567",
        campaignId: 77574,
        candidateFilerId: 54344,
        filingStartDate: "2026-05-21T04:00:00",
        filingEndDate: "2026-06-30T00:00:00",
        isPrimary: true,
        isGeneral: false,
        isPreElection: false,
        isFinal: false,
      },
    ]);
  });

  it("fails closed when a report row belongs to a different filer", async () => {
    const fetchImpl = mockFetch(jsonResponse({ results: [reportRow] }));
    await expect(getSouthCarolinaCandidateReports(99999, { fetchImpl })).rejects.toMatchObject({
      code: "bad_response",
    });
  });

  it("parses report details summary lines, totals, and versions", async () => {
    const fetchImpl = mockFetch(jsonResponse(reportDetails));

    const details = await getSouthCarolinaReportDetails(426376, { fetchImpl });

    expect(fetchImpl).toHaveBeenCalledWith(
      southCarolinaReportDetailsUrl(426376),
      expect.objectContaining({ method: "GET" })
    );
    expect(details.filerName).toBe("Wilson, Michael A");
    expect(details.electionType).toBe("Primary");
    expect(details.isAmendment).toBe(false);
    expect(details.reportSequenceNumber).toBe(10);
    expect(details.contributionsTotal).toBe(1628278.24);
    expect(details.expendituresTotal).toBe(1280018.24);
    expect(details.income).toContainEqual({
      type: "Total",
      filingPeriod: 1628278.24,
      electionCycleTotal: 4859328.27,
    });
    expect(details.expenditures).toContainEqual({
      type: "Returned Contributions",
      filingPeriod: 10500.0,
      electionCycleTotal: 25318.05,
    });
    expect(details.totals).toEqual([
      { totalType: "Campaign Funds", startingBalance: 512395.52, endingBalance: 860655.52 },
    ]);
    expect(details.versions).toEqual([{ id: 426376, name: "Original Report" }]);
  });

  it("rejects the SPA HTML fallback served for unknown routes", async () => {
    const fetchImpl = mockFetch(htmlResponse());
    await expect(getSouthCarolinaReportDetails(426376, { fetchImpl })).rejects.toMatchObject({
      code: "bad_response",
      message: expect.stringContaining("content type"),
    });
  });

  it("sends the contribution search with candidate text plus numeric year and parses rows", async () => {
    const fetchImpl = mockFetch(jsonResponse([contributionRow]));

    const rows = await searchSouthCarolinaContributions(
      { candidate: " Wilson ", contributionYear: 2026 },
      { fetchImpl }
    );

    expect(fetchImpl).toHaveBeenCalledWith(
      `${SOUTH_CAROLINA_ETHICS_API_BASE_URL}/Candidate/Contribution/Search/`,
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({ candidate: "Wilson", contributionYear: 2026 }),
      })
    );
    expect(rows).toEqual([
      {
        contributionId: 1968028,
        candidateId: 54344,
        officeRunId: 77574,
        candidateName: "Michael Wilson",
        officeName: "4",
        electionDate: "2026-06-09T05:00:00",
        date: "2025-06-23T04:00:00",
        amount: 500.0,
        contributorName: "Test Person",
        contributorOccupation: "Attorney",
        group: "No",
        description: null,
      },
    ]);
  });

  it("treats blank occupations as null and accepts group contributor rows", async () => {
    const fetchImpl = mockFetch(
      jsonResponse([
        { ...contributionRow, contributionId: 2, contributorOccupation: " ", group: "Yes" },
      ])
    );
    const rows = await searchSouthCarolinaContributions(
      { candidate: "Wilson", contributionYear: 2026 },
      { fetchImpl }
    );
    expect(rows[0]).toMatchObject({ contributionId: 2, contributorOccupation: null, group: "Yes" });
  });

  it("refuses to search contributions without candidate text or with an invalid year", async () => {
    const fetchImpl = mockFetch(jsonResponse([]));
    await expect(
      searchSouthCarolinaContributions({ candidate: "  ", contributionYear: 2026 }, { fetchImpl })
    ).rejects.toMatchObject({ code: "invalid_request" });
    await expect(
      searchSouthCarolinaContributions({ candidate: "Wilson", contributionYear: 26 }, { fetchImpl })
    ).rejects.toMatchObject({ code: "invalid_request" });
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it("rejects rows with an unrecognized group flag", async () => {
    const fetchImpl = mockFetch(jsonResponse([{ ...contributionRow, group: "Maybe" }]));
    await expect(
      searchSouthCarolinaContributions({ candidate: "Wilson", contributionYear: 2026 }, { fetchImpl })
    ).rejects.toMatchObject({ code: "bad_response" });
  });

  it("wraps HTTP errors with their status", async () => {
    const fetchImpl = mockFetch(
      new Response("{}", { status: 500, headers: { "Content-Type": "application/json" } })
    );
    await expect(getSouthCarolinaCandidateReports(54344, { fetchImpl })).rejects.toMatchObject({
      code: "http_error",
      status: 500,
    });
  });

  it("wraps network failures", async () => {
    const fetchImpl = vi.fn().mockRejectedValue(new Error("socket hang up")) as unknown as typeof fetch;
    await expect(searchSouthCarolinaFilersByName("Wilson", { fetchImpl })).rejects.toMatchObject({
      code: "network_error",
    });
  });

  it("exposes a typed error class", () => {
    const error = new SouthCarolinaEthicsClientError("bad_response", "boom", 502);
    expect(error.name).toBe("SouthCarolinaEthicsClientError");
    expect(error.code).toBe("bad_response");
    expect(error.status).toBe(502);
  });
});
