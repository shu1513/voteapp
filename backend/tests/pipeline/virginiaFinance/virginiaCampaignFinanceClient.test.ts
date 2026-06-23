import { afterEach, describe, expect, it, vi } from "vitest";

import {
  VirginiaCampaignFinanceClientError,
  buildVirginiaCommitteeReportsUrl,
  buildVirginiaCommitteeSearchUrl,
  buildVirginiaReportPageUrl,
  buildVirginiaReportXmlUrl,
  fetchVirginiaCampaignFinanceReport,
  fetchVirginiaCommitteeReportList,
  parseVirginiaCampaignFinanceReportXml,
  parseVirginiaCommitteeReportList,
  parseVirginiaCommitteeSearchResults,
  searchVirginiaCandidateCommittees,
} from "../../../src/pipeline/virginiaFinance/virginiaCampaignFinanceClient.js";

function textResponse(body: string, init: ResponseInit = {}): Response {
  return new Response(body, {
    status: 200,
    statusText: "OK",
    ...init,
  });
}

const SEARCH_HTML = `
  <table>
    <tr>
      <td class="committeeName"><div> Spanberger for Governor </div></td>
      <td class="candidateName"><div> Abigail Spanberger </div></td>
      <td class="committeeType"><div> Candidate Campaign Committee </div></td>
      <td class="action">
        <a href="/Committee/Index/60e10dc7-c59e-4a79-afca-e688c1efed65">View Reports</a>
      </td>
    </tr>
    <tr>
      <td class="committeeName"><div class="alert"> No results matching Bob found! </div></td>
      <td class="candidateName"><div></div></td>
      <td class="committeeType"><div></div></td>
      <td class="action"></td>
    </tr>
  </table>
`;

const REPORT_LIST_HTML = `
  <h2 title="Reports for Spanberger for Governor (CC-23-02436)">Reports for Spanberger for Governor (CC-23-02436)</h2>
  <div class="sooDownload">
    <a href="https://cf.elections.virginia.gov/Printable/StatementOfOrganization/60e10dc7-c59e-4a79-afca-e688c1efed65/pdf/cfreports">
      Download Statement Of Organization
    </a>
  </div>
  <div class="pagetabs" id="ScheduledReports">
    <h3>Scheduled Reports</h3>
    <table>
      <tr class="report"><td><a href="/Report/Index/479054">View Report</a></td></tr>
      <tr class="report"><td><a href="/Report/Index/479044">View Report</a></td></tr>
      <tr class="amendedReport"><td><a href="/Report/Index/479044">View Report</a></td></tr>
    </table>
  </div>
  <div class="pagetabs" id="LargeContributionReports">
    <h3>Large Contribution Reports</h3>
    <table>
      <tr class="report"><td><a href="/Report/Index/468321">View Report</a></td></tr>
    </table>
  </div>
`;

const REPORT_XML = `<?xml version="1.0" encoding="utf-8"?>
<Report xmlns="http://www.sbe.virginia.gov">
  <ReportHeader>
    <CommitteeCode>CC-23-02436</CommitteeCode>
    <CommitteeName>Spanberger for Governor</CommitteeName>
    <ReportYear>2025</ReportYear>
    <ReportType>Scheduled</ReportType>
    <FilingDate>2026-01-15</FilingDate>
    <StartDate>2025-10-24</StartDate>
    <EndDate>2025-11-27</EndDate>
    <ElectionCycle>11/2025</ElectionCycle>
    <OfficeSought>Governor</OfficeSought>
  </ReportHeader>
  <ScheduleA>
    <LiA>
      <Contributor IsIndividual="true">
        <FirstName>Jane</FirstName>
        <MiddleName>Q</MiddleName>
        <LastName>Voter</LastName>
        <NameOfEmployer>Acme Law</NameOfEmployer>
        <OccupationOrTypeOfBusiness>Attorney</OccupationOrTypeOfBusiness>
      </Contributor>
      <TransactionDate>2025-11-01</TransactionDate>
      <Amount>250.00</Amount>
      <TotalToDate>500.00</TotalToDate>
    </LiA>
    <LiA>
      <Contributor IsIndividual="false">
        <LastName>Advancing Democracy and Mobilization PAC</LastName>
        <OccupationOrTypeOfBusiness>Federal PAC</OccupationOrTypeOfBusiness>
      </Contributor>
      <TransactionDate>2025-11-02</TransactionDate>
      <Amount>$5,000.00</Amount>
    </LiA>
    <LiA>
      <Contributor IsIndividual="true">
        <FirstName>Bad</FirstName>
        <LastName>Amount</LastName>
        <OccupationOrTypeOfBusiness>Retired</OccupationOrTypeOfBusiness>
      </Contributor>
      <Amount>not-money</Amount>
    </LiA>
  </ScheduleA>
</Report>`;

describe("virginiaCampaignFinanceClient", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("builds Virginia endpoint URLs", () => {
    const searchUrl = new URL(buildVirginiaCommitteeSearchUrl({ committeeName: " Spanberger " }));
    expect(searchUrl.origin + searchUrl.pathname).toBe("https://cfreports.elections.virginia.gov/");
    expect(searchUrl.searchParams.get("CommitteeName")).toBe("Spanberger");
    expect(searchUrl.searchParams.get("CommitteeType")).toBe("Candidate Campaign Committee");

    expect(buildVirginiaCommitteeReportsUrl("abc-123")).toBe(
      "https://cfreports.elections.virginia.gov/Committee/Index/abc-123"
    );
    expect(buildVirginiaReportPageUrl(479054)).toBe("https://cfreports.elections.virginia.gov/Report/Index/479054");
    expect(buildVirginiaReportXmlUrl(479054)).toBe(
      "https://cfreports.elections.virginia.gov/Report/ReportXML/479054"
    );
    expect(() => buildVirginiaReportXmlUrl(0)).toThrow(VirginiaCampaignFinanceClientError);
  });

  it("parses candidate committee search rows", () => {
    expect(
      parseVirginiaCommitteeSearchResults(
        SEARCH_HTML,
        "https://cfreports.elections.virginia.gov/?CommitteeName=Spanberger"
      )
    ).toEqual([
      {
        committeeId: "60e10dc7-c59e-4a79-afca-e688c1efed65",
        committeeName: "Spanberger for Governor",
        candidateName: "Abigail Spanberger",
        committeeType: "Candidate Campaign Committee",
        reportsUrl: "https://cfreports.elections.virginia.gov/Committee/Index/60e10dc7-c59e-4a79-afca-e688c1efed65",
        sourceUrl: "https://cfreports.elections.virginia.gov/?CommitteeName=Spanberger",
      },
    ]);
  });

  it("fetches and parses candidate committee search rows", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(textResponse(SEARCH_HTML)) as unknown as typeof fetch;

    await expect(
      searchVirginiaCandidateCommittees({ committeeName: "Spanberger" }, { fetchImpl, timeoutMs: 1000 })
    ).resolves.toHaveLength(1);

    expect(String(vi.mocked(fetchImpl).mock.calls[0]?.[0])).toContain("CommitteeName=Spanberger");
    expect(String(vi.mocked(fetchImpl).mock.calls[0]?.[0])).toContain(
      "CommitteeType=Candidate+Campaign+Committee"
    );
  });

  it("parses scheduled report ids separately from large contribution report ids", () => {
    expect(
      parseVirginiaCommitteeReportList(REPORT_LIST_HTML, {
        committeeId: "60e10dc7-c59e-4a79-afca-e688c1efed65",
      })
    ).toEqual({
      committeeId: "60e10dc7-c59e-4a79-afca-e688c1efed65",
      committeeName: "Spanberger for Governor",
      committeeCode: "CC-23-02436",
      statementOfOrganizationUrl:
        "https://cf.elections.virginia.gov/Printable/StatementOfOrganization/60e10dc7-c59e-4a79-afca-e688c1efed65/pdf/cfreports",
      scheduledReportIds: [479054, 479044],
      largeContributionReportIds: [468321],
      sourceUrl:
        "https://cfreports.elections.virginia.gov/Committee/Index/60e10dc7-c59e-4a79-afca-e688c1efed65",
    });
  });

  it("fetches and parses committee report lists", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(textResponse(REPORT_LIST_HTML)) as unknown as typeof fetch;

    await expect(
      fetchVirginiaCommitteeReportList("60e10dc7-c59e-4a79-afca-e688c1efed65", { fetchImpl, timeoutMs: 1000 })
    ).resolves.toMatchObject({
      committeeName: "Spanberger for Governor",
      scheduledReportIds: [479054, 479044],
    });

    expect(String(vi.mocked(fetchImpl).mock.calls[0]?.[0])).toBe(
      "https://cfreports.elections.virginia.gov/Committee/Index/60e10dc7-c59e-4a79-afca-e688c1efed65"
    );
  });

  it("parses report XML header and Schedule A contributions", () => {
    expect(parseVirginiaCampaignFinanceReportXml(REPORT_XML)).toEqual({
      header: {
        committeeCode: "CC-23-02436",
        committeeName: "Spanberger for Governor",
        reportYear: 2025,
        reportType: "Scheduled",
        filingDate: "2026-01-15",
        startDate: "2025-10-24",
        endDate: "2025-11-27",
        electionCycle: "11/2025",
        officeSought: "Governor",
      },
      scheduleA: [
        {
          contributorName: "Jane Q Voter",
          isIndividual: true,
          employer: "Acme Law",
          occupationOrTypeOfBusiness: "Attorney",
          transactionDate: "2025-11-01",
          amount: 250,
          totalToDate: 500,
        },
        {
          contributorName: "Advancing Democracy and Mobilization PAC",
          isIndividual: false,
          employer: null,
          occupationOrTypeOfBusiness: "Federal PAC",
          transactionDate: "2025-11-02",
          amount: 5000,
          totalToDate: null,
        },
      ],
    });
  });

  it("parses prefixed Virginia XML tags and nested contributor values", () => {
    const prefixedXml = `<?xml version="1.0" encoding="utf-8"?>
      <va:Report xmlns:va="http://www.sbe.virginia.gov">
        <va:ReportHeader>
          <va:CommitteeCode>CC-25-00001</va:CommitteeCode>
          <va:CommitteeName><![CDATA[Jane Doe for Attorney General]]></va:CommitteeName>
          <va:ReportYear>2025</va:ReportYear>
          <va:OfficeSought>Attorney General</va:OfficeSought>
        </va:ReportHeader>
        <va:ScheduleA>
          <va:LiA>
            <va:Contributor IsIndividual="yes">
              <va:FirstName>Jane</va:FirstName>
              <va:LastName>Voter</va:LastName>
              <va:NameOfEmployer>Acme &amp; Partners</va:NameOfEmployer>
              <va:OccupationOrTypeOfBusiness>Physician</va:OccupationOrTypeOfBusiness>
            </va:Contributor>
            <va:TransactionDate>2025-10-20</va:TransactionDate>
            <va:Amount>$1,250.50</va:Amount>
          </va:LiA>
        </va:ScheduleA>
      </va:Report>`;

    expect(parseVirginiaCampaignFinanceReportXml(prefixedXml)).toEqual({
      header: expect.objectContaining({
        committeeCode: "CC-25-00001",
        committeeName: "Jane Doe for Attorney General",
        reportYear: 2025,
        officeSought: "Attorney General",
      }),
      scheduleA: [
        {
          contributorName: "Jane Voter",
          isIndividual: true,
          employer: "Acme & Partners",
          occupationOrTypeOfBusiness: "Physician",
          transactionDate: "2025-10-20",
          amount: 1250.5,
          totalToDate: null,
        },
      ],
    });
  });

  it("fetches and parses report XML", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(textResponse(REPORT_XML)) as unknown as typeof fetch;

    const report = await fetchVirginiaCampaignFinanceReport(479054, { fetchImpl, timeoutMs: 1000 });
    expect(report.header.committeeCode).toBe("CC-23-02436");
    expect(report.scheduleA[0]?.contributorName).toBe("Jane Q Voter");

    expect(String(vi.mocked(fetchImpl).mock.calls[0]?.[0])).toBe(
      "https://cfreports.elections.virginia.gov/Report/ReportXML/479054"
    );
  });

  it("throws typed errors on HTTP failures", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      textResponse("not found", { status: 404, statusText: "Not Found" })
    ) as unknown as typeof fetch;

    await expect(
      searchVirginiaCandidateCommittees({ committeeName: "Missing" }, { fetchImpl, timeoutMs: 1000 })
    ).rejects.toThrow(VirginiaCampaignFinanceClientError);
  });
});
