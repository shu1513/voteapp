import { afterEach, describe, expect, it, vi } from "vitest";

import {
  TennesseeCampClientError,
  buildTennesseeCampCandidateSearchFormBody,
  buildTennesseeCampCandidateSearchUrl,
  buildTennesseeCampExpenditureSearchFormBody,
  buildTennesseeCampContributionSearchFormBody,
  buildTennesseeCampContributionSearchUrl,
  fetchTennesseeCampExpenditureRecords,
  fetchTennesseeCampContributionRecords,
  findTennesseeCampElectionYearSelection,
  parseTennesseeCampCandidateRecords,
  parseTennesseeCampCsvRows,
  tennesseeCampExpenditureRecordFromRow,
  searchTennesseeCampCandidates,
  tennesseeCampContributionRecordFromRow,
} from "../../../src/pipeline/tennesseeFinance/tennesseeCampClient.js";

function responseWithText(body: string, init: ResponseInit = {}): Response {
  return new Response(body, {
    status: 200,
    statusText: "OK",
    ...init,
  });
}

const landingHtml = `
  <select name="electionYearSelection">
    <option value="">- Select Election Year-</option>
    <option value="230">2024</option>
    <option value="225">2022</option>
    <option value="237">2023 (HOUSE 3)</option>
  </select>
`;

const candidateResultsHtml = `
  <table id="results">
    <thead><tr><th>Name</th><th>Office Sought</th><th>District</th><th>Election Year</th><th>Report List</th></tr></thead>
    <tbody>
      <tr>
        <td>LEE, BILL </td>
        <td>Governor</td>
        <td></td>
        <td>2022</td>
        <td><a href="/tncamp/public/replist.htm?id=6496&amp;owner=LEE, BILL ">Report List</a></td>
      </tr>
    </tbody>
  </table>
`;

describe("tennesseeCampClient", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("builds endpoint URLs and form bodies", () => {
    expect(buildTennesseeCampCandidateSearchUrl()).toBe("https://apps.tn.gov/tncamp/public/cpsearch.htm");
    expect(buildTennesseeCampContributionSearchUrl()).toBe("https://apps.tn.gov/tncamp/public/cesearch.htm");
    expect(
      buildTennesseeCampCandidateSearchFormBody({
        candidateName: "Lee",
        electionYear: 2022,
        officeSelection: "2",
        electionYearSelection: "225",
      })
    ).toBe(
      "searchType=candidate&name=Lee&officeSelection=2&electionYearSelection=225&nameField=true&officeField=true&districtField=true&electionYearField=true&_continue=Continue"
    );
    expect(
      buildTennesseeCampContributionSearchFormBody({
        recipientName: "Lee",
        electionYear: 2022,
        reportYear: 2022,
        electionYearSelection: "225",
      })
    ).toContain("contributorOccupationField=true");
    const pacContributionBody = new URLSearchParams(
      buildTennesseeCampContributionSearchFormBody({
        recipientName: "Right Tennessee",
        electionYear: 2022,
        reportYear: 2022,
        electionYearSelection: "225",
        recipientType: "pac",
      })
    );
    expect(pacContributionBody.get("toType")).toBe("pac");
    expect(pacContributionBody.get("fromPAC")).toBe("true");
    expect(pacContributionBody.get("fromOrganization")).toBe("true");
    expect(pacContributionBody.get("fromIndividual")).toBe("true");
    const expenditureBody = new URLSearchParams(
      buildTennesseeCampExpenditureSearchFormBody({
        electionYear: 2022,
        reportYear: 2022,
        electionYearSelection: "225",
        expenditureType: "independent",
      })
    );
    expect(expenditureBody.get("typeOf")).toBe("independent");
    expect(expenditureBody.get("candidateForField")).toBe("true");
  });

  it("parses exact non-special election year selection values", () => {
    expect(findTennesseeCampElectionYearSelection(landingHtml, 2022)).toBe("225");
    expect(findTennesseeCampElectionYearSelection(landingHtml, 2023)).toBeNull();
  });

  it("parses candidate result records with CAMP ids and owner names", () => {
    expect(parseTennesseeCampCandidateRecords(candidateResultsHtml)).toEqual([
      {
        campCandidateId: "6496",
        ownerName: "LEE, BILL",
        name: "LEE, BILL",
        officeSought: "Governor",
        district: null,
        electionYear: 2022,
        reportListUrl: "https://apps.tn.gov/tncamp/public/replist.htm?id=6496&owner=LEE,%20BILL",
        sourceUrl: "https://apps.tn.gov/tncamp/public/cpsearch.htm",
      },
    ]);
  });

  it("parses quoted CAMP CSV rows and contribution records", () => {
    const rows = parseTennesseeCampCsvRows(
      'Type,Adj,Amount,Date,Election Year,Report Name,Recipient Name,Contributor Name,Contributor Occupation,Contributor Employer\nMonetary,N,"$1,000.00",02/16/2022,2022,1st Quarter,"LEE, BILL","DOE, JANE",ATTORNEY,"ACME, INC."\n'
    );
    expect(rows).toEqual([
      {
        type: "Monetary",
        adj: "N",
        amount: "$1,000.00",
        date: "02/16/2022",
        election_year: "2022",
        report_name: "1st Quarter",
        recipient_name: "LEE, BILL",
        contributor_name: "DOE, JANE",
        contributor_occupation: "ATTORNEY",
        contributor_employer: "ACME, INC.",
      },
    ]);
    expect(tennesseeCampContributionRecordFromRow(rows[0]!)).toEqual({
      type: "Monetary",
      adjustment: "N",
      amount: 1000,
      date: "02/16/2022",
      electionYear: 2022,
      reportName: "1st Quarter",
      recipientName: "LEE, BILL",
      contributorName: "DOE, JANE",
      contributorOccupation: "ATTORNEY",
      contributorEmployer: "ACME, INC.",
    });
  });

  it("maps Tennessee CAMP expenditure rows", () => {
    const row = parseTennesseeCampCsvRows(
      'Type,Adj,Amount,Date,Election Year,Report Name,Candidate/PAC Name,Vendor Name,Purpose,Candidate For,S/O\nIndependent,N,$533.00,10/01/2022,2022,Pre-General,"RIGHT TENNESSEE",Vendor,Mail,"LEE, BILL",S\n'
    )[0]!;
    expect(tennesseeCampExpenditureRecordFromRow(row)).toEqual({
      type: "Independent",
      adjustment: "N",
      amount: 533,
      date: "10/01/2022",
      electionYear: 2022,
      reportName: "Pre-General",
      candidatePacName: "RIGHT TENNESSEE",
      vendorName: "Vendor",
      purpose: "Mail",
      candidateFor: "LEE, BILL",
      supportOpposeCode: "S",
    });
  });

  it("fetches candidate records through the sessioned CAMP search flow", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(responseWithText(landingHtml, { headers: { "set-cookie": "JSESSIONID=abc; Path=/" } }))
      .mockResolvedValueOnce(responseWithText(candidateResultsHtml)) as unknown as typeof fetch;

    await expect(
      searchTennesseeCampCandidates(
        { candidateName: "Lee", electionYear: 2022, officeSelection: "2" },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toHaveLength(1);

    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect((vi.mocked(fetchImpl).mock.calls[1]?.[1]?.headers as Headers).get("cookie")).toContain("JSESSIONID=abc");
    expect(String(vi.mocked(fetchImpl).mock.calls[1]?.[1]?.body)).toContain("electionYearSelection=225");
  });

  it("preserves multiple Set-Cookie values when the fallback header is combined", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        responseWithText(landingHtml, {
          headers: { "set-cookie": "JSESSIONID=abc; Path=/, AWSALB=xyz; Path=/" },
        })
      )
      .mockResolvedValueOnce(responseWithText(candidateResultsHtml)) as unknown as typeof fetch;

    await searchTennesseeCampCandidates(
      { candidateName: "Lee", electionYear: 2022, officeSelection: "2" },
      { fetchImpl, timeoutMs: 1000 }
    );

    expect((vi.mocked(fetchImpl).mock.calls[1]?.[1]?.headers as Headers).get("cookie")).toBe(
      "JSESSIONID=abc; AWSALB=xyz"
    );
  });

  it("finds CSV export links without depending on the page-specific displaytag id", async () => {
    const csv =
      "Type,Adj,Amount,Date,Election Year,Report Name,Recipient Name,Contributor Name,Contributor Occupation,Contributor Employer\n" +
      'Monetary,N,$250.00,02/18/2022,2022,1st Quarter,"LEE, BILL","DOE, JANE",ATTORNEY,ACME\n';
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(responseWithText(landingHtml))
      .mockResolvedValueOnce(
        responseWithText(
          '<div class="exportlinks"><a href="/tncamp/public/ceresults.htm?d-999999-e=1&amp;6578706f7274=1">CSV</a></div>'
        )
      )
      .mockResolvedValueOnce(responseWithText(csv)) as unknown as typeof fetch;

    await expect(
      fetchTennesseeCampContributionRecords(
        { recipientName: "Lee", electionYear: 2022, reportYear: 2022 },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toMatchObject({
      sourceUrl: "https://apps.tn.gov/tncamp/public/ceresults.htm?d-999999-e=1&6578706f7274=1",
    });
  });

  it("rejects off-host CSV export links before sending session cookies", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(responseWithText(landingHtml, { headers: { "set-cookie": "JSESSIONID=abc; Path=/" } }))
      .mockResolvedValueOnce(
        responseWithText('<div class="exportlinks"><a href="https://example.test/export?6578706f7274=1">CSV</a></div>')
      ) as unknown as typeof fetch;

    await expect(
      fetchTennesseeCampContributionRecords(
        { recipientName: "Lee", electionYear: 2022, reportYear: 2022 },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).rejects.toMatchObject({
      code: "bad_response",
      message: "Tennessee CAMP response linked outside the expected CAMP origin",
    });
    expect(fetchImpl).toHaveBeenCalledTimes(2);
  });

  it("fetches contribution records through result export links", async () => {
    const csv =
      "Type,Adj,Amount,Date,Election Year,Report Name,Recipient Name,Contributor Name,Contributor Occupation,Contributor Employer\n" +
      'Monetary,N,$250.00,02/18/2022,2022,1st Quarter,"LEE, BILL","DOE, JANE",ATTORNEY,ACME\n';
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(responseWithText(landingHtml, { headers: { "set-cookie": "JSESSIONID=abc; Path=/" } }))
      .mockResolvedValueOnce(
        responseWithText(
          '<div class="exportlinks"><a href="/tncamp/public/ceresults.htm?d-1341904-e=1&amp;6578706f7274=1">CSV</a></div>'
        )
      )
      .mockResolvedValueOnce(responseWithText(csv)) as unknown as typeof fetch;

    await expect(
      fetchTennesseeCampContributionRecords(
        { recipientName: "Lee", electionYear: 2022, reportYear: 2022 },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toEqual({
      sourceUrl: "https://apps.tn.gov/tncamp/public/ceresults.htm?d-1341904-e=1&6578706f7274=1",
      records: [
        {
          type: "Monetary",
          adjustment: "N",
          amount: 250,
          date: "02/18/2022",
          electionYear: 2022,
          reportName: "1st Quarter",
          recipientName: "LEE, BILL",
          contributorName: "DOE, JANE",
          contributorOccupation: "ATTORNEY",
          contributorEmployer: "ACME",
        },
      ],
    });
    expect(fetchImpl).toHaveBeenCalledTimes(3);
  });

  it("fetches expenditure records through result export links", async () => {
    const csv =
      "Type,Adj,Amount,Date,Election Year,Report Name,Candidate/PAC Name,Vendor Name,Purpose,Candidate For,S/O\n" +
      'Independent,N,$533.00,10/01/2022,2022,Pre-General,"RIGHT TENNESSEE",Vendor,Mail,"LEE, BILL",S\n';
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(responseWithText(landingHtml, { headers: { "set-cookie": "JSESSIONID=abc; Path=/" } }))
      .mockResolvedValueOnce(
        responseWithText(
          '<div class="exportlinks"><a href="/tncamp/public/ceresults.htm?d-1341904-e=1&amp;6578706f7274=1">CSV</a></div>'
        )
      )
      .mockResolvedValueOnce(responseWithText(csv)) as unknown as typeof fetch;

    await expect(
      fetchTennesseeCampExpenditureRecords(
        { electionYear: 2022, reportYear: 2022 },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toEqual({
      sourceUrl: "https://apps.tn.gov/tncamp/public/ceresults.htm?d-1341904-e=1&6578706f7274=1",
      records: [
        {
          type: "Independent",
          adjustment: "N",
          amount: 533,
          date: "10/01/2022",
          electionYear: 2022,
          reportName: "Pre-General",
          candidatePacName: "RIGHT TENNESSEE",
          vendorName: "Vendor",
          purpose: "Mail",
          candidateFor: "LEE, BILL",
          supportOpposeCode: "S",
        },
      ],
    });
  });

  it("surfaces unavailable election years", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(responseWithText(landingHtml)) as unknown as typeof fetch;
    await expect(
      searchTennesseeCampCandidates({ candidateName: "Lee", electionYear: 2032 }, { fetchImpl, timeoutMs: 1000 })
    ).rejects.toBeInstanceOf(TennesseeCampClientError);
  });
});
