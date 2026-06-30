import { describe, expect, it } from "vitest";

import {
  isOregonOrestarBlockedPage,
  parseOregonOrestarOutsideAssociations,
  parseOregonOrestarSearchForm,
  parseOregonOrestarTransactionDetail,
  parseOregonOrestarTransactionSearchResults,
} from "../../../src/pipeline/oregonFinance/oregonOrestarParser.js";

describe("oregonOrestarParser", () => {
  it("parses the public transaction search form action and CSRF token", () => {
    const html = `
      <html>
        <body>
          <form name="cneSearchForm" method="post" action="/orestar/cneSearch.do;JSESSIONID_ORESTAR=abc123">
            <input type="hidden" name="OWASP_CSRFTOKEN" value="csrf-token-1">
          </form>
        </body>
      </html>
    `;

    expect(parseOregonOrestarSearchForm(html)).toEqual({
      actionUrl: "https://secure.sos.state.or.us/orestar/cneSearch.do;JSESSIONID_ORESTAR=abc123",
      csrfToken: "csrf-token-1",
      cookieHeader: null,
      sessionId: "abc123",
    });
  });

  it("parses live-style transaction search forms whose CSRF token is injected by JavaScript", () => {
    const html = `
      <html>
        <body>
          <form name="cneSearchForm" method="post" action="/orestar/gotoPublicTransactionSearchResults.do;JSESSIONID_ORESTAR=abc123">
            <input type="hidden" name="cneSearchButtonName" value="">
          </form>
        </body>
      </html>
    `;

    expect(parseOregonOrestarSearchForm(html)).toEqual({
      actionUrl: "https://secure.sos.state.or.us/orestar/gotoPublicTransactionSearchResults.do;JSESSIONID_ORESTAR=abc123",
      csrfToken: null,
      cookieHeader: null,
      sessionId: "abc123",
    });
  });

  it("rejects transaction search form actions outside the ORESTAR origin", () => {
    const html = `
      <html>
        <body>
          <form name="cneSearchForm" method="post" action="https://example.test/orestar/cneSearch.do">
            <input type="hidden" name="OWASP_CSRFTOKEN" value="csrf-token-1">
          </form>
        </body>
      </html>
    `;

    expect(() => parseOregonOrestarSearchForm(html)).toThrow(
      "ORESTAR transaction search form action URL is not allowed"
    );
  });

  it("parses transaction search result rows, limits, and export links", () => {
    const html = `
      <html>
        <body>
          <div>Search Criteria : Filer/Committee Name: Friends of Tina Kotek Results : 13,333 records found; maximum 5,000 records are displayed</div>
          <a href="XcelCNESearch;JSESSIONID_ORESTAR=abc?OWASP_CSRFTOKEN=csrf">Export</a>
          <a href="/orestar/cneSearch.do?page=2">Next</a>
          <table>
            <tr>
              <th>Tran ID</th><th>Date</th><th>Status</th><th>Filer/Committee</th><th>Contributor/Payee</th><th>Sub Type</th><th>Amount</th>
            </tr>
            <tr>
              <td><a href="/orestar/gotoPublicTransactionDetail.do?tranRsn=4458653">4458653</a></td>
              <td>10/12/2022</td>
              <td>Original</td>
              <td><a href="/orestar/sooDetail.do?cneCommitteeId=4792">Friends of Tina Kotek</a></td>
              <td>John Ramsbacher **</td>
              <td>Cash Contribution</td>
              <td>$10,000.00</td>
            </tr>
            <tr>
              <td><a href="/orestar/gotoPublicTransactionDetail.do?tranRsn=4459000">4459000</a></td>
              <td>10/13/2022</td>
              <td>Original</td>
              <td><a href="/orestar/sooDetail.do?cneCommitteeId=22333">2022 Our Oregon Voter Guide</a></td>
              <td>Mail Vendor *</td>
              <td>In-Kind Expenditure</td>
              <td>($1,234.56)</td>
            </tr>
          </table>
          <input type="submit" value="Next">
        </body>
      </html>
    `;

    expect(parseOregonOrestarTransactionSearchResults(html)).toEqual({
      criteriaText: "Filer/Committee Name: Friends of Tina Kotek",
      resultCount: 13_333,
      displayedResultLimit: 5_000,
      visibleRowCount: 2,
      hasNextPage: true,
      nextPageUrl: "https://secure.sos.state.or.us/orestar/cneSearch.do?page=2",
      exportUrl: "https://secure.sos.state.or.us/orestar/XcelCNESearch;JSESSIONID_ORESTAR=abc?OWASP_CSRFTOKEN=csrf",
      rows: [
        {
          transactionId: "4458653",
          transactionDate: "10/12/2022",
          status: "Original",
          filerCommitteeName: "Friends of Tina Kotek",
          filerCommitteeId: "4792",
          contributorPayeeName: "John Ramsbacher",
          contributorPayeeOutOfState: true,
          subType: "Cash Contribution",
          amount: 10_000,
          isInKindExpenditure: false,
          detailUrl: "https://secure.sos.state.or.us/orestar/gotoPublicTransactionDetail.do?tranRsn=4458653",
          committeeUrl: "https://secure.sos.state.or.us/orestar/sooDetail.do?cneCommitteeId=4792",
        },
        {
          transactionId: "4459000",
          transactionDate: "10/13/2022",
          status: "Original",
          filerCommitteeName: "2022 Our Oregon Voter Guide",
          filerCommitteeId: "22333",
          contributorPayeeName: "Mail Vendor",
          contributorPayeeOutOfState: false,
          subType: "In-Kind Expenditure",
          amount: -1234.56,
          isInKindExpenditure: true,
          detailUrl: "https://secure.sos.state.or.us/orestar/gotoPublicTransactionDetail.do?tranRsn=4459000",
          committeeUrl: "https://secure.sos.state.or.us/orestar/sooDetail.do?cneCommitteeId=22333",
        },
      ],
    });
  });

  it("drops transaction search result links outside the ORESTAR origin", () => {
    const html = `
      <html>
        <body>
          <div>Results : 1 record found</div>
          <a href="https://example.test/orestar/cneSearch.do?page=2">Next</a>
          <table>
            <tr>
              <th>Tran ID</th><th>Date</th><th>Status</th><th>Filer/Committee</th><th>Contributor/Payee</th><th>Sub Type</th><th>Amount</th>
            </tr>
            <tr>
              <td><a href="https://example.test/orestar/gotoPublicTransactionDetail.do?tranRsn=4458653">4458653</a></td>
              <td>10/12/2022</td>
              <td>Original</td>
              <td><a href="https://example.test/orestar/sooDetail.do?cneCommitteeId=4792">Friends of Tina Kotek</a></td>
              <td>John Ramsbacher</td>
              <td>Cash Contribution</td>
              <td>$10,000.00</td>
            </tr>
          </table>
          <input type="submit" value="Next">
        </body>
      </html>
    `;

    const parsed = parseOregonOrestarTransactionSearchResults(html);

    expect(parsed.nextPageUrl).toBeNull();
    expect(parsed.rows[0]).toMatchObject({
      transactionId: "4458653",
      detailUrl: null,
      committeeUrl: null,
    });
  });

  it("parses contribution transaction detail fields", () => {
    const html = `
      <table>
        <tr><td>Transaction Detail</td><td>Friends of Tina Kotek (4792)</td></tr>
        <tr><td>Transaction ID</td><td>:</td><td>4458653</td></tr>
        <tr><td>Transaction Date</td><td>:</td><td>10/12/2022</td></tr>
        <tr><td>Transaction Type</td><td>:</td><td>Contribution</td></tr>
        <tr><td>Transaction Sub Type</td><td>:</td><td>Cash Contribution</td></tr>
        <tr><td>Filed Date</td><td>:</td><td>10/13/2022</td></tr>
        <tr><td>Amount</td><td>:</td><td>$10,000.00</td></tr>
        <tr><td>Aggregate</td><td>:</td><td>$10,000.00</td></tr>
        <tr><td>Process Status</td><td>:</td><td>Original</td></tr>
        <tr><td>Address Book Type</td><td>:</td><td>Individual</td></tr>
        <tr><td>Name</td><td>:</td><td>John Ramsbacher</td></tr>
        <tr><td>Address</td><td>:</td><td>123 Main St<br>Alamo CA</td></tr>
        <tr><td>Occupation</td><td>:</td><td>Partner</td></tr>
        <tr><td>Employer Name</td><td>:</td><td>A&amp;A Health Services LLC</td></tr>
      </table>
    `;

    expect(parseOregonOrestarTransactionDetail(html, "https://example.test/detail")).toEqual({
      transactionId: "4458653",
      transactionDate: "10/12/2022",
      transactionType: "Contribution",
      transactionSubType: "Cash Contribution",
      filedDate: "10/13/2022",
      amount: 10_000,
      aggregate: 10_000,
      processStatus: "Original",
      purpose: null,
      filerCommitteeName: "Friends of Tina Kotek",
      filerCommitteeId: "4792",
      addressBookType: "Individual",
      contributorPayeeName: "John Ramsbacher",
      address: "123 Main St\nAlamo CA",
      occupation: "Partner",
      employerName: "A&A Health Services LLC",
      outsideAssociations: [],
      sourceUrl: "https://example.test/detail",
    });
  });

  it("parses live ORESTAR detail header committee titles", () => {
    const html = `
      <table cellspacing="0" cellpadding="0" border="0" width="100%">
        <tr>
          <td class="pageheader">
            <div id="header2"><h1> Transaction Detail </h1></div>
          <td class="pageheader" align="right">
            <div id="header2"> Friends of Tina Kotek (4792) </div>
          </td>
        </tr>
      </table>
      <table>
        <tr><td>Transaction ID</td><td>:</td><td>4458653</td></tr>
        <tr><td>Transaction Date</td><td>:</td><td>12/28/2022</td></tr>
        <tr><td>Transaction Type</td><td>:</td><td>Contribution</td></tr>
        <tr><td>Amount</td><td>:</td><td>$10,000.00</td></tr>
      </table>
    `;

    expect(parseOregonOrestarTransactionDetail(html, "https://example.test/detail")).toMatchObject({
      transactionId: "4458653",
      filerCommitteeName: "Friends of Tina Kotek",
      filerCommitteeId: "4792",
    });
  });

  it("parses independent and in-kind outside spending associations", () => {
    const raw = [
      "Independent Expenditure in Support - Friends of Tina Kotek (4792) - $67,766.61",
      "Independent Expenditure in Opposition - Friends of Christine Drazan (22000) - $12,500.00",
      "In-Kind Expenditure - Friends of Tina Kotek (4792) - $1,234.00",
    ].join("\n");

    expect(parseOregonOrestarOutsideAssociations(raw)).toEqual([
      {
        associationType: "independent_expenditure",
        supportOppose: "support",
        targetCommitteeName: "Friends of Tina Kotek",
        targetCommitteeId: "4792",
        amount: 67_766.61,
        rawText: "Independent Expenditure in Support - Friends of Tina Kotek (4792) - $67,766.61",
      },
      {
        associationType: "independent_expenditure",
        supportOppose: "oppose",
        targetCommitteeName: "Friends of Christine Drazan",
        targetCommitteeId: "22000",
        amount: 12_500,
        rawText: "Independent Expenditure in Opposition - Friends of Christine Drazan (22000) - $12,500.00",
      },
      {
        associationType: "in_kind_expenditure",
        supportOppose: "support",
        targetCommitteeName: "Friends of Tina Kotek",
        targetCommitteeId: "4792",
        amount: 1_234,
        rawText: "In-Kind Expenditure - Friends of Tina Kotek (4792) - $1,234.00",
      },
    ]);
  });

  it("detects the Oregon cyber-security block page", () => {
    expect(
      isOregonOrestarBlockedPage(`
        <html>
          <body>Please Contact Us about this cyber-security service. Support ID: 123456</body>
        </html>
      `)
    ).toBe(true);
  });
});
