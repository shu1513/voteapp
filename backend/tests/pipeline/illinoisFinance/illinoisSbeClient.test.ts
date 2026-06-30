import { describe, expect, it, vi } from "vitest";

import {
  fetchIllinoisSbeCandidateContributionsCsv,
  getIllinoisSbeExportCapStatus,
  hasIllinoisSbeExportCapWarning,
  illinoisSbeContributionRecordFromRow,
  illinoisSbeExpenditureRecordFromRow,
  parseIllinoisSbeCsvRows,
  planIllinoisSbeExportPartitions,
  splitIllinoisSbeSetCookieHeader,
  splitIllinoisSbeAmountWindow,
  splitIllinoisSbeDateWindow,
} from "../../../src/pipeline/illinoisFinance/illinoisSbeClient.js";

describe("illinoisSbeClient", () => {
  it("splits combined Set-Cookie headers without breaking Expires dates", () => {
    expect(
      splitIllinoisSbeSetCookieHeader(
        "ASP.NET_SessionId=abc; path=/; secure; HttpOnly; SameSite=Lax, " +
          "53739654b1671269f9d68b7188f1ee11418a3d0b97c07c45e41b2c2b092866e0=def;" +
          "expires=Tue, 30-Jun-2026 05:27:26 GMT;path=/;secure;httponly, " +
          "__cf_bm=ghi; HttpOnly; SameSite=None; Secure; Path=/; Domain=elections.il.gov; " +
          "Expires=Tue, 30 Jun 2026 05:47:26 GMT"
      )
    ).toEqual([
      "ASP.NET_SessionId=abc; path=/; secure; HttpOnly; SameSite=Lax",
      "53739654b1671269f9d68b7188f1ee11418a3d0b97c07c45e41b2c2b092866e0=def;expires=Tue, 30-Jun-2026 05:27:26 GMT;path=/;secure;httponly",
      "__cf_bm=ghi; HttpOnly; SameSite=None; Secure; Path=/; Domain=elections.il.gov; Expires=Tue, 30 Jun 2026 05:47:26 GMT",
    ]);
  });

  it("does not split combined Set-Cookie headers inside quoted cookie values", () => {
    expect(
      splitIllinoisSbeSetCookieHeader(
        'token="abc,key=val"; Path=/; HttpOnly, ASP.NET_SessionId=abc; path=/; secure; HttpOnly'
      )
    ).toEqual([
      'token="abc,key=val"; Path=/; HttpOnly',
      "ASP.NET_SessionId=abc; path=/; secure; HttpOnly",
    ]);
  });

  it("fails clearly when the live SBE form POST returns an empty response", async () => {
    const searchHtml = `
      <html><body>
        <form action="./ContributionSearchByCandidates.aspx">
          <input type="hidden" name="__VIEWSTATE" value="state" />
          <input type="hidden" name="__EVENTVALIDATION" value="event" />
          <input name="ctl00$ContentPlaceHolder1$txtCanElectYear" value="" />
          <input name="ctl00$ContentPlaceHolder1$txtCanLastName" value="" />
          <input name="ctl00$ContentPlaceHolder1$txtCanFirstName" value="" />
          <input type="submit" name="ctl00$ContentPlaceHolder1$btnCanSubmit" value="Search" />
          <select name="ctl00$ContentPlaceHolder1$ddlCanElectType"><option selected value="All Types">All Types</option></select>
          <select name="ctl00$ContentPlaceHolder1$ddlCanLastNameSearchType"><option selected value="Contains">Contains</option></select>
          <select name="ctl00$ContentPlaceHolder1$ddlCanFirstNameSearchType"><option selected value="Contains">Contains</option></select>
        </form>
      </body></html>
    `;
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(searchHtml, {
          status: 200,
          headers: {
            "content-type": "text/html",
            "set-cookie": "ASP.NET_SessionId=abc; path=/; HttpOnly",
          },
        })
      )
      .mockResolvedValueOnce(
        new Response("", {
          status: 200,
          headers: { "content-type": "text/html" },
        })
      );

    await expect(
      fetchIllinoisSbeCandidateContributionsCsv(
        {
          candidateLastName: "Pritzker",
          candidateFirstName: "JB",
          electionYear: 2022,
        },
        { fetchImpl }
      )
    ).rejects.toThrow("Illinois SBE returned an empty response for search request");
  });

  it("parses quoted Illinois SBE CSV rows and normalizes headers", () => {
    expect(
      parseIllinoisSbeCsvRows(
        'Contributed By,Received By,Amount\r\n"Doe, Jane\nOccupation: Attorney\nEmployer: Acme","Individual Contributions\nFriends of Jane","$1,250.50"\r\n'
      )
    ).toEqual([
      {
        contributed_by: "Doe, Jane\nOccupation: Attorney\nEmployer: Acme",
        received_by: "Individual Contributions\nFriends of Jane",
        amount: "$1,250.50",
      },
    ]);
  });

  it("maps Illinois contribution rows with contributor occupation and recipient committee", () => {
    expect(
      illinoisSbeContributionRecordFromRow({
        contributed_by: "Doe, Jane\n123 Main St\nOccupation: Attorney\nEmployer: Acme",
        received_by: "Individual Contributions\nFriends of Jane",
        amount: "$1,250.50",
        amount_received_date: "3/1/2022",
      })
    ).toMatchObject({
      contributorName: "Doe, Jane",
      contributorAddress: "123 Main St",
      occupation: "Attorney",
      employer: "Acme",
      contributionType: "Individual Contributions",
      recipientCommitteeName: "Friends of Jane",
      amount: 1250.5,
      receivedDate: "3/1/2022",
    });
  });

  it("maps Illinois independent expenditure rows conservatively", () => {
    expect(
      illinoisSbeExpenditureRecordFromRow({
        received_by: "Vendor LLC\n1 Market St",
        expended_by: "Independent Expenditures\nPeople for Schools",
        amount: "($500.00)",
        expended_by_date: "10/1/2022",
        candidate_name: "Jane Doe",
        office_district: "Governor",
        supporting_opposing: "Supporting",
      })
    ).toMatchObject({
      payeeName: "Vendor LLC",
      payeeAddress: "1 Market St",
      expenditureType: "Independent Expenditures",
      expendingCommitteeName: "People for Schools",
      amount: -500,
      expendedDate: "10/1/2022",
      candidateName: "Jane Doe",
      officeDistrict: "Governor",
      supportOppose: "support",
    });
  });

  it("detects capped export risk and proposes date or amount partitions", () => {
    expect(hasIllinoisSbeExportCapWarning("The maximum number of records available for download is 25,000.")).toBe(
      true
    );
    expect(
      getIllinoisSbeExportCapStatus({
        csvRowCount: 25_000,
        resultText: "The maximum number of records available for download is 25,000.",
      })
    ).toEqual({
      rowCount: 25000,
      cap: 25000,
      capped: true,
      warningTextPresent: true,
      reason: "row_count_reached_cap",
    });
    expect(
      getIllinoisSbeExportCapStatus({
        csvRowCount: 10,
        resultText: "The maximum number of records available for download is 25,000.",
      })
    ).toEqual({
      rowCount: 10,
      cap: 25000,
      capped: true,
      warningTextPresent: true,
      reason: "warning_text_present",
    });
    expect(splitIllinoisSbeDateWindow({ fromDate: "1/1/2021", toDate: "12/31/2022" })).toEqual([
      { fromDate: "1/1/2021", toDate: "12/31/2021" },
      { fromDate: "1/1/2022", toDate: "12/31/2022" },
    ]);
    expect(splitIllinoisSbeAmountWindow({ minAmount: 0, maxAmount: 1000 })).toEqual([
      { minAmount: 0, maxAmount: 500 },
      { minAmount: 500.01, maxAmount: 1000 },
    ]);
  });

  it("plans capped export partitions by date first and amount as a fallback", () => {
    expect(
      planIllinoisSbeExportPartitions({
        csvRowCount: 25_000,
        fromDate: "1/1/2021",
        toDate: "12/31/2022",
        minAmount: 0,
        maxAmount: 1000,
      })
    ).toMatchObject({
      strategy: "date",
      partitions: [
        { fromDate: "1/1/2021", toDate: "12/31/2021", minAmount: 0, maxAmount: 1000 },
        { fromDate: "1/1/2022", toDate: "12/31/2022", minAmount: 0, maxAmount: 1000 },
      ],
    });

    expect(
      planIllinoisSbeExportPartitions({
        csvRowCount: 100,
        resultText: "The maximum number of records available for download is 25,000.",
        minAmount: 0,
        maxAmount: 1000,
      })
    ).toMatchObject({
      strategy: "amount",
      partitions: [
        { minAmount: 0, maxAmount: 500 },
        { minAmount: 500.01, maxAmount: 1000 },
      ],
    });

    expect(
      planIllinoisSbeExportPartitions({
        csvRowCount: 1,
        fromDate: "1/1/2022",
        toDate: "12/31/2022",
      })
    ).toMatchObject({
      strategy: null,
      partitions: null,
      status: { capped: false },
    });
  });
});
