import { describe, expect, it } from "vitest";

import {
  normalizeMissouriMecElectionDate,
  parseMissouriMecCandidateExport,
  parseMissouriMecCommitteeInfo,
  parseMissouriMecContributionExport,
  parseMissouriMecExpenditureExport,
  parseMissouriMecReportInventory,
  parseMissouriMecReportYears,
} from "../../../src/pipeline/missouriFinance/missouriMecParsers.js";

describe("missouriMecParsers", () => {
  it("parses the pinned candidate-by-election Excel export", () => {
    const html = `
      <table>
        <tr><th>MECID</th><th>Committee Name</th><th>Candidate Name</th><th>Party</th><th>Office Sought</th><th>Status</th></tr>
        <tr><td>C221944</td><td>Forward With Farnan</td><td>Jeff Farnan</td><td>R</td><td>State Representative - District 1 - Missouri House of Representatives</td><td>A</td></tr>
      </table>
    `;

    expect(parseMissouriMecCandidateExport(html)).toEqual([
      {
        mecid: "C221944",
        committeeName: "Forward With Farnan",
        candidateName: "Jeff Farnan",
        party: "R",
        officeSought: "State Representative - District 1 - Missouri House of Representatives",
        status: "A",
      },
    ]);
  });

  it("fails closed when the candidate export schema drifts", () => {
    expect(() =>
      parseMissouriMecCandidateExport(
        "<table><tr><th>MECID</th><th>Candidate Name</th></tr><tr><td>C221944</td><td>Jeff Farnan</td></tr></table>"
      )
    ).toThrow("Unexpected Missouri MEC candidate export header");
  });

  it("parses aligned Committee Info election-history evidence", () => {
    const html = `
      <span id="ContentPlaceHolder_ContentPlaceHolder1_lblMECID">C221944</span>
      <span id="ContentPlaceHolder_ContentPlaceHolder1_lblCommName">Forward With Farnan</span>
      <span id="ContentPlaceHolder_ContentPlaceHolder1_lblCandName">Jeff Farnan</span>
      <span id="ContentPlaceHolder_ContentPlaceHolder1_gvElecHistory_lblElecYear_0">11/3/2026</span>
      <span id="ContentPlaceHolder_ContentPlaceHolder1_gvElecHistory_lblElectionType_0">General Election</span>
      <span id="ContentPlaceHolder_ContentPlaceHolder1_gvElecHistory_lblSub_0">State Representative</span>
      <span id="ContentPlaceHolder_ContentPlaceHolder1_gvElecHistory_lblPolSub_0">Missouri House of Representatives</span>
      <span id="ContentPlaceHolder_ContentPlaceHolder1_gvElecHistory_lblElecYear_1">8/4/2026</span>
      <span id="ContentPlaceHolder_ContentPlaceHolder1_gvElecHistory_lblElectionType_1">Primary Election</span>
      <span id="ContentPlaceHolder_ContentPlaceHolder1_gvElecHistory_lblSub_1">State Representative</span>
      <span id="ContentPlaceHolder_ContentPlaceHolder1_gvElecHistory_lblPolSub_1">Missouri House of Representatives</span>
    `;

    expect(parseMissouriMecCommitteeInfo(html)).toEqual({
      mecid: "C221944",
      committeeName: "Forward With Farnan",
      candidateName: "Jeff Farnan",
      electionHistory: [
        {
          electionDate: "2026-11-03",
          electionType: "General Election",
          office: "State Representative",
          politicalSubdivision: "Missouri House of Representatives",
        },
        {
          electionDate: "2026-08-04",
          electionType: "Primary Election",
          office: "State Representative",
          politicalSubdivision: "Missouri House of Representatives",
        },
      ],
      sourceUrl: "https://www.mec.mo.gov/MEC/Campaign_Finance/CommInfo.aspx?MECID=C221944",
    });
  });

  it("rejects misaligned Committee Info history instead of pairing wrong rows", () => {
    const html = `
      <span id="x_lblMECID">C221944</span><span id="x_lblCommName">Forward With Farnan</span><span id="x_lblCandName">Jeff Farnan</span>
      <span id="x_gvElecHistory_lblElecYear_0">11/3/2026</span>
      <span id="x_gvElecHistory_lblElectionType_0">General Election</span>
      <span id="x_gvElecHistory_lblSub_0">State Representative</span>
    `;
    expect(() => parseMissouriMecCommitteeInfo(html)).toThrow("Misaligned Missouri MEC election history");
  });

  it("normalizes real MEC dates and rejects rollover dates", () => {
    expect(normalizeMissouriMecElectionDate("4/2/2024")).toBe("2024-04-02");
    expect(() => normalizeMissouriMecElectionDate("2/30/2026")).toThrow("Invalid Missouri MEC election date");
  });

  it("parses pinned contribution/expenditure exports without retaining addresses", () => {
    const contributions = `<table><tr>${[
      "MECID", "Committee", "Report", "Contributor-Committee", "Contributor-Company", "Contributor-Last Name",
      "Contributor-First Name", "Address1", "Address2", "City", "State", "Zip", "Employer", "Occupation",
      "Contribution Date", "Contribution Amount", "Monetary/In-Kind", "Committee",
    ].map((value) => `<th>${value}</th>`).join("")}</tr><tr>${[
      "C263985", "Example Committee", "July Quarterly Report", "", "", "Doe", "Jane", "10 Private St", "",
      "Jefferson City", "MO", "65101", "Example LLC", "Engineer", "8/5/2026", "$1,234.56", "Monetary", "Candidate",
    ].map((value) => `<td>${value}</td>`).join("")}</tr></table>`;
    expect(parseMissouriMecContributionExport(contributions)).toEqual([expect.objectContaining({
      mecid: "C263985", report: "July Quarterly Report", contributionDate: "2026-08-05",
      amountCents: 123456, employer: "Example LLC", occupation: "Engineer",
    })]);
    expect(parseMissouriMecContributionExport(contributions)[0]).not.toHaveProperty("address1");

    const expenditures = `<table><tr>${[
      "MECID", "Committee Name", "Report", "Expenditure-Last Name", "Expenditure-First Name", "Expenditure-Company",
      "Expenditure-Address1", "Expenditure-Address2", "Expenditure-City", "Expenditure-State", "Expenditure-Zip",
      "Expenditure Purpose", "Expenditure Date", "Expenditure Amount", "Expenditure Type",
    ].map((value) => `<th>${value}</th>`).join("")}</tr><tr>${[
      "C263985", "Example Committee", "July Quarterly Report", "", "", "Printer", "20 Private St", "", "Columbia",
      "MO", "65201", "Signs", "9/1/2026", "($25.00)", "Paid",
    ].map((value) => `<td>${value}</td>`).join("")}</tr></table>`;
    expect(parseMissouriMecExpenditureExport(expenditures)).toEqual([expect.objectContaining({
      amountCents: -2500, expenditureDate: "2026-09-01", expenditureType: "Paid",
    })]);
  });

  it("parses any expanded report grid index and fails on misalignment", () => {
    const years = `<span id="x_grvReportOutside_lblYear_0">2026</span><span id="x_grvReportOutside_lblYear_1">2025</span>`;
    expect(parseMissouriMecReportYears(years)).toEqual([
      { year: 2026, expandControlName: "grvReportOutside$ctl02$ImgRptRight" },
      { year: 2025, expandControlName: "grvReportOutside$ctl03$ImgRptRight" },
    ]);
    const inventory = `<a id="x_grvReports_2_hlink_0" data-CPID="274835"></a>
      <span id="x_grvReports_2_lblReport_0">AMENDED April Quarterly Report</span>
      <span id="x_grvReports_2_lblDateReceived_0">4/15/2026</span>`;
    expect(parseMissouriMecReportInventory(inventory)).toEqual([{
      reportId: "274835", report: "AMENDED April Quarterly Report", dateFiled: "2026-04-15",
      isAmended: true, lineageKey: "APRIL QUARTERLY REPORT",
    }]);
    expect(() => parseMissouriMecReportInventory(inventory.replace("lblDateReceived", "missing"))).toThrow("Misaligned");
  });
});
