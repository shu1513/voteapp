import { describe, expect, it } from "vitest";

import {
  normalizeMissouriMecElectionDate,
  parseMissouriMecCandidateExport,
  parseMissouriMecCommitteeInfo,
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
});
