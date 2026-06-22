import { describe, expect, it } from "vitest";

import {
  evaluateOklahomaGuardianIeReportForCandidate,
  extractOklahomaGuardianIeReportPdfText,
  parseOklahomaGuardianIeReportText,
} from "../../../src/pipeline/oklahomaFinance/oklahomaGuardianIeReportParser.js";

const LIVE_STYLE_REPORT_TEXT = `
OKLAHOMA
ETHICS
COMMISSION
CONTRIBUTIONS AND EXPENDITURES REPORT
FOR INDEPENDENT EXPENDITURES, ELECTIONEERING COMMUNICATIONS,
AND STATE QUESTION COMMUNICATIONS
AMENDED:
NO
Full Name of Committee or Person Making Expenditure
Acronym
AMERICAN CONSERVATIVE UNION
ACU
Type of Report
Reporting Period:
Ethics Number:
Public IE EC SQ Report - PRE-ELECTION
06/14/2022 - 06/28/2022
TOTAL EXPENDITURES:
$6,000.00
Date
Amount
Type of Expense
[IE, EC, SQ]
Name and Office of Candidate(s),
or State Question Number Stance
[Support/Oppose]
6/14/2022
$6,000.00
Independent
Expenditure
O'CONNOR, JOHN
, ATTORNEY GENERAL
(SUPPORT)
STITT, KEVIN
, GOVERNOR
(SUPPORT)
`;

const SINGLE_CANDIDATE_REPORT_TEXT = `
AMENDED:
NO
Full Name of Committee or Person Making Expenditure
HOMETOWN FREEDOM ACTION NETWORK
Type of Report
IE EC SQ Report
Reporting Period:
10/25/2022 - 10/31/2022
TOTAL EXPENDITURES:
$12,345.67
STITT, KEVIN
, GOVERNOR
(OPPOSE)
`;

function simplePdfWithText(content: string): Buffer {
  const stream = content
    .split("\n")
    .map((line) => `BT /F1 12 Tf 0 0 Td (${line.replace(/([()\\])/g, "\\$1")}) Tj ET`)
    .join("\n");
  return Buffer.from(`%PDF-1.3\n1 0 obj\n<< /Length ${stream.length} >>\nstream\n${stream}\nendstream\nendobj\n%%EOF`, "latin1");
}

describe("Oklahoma Guardian IE report parser", () => {
  it("extracts text from a simple PDF text stream", () => {
    expect(extractOklahomaGuardianIeReportPdfText(simplePdfWithText("AMENDED:\nNO"))).toContain("AMENDED:\nNO");
  });

  it("parses report-level fields and candidate stances", () => {
    expect(parseOklahomaGuardianIeReportText(LIVE_STYLE_REPORT_TEXT)).toMatchObject({
      spenderName: "AMERICAN CONSERVATIVE UNION",
      amended: false,
      reportDescription: "Public IE EC SQ Report - PRE-ELECTION",
      reportingPeriodBegin: "06/14/2022",
      reportingPeriodEnd: "06/28/2022",
      totalExpenditures: 6000,
      candidateStances: [
        { candidateName: "O'CONNOR, JOHN", officeName: "ATTORNEY GENERAL", supportOppose: "support" },
        { candidateName: "STITT, KEVIN", officeName: "GOVERNOR", supportOppose: "support" },
      ],
    });
  });

  it("skips multi-candidate reports instead of assigning a shared amount to one candidate", () => {
    const parsed = parseOklahomaGuardianIeReportText(LIVE_STYLE_REPORT_TEXT);

    expect(evaluateOklahomaGuardianIeReportForCandidate({ parsed, candidateName: "Kevin Stitt" })).toEqual({
      status: "skipped",
      reason: "multiple_candidate_stances",
      matchingCandidateStances: [
        { candidateName: "STITT, KEVIN", officeName: "GOVERNOR", supportOppose: "support" },
      ],
      candidateStances: [
        { candidateName: "O'CONNOR, JOHN", officeName: "ATTORNEY GENERAL", supportOppose: "support" },
        { candidateName: "STITT, KEVIN", officeName: "GOVERNOR", supportOppose: "support" },
      ],
    });
  });

  it("matches a single-candidate report only when amount and stance are clear", () => {
    const parsed = parseOklahomaGuardianIeReportText(SINGLE_CANDIDATE_REPORT_TEXT);

    expect(evaluateOklahomaGuardianIeReportForCandidate({ parsed, candidateName: "Kevin Stitt" })).toEqual({
      status: "matched",
      spenderName: "HOMETOWN FREEDOM ACTION NETWORK",
      candidateName: "STITT, KEVIN",
      officeName: "GOVERNOR",
      supportOppose: "oppose",
      amount: 12345.67,
    });
  });

  it("dedupes repeated identical candidate stances before evaluating ambiguity", () => {
    const parsed = parseOklahomaGuardianIeReportText(`
AMENDED:
NO
Full Name of Committee or Person Making Expenditure
THE OKLAHOMA PROJECT
TOTAL EXPENDITURES:
$9,999.00
KEVIN STITT
, GOVERNOR
(OPPOSE)
KEVIN STITT
, GOVERNOR
(OPPOSE)
`);

    expect(parsed.candidateStances).toEqual([
      { candidateName: "KEVIN STITT", officeName: "GOVERNOR", supportOppose: "oppose" },
    ]);
    expect(evaluateOklahomaGuardianIeReportForCandidate({ parsed, candidateName: "Kevin Stitt" })).toEqual({
      status: "matched",
      spenderName: "THE OKLAHOMA PROJECT",
      candidateName: "KEVIN STITT",
      officeName: "GOVERNOR",
      supportOppose: "oppose",
      amount: 9999,
    });
  });

  it("skips reports that do not mention the candidate", () => {
    const parsed = parseOklahomaGuardianIeReportText(SINGLE_CANDIDATE_REPORT_TEXT);

    expect(evaluateOklahomaGuardianIeReportForCandidate({ parsed, candidateName: "Joy Hofmeister" })).toMatchObject({
      status: "skipped",
      reason: "candidate_not_found",
      matchingCandidateStances: [],
    });
  });
});
