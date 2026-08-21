import { describe, expect, it } from "vitest";

import {
  normalizeNewHampshireCandidateNameForStorage,
  normalizeNewHampshireCandidateNameKeys,
  resolveNewHampshireCandidateFiler,
} from "../../../src/pipeline/newHampshireFinance/newHampshireCandidateFilerResolver.js";
import type { NewHampshireReceiptCsvRow } from "../../../src/pipeline/newHampshireFinance/newHampshireCfsCsv.js";

function receipt(overrides: Partial<NewHampshireReceiptCsvRow> = {}): NewHampshireReceiptCsvRow {
  return {
    "Filing Entity ID": "50450",
    "Candidate Name": "Jane Doe",
    "Committee Name": "Jane Doe Committee",
    "Committee Subtype": "Candidate Committee",
    "Transaction Type": "Receipt",
    "Transaction Sub Type": "Monetary Contribution",
    "Election Period": "General",
    "Election year": "2026",
    "Date of Receipt": "06/01/2026",
    "Amount of receipt": "$100.00",
    "Contributor Type": "Individual / Candidate",
    "Contributor Name": "Sample Donor",
    "Contributor Address Line 1": "REDACTED",
    "Contributor Address Line 2": "",
    "Contributor City": "Concord",
    "Contributor State": "NH",
    "Contributor Zip Code": "00000",
    "Contributor occupation": "",
    "Contributor Employer": "Sample Employer",
    "Contributor Principle place of Business": "",
    Description: "",
    "Timed Report": "",
    ...overrides,
  };
}

describe("newHampshireCandidateFilerResolver", () => {
  it("normalizes direct and comma-form names without fuzzy expansion", () => {
    expect([...normalizeNewHampshireCandidateNameKeys("DOE, Jane Q.")]).toEqual([
      "DOE JANE Q",
      "JANE Q DOE",
    ]);
    expect(normalizeNewHampshireCandidateNameForStorage("DOE, Jane Q.")).toBe("JANE Q DOE");
    expect(normalizeNewHampshireCandidateNameForStorage("Jane Q. Doe")).toBe("JANE Q DOE");
  });

  it("matches one filer, groups rows by filing entity ID, and returns trusted aliases", () => {
    const sourceUrl = "https://cfsapi.sos.nh.gov/api/ExportData/GetExportPublicDownloadData";
    expect(
      resolveNewHampshireCandidateFiler({
        candidateName: "Jane Doe",
        electionYear: 2026,
        sourceUrl,
        receiptRows: [
          receipt(),
          receipt({
            "Candidate Name": "Doe, Jane Q.",
            "Committee Subtype": "Candidate",
            "Date of Receipt": "06/02/2026",
          }),
          receipt({ "Filing Entity ID": "999", "Candidate Name": "Other Person" }),
          receipt({ "Filing Entity ID": "888", "Candidate Name": "Other Person" }),
        ],
      })
    ).toEqual({
      status: "matched",
      filingEntityId: 50450,
      filerName: "Jane Doe Committee",
      candidateAliases: ["Doe, Jane Q.", "Jane Doe"],
      confidence: "exact",
      source: "cfs_bulk",
      sourceUrl,
      matchedReceiptRowCount: 2,
    });
  });

  it("uses exact Election year rather than filing or transaction year", () => {
    const result = resolveNewHampshireCandidateFiler({
      candidateName: "Jane Doe",
      electionYear: 2026,
      receiptRows: [
        receipt({
          "Filing Entity ID": "111",
          "Election year": "2024",
          "Date of Receipt": "06/01/2026",
        }),
        receipt({
          "Filing Entity ID": "222",
          "Election year": "2026 General",
          "Date of Receipt": "06/01/2026",
        }),
        receipt({
          "Filing Entity ID": "333",
          "Election year": "2026",
          "Date of Receipt": "11/01/2025",
        }),
      ],
    });

    expect(result).toMatchObject({ status: "matched", filingEntityId: 333 });
  });

  it("allows missing middle evidence but rejects contradictory middles and generations", () => {
    const resolve = (candidateName: string, rowCandidateName: string) =>
      resolveNewHampshireCandidateFiler({
        candidateName,
        electionYear: 2026,
        receiptRows: [receipt({ "Candidate Name": rowCandidateName })],
      });

    expect(resolve("Jane Doe", "Doe, Jane Q.")).toMatchObject({ status: "matched" });
    expect(resolve("Jane Q. Doe", "Doe, Jane Quinn")).toMatchObject({ status: "matched" });
    expect(resolve("John Doe Jr.", "Doe, John")).toMatchObject({
      status: "matched",
      candidateAliases: ["Doe, John", "John Doe Jr."],
    });
    expect(resolve("Jane A. Doe", "Doe, Jane B.")).toMatchObject({
      status: "unmatched",
      reason: "no_candidate_filer_match",
    });
    expect(resolve("John Doe Jr.", "Doe, John III")).toMatchObject({
      status: "unmatched",
      reason: "no_candidate_filer_match",
    });
  });

  it("does not fuzzy-match typos", () => {
    expect(
      resolveNewHampshireCandidateFiler({
        candidateName: "Jane Doee",
        electionYear: 2026,
        receiptRows: [receipt()],
      })
    ).toEqual({
      status: "unmatched",
      reason: "no_candidate_filer_match",
      candidateNameNormalized: "JANE DOEE",
    });
  });

  it("returns every filer match as ambiguous instead of guessing", () => {
    expect(
      resolveNewHampshireCandidateFiler({
        candidateName: "Jane Doe",
        electionYear: 2026,
        receiptRows: [
          receipt({ "Filing Entity ID": "100", "Committee Name": "Committee One" }),
          receipt({ "Filing Entity ID": "200", "Committee Name": "Committee Two" }),
        ],
      })
    ).toEqual({
      status: "ambiguous",
      reason: "multiple_matching_filers",
      candidateNameNormalized: "JANE DOE",
      matches: [
        {
          filingEntityId: 100,
          filerName: "Committee One",
          candidateAliases: ["Jane Doe"],
          confidence: "exact",
          source: "cfs_bulk",
          sourceUrl: null,
          matchedReceiptRowCount: 1,
        },
        {
          filingEntityId: 200,
          filerName: "Committee Two",
          candidateAliases: ["Jane Doe"],
          confidence: "exact",
          source: "cfs_bulk",
          sourceUrl: null,
          matchedReceiptRowCount: 1,
        },
      ],
    });
  });

  it("selects the newest available filer name independent of row order", () => {
    const rows = [
      receipt({ "Committee Name": "Old Committee Name", "Date of Receipt": "01/01/2026" }),
      receipt({ "Committee Name": "Current Committee Name", "Date of Receipt": "07/01/2026" }),
    ];
    const resolve = (receiptRows: readonly NewHampshireReceiptCsvRow[]) =>
      resolveNewHampshireCandidateFiler({
        candidateName: "Jane Doe",
        electionYear: 2026,
        receiptRows,
      });

    expect(resolve(rows)).toMatchObject({ status: "matched", filerName: "Current Committee Name" });
    expect(resolve([...rows].reverse())).toEqual(resolve(rows));
  });

  it("uses official candidate name when a direct candidate filer has no committee name", () => {
    expect(
      resolveNewHampshireCandidateFiler({
        candidateName: "Jane Doe",
        electionYear: 2026,
        receiptRows: [receipt({ "Committee Name": "", "Committee Subtype": "Candidate" })],
      })
    ).toMatchObject({
      status: "matched",
      filingEntityId: 50450,
      filerName: "Jane Doe",
    });
  });

  it("rejects missing candidate names, malformed filer IDs, and invalid years", () => {
    expect(
      resolveNewHampshireCandidateFiler({
        candidateName: "   ",
        electionYear: 2026,
        receiptRows: [receipt()],
      })
    ).toEqual({
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized: "",
    });
    expect(
      resolveNewHampshireCandidateFiler({
        candidateName: "Jane Doe",
        electionYear: 2026,
        receiptRows: [
          receipt({ "Filing Entity ID": "not-an-id" }),
        ],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_filer_match" });
    expect(() =>
      resolveNewHampshireCandidateFiler({
        candidateName: "Jane Doe",
        electionYear: 2015,
        receiptRows: [],
      })
    ).toThrow("Invalid New Hampshire candidate filer election year");
  });
});
