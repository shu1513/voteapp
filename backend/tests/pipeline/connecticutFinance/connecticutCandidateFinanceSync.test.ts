import { describe, expect, it, vi } from "vitest";

import { syncConnecticutCandidateFinance } from "../../../src/pipeline/connecticutFinance/connecticutCandidateFinanceSync.js";
import type { ConnecticutEcrisArtifactRow } from "../../../src/pipeline/connecticutFinance/connecticutEcrisArtifactReader.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";

function createMockDb() {
  return {
    query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
  };
}

function receipt(overrides: Partial<ConnecticutEcrisArtifactRow> = {}): ConnecticutEcrisArtifactRow {
  return {
    Committee: "ACKERT FOR THE 8TH",
    "Contributor Name": "Carolyn Gerrity",
    District: "8",
    "Office Sought": "State Representative",
    Employer: "RTX-Pratt Whitney",
    "Receipt Type": "Itemized Contributions from Individuals",
    "Committee Type": "Candidate Committee",
    "Transaction Date": "03/31/2026",
    "File To State": "04/01/2026",
    Amount: "50.00",
    "Receipt State": "Original",
    Occupation: "Business Manager",
    ElectionYear: "2026",
    "Committee ID": "14376",
    "Candidate First Name": "Timothy",
    "Candidate Middle Intial": "J",
    "Candidate Last Name": "Ackert",
    ...overrides,
  };
}

function baseInput() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    candidateName: "Timothy Ackert",
    electionYear: 2026,
    officeName: "State Lower Chamber Legislator",
    district: "08",
    sourceUrl: "https://seec.ct.gov/portal/ecris/CurPreYears",
    receiptSourceUrl:
      "https://seec.ct.gov/ecrisreporting/Data/eCrisDownloads/exportdatafiles/Receipts2026ElectionYearCandidateExploratoryCommittees.csv",
    now: new Date("2026-02-03T04:05:06.000Z"),
  };
}

describe("connecticutCandidateFinanceSync", () => {
  it("resolves a candidate committee, aggregates direct receipts, and writes a snapshot", async () => {
    const db = createMockDb();

    const result = await syncConnecticutCandidateFinance({
      db,
      ...baseInput(),
      receiptRows: [
        receipt({ Amount: "100.00", Occupation: "Attorney" }),
        receipt({ Amount: "250.00", Occupation: "Teacher" }),
        receipt({
          "Committee ID": "OTHER",
          "Candidate First Name": "Other",
          "Candidate Last Name": "Candidate",
          Amount: "900.00",
          Occupation: "Doctor",
        }),
      ],
    });

    expect(result).toMatchObject({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2026,
      dryRun: false,
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 4,
      totalReceipts: 350,
      matchedReceiptRowCount: 2,
      includedReceiptRowCount: 2,
      skippedReceiptRowCount: 0,
      resolution: {
        status: "matched",
        committeeId: "14376",
        committeeName: "ACKERT FOR THE 8TH",
      },
    });

    expect(db.query).toHaveBeenCalledTimes(7);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.ct_candidate_finance_links");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "TIMOTHY ACKERT",
      "State Lower Chamber Legislator",
      "08",
      "14376",
      "ACKERT FOR THE 8TH",
      "active",
      "ecris_bulk",
      "https://seec.ct.gov/portal/ecris/CurPreYears",
      "2026-02-03T04:05:06.000Z",
    ]);
    expect(String(db.query.mock.calls[1]?.[0])).toContain("INSERT INTO public.ct_candidate_finance_summaries");
    expect(db.query.mock.calls[1]?.[1]).toEqual([
      LINK_ID,
      2026,
      350,
      null,
      null,
      null,
      "https://seec.ct.gov/ecrisreporting/Data/eCrisDownloads/exportdatafiles/Receipts2026ElectionYearCandidateExploratoryCommittees.csv",
      "2026-02-03T04:05:06.000Z",
    ]);
    expect(
      db.query.mock.calls.filter((call) =>
        String(call[0]).includes("INSERT INTO public.ct_candidate_finance_direct_breakdowns")
      )
    ).toHaveLength(4);
    expect(String(db.query.mock.calls.at(-1)?.[0])).toContain(
      "DELETE FROM public.ct_candidate_finance_direct_breakdowns"
    );
  });

  it("aggregates but does not write in dry-run mode", async () => {
    const db = createMockDb();

    const result = await syncConnecticutCandidateFinance({
      db,
      ...baseInput(),
      dryRun: true,
      receiptRows: [receipt({ Amount: "250.00" })],
    });

    expect(result).toMatchObject({
      dryRun: true,
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      totalReceipts: 250,
      matchedReceiptRowCount: 1,
      includedReceiptRowCount: 1,
      resolution: { status: "matched", committeeId: "14376" },
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("does not write when committee resolution is unmatched", async () => {
    const db = createMockDb();

    const result = await syncConnecticutCandidateFinance({
      db,
      ...baseInput(),
      candidateName: "Different Candidate",
      receiptRows: [receipt({ Amount: "250.00" })],
    });

    expect(result).toMatchObject({
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      totalReceipts: null,
      resolution: { status: "unmatched", reason: "no_candidate_office_year_match" },
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("does not write when committee resolution is ambiguous", async () => {
    const db = createMockDb();

    const result = await syncConnecticutCandidateFinance({
      db,
      ...baseInput(),
      receiptRows: [
        receipt({ Amount: "250.00" }),
        receipt({ Committee: "FRIENDS OF TIM ACKERT", "Committee ID": "99999", Amount: "300.00" }),
      ],
    });

    expect(result).toMatchObject({
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      totalReceipts: null,
      resolution: { status: "ambiguous", reason: "multiple_matching_committees" },
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("validates required sync inputs before resolving or writing", async () => {
    const db = createMockDb();

    await expect(
      syncConnecticutCandidateFinance({
        db,
        ...baseInput(),
        candidateName: " ",
        receiptRows: [],
      })
    ).rejects.toThrow("candidate name is required");

    await expect(
      syncConnecticutCandidateFinance({
        db,
        ...baseInput(),
        electionYear: 2007,
        receiptRows: [],
      })
    ).rejects.toThrow("Invalid Connecticut finance election year");

    expect(db.query).not.toHaveBeenCalled();
  });
});
