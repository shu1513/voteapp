import { describe, expect, it, vi } from "vitest";

import { syncColoradoCandidateFinance } from "../../../src/pipeline/coloradoFinance/coloradoCandidateFinanceSync.js";
import type { ColoradoTracerContributionRow } from "../../../src/pipeline/coloradoFinance/coloradoTracerContributionReader.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";

function createMockDb() {
  return {
    query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
  };
}

function contribution(overrides: Partial<ColoradoTracerContributionRow> = {}): ColoradoTracerContributionRow {
  return {
    CO_ID: "202650001",
    ContributionAmount: "100.00",
    ContributionDate: "01/10/2026",
    LastName: "Doe",
    FirstName: "Jane",
    MI: "",
    Suffix: "",
    Address1: "",
    Address2: "",
    City: "Denver",
    State: "CO",
    Zip: "80203",
    Explanation: "",
    RecordID: "R1",
    FiledDate: "02/01/2026",
    ContributionType: "Monetary",
    ReceiptType: "Contribution",
    ContributorType: "Individual",
    Electioneering: "",
    CommitteeType: "Candidate Committee",
    CommitteeName: "Jane Doe for Colorado Governor",
    CandidateName: "Jane Doe",
    Employer: "Acme Inc",
    Occupation: "Engineer",
    Amended: "False",
    Amendment: "",
    AmendedRecordID: "",
    Jurisdiction: "STATEWIDE",
    OccupationComments: "",
    ...overrides,
  };
}

function baseInput() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    candidateName: "Jane Doe",
    electionYear: 2026,
    officeName: "Governor",
    committeeId: "202650001",
    committeeName: "Jane Doe for Colorado Governor",
    tracerCandidateId: "TRACER-123",
    sourceUrl: "https://tracer.sos.colorado.gov/PublicSite/SearchPages/CandidateDetail.aspx",
    contributionSourceUrl: "https://tracer.sos.colorado.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionData.csv.zip",
    now: new Date("2026-02-03T04:05:06.000Z"),
  };
}

describe("coloradoCandidateFinanceSync", () => {
  it("aggregates direct contributions and writes a Colorado finance snapshot", async () => {
    const db = createMockDb();

    const result = await syncColoradoCandidateFinance({
      db,
      ...baseInput(),
      contributionRows: [
        contribution({ Employer: "Acme Inc", Occupation: "Engineer", ContributionAmount: "100.00" }),
        contribution({ Employer: "School", Occupation: "Teacher", ContributionAmount: "300.00" }),
        contribution({ CO_ID: "OTHER", Employer: "Hospital", Occupation: "Doctor", ContributionAmount: "900.00" }),
      ],
    });

    expect(result).toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2026,
      dryRun: false,
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 6,
      totalReceipts: 400,
      matchedContributionRowCount: 2,
      includedContributionRowCount: 2,
      skippedContributionRowCount: 0,
    });

    expect(db.query).toHaveBeenCalledTimes(9);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.co_candidate_finance_links");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "JANE DOE",
      "Governor",
      "TRACER-123",
      "202650001",
      "Jane Doe for Colorado Governor",
      "active",
      "manual",
      "https://tracer.sos.colorado.gov/PublicSite/SearchPages/CandidateDetail.aspx",
      "2026-02-03T04:05:06.000Z",
    ]);
    expect(String(db.query.mock.calls[1]?.[0])).toContain("INSERT INTO public.co_candidate_finance_summaries");
    expect(db.query.mock.calls[1]?.[1]).toEqual([
      LINK_ID,
      2026,
      400,
      "https://tracer.sos.colorado.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionData.csv.zip",
      "2026-02-03T04:05:06.000Z",
    ]);
    expect(
      db.query.mock.calls.filter((call) =>
        String(call[0]).includes("INSERT INTO public.co_candidate_finance_direct_breakdowns")
      )
    ).toHaveLength(6);
    expect(String(db.query.mock.calls.at(-1)?.[0])).toContain(
      "DELETE FROM public.co_candidate_finance_direct_breakdowns"
    );
  });

  it("aggregates but does not write in dry-run mode", async () => {
    const db = createMockDb();

    const result = await syncColoradoCandidateFinance({
      db,
      ...baseInput(),
      dryRun: true,
      contributionRows: [contribution({ ContributionAmount: "250.00" })],
    });

    expect(result).toMatchObject({
      dryRun: true,
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      totalReceipts: 250,
      matchedContributionRowCount: 1,
      includedContributionRowCount: 1,
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("writes an empty direct snapshot when no current contributions match", async () => {
    const db = createMockDb();

    const result = await syncColoradoCandidateFinance({
      db,
      ...baseInput(),
      contributionRows: [contribution({ CO_ID: "OTHER", ContributionAmount: "900.00" })],
    });

    expect(result).toMatchObject({
      summaryWritten: true,
      directBreakdownsWritten: 0,
      totalReceipts: 0,
      matchedContributionRowCount: 0,
      includedContributionRowCount: 0,
    });
    expect(db.query).toHaveBeenCalledTimes(3);
    expect(String(db.query.mock.calls[1]?.[0])).toContain("INSERT INTO public.co_candidate_finance_summaries");
    expect(db.query.mock.calls[1]?.[1]?.[2]).toBe(0);
    expect(String(db.query.mock.calls[2]?.[0])).toContain("DELETE FROM public.co_candidate_finance_direct_breakdowns");
  });

  it("validates required sync inputs before writing", async () => {
    const db = createMockDb();

    await expect(
      syncColoradoCandidateFinance({
        db,
        ...baseInput(),
        committeeId: " ",
        contributionRows: [],
      })
    ).rejects.toThrow("Colorado committee id is required");

    await expect(
      syncColoradoCandidateFinance({
        db,
        ...baseInput(),
        candidateName: " ",
        contributionRows: [],
      })
    ).rejects.toThrow("candidate name is required");

    await expect(
      syncColoradoCandidateFinance({
        db,
        ...baseInput(),
        electionYear: 2000,
        contributionRows: [],
      })
    ).rejects.toThrow("Invalid Colorado finance election year");

    expect(db.query).not.toHaveBeenCalled();
  });
});
