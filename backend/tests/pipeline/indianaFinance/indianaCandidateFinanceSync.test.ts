import { describe, expect, it, vi } from "vitest";

import { syncIndianaCandidateFinance } from "../../../src/pipeline/indianaFinance/indianaCandidateFinanceSync.js";
import type { IndianaCampaignFinanceContributionRow } from "../../../src/pipeline/indianaFinance/indianaCampaignFinanceReader.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";
const CONTRIBUTION_SOURCE_URL =
  "https://campaignfinance.in.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionData.csv.zip";

function createMockDb() {
  return {
    query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
  };
}

function contribution(overrides: Partial<IndianaCampaignFinanceContributionRow> = {}): IndianaCampaignFinanceContributionRow {
  return {
    FileNumber: "422",
    CommitteeType: "Candidate",
    Committee: "Diego for Indiana",
    CandidateName: "Cesar Diego Morales",
    ContributorType: "Individual",
    Name: "Jane Doe",
    Address: "100 Main St",
    City: "Indianapolis",
    State: "IN",
    Zip: "46204",
    Occupation: "Attorney/Legal",
    Type: "Direct",
    Description: "",
    Amount: "250.0000",
    ContributionDate: "2026-02-17 00:00:00",
    Received_By: "Treasurer",
    Amended: "0",
    ...overrides,
  };
}

function baseInput() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    candidateName: "Cesar Diego Morales",
    electionYear: 2026,
    officeScope: "state_upper",
    officeName: "State Senator",
    district: "30",
    sourceUrl: "https://campaignfinance.in.gov/PublicSite/Reporting/DataDownload.aspx",
    contributionSourceUrl: CONTRIBUTION_SOURCE_URL,
    now: new Date("2026-02-03T04:05:06.000Z"),
  };
}

describe("indianaCandidateFinanceSync", () => {
  it("resolves a candidate committee, aggregates direct contributions, and writes a snapshot", async () => {
    const db = createMockDb();

    const result = await syncIndianaCandidateFinance({
      db,
      ...baseInput(),
      contributionRows: [
        contribution({ Amount: "100.0000", Occupation: "Attorney/Legal" }),
        contribution({ Name: "John Roe", Amount: "250.0000", Occupation: "Teacher/Education" }),
        contribution({ FileNumber: "999", Committee: "Other Committee", CandidateName: "Other Candidate", Amount: "900.0000" }),
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
      directContributionTotal: 350,
      matchedContributionRowCount: 2,
      includedContributionRowCount: 2,
      skippedContributionRowCount: 0,
      resolution: {
        status: "matched",
        committeeId: "422",
        committeeName: "Diego for Indiana",
      },
    });

    expect(db.query).toHaveBeenCalledTimes(9);
    expect(db.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(db.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");

    const linkCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.in_candidate_finance_links")
    );
    expect(linkCall?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "CESAR DIEGO MORALES",
      "State Senator",
      "30",
      "422",
      "Diego for Indiana",
      "active",
      "public_bulk",
      "https://campaignfinance.in.gov/PublicSite/Reporting/DataDownload.aspx",
      "2026-02-03T04:05:06.000Z",
    ]);

    const summaryCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.in_candidate_finance_summaries")
    );
    expect(summaryCall?.[1]).toEqual([LINK_ID, 2026, 350, 350, CONTRIBUTION_SOURCE_URL, "2026-02-03T04:05:06.000Z"]);
    const directBreakdownCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.in_candidate_finance_direct_breakdowns")
    );
    expect(directBreakdownCalls).toHaveLength(4);
    expect(directBreakdownCalls.map((call) => call[1])).toContainEqual([
      LINK_ID,
      2026,
      "occupation",
      "Teacher/Education",
      250,
      1,
      CONTRIBUTION_SOURCE_URL,
      "2026-02-03T04:05:06.000Z",
    ]);
  });

  it("aggregates but does not write in dry-run mode", async () => {
    const db = createMockDb();

    const result = await syncIndianaCandidateFinance({
      db,
      ...baseInput(),
      dryRun: true,
      contributionRows: [contribution({ Amount: "250.0000" })],
    });

    expect(result).toMatchObject({
      dryRun: true,
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      totalReceipts: 250,
      directContributionTotal: 250,
      matchedContributionRowCount: 1,
      includedContributionRowCount: 1,
      resolution: { status: "matched", committeeId: "422" },
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("uses an already-linked committee without re-resolving from contribution-row candidate names", async () => {
    const db = createMockDb();

    const result = await syncIndianaCandidateFinance({
      db,
      ...baseInput(),
      candidateName: "Different Display Name",
      linkedCommittee: {
        committeeId: "422",
        committeeName: "Diego for Indiana",
      },
      contributionRows: [contribution({ Amount: "250.0000" })],
    });

    expect(result).toMatchObject({
      resolution: {
        status: "matched",
        committeeId: "422",
        committeeName: "Diego for Indiana",
      },
      totalReceipts: 250,
      directContributionTotal: 250,
      linkWritten: true,
    });
    expect(db.query).toHaveBeenCalled();
  });

  it("does not write when committee resolution is unmatched", async () => {
    const db = createMockDb();

    const result = await syncIndianaCandidateFinance({
      db,
      ...baseInput(),
      candidateName: "Different Candidate",
      contributionRows: [contribution({ Amount: "250.0000" })],
    });

    expect(result).toMatchObject({
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      totalReceipts: null,
      directContributionTotal: null,
      resolution: { status: "unmatched", reason: "no_candidate_committee_match" },
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("does not write when committee resolution is ambiguous", async () => {
    const db = createMockDb();

    const result = await syncIndianaCandidateFinance({
      db,
      ...baseInput(),
      contributionRows: [
        contribution({ Amount: "250.0000" }),
        contribution({ FileNumber: "423", Committee: "Friends of Cesar Diego Morales", Amount: "300.0000" }),
      ],
    });

    expect(result).toMatchObject({
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      totalReceipts: null,
      directContributionTotal: null,
      resolution: { status: "ambiguous", reason: "multiple_matching_committees" },
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("validates required sync inputs before resolving or writing", async () => {
    const db = createMockDb();

    await expect(
      syncIndianaCandidateFinance({
        db,
        ...baseInput(),
        candidateName: " ",
        contributionRows: [],
      })
    ).rejects.toThrow("candidate name is required");

    await expect(
      syncIndianaCandidateFinance({
        db,
        ...baseInput(),
        electionYear: 1999,
        contributionRows: [],
      })
    ).rejects.toThrow("Invalid Indiana finance election year");

    expect(db.query).not.toHaveBeenCalled();
  });
});
