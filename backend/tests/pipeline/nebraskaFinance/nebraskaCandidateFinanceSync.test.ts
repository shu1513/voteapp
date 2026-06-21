import { describe, expect, it, vi } from "vitest";

import { syncNebraskaCandidateFinance } from "../../../src/pipeline/nebraskaFinance/nebraskaCandidateFinanceSync.js";
import type { NebraskaNadcContributionRow } from "../../../src/pipeline/nebraskaFinance/nebraskaNadcArtifactReader.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";
const CONTRIBUTION_SOURCE_URL =
  "https://nadc-e.nebraska.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip";

function createMockDb() {
  return {
    query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
  };
}

function contribution(overrides: Partial<NebraskaNadcContributionRow> = {}): NebraskaNadcContributionRow {
  return {
    "Receipt ID": "R1",
    "Org ID": "7569",
    "Filer Type": "Candidate Committee",
    "Filer Name": "VOTE VEST",
    "Candidate Name": "Rick Vest",
    "Receipt Transaction/Contribution Type": "Monetary Contribution",
    "Other Funds Type": "",
    "Receipt Date": "01/10/2026",
    "Receipt Amount": "100.00",
    Description: "",
    "Contributor or Transaction Source Type": "Individual",
    "Contributor or Source Name (Individual Last Name)": "Doe",
    "First Name": "Jane",
    "Middle Name": "",
    Suffix: "",
    "Address 1": "",
    "Address 2": "",
    City: "Lincoln",
    State: "NE",
    Zip: "68508",
    "Filed Date": "02/01/2026",
    Amended: "False",
    Employer: "Acme Inc",
    Occupation: "Attorney",
    ...overrides,
  };
}

function baseInput() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    candidateName: "Rick Vest",
    electionYear: 2026,
    officeScope: "state_upper",
    officeName: "State Senator",
    district: "30",
    sourceUrl: "https://nadc-e.nebraska.gov/PublicSite/DataDownload.aspx",
    contributionSourceUrl: CONTRIBUTION_SOURCE_URL,
    now: new Date("2026-02-03T04:05:06.000Z"),
  };
}

describe("nebraskaCandidateFinanceSync", () => {
  it("resolves a candidate committee, aggregates direct contributions, and writes a snapshot", async () => {
    const db = createMockDb();

    const result = await syncNebraskaCandidateFinance({
      db,
      ...baseInput(),
      contributionRows: [
        contribution({ "Receipt Amount": "100.00", Occupation: "Attorney" }),
        contribution({ "Receipt Amount": "250.00", Occupation: "Teacher" }),
        contribution({
          "Org ID": "OTHER",
          "Filer Name": "OTHER COMMITTEE",
          "Candidate Name": "Other Candidate",
          "Receipt Amount": "900.00",
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
      directBreakdownsWritten: 5,
      totalReceipts: 350,
      directContributionTotal: 350,
      matchedContributionRowCount: 2,
      includedContributionRowCount: 2,
      skippedContributionRowCount: 0,
      resolution: {
        status: "matched",
        committeeId: "7569",
        committeeName: "VOTE VEST",
      },
    });

    expect(db.query).toHaveBeenCalledTimes(10);
    expect(db.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(db.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");

    const linkCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ne_candidate_finance_links")
    );
    expect(linkCall?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "RICK VEST",
      "State Senator",
      "30",
      "7569",
      "VOTE VEST",
      "active",
      "nadc_bulk",
      "https://nadc-e.nebraska.gov/PublicSite/DataDownload.aspx",
      "2026-02-03T04:05:06.000Z",
    ]);

    const summaryCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ne_candidate_finance_summaries")
    );
    expect(summaryCall?.[1]).toEqual([LINK_ID, 2026, 350, 350, CONTRIBUTION_SOURCE_URL, "2026-02-03T04:05:06.000Z"]);
    const directBreakdownCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.ne_candidate_finance_direct_breakdowns")
    );
    expect(directBreakdownCalls).toHaveLength(5);
    expect(directBreakdownCalls.map((call) => call[1])).toContainEqual([
      LINK_ID,
      2026,
      "contributor_source_type",
      "individuals",
      350,
      2,
      CONTRIBUTION_SOURCE_URL,
      "2026-02-03T04:05:06.000Z",
    ]);
    expect(
      db.query.mock.calls.some((call) => String(call[0]).includes("DELETE FROM public.ne_candidate_finance_direct_breakdowns"))
    ).toBe(true);
  });

  it("aggregates but does not write in dry-run mode", async () => {
    const db = createMockDb();

    const result = await syncNebraskaCandidateFinance({
      db,
      ...baseInput(),
      dryRun: true,
      contributionRows: [contribution({ "Receipt Amount": "250.00" })],
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
      resolution: { status: "matched", committeeId: "7569" },
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("does not write when committee resolution is unmatched", async () => {
    const db = createMockDb();

    const result = await syncNebraskaCandidateFinance({
      db,
      ...baseInput(),
      candidateName: "Different Candidate",
      contributionRows: [contribution({ "Receipt Amount": "250.00" })],
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

    const result = await syncNebraskaCandidateFinance({
      db,
      ...baseInput(),
      contributionRows: [
        contribution({ "Receipt Amount": "250.00" }),
        contribution({
          "Org ID": "9999",
          "Filer Name": "FRIENDS OF RICK VEST",
          "Receipt Amount": "300.00",
        }),
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
      syncNebraskaCandidateFinance({
        db,
        ...baseInput(),
        candidateName: " ",
        contributionRows: [],
      })
    ).rejects.toThrow("candidate name is required");

    await expect(
      syncNebraskaCandidateFinance({
        db,
        ...baseInput(),
        electionYear: 2020,
        contributionRows: [],
      })
    ).rejects.toThrow("Invalid Nebraska finance election year");

    await expect(
      syncNebraskaCandidateFinance({
        db,
        ...baseInput(),
        officeScope: " ",
        contributionRows: [],
      })
    ).rejects.toThrow("office scope is required");

    expect(db.query).not.toHaveBeenCalled();
  });
});
