import { describe, expect, it, vi } from "vitest";

import { syncOklahomaCandidateFinance } from "../../../src/pipeline/oklahomaFinance/oklahomaCandidateFinanceSync.js";
import type { OklahomaGuardianContributionRow } from "../../../src/pipeline/oklahomaFinance/oklahomaGuardianContributionReader.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";
const CONTRIBUTION_SOURCE_URL =
  "https://guardian.ok.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip";

function createMockDb() {
  return {
    query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
  };
}

function contribution(overrides: Partial<OklahomaGuardianContributionRow> = {}): OklahomaGuardianContributionRow {
  return {
    "Receipt ID": "R1",
    "Org ID": "11954",
    "Receipt Type": "Contribution",
    "Receipt Date": "01/10/2026",
    "Receipt Amount": "100.00",
    Description: "",
    "Receipt Source Type": "Individual",
    "Last Name": "Doe",
    "First Name": "Jane",
    "Middle Name": "",
    Suffix: "",
    "Address 1": "",
    "Address 2": "",
    City: "Oklahoma City",
    State: "OK",
    Zip: "73102",
    "Filed Date": "02/01/2026",
    "Committee Type": "Candidate Committee",
    "Committee Name": "Dishman for Senate",
    "Candidate Name": "C. Brent Dishman",
    Amended: "",
    Employer: "Acme Inc",
    Occupation: "Attorney",
    ...overrides,
  };
}

function baseInput() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    candidateName: "Brent Dishman",
    electionYear: 2026,
    officeScope: "state_upper",
    officeName: "State Senator",
    district: "47",
    sourceUrl: "https://guardian.ok.gov/PublicSite/DataDownload.aspx",
    contributionSourceUrl: CONTRIBUTION_SOURCE_URL,
    now: new Date("2026-02-03T04:05:06.000Z"),
  };
}

describe("oklahomaCandidateFinanceSync", () => {
  it("resolves a candidate committee, aggregates direct contributions, and writes a snapshot", async () => {
    const db = createMockDb();

    const result = await syncOklahomaCandidateFinance({
      db,
      ...baseInput(),
      contributionRows: [
        contribution({ "Receipt Amount": "100.00", Occupation: "Attorney" }),
        contribution({ "Receipt ID": "R2", "Receipt Amount": "250.00", Occupation: "Teacher" }),
        contribution({
          "Org ID": "OTHER",
          "Committee Name": "Other Committee",
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
      directBreakdownsWritten: 4,
      totalReceipts: 350,
      directContributionTotal: 350,
      matchedContributionRowCount: 2,
      includedContributionRowCount: 2,
      skippedContributionRowCount: 0,
      resolution: {
        status: "matched",
        committeeId: "11954",
        committeeName: "Dishman for Senate",
      },
    });

    expect(db.query).toHaveBeenCalledTimes(9);
    expect(db.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(db.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");

    const linkCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ok_candidate_finance_links")
    );
    expect(linkCall?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "BRENT DISHMAN",
      "State Senator",
      "47",
      "11954",
      "Dishman for Senate",
      "active",
      "guardian_bulk",
      "https://guardian.ok.gov/PublicSite/DataDownload.aspx",
      "2026-02-03T04:05:06.000Z",
    ]);

    const summaryCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ok_candidate_finance_summaries")
    );
    expect(summaryCall?.[1]).toEqual([LINK_ID, 2026, 350, 350, CONTRIBUTION_SOURCE_URL, "2026-02-03T04:05:06.000Z"]);
    const directBreakdownCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.ok_candidate_finance_direct_breakdowns")
    );
    expect(directBreakdownCalls).toHaveLength(4);
    expect(directBreakdownCalls.map((call) => call[1])).toContainEqual([
      LINK_ID,
      2026,
      "occupation",
      "Attorney",
      100,
      1,
      CONTRIBUTION_SOURCE_URL,
      "2026-02-03T04:05:06.000Z",
    ]);
    expect(
      db.query.mock.calls.some((call) => String(call[0]).includes("DELETE FROM public.ok_candidate_finance_direct_breakdowns"))
    ).toBe(true);
  });

  it("aggregates but does not write in dry-run mode", async () => {
    const db = createMockDb();

    const result = await syncOklahomaCandidateFinance({
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
      resolution: { status: "matched", committeeId: "11954" },
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("does not write when committee resolution is unmatched", async () => {
    const db = createMockDb();

    const result = await syncOklahomaCandidateFinance({
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

    const result = await syncOklahomaCandidateFinance({
      db,
      ...baseInput(),
      contributionRows: [
        contribution({ "Receipt Amount": "250.00" }),
        contribution({
          "Org ID": "11955",
          "Committee Name": "Friends of Brent Dishman",
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
      syncOklahomaCandidateFinance({
        db,
        ...baseInput(),
        candidateName: " ",
        contributionRows: [],
      })
    ).rejects.toThrow("candidate name is required");

    await expect(
      syncOklahomaCandidateFinance({
        db,
        ...baseInput(),
        electionYear: 2013,
        contributionRows: [],
      })
    ).rejects.toThrow("Invalid Oklahoma finance election year");

    await expect(
      syncOklahomaCandidateFinance({
        db,
        ...baseInput(),
        officeScope: " ",
        contributionRows: [],
      })
    ).rejects.toThrow("office scope is required");

    expect(db.query).not.toHaveBeenCalled();
  });
});
