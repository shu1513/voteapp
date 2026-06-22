import { describe, expect, it, vi } from "vitest";

import { syncNewMexicoCandidateFinance } from "../../../src/pipeline/newMexicoFinance/newMexicoCandidateFinanceSync.js";
import type {
  NewMexicoCfisContributionRow,
  NewMexicoCfisExpenditureRow,
} from "../../../src/pipeline/newMexicoFinance/newMexicoCfisArtifactReader.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";
const CONTRIBUTION_SOURCE_URL =
  "https://login.cfis.sos.state.nm.us/api/DataDownload/GetCSVDownloadReport?year=2026&transactionType=CON";
const EXPENDITURE_SOURCE_URL =
  "https://login.cfis.sos.state.nm.us/api/DataDownload/GetCSVDownloadReport?year=2026&transactionType=EXP";

function createMockDb() {
  return {
    query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
  };
}

function contribution(overrides: Partial<NewMexicoCfisContributionRow> = {}): NewMexicoCfisContributionRow {
  return {
    OrgID: "1001",
    "Transaction Amount": "100.00",
    "Transaction Date": "01/10/2026",
    "Last Name": "Doe",
    "First Name": "Jane",
    "Middle Name": "",
    Prefix: "",
    Suffix: "",
    "Contributor Address Line 1": "",
    "Contributor Address Line 2": "",
    "Contributor City": "Santa Fe",
    "Contributor State": "NM",
    "Contributor Zip Code": "87501",
    Description: "",
    "Check Number": "",
    "Transaction ID": "T1",
    "Filed Date": "02/01/2026",
    Election: "2026 General",
    "Report Name": "First Report",
    "Start of Period": "01/01/2026",
    "End of Period": "01/31/2026",
    "Contributor Code": "Individual",
    "Contribution Type": "Contributions - Monetary",
    "Report Entity Type": "Candidate",
    "Committee Name": "Haaland for New Mexico",
    "Candidate Last Name": "Haaland",
    "Candidate First Name": "Deb",
    "Candidate Middle Name": "",
    "Candidate Prefix": "",
    "Candidate Suffix": "",
    Amended: "",
    "Contributor Employer": "Acme",
    "Contributor Occupation": "Attorney",
    "Occupation Comment": "",
    "Employment Information Requested": "",
    ...overrides,
  };
}

function expenditure(overrides: Partial<NewMexicoCfisExpenditureRow> = {}): NewMexicoCfisExpenditureRow {
  return {
    OrgID: "9001",
    "Expenditure Amount": "70000.00",
    "Expenditure Date": "04/01/2026",
    "Payee Last Name": "Vendor",
    "Payee First Name": "",
    "Payee Middle Name": "",
    "Payee Prefix": "",
    "Payee Suffix": "",
    "Payee Address 1": "",
    "Payee Address 2": "",
    "Payee City": "Santa Fe",
    "Payee State": "NM",
    "Payee Zip Code": "87501",
    Description: "Independent expenditure",
    "Expenditure ID": "E1",
    "Filed Date": "04/02/2026",
    Election: "2026 General",
    "Report Name": "First Report",
    "Start of Period": "04/01/2026",
    "End of Period": "04/30/2026",
    Purpose: "Independent expenditure supporting/opposing others (explain)*",
    "Expenditure Type": "Independent Expenditure",
    Reason: "Haaland, Deb",
    Stance: "Support",
    "Report Entity Type": "PAC - Independent Expenditure",
    "Committee Name": "Accountable New Mexico",
    "Candidate Last Name": "",
    "Candidate First Name": "",
    "Candidate Middle Name": "",
    "Candidate Prefix": "",
    "Candidate Suffix": "",
    Amended: "",
    ...overrides,
  };
}

function baseInput() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    candidateName: "Deb Haaland",
    electionYear: 2026,
    officeScope: "statewide",
    officeName: "Governor",
    sourceUrl: "https://login.cfis.sos.state.nm.us/",
    contributionSourceUrl: CONTRIBUTION_SOURCE_URL,
    expenditureSourceUrl: EXPENDITURE_SOURCE_URL,
    now: new Date("2026-02-03T04:05:06.000Z"),
  };
}

describe("newMexicoCandidateFinanceSync", () => {
  it("resolves a candidate committee, aggregates direct and outside data, and writes a snapshot", async () => {
    const db = createMockDb();

    const result = await syncNewMexicoCandidateFinance({
      db,
      ...baseInput(),
      contributionRows: [
        contribution({ "Transaction Amount": "100.00", "Contributor Occupation": "Attorney" }),
        contribution({ "Transaction Amount": "250.00", "Contributor Occupation": "Teacher" }),
        contribution({
          OrgID: "9001",
          "Report Entity Type": "PAC - Independent Expenditure",
          "Committee Name": "Accountable New Mexico",
          "Transaction ID": "OUT1",
          "Transaction Amount": "25000.00",
          "Last Name": "Guzman Construction Solutions LLC",
          "First Name": "",
          "Contributor Code": "Other (e.g. business entity)",
          "Contributor Occupation": "",
        }),
        contribution({
          OrgID: "OTHER",
          "Committee Name": "Other Committee",
          "Candidate First Name": "Other",
          "Candidate Last Name": "Candidate",
          "Transaction Amount": "900.00",
          "Contributor Occupation": "Doctor",
        }),
      ],
      expenditureRows: [expenditure({ "Expenditure Amount": "1200.00" })],
      aiClassificationMinAmount: 100_000,
    });

    expect(result).toMatchObject({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2026,
      dryRun: false,
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 5,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 2,
      totalReceipts: 350,
      directContributionTotal: 350,
      outsideSupportTotal: 1200,
      outsideOpposeTotal: 0,
      matchedContributionRowCount: 2,
      includedContributionRowCount: 2,
      skippedContributionRowCount: 0,
      matchedExpenditureRowCount: 1,
      includedExpenditureRowCount: 1,
      skippedExpenditureRowCount: 0,
      resolution: {
        status: "matched",
        committeeId: "1001",
        committeeName: "Haaland for New Mexico",
      },
    });

    expect(db.query).toHaveBeenCalledTimes(16);
    expect(db.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(db.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");

    const linkCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.nm_candidate_finance_links")
    );
    expect(linkCall?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "DEB HAALAND",
      "Governor",
      null,
      "1001",
      "Haaland for New Mexico",
      "active",
      "cfis_bulk",
      "https://login.cfis.sos.state.nm.us/",
      "2026-02-03T04:05:06.000Z",
    ]);

    const summaryCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.nm_candidate_finance_summaries")
    );
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2026,
      350,
      350,
      null,
      1200,
      0,
      CONTRIBUTION_SOURCE_URL,
      "2026-02-03T04:05:06.000Z",
    ]);
    expect(
      db.query.mock.calls.filter((call) =>
        String(call[0]).includes("INSERT INTO public.nm_candidate_finance_direct_breakdowns")
      )
    ).toHaveLength(5);
    expect(
      db.query.mock.calls.filter((call) =>
        String(call[0]).includes("INSERT INTO public.nm_candidate_finance_outside_groups")
      )
    ).toHaveLength(1);
    const outsideBreakdownCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.nm_candidate_finance_outside_group_breakdowns")
    );
    expect(outsideBreakdownCalls).toHaveLength(2);
    expect(outsideBreakdownCalls.map((call) => call[1])).toEqual([
      [
        LINK_ID,
        2026,
        "9001",
        "support",
        "donor",
        "Guzman Construction Solutions LLC",
        25000,
        1,
        CONTRIBUTION_SOURCE_URL,
        "2026-02-03T04:05:06.000Z",
      ],
      [
        LINK_ID,
        2026,
        "9001",
        "support",
        "industry",
        "construction",
        25000,
        1,
        CONTRIBUTION_SOURCE_URL,
        "2026-02-03T04:05:06.000Z",
      ],
    ]);
    const classificationCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.finance_label_classifications")
    );
    expect(classificationCall?.[1]).toEqual([
      "Guzman Construction Solutions LLC",
      "donor",
      "GUZMAN CONSTRUCTION SOLUTIONS",
      "construction",
      "medium",
      "rule",
    ]);
  });

  it("aggregates but does not write in dry-run mode", async () => {
    const db = createMockDb();

    const result = await syncNewMexicoCandidateFinance({
      db,
      ...baseInput(),
      dryRun: true,
      contributionRows: [contribution({ "Transaction Amount": "250.00" })],
      expenditureRows: [expenditure({ "Expenditure Amount": "500.00" })],
    });

    expect(result).toMatchObject({
      dryRun: true,
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      totalReceipts: 250,
      directContributionTotal: 250,
      outsideSupportTotal: 500,
      resolution: { status: "matched", committeeId: "1001" },
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("does not write when committee resolution is unmatched", async () => {
    const db = createMockDb();

    const result = await syncNewMexicoCandidateFinance({
      db,
      ...baseInput(),
      candidateName: "Different Candidate",
      contributionRows: [contribution({ "Transaction Amount": "250.00" })],
      expenditureRows: [expenditure({ "Expenditure Amount": "500.00" })],
    });

    expect(result).toMatchObject({
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      totalReceipts: null,
      directContributionTotal: null,
      outsideSupportTotal: null,
      resolution: { status: "unmatched", reason: "no_candidate_committee_match" },
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("uses a trusted linked committee without re-resolving by candidate name", async () => {
    const db = createMockDb();

    const result = await syncNewMexicoCandidateFinance({
      db,
      ...baseInput(),
      candidateName: "Updated Display Name",
      contributionRows: [
        contribution({
          "Candidate First Name": "Deb",
          "Candidate Last Name": "Haaland",
          "Transaction Amount": "250.00",
        }),
      ],
      trustedCommittee: {
        committeeId: "1001",
        committeeName: "Haaland for New Mexico",
        sourceUrl: "https://login.cfis.sos.state.nm.us/",
      },
    });

    expect(result).toMatchObject({
      linkWritten: true,
      summaryWritten: true,
      totalReceipts: 250,
      directContributionTotal: 250,
      resolution: {
        status: "matched",
        committeeId: "1001",
        committeeName: "Haaland for New Mexico",
      },
    });

    const linkCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.nm_candidate_finance_links")
    );
    expect(linkCall?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "UPDATED DISPLAY NAME",
      "Governor",
      null,
      "1001",
      "Haaland for New Mexico",
      "active",
      "cfis_bulk",
      "https://login.cfis.sos.state.nm.us/",
      "2026-02-03T04:05:06.000Z",
    ]);
  });

  it("does not write when committee resolution is ambiguous", async () => {
    const db = createMockDb();

    const result = await syncNewMexicoCandidateFinance({
      db,
      ...baseInput(),
      contributionRows: [
        contribution({ "Transaction Amount": "250.00" }),
        contribution({
          OrgID: "9999",
          "Committee Name": "Friends of Deb Haaland",
          "Transaction Amount": "300.00",
        }),
      ],
      expenditureRows: [expenditure({ "Expenditure Amount": "500.00" })],
    });

    expect(result).toMatchObject({
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      totalReceipts: null,
      directContributionTotal: null,
      outsideSupportTotal: null,
      resolution: { status: "ambiguous", reason: "multiple_matching_committees" },
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("does not clear existing outside groups when expenditures are omitted", async () => {
    const db = createMockDb();

    const result = await syncNewMexicoCandidateFinance({
      db,
      ...baseInput(),
      contributionRows: [contribution({ "Transaction Amount": "250.00" })],
    });

    expect(result).toMatchObject({
      outsideGroupsWritten: 0,
      outsideSupportTotal: null,
      matchedExpenditureRowCount: 0,
    });
    const sql = db.query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("DELETE FROM public.nm_candidate_finance_outside_groups"))).toBe(false);
  });

  it("clears outside totals when expenditure data is present and no outside spending remains", async () => {
    const db = createMockDb();

    const result = await syncNewMexicoCandidateFinance({
      db,
      ...baseInput(),
      contributionRows: [contribution({ "Transaction Amount": "100.00" })],
      expenditureRows: [],
    });

    expect(result).toMatchObject({
      outsideGroupsWritten: 0,
      outsideSupportTotal: 0,
      outsideOpposeTotal: 0,
      matchedExpenditureRowCount: 0,
      includedExpenditureRowCount: 0,
      skippedExpenditureRowCount: 0,
    });

    const summaryCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.nm_candidate_finance_summaries")
    );
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2026,
      100,
      100,
      null,
      0,
      0,
      CONTRIBUTION_SOURCE_URL,
      "2026-02-03T04:05:06.000Z",
    ]);
    const sql = db.query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("DELETE FROM public.nm_candidate_finance_outside_groups"))).toBe(true);
  });

  it("validates required sync inputs before resolving or writing", async () => {
    const db = createMockDb();

    await expect(
      syncNewMexicoCandidateFinance({
        db,
        ...baseInput(),
        candidateName: " ",
        contributionRows: [],
      })
    ).rejects.toThrow("candidate name is required");

    await expect(
      syncNewMexicoCandidateFinance({
        db,
        ...baseInput(),
        electionYear: 2019,
        contributionRows: [],
      })
    ).rejects.toThrow("Invalid New Mexico finance election year");

    await expect(
      syncNewMexicoCandidateFinance({
        db,
        ...baseInput(),
        officeScope: " ",
        contributionRows: [],
      })
    ).rejects.toThrow("office scope is required");

    expect(db.query).not.toHaveBeenCalled();
  });
});
