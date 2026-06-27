import { describe, expect, it, vi } from "vitest";

import { syncUtahCandidateFinance } from "../../../src/pipeline/utahFinance/utahCandidateFinanceSync.js";
import type {
  UtahDisclosuresEntitySearchRow,
  UtahDisclosuresTransactionRow,
} from "../../../src/pipeline/utahFinance/utahDisclosuresClient.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";
const FOLDER_SOURCE_URL = "https://disclosures.utah.gov/Search/AdvancedSearch/FolderDetails/98765";
const TRANSACTION_SOURCE_URL = "https://disclosures.utah.gov/Search/AdvancedSearch/GenerateReport/98765?ReportYear=2024";

function createMockDb() {
  return {
    query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
  };
}

function entityRow(overrides: Partial<UtahDisclosuresEntitySearchRow> = {}): UtahDisclosuresEntitySearchRow {
  return {
    folderId: "98765",
    entityName: "Friends of Jane Doe",
    reportYears: [2024],
    sourceUrl: FOLDER_SOURCE_URL,
    ...overrides,
  };
}

function transaction(overrides: Partial<UtahDisclosuresTransactionRow> = {}): UtahDisclosuresTransactionRow {
  return {
    filed: "01/05/2024",
    entityType: "PCC",
    entityName: "Friends of Jane Doe",
    report: "Year End",
    transactionId: "T100",
    transactionType: "Contribution",
    transactionDate: "01/02/2024",
    amount: 100,
    name: "John Smith",
    address1: "1 Main",
    city: "Salt Lake City",
    state: "UT",
    zip: "84111",
    inKind: false,
    loan: false,
    ...overrides,
  };
}

function baseInput() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    candidateName: "Jane Doe",
    electionYear: 2024,
    officeScope: "statewide",
    officeName: "Governor",
    sourceUrl: "https://disclosures.utah.gov/Search/AdvancedSearch",
    transactionSourceUrl: TRANSACTION_SOURCE_URL,
    now: new Date("2026-02-03T04:05:06.000Z"),
  };
}

describe("utahCandidateFinanceSync", () => {
  it("resolves a Utah folder, aggregates direct contributions, and writes a snapshot", async () => {
    const db = createMockDb();

    const result = await syncUtahCandidateFinance({
      db,
      ...baseInput(),
      entityRows: [entityRow()],
      transactions: [
        transaction({ transactionId: "T1", amount: 100 }),
        transaction({ transactionId: "T2", amount: 250, name: "Jane Roe", address1: "2 Main" }),
        transaction({ transactionId: "T3", amount: 125, transactionType: "Expenditure" }),
        transaction({
          transactionId: "OTHER",
          entityName: "Other Committee",
          amount: 900,
        }),
      ],
    });

    expect(result).toMatchObject({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2024,
      dryRun: false,
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 2,
      totalReceipts: 350,
      directContributionTotal: 350,
      totalDisbursements: 125,
      matchedTransactionRowCount: 3,
      includedContributionRowCount: 2,
      skippedTransactionRowCount: 1,
      resolution: {
        status: "matched",
        folderId: "98765",
        committeeName: "Friends of Jane Doe",
      },
    });

    expect(db.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(db.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");

    const linkCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ut_candidate_finance_links")
    );
    expect(linkCall?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2024,
      "JANE DOE",
      "Governor",
      null,
      "98765",
      "Friends of Jane Doe",
      "active",
      "disclosures_advanced_search",
      FOLDER_SOURCE_URL,
      "2026-02-03T04:05:06.000Z",
    ]);

    const summaryCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ut_candidate_finance_summaries")
    );
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2024,
      350,
      350,
      125,
      null,
      TRANSACTION_SOURCE_URL,
      "2026-02-03T04:05:06.000Z",
    ]);
    const directBreakdownCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.ut_candidate_finance_direct_breakdowns")
    );
    expect(directBreakdownCalls).toHaveLength(2);
    expect(directBreakdownCalls.map((call) => call[1])).toContainEqual([
      LINK_ID,
      2024,
      "contribution_size",
      "$250-$499",
      250,
      1,
      TRANSACTION_SOURCE_URL,
      "2026-02-03T04:05:06.000Z",
    ]);
  });

  it("aggregates but does not write in dry-run mode", async () => {
    const db = createMockDb();

    const result = await syncUtahCandidateFinance({
      db,
      ...baseInput(),
      dryRun: true,
      entityRows: [entityRow()],
      transactions: [transaction({ amount: 250 })],
    });

    expect(result).toMatchObject({
      dryRun: true,
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      totalReceipts: 250,
      directContributionTotal: 250,
      resolution: { status: "matched", folderId: "98765" },
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("uses trusted linked folder details without re-matching committee text", async () => {
    const db = createMockDb();

    const result = await syncUtahCandidateFinance({
      db,
      ...baseInput(),
      candidateName: "Jane Doe",
      trustedCommittee: {
        folderId: "98765",
        committeeName: "Utah Future Fund",
        reportYears: [2024],
        sourceUrl: FOLDER_SOURCE_URL,
      },
      entityRows: [entityRow({ entityName: "Utah Future Fund" })],
      transactions: [transaction({ entityName: "Utah Future Fund", amount: 250 })],
    });

    expect(result).toMatchObject({
      linkWritten: true,
      summaryWritten: true,
      totalReceipts: 250,
      resolution: {
        status: "matched",
        folderId: "98765",
        committeeName: "Utah Future Fund",
        sourceUrl: FOLDER_SOURCE_URL,
      },
    });
  });

  it("aggregates supporting PAC organization industries when PAC rows are supplied", async () => {
    const db = createMockDb();

    const result = await syncUtahCandidateFinance({
      db,
      ...baseInput(),
      supportingCommitteeSourceUrl: "https://disclosures.utah.gov/Search/AdvancedSearch/GenerateReport?ReportYear=2024&EntityType=PAC",
      supportingCommitteeIndustryMinAmount: 5_000,
      entityRows: [entityRow()],
      transactions: [
        transaction({ transactionId: "candidate-direct", amount: 100 }),
        transaction({ transactionId: "candidate-pac", amount: 2_500, name: "Utah Builders PAC" }),
      ],
      supportingCommitteeTransactions: [
        transaction({
          entityType: "PAC",
          entityName: "Utah Builders PAC",
          transactionId: "pac-donor",
          amount: 25_000,
          name: "Wasatch Construction LLC",
        }),
      ],
    });

    expect(result).toMatchObject({
      supportingCommitteeCount: 1,
      supportingCommitteeIndustryCount: 1,
      supportingCommitteeMatchedTransactionRowCount: 1,
      supportingCommitteeIncludedOrganizationDonorRowCount: 1,
      supportingCommitteeSkippedTransactionRowCount: 0,
    });

    const supportingCommitteeCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ut_candidate_finance_supporting_committees")
    );
    expect(supportingCommitteeCall?.[1]).toEqual([
      LINK_ID,
      2024,
      "Utah Builders PAC",
      2500,
      1,
      TRANSACTION_SOURCE_URL,
      "2026-02-03T04:05:06.000Z",
    ]);
    const industryCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ut_candidate_finance_supporting_committee_industries")
    );
    expect(industryCall?.[1]).toEqual([
      LINK_ID,
      2024,
      "Utah Builders PAC",
      "construction",
      25000,
      1,
      "https://disclosures.utah.gov/Search/AdvancedSearch/GenerateReport?ReportYear=2024&EntityType=PAC",
      "2026-02-03T04:05:06.000Z",
    ]);
  });

  it("does not write when committee resolution is unmatched", async () => {
    const db = createMockDb();

    const result = await syncUtahCandidateFinance({
      db,
      ...baseInput(),
      candidateName: "Different Candidate",
      entityRows: [entityRow()],
      transactions: [transaction({ amount: 250 })],
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

    const result = await syncUtahCandidateFinance({
      db,
      ...baseInput(),
      entityRows: [
        entityRow({ folderId: "98765", entityName: "Friends of Jane Doe" }),
        entityRow({ folderId: "11111", entityName: "Jane Doe for Utah" }),
      ],
      transactions: [transaction({ amount: 250 })],
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
      syncUtahCandidateFinance({
        db,
        ...baseInput(),
        candidateName: " ",
        entityRows: [],
        transactions: [],
      })
    ).rejects.toThrow("candidate name is required");

    await expect(
      syncUtahCandidateFinance({
        db,
        ...baseInput(),
        electionYear: 1997,
        entityRows: [],
        transactions: [],
      })
    ).rejects.toThrow("Invalid Utah finance election year");

    await expect(
      syncUtahCandidateFinance({
        db,
        ...baseInput(),
        officeScope: " ",
        entityRows: [],
        transactions: [],
      })
    ).rejects.toThrow("office scope is required");

    expect(db.query).not.toHaveBeenCalled();
  });
});
