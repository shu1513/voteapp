import { describe, expect, it, vi } from "vitest";

import { syncLouisianaCandidateFinance } from "../../../src/pipeline/louisianaFinance/louisianaCandidateFinanceSync.js";
import type { LouisianaCampaignFinanceCsvRow } from "../../../src/pipeline/louisianaFinance/louisianaCampaignFinanceArtifactReader.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const LINK_ID = "33333333-3333-4333-8333-333333333333";

function createTransactionalMockDb() {
  const clientQuery = vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 });
  const client = { query: clientQuery, release: vi.fn() };
  return {
    query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
    connect: vi.fn().mockResolvedValue(client),
    client,
  };
}

function contributionRow(overrides: Partial<LouisianaCampaignFinanceCsvRow> = {}): LouisianaCampaignFinanceCsvRow {
  return {
    FilerNumber: "12345",
    FilerLastName: "Edwards",
    FilerFirstName: "John Bel",
    ReportCode: "10-G",
    ReportType: "Candidate",
    ReportNumber: "1",
    ContributorTypeCode: "IND",
    ContributorName: "Jane Donor",
    ContributorAddr1: "1 Main St",
    ContributorAddr2: "",
    ContributorCity: "Baton Rouge",
    ContributorrState: "LA",
    ContributorZip: "70801",
    ContributionType: "Contribution",
    ContributionDescription: "",
    ContributionDate: "9/1/2027",
    ContributionAmt: "1000.00",
    ContributionDesignatedElectionAdditionInfo: "",
    ...overrides,
  };
}

function expenditureRow(overrides: Partial<LouisianaCampaignFinanceCsvRow> = {}): LouisianaCampaignFinanceCsvRow {
  return {
    FilerNumber: "PAC1",
    FilerLastName: "Better Louisiana PAC",
    FilerFirstName: "",
    ReportCode: "F202",
    ReportType: "PAC",
    ReportNumber: "1",
    Schedule: "E-3",
    RecipientName: "John Bel Edwards",
    RecipientAddr1: "",
    RecipientAddr2: "",
    RecipientCity: "",
    RecipientState: "LA",
    RecipientZip: "",
    ExpenditureDescription: "Campaign contribution",
    CandidateBeneficiary: "John Bel Edwards",
    ExpenditureDate: "9/15/2027",
    ExpenditureAmt: "5000.00",
    ...overrides,
  };
}

describe("Louisiana candidate finance sync", () => {
  it("writes direct totals, PAC support groups, and outside donor/industry backtrace rows", async () => {
    const db = createTransactionalMockDb();

    const result = await syncLouisianaCandidateFinance({
      db,
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      candidateName: "John Bel Edwards",
      electionYear: 2027,
      officeScope: "statewide",
      officeName: "Governor",
      contributionRows: [
        contributionRow(),
        contributionRow({
          FilerNumber: "PAC1",
          FilerLastName: "Better Louisiana PAC",
          FilerFirstName: "",
          ReportCode: "F202",
          ContributorTypeCode: "BUS",
          ContributorName: "Entergy Corporation",
          ContributionAmt: "5000.00",
        }),
      ],
      expenditureRows: [expenditureRow()],
      now: new Date("2026-06-01T00:00:00.000Z"),
    });

    expect(result.resolution).toMatchObject({
      status: "matched",
      filerNumber: "12345",
      filerName: "Edwards, John Bel",
    });
    expect(result.linkWritten).toBe(true);
    expect(result.summaryWritten).toBe(true);
    expect(result.totalReceipts).toBe(1000);
    expect(result.directContributionTotal).toBe(1000);
    expect(result.outsideSupportTotal).toBe(5000);
    expect(result.outsideOpposeTotal).toBe(0);
    expect(result.directBreakdownsWritten).toBeGreaterThan(0);
    expect(result.outsideGroupsWritten).toBe(1);
    expect(result.outsideGroupBreakdownsWritten).toBeGreaterThan(0);
    expect(result.matchedContributionRowCount).toBe(1);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.matchedOutsideExpenditureRowCount).toBe(1);
    expect(result.includedOutsideExpenditureRowCount).toBe(1);
    expect(result.matchedOutsideContributionRowCount).toBe(1);
    expect(result.includedOutsideContributionRowCount).toBe(1);

    const sql = db.client.query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("INSERT INTO public.la_candidate_finance_links"))).toBe(true);
    expect(sql.some((statement) => statement.includes("INSERT INTO public.la_candidate_finance_summaries"))).toBe(true);
    expect(sql.some((statement) => statement.includes("INSERT INTO public.la_candidate_finance_direct_breakdowns"))).toBe(true);
    expect(sql.some((statement) => statement.includes("INSERT INTO public.la_candidate_finance_outside_groups"))).toBe(true);
    expect(sql.some((statement) => statement.includes("INSERT INTO public.la_candidate_finance_outside_group_breakdowns"))).toBe(true);
  });

  it("does not write when candidate committee resolution is ambiguous", async () => {
    const db = createTransactionalMockDb();

    const result = await syncLouisianaCandidateFinance({
      db,
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      candidateName: "John Bel Edwards",
      electionYear: 2027,
      officeScope: "statewide",
      officeName: "Governor",
      contributionRows: [
        contributionRow(),
        contributionRow({
          FilerNumber: "67890",
        }),
      ],
      now: new Date("2026-06-01T00:00:00.000Z"),
    });

    expect(result.resolution.status).toBe("ambiguous");
    expect(result.linkWritten).toBe(false);
    expect(db.connect).not.toHaveBeenCalled();
  });
});
