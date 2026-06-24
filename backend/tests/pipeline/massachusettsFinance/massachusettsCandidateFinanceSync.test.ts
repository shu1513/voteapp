import { describe, expect, it, vi } from "vitest";

import { normalizeFinanceLabel, type FinanceLabelClassification } from "../../../src/pipeline/finance/financeLabelClassifier.js";
import { syncMassachusettsCandidateFinance } from "../../../src/pipeline/massachusettsFinance/massachusettsCandidateFinanceSync.js";
import type { MassachusettsCandidateCommitteeResolution } from "../../../src/pipeline/massachusettsFinance/massachusettsCandidateCommitteeResolver.js";
import type {
  MassachusettsOcpfContributionItem,
  MassachusettsOcpfIepacReportSummary,
  MassachusettsOcpfReportDetail,
} from "../../../src/pipeline/massachusettsFinance/massachusettsOcpfClient.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";
const SOURCE_URL = "https://api.ocpf.us/filers/listings/A?searchPhrase=Maura%20Healey";

function createMockDb() {
  const query = vi.fn(async (sql: string) => {
    if (String(sql).includes("FROM public.finance_label_classifications AS classification")) {
      return { rows: [], rowCount: 0 };
    }
    return { rows: [{ id: LINK_ID }], rowCount: 1 };
  });
  const client = { query, release: vi.fn() };
  return {
    query,
    connect: vi.fn().mockResolvedValue(client),
    client,
  };
}

function matchedResolution(
  overrides: Partial<Extract<MassachusettsCandidateCommitteeResolution, { status: "matched" }>> = {}
): Extract<MassachusettsCandidateCommitteeResolution, { status: "matched" }> {
  return {
    status: "matched",
    candidateCpfId: "15710",
    filerName: "Healey, Maura T.",
    committeeName: "Healey Committee",
    officeSought: "Statewide, Governor",
    confidence: "exact",
    source: "ocpf_api",
    sourceUrl: SOURCE_URL,
    matchedFilerRowCount: 1,
    ...overrides,
  };
}

function contribution(overrides: Partial<MassachusettsOcpfContributionItem> = {}): MassachusettsOcpfContributionItem {
  return {
    itemId: "1",
    reportId: 812510,
    cpfId: "15710",
    filerName: "Healey, Maura T.",
    contributorName: "Donor, Jane",
    contributorType: "Individual",
    occupation: "Attorney",
    employer: "Law Firm",
    recordTypeDescription: "Individual",
    amount: 250,
    date: "10/01/2022",
    sourceUrl: "https://www.ocpf.us/item/1",
    ...overrides,
  };
}

function reportSummary(overrides: Partial<MassachusettsOcpfIepacReportSummary> = {}): MassachusettsOcpfIepacReportSummary {
  return {
    reportId: 858575,
    cpfId: "81068",
    committeeName: "Local 103 International Brotherhood of Electrical Workers Independent Expenditure PAC",
    reportYear: 2022,
    reportType: "IEPAC Report",
    reportingPeriod: "2022 Pre-election",
    candidateListing: "Maura T. Healey",
    candidateSpendingBreakdown: "Maura T. Healey (Supported) $32,420.00<br>",
    receiptsTotal: 32_420,
    expendituresTotal: 32_420,
    sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
    ...overrides,
  };
}

function reportDetail(overrides: Partial<MassachusettsOcpfReportDetail> = {}): MassachusettsOcpfReportDetail {
  return {
    ...reportSummary(),
    receipts: [
      {
        contributorName: "IBEW 103",
        recordTypeDescription: "Union/Association",
        amount: 32_420,
        date: "11/08/2022",
        sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
      },
      {
        contributorName: "Unknown Civic Fund",
        recordTypeDescription: "Committee",
        amount: 50_000,
        date: "11/01/2022",
        sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
      },
    ],
    expenditures: [
      {
        affectedCandidateName: "Maura T. Healey",
        relatedCpfId: "15710",
        isSupported: true,
        recordTypeDescription: "Independent Expenditure",
        ieInfo: "support Maura T. Healey",
        amount: 32_420,
        date: "11/08/2022",
        sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
      },
    ],
    ...overrides,
  };
}

function createOcpfClient(input: {
  resolution?: MassachusettsCandidateCommitteeResolution;
  contributions?: MassachusettsOcpfContributionItem[];
  reports?: MassachusettsOcpfIepacReportSummary[];
  details?: MassachusettsOcpfReportDetail[];
} = {}) {
  const details = input.details ?? [reportDetail()];
  return {
    searchAndResolveCandidateCommittee: vi.fn(async () => input.resolution ?? matchedResolution()),
    getContributionItems: vi.fn(async () =>
      input.contributions ?? [
        contribution({ itemId: "1", amount: 250, occupation: "Attorney" }),
        contribution({ itemId: "2", contributorName: "Donor, John", amount: 500, occupation: "Attorney" }),
        contribution({ itemId: "3", amount: 1_000, recordTypeDescription: "Committee", occupation: "" }),
      ]
    ),
    getIepacReportSummaries: vi.fn(async () => input.reports ?? [reportSummary()]),
    getReportDetail: vi.fn(async ({ reportId }: { reportId: number }) => details.find((detail) => detail.reportId === reportId) ?? details[0]),
  };
}

function baseInput() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    candidateName: "Maura Healey",
    electionYear: 2022,
    officeScope: "statewide",
    officeName: "Governor",
    sourceUrl: SOURCE_URL,
    now: new Date("2026-06-02T03:04:05.000Z"),
  };
}

describe("massachusettsCandidateFinanceSync", () => {
  it("resolves a candidate CPF, aggregates OCPF direct/outside finance, enriches industries, and writes a snapshot", async () => {
    const db = createMockDb();
    const ocpfClient = createOcpfClient();
    const normalizedUnknown = normalizeFinanceLabel("Unknown Civic Fund", "donor");
    const financeIndustryClassifier = vi.fn(async (): Promise<FinanceLabelClassification[]> => [
      {
        rawLabel: "Unknown Civic Fund",
        labelType: "donor",
        normalizedLabel: normalizedUnknown,
        industrySlug: "environmental_group",
        confidence: "medium",
        classificationSource: "ai",
        matchedRule: null,
      },
    ]);

    const result = await syncMassachusettsCandidateFinance({
      db,
      ...baseInput(),
      ocpfClient,
      financeIndustryClassifier,
    });

    expect(result).toMatchObject({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2022,
      dryRun: false,
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 3,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 4,
      totalReceipts: 1750,
      directContributionTotal: 750,
      outsideSupportTotal: 32420,
      outsideOpposeTotal: 0,
      matchedContributionRowCount: 3,
      includedContributionRowCount: 2,
      skippedContributionRowCount: 1,
      matchedExpenditureRowCount: 1,
      includedExpenditureRowCount: 1,
      skippedExpenditureRowCount: 0,
      matchedReceiptRowCount: 2,
      includedReceiptRowCount: 2,
      skippedReceiptRowCount: 0,
      iepacReportCount: 1,
      iepacReportDetailCount: 1,
      resolution: { status: "matched", candidateCpfId: "15710" },
    });

    expect(ocpfClient.searchAndResolveCandidateCommittee).toHaveBeenCalledWith(
      expect.objectContaining({ candidateName: "Maura Healey", officeName: "Governor", electionYear: 2022 }),
      undefined
    );
    expect(ocpfClient.getContributionItems).toHaveBeenCalledWith(
      { candidateCpfId: "15710", electionYear: 2022, limit: undefined },
      undefined
    );
    expect(ocpfClient.getIepacReportSummaries).toHaveBeenCalledWith(2022, undefined);
    expect(ocpfClient.getReportDetail).toHaveBeenCalledWith({ reportId: 858575 }, undefined);
    expect(financeIndustryClassifier).toHaveBeenCalledWith({
      labels: [
        {
          rawLabel: "Unknown Civic Fund",
          labelType: "donor",
          normalizedLabel: normalizedUnknown,
          amount: 50_000,
        },
      ],
    });

    expect(db.query.mock.calls.some((call) => call[0] === "BEGIN")).toBe(true);
    expect(db.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");

    const linkCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ma_candidate_finance_links")
    );
    expect(linkCall?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2022,
      "MAURA HEALEY",
      "Governor",
      null,
      "15710",
      "Healey, Maura T.",
      "Healey Committee",
      "active",
      "ocpf_api",
      SOURCE_URL,
      "2026-06-02T03:04:05.000Z",
    ]);

    const summaryCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ma_candidate_finance_summaries")
    );
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2022,
      1750,
      750,
      null,
      null,
      32420,
      0,
      expect.stringContaining("https://api.ocpf.us/search/items"),
      "2026-06-02T03:04:05.000Z",
    ]);

    expect(
      db.query.mock.calls.filter((call) => String(call[0]).includes("INSERT INTO public.ma_candidate_finance_direct_breakdowns"))
    ).toHaveLength(3);
    expect(
      db.query.mock.calls.filter((call) => String(call[0]).includes("INSERT INTO public.ma_candidate_finance_outside_groups"))
    ).toHaveLength(1);
    expect(
      db.query.mock.calls.filter((call) => String(call[0]).includes("INSERT INTO public.ma_candidate_finance_outside_group_breakdowns"))
    ).toHaveLength(4);
    expect(
      db.query.mock.calls.filter((call) => String(call[0]).includes("INSERT INTO public.finance_label_classifications"))
    ).toHaveLength(2);
  });

  it("does not write in dry-run mode but returns aggregation counts", async () => {
    const db = createMockDb();
    const ocpfClient = createOcpfClient();

    const result = await syncMassachusettsCandidateFinance({
      db,
      ...baseInput(),
      dryRun: true,
      ocpfClient,
    });

    expect(result).toMatchObject({
      dryRun: true,
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      totalReceipts: 1750,
      directContributionTotal: 750,
      outsideSupportTotal: 32420,
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("returns an empty result when committee resolution is not matched", async () => {
    const db = createMockDb();
    const ocpfClient = createOcpfClient({
      resolution: {
        status: "unmatched",
        reason: "no_candidate_committee_match",
        candidateNameNormalized: "MISSING CANDIDATE",
        officeNameNormalized: "Statewide, Governor",
      },
    });

    const result = await syncMassachusettsCandidateFinance({
      db,
      ...baseInput(),
      candidateName: "Missing Candidate",
      ocpfClient,
    });

    expect(result).toMatchObject({
      resolution: { status: "unmatched", reason: "no_candidate_committee_match" },
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
    });
    expect(ocpfClient.getContributionItems).not.toHaveBeenCalled();
    expect(db.query).not.toHaveBeenCalled();
  });

  it("trusts an existing CPF link instead of re-resolving by name", async () => {
    const db = createMockDb();
    const ocpfClient = createOcpfClient();

    await syncMassachusettsCandidateFinance({
      db,
      ...baseInput(),
      ocpfClient,
      trustedCommittee: {
        candidateCpfId: "15710",
        filerName: "Healey, Maura T.",
        committeeName: "Healey Committee",
        sourceUrl: SOURCE_URL,
      },
    });

    expect(ocpfClient.searchAndResolveCandidateCommittee).not.toHaveBeenCalled();
    expect(ocpfClient.getContributionItems).toHaveBeenCalledWith(
      { candidateCpfId: "15710", electionYear: 2022, limit: undefined },
      undefined
    );
  });
});
