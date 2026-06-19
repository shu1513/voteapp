import { describe, expect, it, vi } from "vitest";

import {
  type CaliforniaCandidateFinancePowerSearchClient,
  syncCaliforniaCandidateFinance,
} from "../../../src/pipeline/californiaFinance/californiaCandidateFinanceSync.js";
import type { CalAccessReceiptRow } from "../../../src/pipeline/californiaFinance/californiaDirectContributionAggregator.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";

function createMockDb() {
  return {
    query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
  };
}

function createPowerSearchClient(
  overrides: Partial<CaliforniaCandidateFinancePowerSearchClient> = {}
): CaliforniaCandidateFinancePowerSearchClient {
  return {
    summarizeIndependentSpendingByCandidate: vi.fn().mockResolvedValue({
      candidateName: "Newsom, Gavin",
      electionCycle: 2025,
      supportTotal: 300,
      opposeTotal: 50,
      sourceUrl: "https://powersearch.sos.ca.gov:3000/ie/search?candidatename=Newsom%2C+Gavin&electioncycle=2025",
      groups: [
        {
          expenderId: "1267335",
          expenderName: "Democratic Club of Ventura",
          supportOppose: "support",
          amount: 300,
          count: 2,
          sourceUrl: "https://powersearch.sos.ca.gov:3000/ie/search?candidatename=Newsom%2C+Gavin&electioncycle=2025",
        },
        {
          expenderId: "1442978",
          expenderName: "SAFE CA INC",
          supportOppose: "oppose",
          amount: 50,
          count: 1,
          sourceUrl: "https://powersearch.sos.ca.gov:3000/ie/search?candidatename=Newsom%2C+Gavin&electioncycle=2025",
        },
      ],
    }),
    ...overrides,
  };
}

function baseInput() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    candidateName: "Newsom, Gavin",
    electionYear: 2026,
    officeName: "Governor",
    controlledCommitteeId: "1456045",
    controlledCommitteeName: "Newsom for California Governor 2026",
    sourceUrl: "https://powersearch.sos.ca.gov/advanced.php",
    now: new Date("2026-02-03T04:05:06.000Z"),
  };
}

function receipt(overrides: Partial<CalAccessReceiptRow> = {}): CalAccessReceiptRow {
  return {
    FILING_ID: "F1",
    TRAN_ID: "T1",
    CTRIB_EMP: "GOOGLE LLC",
    CTRIB_OCC: "Engineer",
    RCPT_DATE: "2/1/2026 12:00:00 AM",
    AMOUNT: "100.00",
    CMTE_ID: "1456045",
    ...overrides,
  };
}

describe("californiaCandidateFinanceSync", () => {
  it("fetches outside spending and writes a California finance snapshot", async () => {
    const db = createMockDb();
    const powerSearchClient = createPowerSearchClient();

    const result = await syncCaliforniaCandidateFinance({
      db,
      ...baseInput(),
      powerSearchClient,
      powerSearchOptions: { timeoutMs: 1000 },
    });

    expect(result).toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2026,
      dryRun: false,
      outsideIncluded: true,
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 2,
      outsideGroupBreakdownsWritten: 0,
      outsideSupportTotal: 300,
      outsideOpposeTotal: 50,
    });

    expect(powerSearchClient.summarizeIndependentSpendingByCandidate).toHaveBeenCalledWith(
      { candidateName: "Newsom, Gavin", electionYear: 2026 },
      { timeoutMs: 1000 }
    );
    expect(db.query).toHaveBeenCalledTimes(6);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.ca_candidate_finance_links");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "NEWSOM, GAVIN",
      "Governor",
      "1456045",
      "Newsom for California Governor 2026",
      "active",
      "manual",
      "https://powersearch.sos.ca.gov/advanced.php",
      "2026-02-03T04:05:06.000Z",
    ]);
    expect(String(db.query.mock.calls[1]?.[0])).toContain("INSERT INTO public.ca_candidate_finance_summaries");
    expect(db.query.mock.calls[1]?.[1]).toEqual([
      LINK_ID,
      2026,
      null,
      null,
      null,
      null,
      300,
      50,
      "https://powersearch.sos.ca.gov:3000/ie/search?candidatename=Newsom%2C+Gavin&electioncycle=2025",
      "2026-02-03T04:05:06.000Z",
    ]);
    expect(String(db.query.mock.calls[2]?.[0])).toContain("INSERT INTO public.ca_candidate_finance_outside_groups");
    expect(String(db.query.mock.calls[4]?.[0])).toContain("DELETE FROM public.ca_candidate_finance_outside_group_breakdowns");
    expect(String(db.query.mock.calls[5]?.[0])).toContain("DELETE FROM public.ca_candidate_finance_outside_groups");
    expect(
      db.query.mock.calls.some((call) => String(call[0]).includes("DELETE FROM public.ca_candidate_finance_direct_breakdowns"))
    ).toBe(false);
  });

  it("fetches but does not write in dry-run mode", async () => {
    const db = createMockDb();
    const powerSearchClient = createPowerSearchClient();

    const result = await syncCaliforniaCandidateFinance({
      db,
      ...baseInput(),
      powerSearchClient,
      dryRun: true,
    });

    expect(result).toMatchObject({
      dryRun: true,
      linkWritten: false,
      summaryWritten: false,
      outsideGroupsWritten: 0,
      outsideSupportTotal: 300,
      outsideOpposeTotal: 50,
    });
    expect(powerSearchClient.summarizeIndependentSpendingByCandidate).toHaveBeenCalledTimes(1);
    expect(db.query).not.toHaveBeenCalled();
  });

  it("writes a heartbeat summary when outside spending is disabled", async () => {
    const db = createMockDb();
    const powerSearchClient = createPowerSearchClient();

    const result = await syncCaliforniaCandidateFinance({
      db,
      ...baseInput(),
      powerSearchClient,
      includeOutside: false,
    });

    expect(result).toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2026,
      dryRun: false,
      outsideIncluded: false,
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      outsideSupportTotal: null,
      outsideOpposeTotal: null,
    });
    expect(powerSearchClient.summarizeIndependentSpendingByCandidate).not.toHaveBeenCalled();
    expect(db.query).toHaveBeenCalledTimes(2);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.ca_candidate_finance_links");
    expect(String(db.query.mock.calls[1]?.[0])).toContain("INSERT INTO public.ca_candidate_finance_summaries");
    expect(db.query.mock.calls[1]?.[1]).toEqual([
      LINK_ID,
      2026,
      null,
      null,
      null,
      null,
      null,
      null,
      "https://powersearch.sos.ca.gov/advanced.php",
      "2026-02-03T04:05:06.000Z",
    ]);
  });

  it("aggregates direct CAL-ACCESS receipts and writes deterministic industry breakdowns", async () => {
    const db = createMockDb();
    const powerSearchClient = createPowerSearchClient();

    const result = await syncCaliforniaCandidateFinance({
      db,
      ...baseInput(),
      powerSearchClient,
      includeOutside: false,
      directReceiptRows: [
        receipt({ TRAN_ID: "T1", CTRIB_EMP: "Google LLC", CTRIB_OCC: "Engineer", AMOUNT: "100.00" }),
        receipt({ TRAN_ID: "T2", CTRIB_EMP: "Google LLC", CTRIB_OCC: "Engineer", AMOUNT: "250.00" }),
      ],
      directSourceUrl: "https://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip",
    });

    expect(result).toMatchObject({
      summaryWritten: true,
      directBreakdownsWritten: 5,
      outsideIncluded: false,
      outsideGroupsWritten: 0,
    });
    expect(powerSearchClient.summarizeIndependentSpendingByCandidate).not.toHaveBeenCalled();

    const summaryCall = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ca_candidate_finance_summaries")
    );
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2026,
      350,
      null,
      null,
      null,
      null,
      null,
      "https://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip",
      "2026-02-03T04:05:06.000Z",
    ]);

    const directBreakdownCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.ca_candidate_finance_direct_breakdowns")
    );
    expect(directBreakdownCalls.map((call) => call[1]?.slice(2, 6))).toEqual(
      expect.arrayContaining([
        ["occupation", "Engineer", 350, 2],
        ["employer", "Google LLC", 350, 2],
        ["contribution_size", "$100-$249", 100, 1],
        ["contribution_size", "$250-$499", 250, 1],
        ["industry", "technology", 350, 2],
      ])
    );
    expect(
      db.query.mock.calls.some((call) => String(call[0]).includes("DELETE FROM public.ca_candidate_finance_direct_breakdowns"))
    ).toBe(true);
  });

  it("uses shared AI industry classification for high-value unknown direct employers", async () => {
    const db = {
      query: vi.fn().mockImplementation(async (sql: string) => {
        if (String(sql).includes("FROM public.finance_label_classifications AS classification")) {
          return { rows: [], rowCount: 0 };
        }
        return { rows: [{ id: LINK_ID }], rowCount: 1 };
      }),
    };
    const financeIndustryClassifier = vi.fn().mockResolvedValue([
      {
        rawLabel: "Acme Quantum Labs LLC",
        labelType: "employer",
        normalizedLabel: "ACME QUANTUM LABS",
        industrySlug: "technology",
        confidence: "medium",
        classificationSource: "ai",
        matchedRule: null,
      },
    ]);

    const result = await syncCaliforniaCandidateFinance({
      db,
      ...baseInput(),
      includeOutside: false,
      directReceiptRows: [
        receipt({
          CTRIB_EMP: "Acme Quantum Labs LLC",
          CTRIB_OCC: "Founder",
          AMOUNT: "150000.00",
        }),
      ],
      financeIndustryClassifier,
    });

    expect(financeIndustryClassifier).toHaveBeenCalledWith({
      labels: [
        {
          rawLabel: "Acme Quantum Labs LLC",
          labelType: "employer",
          normalizedLabel: "ACME QUANTUM LABS",
          amount: 150_000,
        },
      ],
    });
    expect(result.directBreakdownsWritten).toBe(4);

    const directBreakdownCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.ca_candidate_finance_direct_breakdowns")
    );
    expect(directBreakdownCalls.map((call) => call[1]?.slice(2, 6))).toEqual(
      expect.arrayContaining([
        ["employer", "Acme Quantum Labs LLC", 150_000, 1],
        ["industry", "technology", 150_000, 1],
      ])
    );

    const classificationCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.finance_label_classifications")
    );
    expect(classificationCalls.map((call) => call[1]?.slice(0, 6))).toEqual(
      expect.arrayContaining([
        ["Acme Quantum Labs LLC", "employer", "ACME QUANTUM LABS", "technology", "medium", "ai"],
      ])
    );
  });

  it("validates inputs before fetching or writing", async () => {
    const db = createMockDb();
    const powerSearchClient = createPowerSearchClient();

    await expect(
      syncCaliforniaCandidateFinance({
        db,
        ...baseInput(),
        controlledCommitteeName: " ",
        powerSearchClient,
      })
    ).rejects.toThrow("California controlled committee name is required");

    expect(powerSearchClient.summarizeIndependentSpendingByCandidate).not.toHaveBeenCalled();
    expect(db.query).not.toHaveBeenCalled();
  });
});
