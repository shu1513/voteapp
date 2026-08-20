import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

import {
  parseManualCandidateFinancePayload,
  type ManualCandidateFinancePayload,
} from "../../../src/contracts/manualCandidateFinancePayloadContract.js";
import { compileManualCandidateFinancePreview } from "../../../src/pipeline/finance/manualCandidateFinancePreview.js";

function loadRawFixture(name: string): Record<string, unknown> {
  const path = fileURLToPath(new URL(`../../fixtures/manualCandidateFinance/${name}`, import.meta.url));
  return JSON.parse(readFileSync(path, "utf8")) as Record<string, unknown>;
}

function loadFixture(name: string): ManualCandidateFinancePayload {
  const parsed = parseManualCandidateFinancePayload(loadRawFixture(name));
  if (!parsed.ok) {
    throw new Error(parsed.reason);
  }
  return parsed.payload;
}

function candidate(preview: ReturnType<typeof compileManualCandidateFinancePreview>, name: string) {
  const found = preview.candidates.find((entry) => entry.candidateName === name);
  if (!found) {
    throw new Error(`Missing preview candidate ${name}`);
  }
  return found;
}

describe("compileManualCandidateFinancePreview", () => {
  it("compiles all three real Mississippi fixtures without guessing unallocated IE amounts", () => {
    const preview = compileManualCandidateFinancePreview([
      loadFixture("ms_candidate_report_presley_2023.json"),
      loadFixture("ms_ie_single_target_griffis_2020.json"),
      loadFixture("ms_ie_multi_target_house_22_2025.json"),
    ]);

    expect(preview).toMatchObject({
      schemaVersion: "manual_candidate_finance.v1",
      state: "MS",
      inputFilingCount: 3,
      uniqueFilingCount: 3,
    });
    expect(preview.candidates).toHaveLength(4);

    const presley = candidate(preview, "Brandon Presley");
    expect(presley.selectedCandidateReport).toMatchObject({
      reportDate: "2023-08-01",
      reportedTotals: { contributions_calendar_ytd: 3403383.7 },
      occupationBreakdowns: [{ categoryName: "Retired", amount: 500, receiptCount: 1 }],
      employerBreakdowns: [{ categoryName: "Not Employed", amount: 500, receiptCount: 1 }],
    });
    expect(presley.warnings.map((warning) => warning.code)).toContain("latest_report_receipts_only");

    const griffis = candidate(preview, "Justice Kenny Griffis");
    expect(griffis.outsideSpending).toMatchObject({
      supportTotal: null,
      opposeTotal: null,
      knownAllocatedSupportAmount: 0,
      groups: [],
    });
    expect(griffis.outsideSpending.unallocatedEdges).toHaveLength(1);
    expect(griffis.warnings.map((warning) => warning.code)).toContain("unallocated_outside_spending");

    expect(candidate(preview, "Jen Lancaster").outsideSpending.supportTotal).toBeNull();
    expect(candidate(preview, "Justin Crosby").outsideSpending.opposeTotal).toBeNull();
  });

  it("publishes a direction total only when every edge in that direction has an explicit amount", () => {
    const raw = loadRawFixture("ms_ie_single_target_griffis_2020.json") as {
      candidate_edges: Array<Record<string, unknown>>;
    };
    raw.candidate_edges[0]!.amount = 69921.75;
    const parsed = parseManualCandidateFinancePayload(raw);
    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }

    const griffis = candidate(compileManualCandidateFinancePreview([parsed.payload]), "Justice Kenny Griffis");

    expect(griffis.outsideSpending).toMatchObject({
      supportTotal: 69921.75,
      opposeTotal: null,
      knownAllocatedSupportAmount: 69921.75,
      unallocatedEdges: [],
      groups: [
        {
          sourceEntityId: "c418043d-9201-479c-b599-defd9387b0cf",
          name: "Improve Mississippi PAC",
          supportOppose: "support",
          amount: 69921.75,
        },
      ],
    });
    expect(griffis.warnings.map((warning) => warning.code)).not.toContain("unallocated_outside_spending");
  });

  it("keeps a direction total null when known allocations coexist with an unresolved edge", () => {
    const allocatedRaw = loadRawFixture("ms_ie_single_target_griffis_2020.json") as {
      candidate_edges: Array<Record<string, unknown>>;
    };
    allocatedRaw.candidate_edges[0]!.amount = 69921.75;
    const allocated = parseManualCandidateFinancePayload(allocatedRaw);

    const unresolvedRaw = loadRawFixture("ms_ie_multi_target_house_22_2025.json") as {
      candidate_edges: Array<Record<string, unknown>>;
    };
    Object.assign(unresolvedRaw.candidate_edges[0]!, {
      candidate_id: "33333333-3333-4333-8333-333333333333",
      election_id: "44444444-4444-4444-8444-444444444444",
      candidate_name: "Justice Kenny Griffis",
    });
    const unresolved = parseManualCandidateFinancePayload(unresolvedRaw);
    expect(allocated.ok && unresolved.ok).toBe(true);
    if (!allocated.ok || !unresolved.ok) {
      return;
    }

    const griffis = candidate(
      compileManualCandidateFinancePreview([allocated.payload, unresolved.payload]),
      "Justice Kenny Griffis"
    );

    expect(griffis.outsideSpending.supportTotal).toBeNull();
    expect(griffis.outsideSpending.knownAllocatedSupportAmount).toBe(69921.75);
    expect(griffis.outsideSpending.groups).toEqual([
      expect.objectContaining({ supportOppose: "support", amount: 69921.75 }),
    ]);
    expect(griffis.outsideSpending.unallocatedEdges).toHaveLength(1);
  });

  it("deduplicates identical filing payloads and rejects conflicting reuse of a filing ID", () => {
    const report = loadFixture("ms_candidate_report_presley_2023.json");
    const deduplicated = compileManualCandidateFinancePreview([report, report]);
    expect(deduplicated.inputFilingCount).toBe(2);
    expect(deduplicated.uniqueFilingCount).toBe(1);

    expect(() =>
      compileManualCandidateFinancePreview([
        report,
        { ...report, candidate_name: "Conflicting Name" },
      ])
    ).toThrow(`Conflicting manual candidate-finance payloads share filing_id ${report.filing_id}`);
  });

  it("selects the unique latest candidate cover without summing calendar-YTD totals", () => {
    const earlier = loadFixture("ms_candidate_report_presley_2023.json");
    if (earlier.filing_type !== "candidate_report") {
      throw new Error("Expected candidate fixture");
    }
    const later = {
      ...earlier,
      filing_id: "0128a36a-d463-459b-9f8e-2657fc7697df",
      report_date: "2024-01-30",
      source_url:
        "https://cfportal.sos.ms.gov/online/ExecuteWorkflow.aspx?WorkflowId=g729911d7-f399-46d6-a1ca-f15c1294f82d&FilingId=0128A36A-D463-459B-9F8E-2657FC7697DF",
      reported_totals: {
        ...earlier.reported_totals,
        contributions_calendar_ytd: 25,
        disbursements_calendar_ytd: 10,
      },
      itemized_receipts: [],
    } satisfies typeof earlier;

    const presley = candidate(compileManualCandidateFinancePreview([earlier, later]), "Brandon Presley");

    expect(presley.selectedCandidateReport?.filingId).toBe(later.filing_id);
    expect(presley.selectedCandidateReport?.reportedTotals.contributions_calendar_ytd).toBe(25);
    expect(presley.warnings.map((warning) => warning.code)).toContain("latest_report_receipts_only");
  });

  it("fails closed when amendment order is unknowable for same-date candidate reports", () => {
    const first = loadFixture("ms_candidate_report_presley_2023.json");
    if (first.filing_type !== "candidate_report") {
      throw new Error("Expected candidate fixture");
    }
    const second = {
      ...first,
      filing_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      source_url:
        "https://cfportal.sos.ms.gov/online/ExecuteWorkflow.aspx?WorkflowId=g729911d7-f399-46d6-a1ca-f15c1294f82d&FilingId=AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
    } satisfies typeof first;

    const presley = candidate(compileManualCandidateFinancePreview([first, second]), "Brandon Presley");

    expect(presley.selectedCandidateReport).toBeNull();
    expect(presley.warnings).toEqual([
      expect.objectContaining({ code: "ambiguous_latest_candidate_report" }),
    ]);
  });

  it("combines category spelling variants with cent-safe receipt arithmetic", () => {
    const raw = loadRawFixture("ms_candidate_report_presley_2023.json") as {
      itemized_receipts: Array<Record<string, unknown>>;
    };
    raw.itemized_receipts.push({
      received_date: "2023-07-29",
      amount: 25.25,
      occupation: " retired ",
      employer: "NOT EMPLOYED",
    });
    const parsed = parseManualCandidateFinancePayload(raw);
    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }

    const report = candidate(compileManualCandidateFinancePreview([parsed.payload]), "Brandon Presley")
      .selectedCandidateReport;

    expect(report?.occupationBreakdowns).toEqual([
      { categoryName: "Retired", amount: 525.25, receiptCount: 2 },
    ]);
    expect(report?.employerBreakdowns).toEqual([
      { categoryName: "Not Employed", amount: 525.25, receiptCount: 2 },
    ]);
  });

  it("uses an IE amendment instead of adding it to the superseded filing", () => {
    const base = loadFixture("ms_ie_single_target_griffis_2020.json");
    if (base.filing_type !== "independent_expenditure") {
      throw new Error("Expected IE fixture");
    }
    const original = {
      ...base,
      candidate_edges: base.candidate_edges.map((edge) => ({ ...edge, amount: 69921.75 })),
    };
    const amendment = {
      ...original,
      filing_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      amends_filing_id: original.filing_id,
      source_url:
        "https://cfportal.sos.ms.gov/online/ExecuteWorkflow.aspx?WorkflowId=g729911d7-f399-46d6-a1ca-f15c1294f82d&FilingId=AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
      candidate_edges: original.candidate_edges.map((edge) => ({ ...edge, amount: 60000 })),
    };

    const griffis = candidate(
      compileManualCandidateFinancePreview([original, amendment]),
      "Justice Kenny Griffis"
    );

    expect(griffis.outsideSpending.supportTotal).toBe(60000);
    expect(griffis.outsideSpending.groups).toEqual([
      expect.objectContaining({ amount: 60000 }),
    ]);
  });

  it("fails closed when two filings claim to amend the same IE filing", () => {
    const base = loadFixture("ms_ie_single_target_griffis_2020.json");
    if (base.filing_type !== "independent_expenditure") {
      throw new Error("Expected IE fixture");
    }
    const firstAmendment = {
      ...base,
      filing_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      amends_filing_id: base.filing_id,
    };
    const secondAmendment = {
      ...base,
      filing_id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
      amends_filing_id: base.filing_id,
    };

    expect(() =>
      compileManualCandidateFinancePreview([base, firstAmendment, secondAmendment])
    ).toThrow("Ambiguous manual candidate-finance amendment order");
  });

  it("groups spender name variants by stable source entity ID", () => {
    const first = loadFixture("ms_ie_single_target_griffis_2020.json");
    if (first.filing_type !== "independent_expenditure") {
      throw new Error("Expected IE fixture");
    }
    const firstAllocated = {
      ...first,
      candidate_edges: first.candidate_edges.map((edge) => ({ ...edge, amount: 100 })),
    };
    const second = {
      ...firstAllocated,
      filing_id: "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
      source_url:
        "https://cfportal.sos.ms.gov/online/ExecuteWorkflow.aspx?WorkflowId=g729911d7-f399-46d6-a1ca-f15c1294f82d&FilingId=CCCCCCCC-CCCC-4CCC-8CCC-CCCCCCCCCCCC",
      outside_spender: {
        ...firstAllocated.outside_spender,
        name: "Improve Mississippi Political Action Committee",
      },
      candidate_edges: firstAllocated.candidate_edges.map((edge) => ({ ...edge, amount: 50 })),
    };

    const groups = candidate(
      compileManualCandidateFinancePreview([firstAllocated, second]),
      "Justice Kenny Griffis"
    ).outsideSpending.groups;

    expect(groups).toEqual([
      {
        sourceEntityId: first.outside_spender.source_entity_id,
        name: "Improve Mississippi PAC",
        supportOppose: "support",
        amount: 150,
      },
    ]);
  });
});
