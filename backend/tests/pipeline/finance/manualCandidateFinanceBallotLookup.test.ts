import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, it, vi } from "vitest";

import {
  parseManualCandidateFinancePayload,
  type ManualCandidateFinanceIndependentExpenditurePayload,
  type ManualCandidateFinancePayload,
} from "../../../src/contracts/manualCandidateFinancePayloadContract.js";
import { candidateElectionKey } from "../../../src/pipeline/address/ballotLookupFinanceShared.js";
import { loadManualCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/finance/manualCandidateFinanceBallotLookup.js";
import { manualCandidateFinancePayloadSha256 } from "../../../src/pipeline/finance/manualCandidateFinancePersistence.js";

function loadFixture(name: string): ManualCandidateFinancePayload {
  const path = fileURLToPath(new URL(`../../fixtures/manualCandidateFinance/${name}`, import.meta.url));
  const parsed = parseManualCandidateFinancePayload(JSON.parse(readFileSync(path, "utf8")) as unknown);
  if (!parsed.ok) {
    throw new Error(parsed.reason);
  }
  return parsed.payload;
}

function dbWithPayloads(payloads: readonly ManualCandidateFinancePayload[]) {
  return {
    query: vi.fn(async (sql: unknown) => {
      expect(String(sql)).toContain("FROM public.manual_candidate_finance_filings");
      return {
        rows: payloads.map((payload) => ({
          filing_id: payload.filing_id,
          payload,
          payload_sha256: manualCandidateFinancePayloadSha256(payload),
        })),
        rowCount: payloads.length,
      };
    }),
  };
}

describe("loadManualCandidateFinanceSummariesByCandidateElection", () => {
  it("does not query the manual ledger when no Mississippi election was requested", async () => {
    const db = { query: vi.fn() };

    await expect(
      loadManualCandidateFinanceSummariesByCandidateElection(
        db as never,
        [{ candidate_id: "11111111-1111-4111-8111-111111111111", election_id: "election-ca" }],
        [{ election_id: "election-ca", state: "CA", election_date: "2026-11-03" }]
      )
    ).resolves.toEqual(new Map());
    expect(db.query).not.toHaveBeenCalled();
  });

  it("maps nullable cover totals, receipt occupations/employers, and allocated outside spending", async () => {
    const report = loadFixture("ms_candidate_report_presley_2023.json");
    const outside = loadFixture("ms_ie_single_target_griffis_2020.json");
    if (report.filing_type !== "candidate_report" || outside.filing_type !== "independent_expenditure") {
      throw new Error("Unexpected fixture types");
    }
    const allocatedOutside: ManualCandidateFinanceIndependentExpenditurePayload = {
      ...outside,
      candidate_edges: [
        {
          candidate_id: report.candidate_id,
          election_id: report.election_id,
          candidate_name: report.candidate_name,
          support_oppose: "support",
          amount: 1234.56,
        },
      ],
    };
    const db = dbWithPayloads([report, allocatedOutside]);

    const summaries = await loadManualCandidateFinanceSummariesByCandidateElection(
      db as never,
      [{ candidate_id: report.candidate_id, election_id: report.election_id }],
      [{ election_id: report.election_id, state: "MS", election_date: "2023-11-07" }]
    );

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(summaries.get(candidateElectionKey(report.candidate_id, report.election_id))).toEqual({
      source: "MISSISSIPPI_SOS",
      cycle: 2023,
      fec_candidate_id: null,
      controlled_committee_id: null,
      last_synced_at: "2026-08-19T19:10:00Z",
      direct_campaign: {
        total_raised: 3403383.7,
        total_spent: 2585071.52,
        cash_on_hand: 1545844.72,
        debts_owed: null,
        top_occupations: [
          {
            category_name: "Retired",
            amount: 500,
            contributor_count: null,
            source_url: report.source_url,
          },
        ],
        top_employers: [
          {
            category_name: "Not Employed",
            amount: 500,
            contributor_count: null,
            source_url: report.source_url,
          },
        ],
        top_industries: [],
        direct_coverage_note: expect.stringContaining("an empty breakdown does not prove zero receipts"),
      },
      outside_spending: {
        support_total: 1234.56,
        oppose_total: null,
        outside_coverage_note: expect.not.stringContaining("remain null"),
        top_supporting_groups: [
          {
            committee_id: allocatedOutside.outside_spender.source_entity_id,
            committee_name: "Improve Mississippi PAC",
            support_oppose: "support",
            amount: 1234.56,
            source_url: allocatedOutside.source_url,
          },
        ],
        top_opposing_groups: [],
        unallocated_candidate_edges: [],
        top_supporting_industries: [],
        top_opposing_industries: [],
      },
      backing_summary: {
        top_direct_donor_occupations: [
          {
            category_name: "Retired",
            amount: 500,
            contributor_count: null,
            source_url: report.source_url,
          },
        ],
        top_outside_supporting_industries: [],
      },
    });
  });

  it("does not cite one partial filing for an outside-group aggregate", async () => {
    const report = loadFixture("ms_candidate_report_presley_2023.json");
    const outside = loadFixture("ms_ie_single_target_griffis_2020.json");
    if (report.filing_type !== "candidate_report" || outside.filing_type !== "independent_expenditure") {
      throw new Error("Unexpected fixture types");
    }
    const first: ManualCandidateFinanceIndependentExpenditurePayload = {
      ...outside,
      candidate_edges: [
        {
          candidate_id: report.candidate_id,
          election_id: report.election_id,
          candidate_name: report.candidate_name,
          support_oppose: "support",
          amount: 1234.56,
        },
      ],
    };
    const second: ManualCandidateFinanceIndependentExpenditurePayload = {
      ...first,
      filing_id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
      source_url:
        "https://cfportal.sos.ms.gov/online/ExecuteWorkflow.aspx?" +
        "WorkflowId=g729911d7-f399-46d6-a1ca-f15c1294f82d&FilingId=bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
      report_date: "2020-10-28",
      researched_at: "2026-08-20T21:00:00Z",
      candidate_edges: [{ ...first.candidate_edges[0]!, amount: 100 }],
    };
    const db = dbWithPayloads([report, first, second]);

    const summaries = await loadManualCandidateFinanceSummariesByCandidateElection(
      db as never,
      [{ candidate_id: report.candidate_id, election_id: report.election_id }],
      [{ election_id: report.election_id, state: "MS", election_date: "2023-11-07" }]
    );

    expect(
      summaries.get(candidateElectionKey(report.candidate_id, report.election_id))?.outside_spending
        .top_supporting_groups
    ).toEqual([
      expect.objectContaining({
        amount: 1334.56,
        source_url: null,
      }),
    ]);
  });

  it("keeps unallocated multi-candidate outside totals null", async () => {
    const jon = loadFixture("ms_hd22_jon_lancaster_2025_pre_election.json");
    const justin = loadFixture("ms_hd22_justin_crosby_2025_pre_election.json");
    const outside = loadFixture("ms_ie_multi_target_house_22_2025.json");
    if (
      jon.filing_type !== "candidate_report" ||
      justin.filing_type !== "candidate_report" ||
      outside.filing_type !== "independent_expenditure"
    ) {
      throw new Error("Unexpected fixture types");
    }
    const db = dbWithPayloads([jon, justin, outside]);

    const summaries = await loadManualCandidateFinanceSummariesByCandidateElection(
      db as never,
      [
        { candidate_id: jon.candidate_id, election_id: jon.election_id },
        { candidate_id: justin.candidate_id, election_id: justin.election_id },
      ],
      [{ election_id: jon.election_id, state: "MS", election_date: "2025-11-04" }]
    );

    expect(summaries).toHaveLength(2);
    expect(summaries.get(candidateElectionKey(jon.candidate_id, jon.election_id))).toMatchObject({
      direct_campaign: { total_raised: 47052, total_spent: 41826.12 },
      outside_spending: {
        support_total: null,
        oppose_total: null,
        top_supporting_groups: [],
        unallocated_candidate_edges: [
          {
            filing_id: outside.filing_id,
            report_date: "2025-10-28",
            committee_id: outside.outside_spender.source_entity_id,
            committee_name: "Improve Mississippi PAC",
            support_oppose: "support",
            source_url: outside.source_url,
          },
        ],
        outside_coverage_note: expect.stringContaining("remain null"),
      },
    });
    expect(summaries.get(candidateElectionKey(justin.candidate_id, justin.election_id))).toMatchObject({
      direct_campaign: { total_raised: 38341.11, total_spent: 24222.41 },
      outside_spending: {
        support_total: null,
        oppose_total: null,
        top_opposing_groups: [],
        unallocated_candidate_edges: [
          expect.objectContaining({
            filing_id: outside.filing_id,
            support_oppose: "oppose",
          }),
        ],
        outside_coverage_note: expect.stringContaining("remain null"),
      },
    });
  });

  it("does not revive a candidate edge removed by an amendment", async () => {
    const original = loadFixture("ms_ie_single_target_griffis_2020.json");
    if (original.filing_type !== "independent_expenditure") {
      throw new Error("Unexpected fixture type");
    }
    const replacementCandidateId = "99999999-9999-4999-8999-999999999999";
    const amendmentId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    const amendment: ManualCandidateFinanceIndependentExpenditurePayload = {
      ...original,
      filing_id: amendmentId,
      amends_filing_id: original.filing_id,
      source_url:
        "https://cfportal.sos.ms.gov/online/ExecuteWorkflow.aspx?" +
        `WorkflowId=g729911d7-f399-46d6-a1ca-f15c1294f82d&FilingId=${amendmentId}`,
      researched_at: "2026-08-20T20:00:00Z",
      candidate_edges: [
        {
          candidate_id: replacementCandidateId,
          election_id: original.candidate_edges[0]!.election_id,
          candidate_name: "Replacement Candidate",
          support_oppose: "support",
          amount: null,
        },
      ],
    };
    const electionId = original.candidate_edges[0]!.election_id;
    const originalCandidateId = original.candidate_edges[0]!.candidate_id;
    const db = dbWithPayloads([original, amendment]);

    const summaries = await loadManualCandidateFinanceSummariesByCandidateElection(
      db as never,
      [
        { candidate_id: originalCandidateId, election_id: electionId },
        { candidate_id: replacementCandidateId, election_id: electionId },
      ],
      [{ election_id: electionId, state: "MS", election_date: "2020-11-03" }]
    );

    expect(summaries.has(candidateElectionKey(originalCandidateId, electionId))).toBe(false);
    expect(summaries.get(candidateElectionKey(replacementCandidateId, electionId))).toMatchObject({
      source: "MISSISSIPPI_SOS",
      outside_spending: { support_total: null },
    });
  });
});
