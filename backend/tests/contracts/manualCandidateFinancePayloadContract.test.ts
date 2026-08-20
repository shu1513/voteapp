import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

import {
  MISSISSIPPI_SOS_FINANCE_COVERAGE_NOTE,
  parseManualCandidateFinancePayload,
} from "../../src/contracts/manualCandidateFinancePayloadContract.js";

function loadFixture(name: string): Record<string, unknown> {
  const path = fileURLToPath(new URL(`../fixtures/manualCandidateFinance/${name}`, import.meta.url));
  return JSON.parse(readFileSync(path, "utf8")) as Record<string, unknown>;
}

describe("parseManualCandidateFinancePayload", () => {
  it("parses the sanitized real Mississippi candidate report", () => {
    const parsed = parseManualCandidateFinancePayload(
      loadFixture("ms_candidate_report_presley_2023.json")
    );

    expect(parsed.ok).toBe(true);
    if (!parsed.ok || parsed.payload.filing_type !== "candidate_report") {
      return;
    }
    expect(parsed.payload.candidate_name).toBe("Brandon Presley");
    expect(parsed.payload.reported_totals).toEqual({
      contributions_this_period: 1128863.93,
      contributions_calendar_ytd: 3403383.7,
      disbursements_this_period: 1465725.46,
      disbursements_calendar_ytd: 2585071.52,
      cash_on_hand: 1545844.72,
      debts_owed: null,
    });
    expect(parsed.payload.itemized_receipts).toEqual([
      {
        received_date: "2023-07-28",
        amount: 500,
        occupation: "Retired",
        employer: "Not Employed",
      },
    ]);
    expect(parsed.payload.coverage_note).toBe(MISSISSIPPI_SOS_FINANCE_COVERAGE_NOTE);
  });

  it("parses a true single-target Mississippi IE filing without inventing an edge amount", () => {
    const parsed = parseManualCandidateFinancePayload(
      loadFixture("ms_ie_single_target_griffis_2020.json")
    );

    expect(parsed.ok).toBe(true);
    if (!parsed.ok || parsed.payload.filing_type !== "independent_expenditure") {
      return;
    }
    expect(parsed.payload.outside_spender.name).toBe("Improve Mississippi PAC");
    expect(parsed.payload.candidate_edges).toEqual([
      {
        candidate_id: "33333333-3333-4333-8333-333333333333",
        election_id: "44444444-4444-4444-8444-444444444444",
        candidate_name: "Justice Kenny Griffis",
        support_oppose: "support",
        amount: null,
      },
    ]);
  });

  it("preserves both targets and both directions in one Mississippi IE filing", () => {
    const parsed = parseManualCandidateFinancePayload(
      loadFixture("ms_ie_multi_target_house_22_2025.json")
    );

    expect(parsed.ok).toBe(true);
    if (!parsed.ok || parsed.payload.filing_type !== "independent_expenditure") {
      return;
    }
    expect(parsed.payload.candidate_edges.map((edge) => [edge.candidate_name, edge.support_oppose])).toEqual([
      ["Jon Lancaster", "support"],
      ["Justin Crosby", "oppose"],
    ]);
    expect(parsed.payload.candidate_edges.every((edge) => edge.amount === null)).toBe(true);
    expect(parsed.payload.reported_totals.disbursements_this_period).toBe(6261);
  });

  it("parses both candidate reports in the House District 22 acceptance cohort", () => {
    const jon = parseManualCandidateFinancePayload(
      loadFixture("ms_hd22_jon_lancaster_2025_pre_election.json")
    );
    const justin = parseManualCandidateFinancePayload(
      loadFixture("ms_hd22_justin_crosby_2025_pre_election.json")
    );

    expect(jon.ok).toBe(true);
    expect(justin.ok).toBe(true);
    if (!jon.ok || !justin.ok) {
      return;
    }
    expect(jon.payload).toMatchObject({
      filing_type: "candidate_report",
      candidate_name: "Jon Lancaster",
      election_id: "77777777-7777-4777-8777-777777777777",
      reported_totals: {
        contributions_calendar_ytd: 47052,
        disbursements_calendar_ytd: 41826.12,
        cash_on_hand: 15216.53,
        debts_owed: null,
      },
    });
    expect(justin.payload).toMatchObject({
      filing_type: "candidate_report",
      candidate_name: "Justin Crosby",
      election_id: "77777777-7777-4777-8777-777777777777",
      reported_totals: {
        contributions_calendar_ytd: 38341.11,
        disbursements_calendar_ytd: 24222.41,
        cash_on_hand: 14118.70,
        debts_owed: null,
      },
    });
  });

  it("requires every total and tells researchers to use null for missing values", () => {
    const payload = loadFixture("ms_candidate_report_presley_2023.json") as {
      reported_totals: Record<string, unknown>;
    };
    delete payload.reported_totals.debts_owed;

    const parsed = parseManualCandidateFinancePayload(payload);

    expect(parsed.ok).toBe(false);
    if (!parsed.ok) {
      expect(parsed.reason).toBe(
        "payload.reported_totals.debts_owed is required; use null when the filing does not report it"
      );
    }
  });

  it("keeps explicit null distinct from an explicitly reported zero", () => {
    const nullPayload = loadFixture("ms_candidate_report_presley_2023.json");
    const parsedNull = parseManualCandidateFinancePayload(nullPayload);
    expect(parsedNull.ok).toBe(true);
    if (parsedNull.ok) {
      expect(parsedNull.payload.reported_totals.debts_owed).toBeNull();
    }

    const zeroPayload = loadFixture("ms_candidate_report_presley_2023.json") as {
      reported_totals: Record<string, unknown>;
    };
    zeroPayload.reported_totals.debts_owed = 0;
    const parsedZero = parseManualCandidateFinancePayload(zeroPayload);
    expect(parsedZero.ok).toBe(true);
    if (parsedZero.ok) {
      expect(parsedZero.payload.reported_totals.debts_owed).toBe(0);
    }
  });

  it("requires occupation and employer as separate nullable fields", () => {
    const payload = loadFixture("ms_candidate_report_presley_2023.json") as {
      itemized_receipts: Array<Record<string, unknown>>;
    };
    payload.itemized_receipts[0]!.occupation = null;
    payload.itemized_receipts[0]!.employer = "University of Mississippi";

    const parsed = parseManualCandidateFinancePayload(payload);

    expect(parsed.ok).toBe(true);
    if (parsed.ok) {
      expect(parsed.payload.itemized_receipts[0]).toMatchObject({
        occupation: null,
        employer: "University of Mississippi",
      });
    }
  });

  it("requires null rather than omission for an unallocated candidate-edge amount", () => {
    const payload = loadFixture("ms_ie_multi_target_house_22_2025.json") as {
      candidate_edges: Array<Record<string, unknown>>;
    };
    delete payload.candidate_edges[0]!.amount;

    const parsed = parseManualCandidateFinancePayload(payload);

    expect(parsed.ok).toBe(false);
    if (!parsed.ok) {
      expect(parsed.reason).toBe(
        "payload.candidate_edges[0].amount is required; use null when the filing does not allocate it"
      );
    }
  });

  it("rejects a source URL whose filing ID does not match the payload", () => {
    const payload = loadFixture("ms_ie_single_target_griffis_2020.json");
    payload.source_url =
      "https://cfportal.sos.ms.gov/online/ExecuteWorkflow.aspx?WorkflowId=g729911d7-f399-46d6-a1ca-f15c1294f82d&FilingId=D2EE3D0C-08D1-4E87-9959-34ADADDEBA0C";

    const parsed = parseManualCandidateFinancePayload(payload);

    expect(parsed.ok).toBe(false);
    if (!parsed.ok) {
      expect(parsed.reason).toContain("FilingId matches payload.filing_id");
    }
  });

  it("requires canonical UUID-shaped VoteApp candidate and election IDs", () => {
    const payload = loadFixture("ms_candidate_report_presley_2023.json");
    payload.candidate_id = "candidate-1";

    const parsed = parseManualCandidateFinancePayload(payload);

    expect(parsed.ok).toBe(false);
    if (!parsed.ok) {
      expect(parsed.reason).toBe("payload.candidate_id must be a UUID");
    }
  });

  it("requires a timezone-bearing research timestamp", () => {
    const payload = loadFixture("ms_ie_single_target_griffis_2020.json");
    payload.researched_at = "2026-08-19";

    const parsed = parseManualCandidateFinancePayload(payload);

    expect(parsed.ok).toBe(false);
    if (!parsed.ok) {
      expect(parsed.reason).toBe("payload.researched_at must be an ISO timestamp with a timezone");
    }
  });

  it("rejects money with sub-cent precision", () => {
    const payload = loadFixture("ms_candidate_report_presley_2023.json") as {
      reported_totals: Record<string, unknown>;
    };
    payload.reported_totals.cash_on_hand = 1.001;

    const parsed = parseManualCandidateFinancePayload(payload);

    expect(parsed.ok).toBe(false);
    if (!parsed.ok) {
      expect(parsed.reason).toContain("at most two decimals");
    }
  });

  it("requires explicit amendment status and rejects self-reference", () => {
    const missing = loadFixture("ms_ie_single_target_griffis_2020.json");
    delete missing.amends_filing_id;
    const missingParsed = parseManualCandidateFinancePayload(missing);
    expect(missingParsed.ok).toBe(false);
    if (!missingParsed.ok) {
      expect(missingParsed.reason).toContain("use null only after verifying");
    }

    const selfAmending = loadFixture("ms_ie_single_target_griffis_2020.json");
    selfAmending.amends_filing_id = selfAmending.filing_id;
    const selfParsed = parseManualCandidateFinancePayload(selfAmending);
    expect(selfParsed.ok).toBe(false);
    if (!selfParsed.ok) {
      expect(selfParsed.reason).toContain("must not equal payload.filing_id");
    }
  });

  it("rejects candidate allocations above known filing disbursements", () => {
    const payload = loadFixture("ms_ie_single_target_griffis_2020.json") as {
      candidate_edges: Array<Record<string, unknown>>;
    };
    payload.candidate_edges[0]!.amount = 999999.99;

    const parsed = parseManualCandidateFinancePayload(payload);

    expect(parsed.ok).toBe(false);
    if (!parsed.ok) {
      expect(parsed.reason).toContain("must not exceed payload.reported_totals.disbursements_this_period");
    }
  });
});
