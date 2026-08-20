import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, it, vi } from "vitest";

import {
  parseManualCandidateFinancePayload,
  type ManualCandidateFinancePayload,
} from "../../../src/contracts/manualCandidateFinancePayloadContract.js";
import {
  buildManualCandidateFinanceFilingRecords,
  manualCandidateFinancePayloadSha256,
  persistManualCandidateFinanceFilings,
  planManualCandidateFinanceImport,
} from "../../../src/pipeline/finance/manualCandidateFinancePersistence.js";

function loadFixture(name: string): ManualCandidateFinancePayload {
  const path = fileURLToPath(new URL(`../../fixtures/manualCandidateFinance/${name}`, import.meta.url));
  const parsed = parseManualCandidateFinancePayload(JSON.parse(readFileSync(path, "utf8")) as unknown);
  if (!parsed.ok) {
    throw new Error(parsed.reason);
  }
  return parsed.payload;
}

function candidateRows(values: unknown[] | undefined) {
  const candidateIds = values?.[0] as string[];
  const electionIds = values?.[1] as string[];
  return candidateIds.map((candidateId, index) => ({
    candidate_id: candidateId,
    election_id: electionIds[index]!,
    candidate_exists: true,
    candidate_deleted: false,
    candidate_merged: false,
    candidate_state: "MS",
    candidate_name: candidateId.startsWith("5555") ? "Jon Lancaster" : "Justin Crosby",
    election_exists: true,
    district_state: "MS",
    linked: true,
  }));
}

function validPlanningDb(storedRows: unknown[] = []) {
  return {
    query: vi.fn((sql: unknown, values?: unknown[]) => {
      const statement = String(sql);
      if (statement.includes("FROM public.manual_candidate_finance_filings")) {
        return Promise.resolve({ rows: storedRows, rowCount: storedRows.length });
      }
      if (statement.includes("WITH requested(candidate_id, election_id)")) {
        const rows = candidateRows(values);
        return Promise.resolve({ rows, rowCount: rows.length });
      }
      throw new Error(`Unexpected SQL: ${statement}`);
    }),
  };
}

describe("manual candidate-finance persistence", () => {
  it("builds lossless filing records and keeps nullable multi-target IE allocations nullable", () => {
    const payload = loadFixture("ms_ie_multi_target_house_22_2025.json");
    const [record] = buildManualCandidateFinanceFilingRecords([payload]);

    expect(record?.payload).toEqual(payload);
    expect(record?.payloadSha256).toMatch(/^[0-9a-f]{64}$/);
    expect(record?.targets).toEqual([
      expect.objectContaining({
        candidateName: "Jon Lancaster",
        relationship: "support",
        amount: null,
      }),
      expect.objectContaining({
        candidateName: "Justin Crosby",
        relationship: "oppose",
        amount: null,
      }),
    ]);
  });

  it("treats an exact existing filing as unchanged without revalidating its target", async () => {
    const payload = loadFixture("ms_hd22_jon_lancaster_2025_pre_election.json");
    const db = validPlanningDb([
      {
        filing_id: payload.filing_id,
        payload,
        payload_sha256: manualCandidateFinancePayloadSha256(payload),
      },
    ]);

    await expect(planManualCandidateFinanceImport({ db: db as never, payloads: [payload] })).resolves.toMatchObject({
      insertFilingIds: [],
      unchangedFilingIds: [payload.filing_id],
      targetRowCount: 0,
    });
    expect(db.query).toHaveBeenCalledTimes(1);
  });

  it("rejects reuse of a filing ID with different content", async () => {
    const incoming = loadFixture("ms_hd22_jon_lancaster_2025_pre_election.json");
    const stored = { ...incoming, candidate_name: "Different Candidate" };
    const db = validPlanningDb([
      {
        filing_id: incoming.filing_id,
        payload: stored,
        payload_sha256: manualCandidateFinancePayloadSha256(stored),
      },
    ]);

    await expect(
      planManualCandidateFinanceImport({ db: db as never, payloads: [incoming] })
    ).rejects.toThrow(`filing_id ${incoming.filing_id} already exists with different content`);
  });

  it("fails closed when an amendment's parent is absent", async () => {
    const base = loadFixture("ms_ie_single_target_griffis_2020.json");
    if (base.filing_type !== "independent_expenditure") {
      throw new Error("Expected IE fixture");
    }
    const amendment = {
      ...base,
      filing_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      amends_filing_id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
      source_url:
        "https://cfportal.sos.ms.gov/online/ExecuteWorkflow.aspx?WorkflowId=g729911d7-f399-46d6-a1ca-f15c1294f82d&FilingId=AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
    } satisfies ManualCandidateFinancePayload;
    const db = validPlanningDb();

    await expect(
      planManualCandidateFinanceImport({ db: db as never, payloads: [amendment] })
    ).rejects.toThrow(`references missing filing ${amendment.amends_filing_id}`);
    expect(db.query).toHaveBeenCalledTimes(1);
  });

  it("rejects targets that are not active Mississippi candidate-election links", async () => {
    const payload = loadFixture("ms_hd22_jon_lancaster_2025_pre_election.json");
    const db = {
      query: vi
        .fn()
        .mockResolvedValueOnce({ rows: [], rowCount: 0 })
        .mockResolvedValueOnce({
          rows: [
            {
              candidate_id: payload.candidate_id,
              election_id: payload.election_id,
              candidate_exists: true,
              candidate_deleted: true,
              candidate_merged: false,
              candidate_state: "MS",
              candidate_name: payload.candidate_name,
              election_exists: true,
              district_state: "MS",
              linked: true,
            },
          ],
          rowCount: 1,
        }),
    };

    await expect(
      planManualCandidateFinanceImport({ db: db as never, payloads: [payload] })
    ).rejects.toThrow(`candidate ${payload.candidate_id} is deleted or merged`);
  });

  it("warns, but preserves, a filing name variant when stable IDs match", async () => {
    const payload = loadFixture("ms_hd22_jon_lancaster_2025_pre_election.json");
    const db = {
      query: vi
        .fn()
        .mockResolvedValueOnce({ rows: [], rowCount: 0 })
        .mockResolvedValueOnce({
          rows: [
            {
              candidate_id: payload.candidate_id,
              election_id: payload.election_id,
              candidate_exists: true,
              candidate_deleted: false,
              candidate_merged: false,
              candidate_state: "MS",
              candidate_name: "Jonathan Lancaster",
              election_exists: true,
              district_state: "MS",
              linked: true,
            },
          ],
          rowCount: 1,
        }),
    };

    const plan = await planManualCandidateFinanceImport({ db: db as never, payloads: [payload] });
    expect(plan.warnings).toEqual([
      expect.stringContaining('stored as "Jonathan Lancaster" but filing name(s) are "Jon Lancaster"'),
    ]);
  });

  it("writes new filings and nullable target amounts in one transaction", async () => {
    const payload = loadFixture("ms_ie_multi_target_house_22_2025.json");
    const client = {
      query: vi.fn((sql: unknown, values?: unknown[]) => {
        const statement = String(sql);
        if (statement === "BEGIN" || statement === "COMMIT" || statement === "ROLLBACK") {
          return Promise.resolve({ rows: [], rowCount: null });
        }
        if (statement.includes("FROM public.manual_candidate_finance_filings")) {
          return Promise.resolve({ rows: [], rowCount: 0 });
        }
        if (statement.includes("WITH requested(candidate_id, election_id)")) {
          const rows = candidateRows(values);
          return Promise.resolve({ rows, rowCount: rows.length });
        }
        if (statement.includes("INSERT INTO public.manual_candidate_finance_filings")) {
          return Promise.resolve({ rows: [{ filing_id: payload.filing_id }], rowCount: 1 });
        }
        if (statement.includes("INSERT INTO public.manual_candidate_finance_filing_targets")) {
          return Promise.resolve({ rows: [], rowCount: 1 });
        }
        throw new Error(`Unexpected SQL: ${statement}`);
      }),
      release: vi.fn(),
    };
    const db = { query: vi.fn(), connect: vi.fn().mockResolvedValue(client) };

    await expect(
      persistManualCandidateFinanceFilings({ db: db as never, payloads: [payload] })
    ).resolves.toMatchObject({
      insertedFilingCount: 1,
      unchangedFilingCount: 0,
      insertedTargetRowCount: 2,
    });
    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
    const targetCalls = client.query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.manual_candidate_finance_filing_targets")
    );
    expect(targetCalls).toHaveLength(2);
    expect(targetCalls.every((call) => call[1]?.[5] === null)).toBe(true);
    expect(client.release).toHaveBeenCalledTimes(1);
  });

  it("rolls back the whole import when a target insert fails", async () => {
    const payload = loadFixture("ms_hd22_jon_lancaster_2025_pre_election.json");
    const client = {
      query: vi.fn((sql: unknown, values?: unknown[]) => {
        const statement = String(sql);
        if (statement === "BEGIN" || statement === "ROLLBACK") {
          return Promise.resolve({ rows: [], rowCount: null });
        }
        if (statement.includes("FROM public.manual_candidate_finance_filings")) {
          return Promise.resolve({ rows: [], rowCount: 0 });
        }
        if (statement.includes("WITH requested(candidate_id, election_id)")) {
          const rows = candidateRows(values);
          return Promise.resolve({ rows, rowCount: rows.length });
        }
        if (statement.includes("INSERT INTO public.manual_candidate_finance_filings")) {
          return Promise.resolve({ rows: [{ filing_id: payload.filing_id }], rowCount: 1 });
        }
        if (statement.includes("INSERT INTO public.manual_candidate_finance_filing_targets")) {
          return Promise.reject(new Error("target insert failed"));
        }
        throw new Error(`Unexpected SQL: ${statement}`);
      }),
      release: vi.fn(),
    };
    const db = { query: vi.fn(), connect: vi.fn().mockResolvedValue(client) };

    await expect(
      persistManualCandidateFinanceFilings({ db: db as never, payloads: [payload] })
    ).rejects.toThrow("target insert failed");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("ROLLBACK");
    expect(client.release).toHaveBeenCalledTimes(1);
  });
});
