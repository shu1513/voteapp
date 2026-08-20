import { createHash } from "node:crypto";

import type { Pool, PoolClient } from "pg";

import {
  parseManualCandidateFinancePayload,
  type ManualCandidateFinancePayload,
} from "../../contracts/manualCandidateFinancePayloadContract.js";
import { compileManualCandidateFinancePreview } from "./manualCandidateFinancePreview.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & Pick<Pool, "connect">;

export type ManualCandidateFinanceTargetRecord = {
  filingId: string;
  candidateId: string;
  electionId: string;
  candidateName: string;
  relationship: "candidate_report" | "support" | "oppose";
  amount: number | null;
};

export type ManualCandidateFinanceFilingRecord = {
  payload: ManualCandidateFinancePayload;
  payloadSha256: string;
  targets: ManualCandidateFinanceTargetRecord[];
};

export type ManualCandidateFinanceImportPlan = {
  inputFilingCount: number;
  uniqueInputFilingCount: number;
  insertFilingIds: string[];
  unchangedFilingIds: string[];
  targetRowCount: number;
  warnings: string[];
};

export type ManualCandidateFinanceImportResult = ManualCandidateFinanceImportPlan & {
  insertedFilingCount: number;
  unchangedFilingCount: number;
  insertedTargetRowCount: number;
};

type StoredFilingRow = {
  filing_id: string;
  payload: unknown;
  payload_sha256: string;
};

type CandidateElectionRow = {
  candidate_id: string;
  election_id: string;
  candidate_exists: boolean;
  candidate_deleted: boolean;
  candidate_merged: boolean;
  candidate_state: string | null;
  candidate_name: string | null;
  election_exists: boolean;
  district_state: string | null;
  linked: boolean;
};

function candidateElectionKey(candidateId: string, electionId: string): string {
  return `${candidateId}\u0000${electionId}`;
}

function normalizedName(value: string): string {
  return value.trim().replace(/\s+/g, " ").toLocaleUpperCase("en-US");
}

export function manualCandidateFinancePayloadSha256(payload: ManualCandidateFinancePayload): string {
  return createHash("sha256").update(JSON.stringify(payload)).digest("hex");
}

function targetsForPayload(payload: ManualCandidateFinancePayload): ManualCandidateFinanceTargetRecord[] {
  if (payload.filing_type === "candidate_report") {
    return [
      {
        filingId: payload.filing_id,
        candidateId: payload.candidate_id,
        electionId: payload.election_id,
        candidateName: payload.candidate_name,
        relationship: "candidate_report",
        amount: null,
      },
    ];
  }

  return payload.candidate_edges.map((edge) => ({
    filingId: payload.filing_id,
    candidateId: edge.candidate_id,
    electionId: edge.election_id,
    candidateName: edge.candidate_name,
    relationship: edge.support_oppose,
    amount: edge.amount,
  }));
}

export function buildManualCandidateFinanceFilingRecords(
  payloads: readonly ManualCandidateFinancePayload[]
): ManualCandidateFinanceFilingRecord[] {
  if (payloads.length === 0) {
    throw new Error("Manual candidate-finance import requires at least one filing payload");
  }

  const records = new Map<string, ManualCandidateFinanceFilingRecord>();
  for (const payload of payloads) {
    const record = {
      payload,
      payloadSha256: manualCandidateFinancePayloadSha256(payload),
      targets: targetsForPayload(payload),
    };
    const existing = records.get(payload.filing_id);
    if (!existing) {
      records.set(payload.filing_id, record);
      continue;
    }
    if (existing.payloadSha256 !== record.payloadSha256) {
      throw new Error(`Conflicting manual candidate-finance payloads share filing_id ${payload.filing_id}`);
    }
  }
  return [...records.values()].sort((left, right) =>
    left.payload.filing_id.localeCompare(right.payload.filing_id)
  );
}

function parseStoredFiling(row: StoredFilingRow): ManualCandidateFinanceFilingRecord {
  const parsed = parseManualCandidateFinancePayload(row.payload);
  if (!parsed.ok) {
    throw new Error(`Stored manual candidate-finance filing ${row.filing_id} is invalid: ${parsed.reason}`);
  }
  if (parsed.payload.filing_id !== row.filing_id) {
    throw new Error(
      `Stored manual candidate-finance filing key ${row.filing_id} does not match payload filing_id ${parsed.payload.filing_id}`
    );
  }
  const record = buildManualCandidateFinanceFilingRecords([parsed.payload])[0]!;
  if (record.payloadSha256 !== row.payload_sha256) {
    throw new Error(`Stored manual candidate-finance filing ${row.filing_id} has a payload hash mismatch`);
  }
  return record;
}

function assertCompleteAmendmentLineage(records: ReadonlyMap<string, ManualCandidateFinanceFilingRecord>): void {
  for (const record of records.values()) {
    const parentId = record.payload.amends_filing_id;
    if (parentId !== null && !records.has(parentId)) {
      throw new Error(
        `Manual candidate-finance amendment ${record.payload.filing_id} references missing filing ${parentId}`
      );
    }
  }
}

async function validateCandidateElectionTargets(
  db: Queryable,
  records: readonly ManualCandidateFinanceFilingRecord[]
): Promise<string[]> {
  const requested = new Map<
    string,
    { candidateId: string; electionId: string; candidateNames: Set<string> }
  >();
  for (const target of records.flatMap((record) => record.targets)) {
    const key = candidateElectionKey(target.candidateId, target.electionId);
    const existing = requested.get(key);
    if (existing) {
      existing.candidateNames.add(target.candidateName);
    } else {
      requested.set(key, {
        candidateId: target.candidateId,
        electionId: target.electionId,
        candidateNames: new Set([target.candidateName]),
      });
    }
  }
  if (requested.size === 0) {
    return [];
  }

  const ordered = [...requested.values()].sort(
    (left, right) =>
      left.electionId.localeCompare(right.electionId) || left.candidateId.localeCompare(right.candidateId)
  );
  const result = await db.query<CandidateElectionRow>(
    `
      WITH requested(candidate_id, election_id) AS (
        SELECT *
        FROM unnest($1::uuid[], $2::uuid[])
      )
      SELECT
        requested.candidate_id::text AS candidate_id,
        requested.election_id::text AS election_id,
        candidate.id IS NOT NULL AS candidate_exists,
        candidate.deleted_at IS NOT NULL AS candidate_deleted,
        candidate.merged_into_candidate_id IS NOT NULL AS candidate_merged,
        candidate.state AS candidate_state,
        COALESCE(
          NULLIF(btrim(candidate.display_name), ''),
          NULLIF(btrim(concat_ws(' ', candidate.first_name, candidate.last_name)), '')
        ) AS candidate_name,
        election.id IS NOT NULL AS election_exists,
        district.state AS district_state,
        candidate_election.id IS NOT NULL AS linked
      FROM requested
      LEFT JOIN public.candidates candidate ON candidate.id = requested.candidate_id
      LEFT JOIN public.elections election ON election.id = requested.election_id
      LEFT JOIN public.districts district ON district.id = election.district_id
      LEFT JOIN public.candidate_elections candidate_election
        ON candidate_election.candidate_id = requested.candidate_id
       AND candidate_election.election_id = requested.election_id
      ORDER BY requested.election_id, requested.candidate_id
    `,
    [
      ordered.map((entry) => entry.candidateId),
      ordered.map((entry) => entry.electionId),
    ]
  );

  const warnings: string[] = [];
  for (const row of result.rows) {
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    const target = requested.get(key);
    if (!target) {
      throw new Error(`Candidate-election preflight returned unexpected target ${row.candidate_id}/${row.election_id}`);
    }
    if (!row.candidate_exists) {
      throw new Error(`Manual candidate-finance target candidate ${row.candidate_id} does not exist`);
    }
    if (row.candidate_deleted || row.candidate_merged) {
      throw new Error(`Manual candidate-finance target candidate ${row.candidate_id} is deleted or merged`);
    }
    if (!row.election_exists) {
      throw new Error(`Manual candidate-finance target election ${row.election_id} does not exist`);
    }
    if (!row.linked) {
      throw new Error(
        `Manual candidate-finance target candidate ${row.candidate_id} is not linked to election ${row.election_id}`
      );
    }
    if (row.candidate_state !== "MS" || row.district_state !== "MS") {
      throw new Error(
        `Manual candidate-finance target ${row.candidate_id}/${row.election_id} is outside Mississippi`
      );
    }

    if (row.candidate_name) {
      const sourceNames = [...target.candidateNames].sort();
      if (sourceNames.some((name) => normalizedName(name) !== normalizedName(row.candidate_name!))) {
        warnings.push(
          `Candidate ${row.candidate_id} is stored as "${row.candidate_name}" but filing name(s) are ${sourceNames
            .map((name) => `"${name}"`)
            .join(", ")}`
        );
      }
    }
  }

  if (result.rows.length !== requested.size) {
    throw new Error("Candidate-election preflight did not return every requested target");
  }
  return warnings.sort();
}

export async function planManualCandidateFinanceImport(input: {
  db: Queryable;
  payloads: readonly ManualCandidateFinancePayload[];
}): Promise<ManualCandidateFinanceImportPlan> {
  const incoming = buildManualCandidateFinanceFilingRecords(input.payloads);
  const storedResult = await input.db.query<StoredFilingRow>(
    `
      SELECT filing_id::text AS filing_id, payload, payload_sha256
      FROM public.manual_candidate_finance_filings
      ORDER BY filing_id
    `
  );
  const allRecords = new Map<string, ManualCandidateFinanceFilingRecord>();
  for (const row of storedResult.rows) {
    const record = parseStoredFiling(row);
    allRecords.set(record.payload.filing_id, record);
  }

  const toInsert: ManualCandidateFinanceFilingRecord[] = [];
  const unchangedFilingIds: string[] = [];
  for (const record of incoming) {
    const stored = allRecords.get(record.payload.filing_id);
    if (!stored) {
      allRecords.set(record.payload.filing_id, record);
      toInsert.push(record);
      continue;
    }
    if (stored.payloadSha256 !== record.payloadSha256) {
      throw new Error(
        `Manual candidate-finance filing_id ${record.payload.filing_id} already exists with different content`
      );
    }
    unchangedFilingIds.push(record.payload.filing_id);
  }

  assertCompleteAmendmentLineage(allRecords);
  compileManualCandidateFinancePreview([...allRecords.values()].map((record) => record.payload));
  const warnings = await validateCandidateElectionTargets(input.db, toInsert);

  return {
    inputFilingCount: input.payloads.length,
    uniqueInputFilingCount: incoming.length,
    insertFilingIds: toInsert.map((record) => record.payload.filing_id),
    unchangedFilingIds,
    targetRowCount: toInsert.reduce((total, record) => total + record.targets.length, 0),
    warnings,
  };
}

async function insertFiling(db: Queryable, record: ManualCandidateFinanceFilingRecord): Promise<boolean> {
  const payload = record.payload;
  const inserted = await db.query<{ filing_id: string }>(
    `
      INSERT INTO public.manual_candidate_finance_filings (
        filing_id,
        schema_version,
        state,
        filing_type,
        amends_filing_id,
        report_date,
        source_url,
        coverage_note,
        researched_at,
        payload,
        payload_sha256
      )
      VALUES (
        $1::uuid,
        $2,
        $3,
        $4,
        $5::uuid,
        $6::date,
        $7,
        $8,
        $9::timestamptz,
        $10::jsonb,
        $11
      )
      ON CONFLICT (filing_id) DO NOTHING
      RETURNING filing_id::text AS filing_id
    `,
    [
      payload.filing_id,
      payload.schema_version,
      payload.state,
      payload.filing_type,
      payload.amends_filing_id,
      payload.report_date,
      payload.source_url,
      payload.coverage_note,
      payload.researched_at,
      JSON.stringify(payload),
      record.payloadSha256,
    ]
  );
  if (inserted.rowCount === 1) {
    return true;
  }

  const existing = await db.query<{ payload_sha256: string }>(
    `
      SELECT payload_sha256
      FROM public.manual_candidate_finance_filings
      WHERE filing_id = $1::uuid
    `,
    [payload.filing_id]
  );
  if (existing.rows[0]?.payload_sha256 !== record.payloadSha256) {
    throw new Error(`Manual candidate-finance filing_id ${payload.filing_id} was concurrently written with different content`);
  }
  return false;
}

async function insertTarget(db: Queryable, target: ManualCandidateFinanceTargetRecord): Promise<void> {
  await db.query(
    `
      INSERT INTO public.manual_candidate_finance_filing_targets (
        filing_id,
        candidate_id,
        election_id,
        candidate_name,
        relationship,
        amount
      )
      VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6::numeric)
    `,
    [
      target.filingId,
      target.candidateId,
      target.electionId,
      target.candidateName,
      target.relationship,
      target.amount,
    ]
  );
}

export async function persistManualCandidateFinanceFilings(input: {
  db: ConnectableQueryable;
  payloads: readonly ManualCandidateFinancePayload[];
}): Promise<ManualCandidateFinanceImportResult> {
  const client = await input.db.connect();
  try {
    await client.query("BEGIN");
    const plan = await planManualCandidateFinanceImport({ db: client, payloads: input.payloads });
    const recordsById = new Map(
      buildManualCandidateFinanceFilingRecords(input.payloads).map((record) => [record.payload.filing_id, record])
    );
    let insertedFilingCount = 0;
    let unchangedFilingCount = plan.unchangedFilingIds.length;
    let insertedTargetRowCount = 0;

    for (const filingId of plan.insertFilingIds) {
      const record = recordsById.get(filingId)!;
      const inserted = await insertFiling(client, record);
      if (!inserted) {
        unchangedFilingCount += 1;
        continue;
      }
      insertedFilingCount += 1;
      for (const target of record.targets) {
        await insertTarget(client, target);
        insertedTargetRowCount += 1;
      }
    }

    await client.query("COMMIT");
    return {
      ...plan,
      insertedFilingCount,
      unchangedFilingCount,
      insertedTargetRowCount,
    };
  } catch (error) {
    try {
      await client.query("ROLLBACK");
    } catch {
      // Preserve the original import failure.
    }
    throw error;
  } finally {
    client.release();
  }
}
