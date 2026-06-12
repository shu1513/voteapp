import type { Pool, PoolClient } from "pg";

import { STATE_INFO_BY_FIPS } from "../../constants/usStates.js";
import { isUuid } from "../../utils/uuid.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type PresidentialPrimaryDateBootstrapErrorCode =
  | "invalid_cycle_ids"
  | "unknown_cycle_ids"
  | "non_primary_cycles";

export class PresidentialPrimaryDateBootstrapError extends Error {
  constructor(
    readonly code: PresidentialPrimaryDateBootstrapErrorCode,
    message: string,
    readonly details: {
      invalidCycleIds?: string[];
      unknownCycleIds?: string[];
      nonPrimaryCycleIds?: string[];
    } = {}
  ) {
    super(message);
    this.name = "PresidentialPrimaryDateBootstrapError";
  }
}

export type EnsurePresidentialStatePrimaryDateRowsResult = {
  requestedCycleCount: number;
  stateCount: number;
  requestedRowCount: number;
  insertedRowCount: number;
  existingRowCount: number;
};

type CycleValidationRow = {
  id: string;
  stage: string;
};

type InsertStateRowsResultRow = {
  inserted_count: string | number;
};

export const PRESIDENTIAL_PRIMARY_DATE_STATE_FIPS: readonly string[] = Object.keys(STATE_INFO_BY_FIPS).sort();

function parseCount(value: string | number | null | undefined): number {
  if (typeof value === "number") {
    return value;
  }
  const parsed = Number.parseInt(value ?? "0", 10);
  return Number.isFinite(parsed) ? parsed : 0;
}

function normalizeCycleIds(cycleIds: readonly string[]): string[] {
  const normalized: string[] = [];
  const invalidCycleIds: string[] = [];
  const seen = new Set<string>();

  for (const cycleId of cycleIds) {
    const trimmed = cycleId.trim();
    if (trimmed.length === 0 || !isUuid(trimmed)) {
      invalidCycleIds.push(cycleId);
      continue;
    }
    const key = trimmed.toLowerCase();
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    normalized.push(trimmed);
  }

  if (invalidCycleIds.length > 0) {
    throw new PresidentialPrimaryDateBootstrapError(
      "invalid_cycle_ids",
      `Invalid presidential cycle IDs: ${invalidCycleIds.join(", ")}`,
      { invalidCycleIds }
    );
  }

  return normalized;
}

async function validatePrimaryCycleIds(db: Queryable, cycleIds: readonly string[]): Promise<void> {
  const result = await db.query<CycleValidationRow>(
    `
      SELECT id, stage
      FROM public.presidential_cycles
      WHERE id = ANY($1::uuid[])
    `,
    [cycleIds]
  );

  const rowsById = new Map(result.rows.map((row) => [row.id.toLowerCase(), row]));
  const unknownCycleIds = cycleIds.filter((cycleId) => !rowsById.has(cycleId.toLowerCase()));
  if (unknownCycleIds.length > 0) {
    throw new PresidentialPrimaryDateBootstrapError(
      "unknown_cycle_ids",
      `Unknown presidential cycle IDs: ${unknownCycleIds.join(", ")}`,
      { unknownCycleIds }
    );
  }

  const nonPrimaryCycleIds = cycleIds.filter((cycleId) => {
    const row = rowsById.get(cycleId.toLowerCase());
    return row?.stage !== "primary";
  });
  if (nonPrimaryCycleIds.length > 0) {
    throw new PresidentialPrimaryDateBootstrapError(
      "non_primary_cycles",
      `Presidential primary date rows can only be bootstrapped for primary cycles: ${nonPrimaryCycleIds.join(", ")}`,
      { nonPrimaryCycleIds }
    );
  }
}

export async function ensurePresidentialStatePrimaryDateRows(
  db: Queryable,
  cycleIds: readonly string[]
): Promise<EnsurePresidentialStatePrimaryDateRowsResult> {
  const normalizedCycleIds = normalizeCycleIds(cycleIds);
  const stateFips = PRESIDENTIAL_PRIMARY_DATE_STATE_FIPS;
  const requestedRowCount = normalizedCycleIds.length * stateFips.length;

  if (normalizedCycleIds.length === 0) {
    return {
      requestedCycleCount: 0,
      stateCount: stateFips.length,
      requestedRowCount: 0,
      insertedRowCount: 0,
      existingRowCount: 0,
    };
  }

  await validatePrimaryCycleIds(db, normalizedCycleIds);

  const result = await db.query<InsertStateRowsResultRow>(
    `
      WITH requested_cycles AS (
        SELECT cycle_id
        FROM unnest($1::uuid[]) AS requested(cycle_id)
      ),
      requested_states AS (
        SELECT state_fips
        FROM unnest($2::text[]) AS requested(state_fips)
      ),
      inserted AS (
        INSERT INTO public.presidential_state_primary_dates (cycle_id, state_fips)
        SELECT requested_cycles.cycle_id, requested_states.state_fips
        FROM requested_cycles
        CROSS JOIN requested_states
        ON CONFLICT (cycle_id, state_fips) DO NOTHING
        RETURNING id
      )
      SELECT COUNT(*) AS inserted_count
      FROM inserted
    `,
    [normalizedCycleIds, stateFips]
  );

  const insertedRowCount = parseCount(result.rows[0]?.inserted_count);
  return {
    requestedCycleCount: normalizedCycleIds.length,
    stateCount: stateFips.length,
    requestedRowCount,
    insertedRowCount,
    existingRowCount: requestedRowCount - insertedRowCount,
  };
}
