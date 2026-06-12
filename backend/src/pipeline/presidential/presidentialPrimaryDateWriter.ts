import type { QueryResult, QueryResultRow } from "pg";

import type {
  PresidentialPrimaryDatePayload,
  PresidentialPrimaryDatePayloadRow,
} from "../../contracts/presidentialPrimaryDatePayloadContract.js";
import { addPresidentialPrimaryDateResearchRetryDelay } from "./presidentialPrimaryDateResearchPolicy.js";

type Queryable = {
  query<T extends QueryResultRow = QueryResultRow>(text: string, values?: unknown[]): Promise<QueryResult<T>>;
};

export type PresidentialPrimaryDateWriteInput = {
  cycleId: string;
  payload: PresidentialPrimaryDatePayload;
  researchedAt?: Date;
};

export type PresidentialPrimaryDateErrorMarkInput = {
  cycleId: string;
  stateFipsList: readonly string[];
  error: string;
  researchedAt?: Date;
};

export type PresidentialPrimaryDateWriteResult = {
  officialFoundCount: number;
  notOfficialYetCount: number;
  rowsUpdated: number;
  nextResearchAt: string | null;
};

export type PresidentialPrimaryDateErrorMarkResult = {
  rowsUpdated: number;
  nextResearchAt: string;
};

function assertValidDate(date: Date, label: string): void {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    throw new Error(`Invalid presidential primary date writer ${label}`);
  }
}

function normalizeNullableText(value: string): string | null {
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function normalizeErrorText(value: string): string {
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : "Presidential primary date research failed";
}

async function updatePayloadRow(
  db: Queryable,
  cycleId: string,
  row: PresidentialPrimaryDatePayloadRow,
  researchedAt: Date,
  nextResearchAt: Date
): Promise<number> {
  const isOfficialFound = row.status === "official_found";
  const result = await db.query(
    `
      UPDATE public.presidential_state_primary_dates
      SET primary_date = $3::date,
          date_research_status = $4,
          last_researched_at = $5::timestamptz,
          next_research_at = $6::timestamptz,
          research_attempt_count = research_attempt_count + 1,
          last_research_summary = $7,
          last_research_error = NULL,
          sources = $8::jsonb
      WHERE cycle_id = $1::uuid
        AND state_fips = $2
    `,
    [
      cycleId,
      row.state_fips,
      row.primary_date,
      row.status,
      researchedAt.toISOString(),
      isOfficialFound ? null : nextResearchAt.toISOString(),
      normalizeNullableText(row.notes),
      JSON.stringify(row.sources),
    ]
  );
  if (result.rowCount !== 1) {
    throw new Error(
      `Expected to update one presidential primary date row for cycle=${cycleId} state_fips=${row.state_fips}; updated ${result.rowCount}`
    );
  }
  return result.rowCount;
}

export async function writePresidentialPrimaryDatePayloadRows(
  db: Queryable,
  input: PresidentialPrimaryDateWriteInput
): Promise<PresidentialPrimaryDateWriteResult> {
  const researchedAt = input.researchedAt ?? new Date();
  assertValidDate(researchedAt, "researchedAt");
  const nextResearchAt = addPresidentialPrimaryDateResearchRetryDelay(researchedAt);

  let rowsUpdated = 0;
  let officialFoundCount = 0;
  let notOfficialYetCount = 0;

  for (const row of input.payload.results) {
    rowsUpdated += await updatePayloadRow(db, input.cycleId, row, researchedAt, nextResearchAt);
    if (row.status === "official_found") {
      officialFoundCount += 1;
    } else {
      notOfficialYetCount += 1;
    }
  }

  return {
    officialFoundCount,
    notOfficialYetCount,
    rowsUpdated,
    nextResearchAt: notOfficialYetCount > 0 ? nextResearchAt.toISOString() : null,
  };
}

export async function markPresidentialPrimaryDateResearchError(
  db: Queryable,
  input: PresidentialPrimaryDateErrorMarkInput
): Promise<PresidentialPrimaryDateErrorMarkResult> {
  const researchedAt = input.researchedAt ?? new Date();
  assertValidDate(researchedAt, "researchedAt");
  const stateFipsList = [...new Set(input.stateFipsList.map((stateFips) => stateFips.trim()))].filter(Boolean);
  if (stateFipsList.length === 0) {
    throw new Error("markPresidentialPrimaryDateResearchError requires at least one state_fips");
  }

  const nextResearchAt = addPresidentialPrimaryDateResearchRetryDelay(researchedAt);
  const result = await db.query(
    `
      UPDATE public.presidential_state_primary_dates
      SET primary_date = NULL,
          date_research_status = 'error',
          last_researched_at = $3::timestamptz,
          next_research_at = $4::timestamptz,
          research_attempt_count = research_attempt_count + 1,
          last_research_error = $5,
          last_research_summary = NULL
      WHERE cycle_id = $1::uuid
        AND state_fips = ANY($2::text[])
    `,
    [
      input.cycleId,
      stateFipsList,
      researchedAt.toISOString(),
      nextResearchAt.toISOString(),
      normalizeErrorText(input.error),
    ]
  );

  if (result.rowCount !== stateFipsList.length) {
    throw new Error(
      `Expected to mark ${stateFipsList.length} presidential primary date rows as error for cycle=${input.cycleId}; updated ${result.rowCount}`
    );
  }

  return {
    rowsUpdated: result.rowCount,
    nextResearchAt: nextResearchAt.toISOString(),
  };
}
