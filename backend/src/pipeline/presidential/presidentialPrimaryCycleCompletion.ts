import type { QueryResult, QueryResultRow } from "pg";

import { getPresidentialPrimaryDateResearchStopAt } from "./presidentialPrimaryDateResearchPolicy.js";

type Queryable = {
  query<T extends QueryResultRow = QueryResultRow>(text: string, values?: unknown[]): Promise<QueryResult<T>>;
};

type PrimaryCycleRow = {
  id: string;
  election_year: number;
};

export type CompleteExpiredPresidentialPrimaryCyclesInput = {
  now?: Date;
  dryRun?: boolean;
};

export type CompleteExpiredPresidentialPrimaryCyclesResult = {
  dryRun: boolean;
  now: string;
  scannedCycleCount: number;
  expiredCycleCount: number;
  completedCycleCount: number;
  completedCycleIds: string[];
};

function assertValidDate(date: Date, label: string): void {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    throw new Error(`Invalid presidential primary cycle completion ${label}`);
  }
}

function isExpiredPrimaryCycle(row: PrimaryCycleRow, now: Date): boolean {
  return now.getTime() >= getPresidentialPrimaryDateResearchStopAt(row.election_year).getTime();
}

export async function completeExpiredPresidentialPrimaryCycles(
  db: Queryable,
  input: CompleteExpiredPresidentialPrimaryCyclesInput = {}
): Promise<CompleteExpiredPresidentialPrimaryCyclesResult> {
  const now = input.now ?? new Date();
  assertValidDate(now, "now");
  const dryRun = Boolean(input.dryRun);

  const cycles = await db.query<PrimaryCycleRow>(
    `
      SELECT id, election_year
      FROM public.presidential_cycles
      WHERE stage = 'primary'
        AND status IN ('upcoming', 'active')
      ORDER BY election_year ASC, party ASC, id ASC
    `
  );
  const expiredCycleIds = cycles.rows
    .filter((row) => isExpiredPrimaryCycle(row, now))
    .map((row) => row.id);

  if (dryRun || expiredCycleIds.length === 0) {
    return {
      dryRun,
      now: now.toISOString(),
      scannedCycleCount: cycles.rows.length,
      expiredCycleCount: expiredCycleIds.length,
      completedCycleCount: 0,
      completedCycleIds: [],
    };
  }

  const update = await db.query<{ id: string }>(
    `
      UPDATE public.presidential_cycles
      SET status = 'completed'
      WHERE id = ANY($1::uuid[])
        AND status IN ('upcoming', 'active')
      RETURNING id
    `,
    [expiredCycleIds]
  );

  return {
    dryRun,
    now: now.toISOString(),
    scannedCycleCount: cycles.rows.length,
    expiredCycleCount: expiredCycleIds.length,
    completedCycleCount: update.rowCount ?? 0,
    completedCycleIds: update.rows.map((row) => row.id),
  };
}
