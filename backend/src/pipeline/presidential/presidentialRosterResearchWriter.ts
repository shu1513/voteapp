import type { Pool, PoolClient } from "pg";

import { addPresidentialRosterResearchDelay } from "./presidentialRosterResearchPolicy.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type PresidentialRosterResearchWriteResult = {
  rowsUpdated: number;
  nextResearchAt: string | null;
};

function toErrorSummary(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  const trimmed = message.trim();
  return trimmed.length > 500 ? `${trimmed.slice(0, 497)}...` : trimmed || "unknown error";
}

function requireUpdatedOneCycle(rowCount: number | null, label: string, cycleId: string): number {
  if (rowCount !== 1) {
    throw new Error(`${label} expected to update exactly one presidential primary cycle, updated ${rowCount ?? 0}: ${cycleId}`);
  }
  return rowCount;
}

export async function markPresidentialRosterResearchSuccess(
  db: Queryable,
  input: {
    cycleId: string;
    electionYear: number;
    researchedAt: Date;
  }
): Promise<PresidentialRosterResearchWriteResult> {
  const nextResearchAt = addPresidentialRosterResearchDelay(input.researchedAt, input.electionYear);
  const result = await db.query(
    `
      UPDATE public.presidential_cycles
      SET roster_research_last_attempted_at = $2::timestamptz,
          roster_research_next_at = $3::timestamptz,
          roster_research_attempt_count = roster_research_attempt_count + 1,
          roster_research_last_status = 'succeeded',
          roster_research_last_error = NULL
      WHERE id = $1::uuid
        AND stage = 'primary'
    `,
    [input.cycleId, input.researchedAt.toISOString(), nextResearchAt?.toISOString() ?? null]
  );
  const rowsUpdated = requireUpdatedOneCycle(
    result.rowCount,
    "presidential roster research success tracking",
    input.cycleId
  );

  return {
    rowsUpdated,
    nextResearchAt: nextResearchAt?.toISOString() ?? null,
  };
}

export async function markPresidentialRosterResearchError(
  db: Queryable,
  input: {
    cycleId: string;
    electionYear: number;
    researchedAt: Date;
    error: unknown;
  }
): Promise<PresidentialRosterResearchWriteResult> {
  const nextResearchAt = addPresidentialRosterResearchDelay(input.researchedAt, input.electionYear);
  const result = await db.query(
    `
      UPDATE public.presidential_cycles
      SET roster_research_last_attempted_at = $2::timestamptz,
          roster_research_next_at = $3::timestamptz,
          roster_research_attempt_count = roster_research_attempt_count + 1,
          roster_research_last_status = 'failed',
          roster_research_last_error = $4
      WHERE id = $1::uuid
        AND stage = 'primary'
    `,
    [
      input.cycleId,
      input.researchedAt.toISOString(),
      nextResearchAt?.toISOString() ?? null,
      toErrorSummary(input.error),
    ]
  );
  const rowsUpdated = requireUpdatedOneCycle(
    result.rowCount,
    "presidential roster research error tracking",
    input.cycleId
  );

  return {
    rowsUpdated,
    nextResearchAt: nextResearchAt?.toISOString() ?? null,
  };
}
