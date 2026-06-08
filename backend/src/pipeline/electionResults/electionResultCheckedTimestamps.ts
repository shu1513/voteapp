import type { PoolClient } from "pg";

import type { ParsedElectionResultPayloadRow } from "../../contracts/electionResultPayloadContract.js";
import type { ElectionResultPassType } from "../../types/electionResults.js";
import { shouldMarkElectionResultPassChecked } from "./electionResultRetryPolicy.js";

type Queryable = Pick<PoolClient, "query">;

export async function markElectionResultPassChecked(
  db: Queryable,
  input: { electionIds: readonly string[]; passType: ElectionResultPassType; checkedAt?: Date }
): Promise<number> {
  const uniqueElectionIds = [...new Set(input.electionIds.map((id) => id.trim()).filter((id) => id.length > 0))];
  if (uniqueElectionIds.length === 0) {
    return 0;
  }

  const column =
    input.passType === "election_night"
      ? "election_night_results_checked_at"
      : "certified_results_checked_at";
  const checkedAt = input.checkedAt ?? new Date();

  const result = await db.query(
    `
      UPDATE public.elections
      SET ${column} = $2::timestamptz,
          updated_at = now()
      WHERE id = ANY($1::uuid[])
    `,
    [uniqueElectionIds, checkedAt.toISOString()]
  );

  return result.rowCount ?? 0;
}

export async function markElectionResultPassAttemptAndChecked(
  db: Queryable,
  input: {
    rows: readonly ParsedElectionResultPayloadRow[];
    passType: ElectionResultPassType;
    checkedAt?: Date;
  }
): Promise<{
  attemptedElectionCount: number;
  checkedElectionCount: number;
  checkedElectionIds: string[];
  uncheckedElectionIds: string[];
}> {
  const rowByElectionId = new Map<string, ParsedElectionResultPayloadRow>();
  for (const row of input.rows) {
    const electionId = row.election_id.trim();
    if (electionId.length > 0) {
      rowByElectionId.set(electionId, row);
    }
  }
  const electionIds = [...rowByElectionId.keys()];

  if (input.passType !== "election_night") {
    if (electionIds.length === 0) {
      return { attemptedElectionCount: 0, checkedElectionCount: 0, checkedElectionIds: [], uncheckedElectionIds: [] };
    }

    const checkedAt = input.checkedAt ?? new Date();
    const attempts = await db.query<{ id: string; certified_results_attempt_count: number }>(
      `
        UPDATE public.elections
        SET certified_results_attempt_count = certified_results_attempt_count + 1,
            certified_results_last_attempted_at = $2::timestamptz,
            updated_at = now()
        WHERE id = ANY($1::uuid[])
        RETURNING id, certified_results_attempt_count
      `,
      [electionIds, checkedAt.toISOString()]
    );

    const checkedElectionIds = attempts.rows
      .filter((attempt) => {
        const row = rowByElectionId.get(attempt.id);
        return row
          ? shouldMarkElectionResultPassChecked({
              passType: input.passType,
              row,
              electionNightAttemptCountAfterThisRun: 0,
              certifiedAttemptCountAfterThisRun: attempt.certified_results_attempt_count,
            })
          : false;
      })
      .map((attempt) => attempt.id);

    return {
      attemptedElectionCount: attempts.rowCount ?? 0,
      checkedElectionCount: await markElectionResultPassChecked(db, {
        electionIds: checkedElectionIds,
        passType: input.passType,
        checkedAt,
      }),
      checkedElectionIds,
      uncheckedElectionIds: electionIds.filter((electionId) => !checkedElectionIds.includes(electionId)),
    };
  }

  if (electionIds.length === 0) {
    return { attemptedElectionCount: 0, checkedElectionCount: 0, checkedElectionIds: [], uncheckedElectionIds: [] };
  }

  const checkedAt = input.checkedAt ?? new Date();
  const attempts = await db.query<{ id: string; election_night_results_attempt_count: number }>(
    `
      UPDATE public.elections
      SET election_night_results_attempt_count = election_night_results_attempt_count + 1,
          election_night_results_last_attempted_at = $2::timestamptz,
          updated_at = now()
      WHERE id = ANY($1::uuid[])
      RETURNING id, election_night_results_attempt_count
    `,
    [electionIds, checkedAt.toISOString()]
  );

  const checkedElectionIds = attempts.rows
    .filter((attempt) => {
      const row = rowByElectionId.get(attempt.id);
      return row
        ? shouldMarkElectionResultPassChecked({
            passType: input.passType,
            row,
            electionNightAttemptCountAfterThisRun: attempt.election_night_results_attempt_count,
          })
        : false;
    })
    .map((attempt) => attempt.id);

  return {
    attemptedElectionCount: attempts.rowCount ?? 0,
    checkedElectionCount: await markElectionResultPassChecked(db, {
      electionIds: checkedElectionIds,
      passType: input.passType,
      checkedAt,
    }),
    checkedElectionIds,
    uncheckedElectionIds: electionIds.filter((electionId) => !checkedElectionIds.includes(electionId)),
  };
}
