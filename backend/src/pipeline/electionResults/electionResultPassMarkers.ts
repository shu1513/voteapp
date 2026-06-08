import type { ElectionResultPassType } from "../../types/electionResults.js";

export const ELECTION_RESULT_PASS_EMITTED_MARKER_PREFIX = "staging:election_result_emitted:";
export const ELECTION_RESULT_PASS_EMITTED_MARKER_TTL_SECONDS = 7 * 24 * 60 * 60;

type RedisMarkerClient = {
  exists(key: string): Promise<number>;
  set(key: string, value: string, options?: { EX?: number }): Promise<unknown>;
  del(key: string): Promise<number>;
};

export function buildElectionResultPassEmittedMarkerKey(input: {
  electionId: string;
  passType: ElectionResultPassType;
}): string {
  return `${ELECTION_RESULT_PASS_EMITTED_MARKER_PREFIX}${input.passType}:${input.electionId}`;
}

export async function isElectionResultPassEmitted(
  redis: Pick<RedisMarkerClient, "exists">,
  input: { electionId: string; passType: ElectionResultPassType }
): Promise<boolean> {
  const count = await redis.exists(buildElectionResultPassEmittedMarkerKey(input));
  return count > 0;
}

export async function markElectionResultPassEmitted(
  redis: Pick<RedisMarkerClient, "set">,
  input: {
    electionId: string;
    passType: ElectionResultPassType;
    emittedAt: string;
    ttlSeconds?: number;
  }
): Promise<void> {
  await redis.set(
    buildElectionResultPassEmittedMarkerKey(input),
    input.emittedAt,
    { EX: input.ttlSeconds ?? ELECTION_RESULT_PASS_EMITTED_MARKER_TTL_SECONDS }
  );
}

export async function clearElectionResultPassEmitted(
  redis: Pick<RedisMarkerClient, "del">,
  input: { electionId: string; passType: ElectionResultPassType }
): Promise<void> {
  await redis.del(buildElectionResultPassEmittedMarkerKey(input));
}
