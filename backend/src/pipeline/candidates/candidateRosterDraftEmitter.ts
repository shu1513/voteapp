import {
  STAGING_CANDIDATE_ROSTER_DRAFT_STREAM,
  STAGING_ITEM_TYPE_CANDIDATE_ROSTER,
} from "../../config/electionsPipeline.js";

type RedisSendCommandClient = {
  sendCommand(args: string[]): Promise<unknown>;
};

const CANDIDATE_ROSTER_EMIT_MARKER_PREFIX = "staging:candidate_roster_emitted:";
const CANDIDATE_ROSTER_EMIT_MARKER_TTL_SECONDS = 86_400;

const EMIT_CANDIDATE_ROSTER_DRAFT_IF_NEEDED_LUA = `
if redis.call("EXISTS", KEYS[2]) == 1 then
  return 0
end
redis.call(
  "XADD",
  KEYS[1],
  "*",
  "election_id",
  ARGV[1],
  "item_type",
  ARGV[2],
  "run_id",
  ARGV[3]
)
redis.call("SET", KEYS[2], ARGV[4], "EX", ARGV[5])
return 1
`;

export async function enqueueCandidateRosterDrafts(
  redis: RedisSendCommandClient,
  electionIds: readonly string[],
  runId: string | null
): Promise<{ emittedCount: number; skippedCount: number }> {
  const emittedAt = new Date().toISOString();
  const uniqueElectionIds = [...new Set(electionIds)];
  let emittedCount = 0;
  let skippedCount = 0;

  for (const electionId of uniqueElectionIds) {
    const markerKey = `${CANDIDATE_ROSTER_EMIT_MARKER_PREFIX}${electionId}`;
    const raw = await redis.sendCommand([
      "EVAL",
      EMIT_CANDIDATE_ROSTER_DRAFT_IF_NEEDED_LUA,
      "2",
      STAGING_CANDIDATE_ROSTER_DRAFT_STREAM,
      markerKey,
      electionId,
      STAGING_ITEM_TYPE_CANDIDATE_ROSTER,
      runId ?? "",
      emittedAt,
      String(CANDIDATE_ROSTER_EMIT_MARKER_TTL_SECONDS),
    ]);
    const value = typeof raw === "number" ? raw : Number.parseInt(String(raw), 10);
    if (Number.isFinite(value) && value === 1) {
      emittedCount += 1;
    } else {
      skippedCount += 1;
    }
  }

  return { emittedCount, skippedCount };
}
