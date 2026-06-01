import {
  STAGING_CANDIDATE_RECORD_DRAFT_STREAM,
  STAGING_ITEM_TYPE_CANDIDATE_RECORD,
} from "../../config/electionsPipeline.js";

type RedisSendCommandClient = {
  sendCommand(args: string[]): Promise<unknown>;
};

export type CandidateRecordDraftEmitInput = {
  candidateId: string;
  electionId: string;
  runId: string | null;
};

const CANDIDATE_RECORD_DRAFT_EMIT_MARKER_PREFIX = "staging:candidate_record_draft_emitted:";
const CANDIDATE_RECORD_DRAFT_EMIT_MARKER_TTL_SECONDS = 86_400;

const EMIT_CANDIDATE_RECORD_DRAFT_IF_NEEDED_LUA = `
if redis.call("EXISTS", KEYS[2]) == 1 then
  return 0
end
redis.call(
  "XADD",
  KEYS[1],
  "*",
  "candidate_id",
  ARGV[1],
  "election_id",
  ARGV[2],
  "item_type",
  ARGV[3],
  "run_id",
  ARGV[4],
  "emitted_at",
  ARGV[5]
)
redis.call("SET", KEYS[2], ARGV[5], "EX", ARGV[6])
return 1
`;

export async function enqueueCandidateRecordDrafts(
  redis: RedisSendCommandClient,
  inputs: readonly CandidateRecordDraftEmitInput[]
): Promise<{ emittedCount: number; skippedCount: number }> {
  const emittedAt = new Date().toISOString();
  let emittedCount = 0;
  let skippedCount = 0;
  const seenCandidates = new Set<string>();

  for (const input of inputs) {
    const candidateId = input.candidateId.trim();
    const electionId = input.electionId.trim();
    if (candidateId.length === 0 || electionId.length === 0) {
      continue;
    }
    if (seenCandidates.has(candidateId)) {
      skippedCount += 1;
      continue;
    }
    seenCandidates.add(candidateId);

    const markerKey = `${CANDIDATE_RECORD_DRAFT_EMIT_MARKER_PREFIX}${candidateId}`;
    const raw = await redis.sendCommand([
      "EVAL",
      EMIT_CANDIDATE_RECORD_DRAFT_IF_NEEDED_LUA,
      "2",
      STAGING_CANDIDATE_RECORD_DRAFT_STREAM,
      markerKey,
      candidateId,
      electionId,
      STAGING_ITEM_TYPE_CANDIDATE_RECORD,
      input.runId ?? "",
      emittedAt,
      String(CANDIDATE_RECORD_DRAFT_EMIT_MARKER_TTL_SECONDS),
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
