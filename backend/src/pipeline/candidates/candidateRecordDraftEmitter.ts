import {
  STAGING_CANDIDATE_RECORD_DRAFT_STREAM,
  STAGING_ITEM_TYPE_CANDIDATE_RECORD,
} from "../../config/electionsPipeline.js";

type RedisSendCommandClient = {
  sendCommand(args: string[]): Promise<unknown>;
};

export type PresidentialCandidateRecordDraftRole = "president" | "vice_president";

export type ElectionCandidateRecordDraftEmitInput = {
  contextType?: "election";
  candidateId: string;
  electionId: string;
  presidentialCycleId?: never;
  presidentialRole?: never;
  runId: string | null;
};

export type PresidentialCycleCandidateRecordDraftEmitInput = {
  contextType: "presidential_cycle";
  candidateId: string;
  presidentialCycleId: string;
  presidentialRole: PresidentialCandidateRecordDraftRole;
  electionId?: never;
  runId: string | null;
};

export type CandidateRecordDraftEmitInput =
  | ElectionCandidateRecordDraftEmitInput
  | PresidentialCycleCandidateRecordDraftEmitInput;

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
  ARGV[5],
  "context_type",
  ARGV[7],
  "presidential_cycle_id",
  ARGV[8],
  "presidential_role",
  ARGV[9]
)
redis.call("SET", KEYS[2], ARGV[5], "EX", ARGV[6])
return 1
`;

function contextTypeForInput(input: CandidateRecordDraftEmitInput): "election" | "presidential_cycle" {
  return input.contextType ?? "election";
}

function electionIdForInput(input: CandidateRecordDraftEmitInput): string {
  return input.contextType === "presidential_cycle" ? "" : input.electionId.trim();
}

function presidentialCycleIdForInput(input: CandidateRecordDraftEmitInput): string {
  return input.contextType === "presidential_cycle" ? input.presidentialCycleId.trim() : "";
}

function presidentialRoleForInput(input: CandidateRecordDraftEmitInput): string {
  return input.contextType === "presidential_cycle" ? input.presidentialRole : "";
}

function markerKeyForInput(input: CandidateRecordDraftEmitInput, candidateId: string): string {
  if (input.contextType === "presidential_cycle") {
    return `${CANDIDATE_RECORD_DRAFT_EMIT_MARKER_PREFIX}presidential_cycle:${candidateId}:${input.presidentialCycleId.trim()}:${input.presidentialRole}`;
  }
  return `${CANDIDATE_RECORD_DRAFT_EMIT_MARKER_PREFIX}${candidateId}`;
}

export async function enqueueCandidateRecordDrafts(
  redis: RedisSendCommandClient,
  inputs: readonly CandidateRecordDraftEmitInput[]
): Promise<{ emittedCount: number; skippedCount: number }> {
  const emittedAt = new Date().toISOString();
  let emittedCount = 0;
  let skippedCount = 0;
  const seenMarkerKeys = new Set<string>();

  for (const input of inputs) {
    const candidateId = input.candidateId.trim();
    const electionId = electionIdForInput(input);
    const contextId = input.contextType === "presidential_cycle" ? presidentialCycleIdForInput(input) : electionId;
    if (candidateId.length === 0 || contextId.length === 0) {
      continue;
    }
    const markerKey = markerKeyForInput(input, candidateId);
    if (seenMarkerKeys.has(markerKey)) {
      skippedCount += 1;
      continue;
    }
    seenMarkerKeys.add(markerKey);

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
      contextTypeForInput(input),
      presidentialCycleIdForInput(input),
      presidentialRoleForInput(input),
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
