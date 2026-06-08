import {
  STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
  STAGING_ITEM_TYPE_CANDIDATE_PROFILE,
} from "../../config/electionsPipeline.js";
import { normalizeCandidateName } from "../../utils/candidateIdentity.js";

type RedisSendCommandClient = {
  sendCommand(args: string[]): Promise<unknown>;
};

export type CandidateProfileDraftEmitInput = {
  electionId: string;
  runId: string | null;
  displayName: string;
  rosterIndex: number;
  rosterParty?: string;
  rosterIsIncumbent?: boolean;
  disambiguationHint?: string;
  fecIds?: readonly string[];
  stateFilingIdsHint?: readonly string[];
  skipPerElectionNameDedupe?: boolean;
  seedUrls: readonly string[];
  dedupeKey?: string;
};

const PROFILE_DRAFT_EMIT_MARKER_PREFIX = "staging:candidate_profile_draft_emitted:";

const EMIT_CANDIDATE_PROFILE_DRAFT_IF_NEEDED_LUA = `
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
  ARGV[3],
  "candidate_display_name",
  ARGV[4],
  "roster_party",
  ARGV[5],
  "roster_is_incumbent",
  ARGV[6],
  "seed_urls",
  ARGV[7],
  "roster_index",
  ARGV[8],
  "disambiguation_hint",
  ARGV[9],
  "skip_per_election_name_dedupe",
  ARGV[10],
  "roster_fec_ids",
  ARGV[11],
  "roster_state_filing_ids",
  ARGV[12],
  "emitted_at",
  ARGV[13]
)
redis.call("SET", KEYS[2], ARGV[13])
return 1
`;

function normalizeStringArray(values: readonly string[] | undefined, transform: (value: string) => string): string[] {
  return [...new Set((values ?? []).map((value) => transform(value.trim())).filter((value) => value.length > 0))].sort();
}

function normalizeSeedUrls(values: readonly string[]): string[] {
  const urls: string[] = [];
  const seen = new Set<string>();
  for (const value of values) {
    const trimmed = value.trim();
    if (trimmed.length === 0 || seen.has(trimmed)) {
      continue;
    }
    seen.add(trimmed);
    urls.push(trimmed);
  }
  return urls;
}

function markerKeyForInput(input: CandidateProfileDraftEmitInput, normalizedName: string): string {
  if (input.dedupeKey?.trim()) {
    return `${PROFILE_DRAFT_EMIT_MARKER_PREFIX}${input.dedupeKey.trim()}`;
  }
  return `${PROFILE_DRAFT_EMIT_MARKER_PREFIX}${input.electionId}:${normalizedName}:${input.rosterIndex}`;
}

export async function enqueueCandidateProfileDrafts(
  redis: RedisSendCommandClient,
  inputs: readonly CandidateProfileDraftEmitInput[]
): Promise<{ emittedCount: number; skippedCount: number }> {
  const emittedAt = new Date().toISOString();
  let emittedCount = 0;
  let skippedCount = 0;
  const seenMarkerKeys = new Set<string>();

  for (const input of inputs) {
    const electionId = input.electionId.trim();
    const displayName = input.displayName.trim();
    const normalizedName = normalizeCandidateName(displayName);
    if (electionId.length === 0 || normalizedName.length === 0) {
      continue;
    }

    const markerKey = markerKeyForInput(input, normalizedName);
    if (seenMarkerKeys.has(markerKey)) {
      skippedCount += 1;
      continue;
    }
    seenMarkerKeys.add(markerKey);

    const raw = await redis.sendCommand([
      "EVAL",
      EMIT_CANDIDATE_PROFILE_DRAFT_IF_NEEDED_LUA,
      "2",
      STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
      markerKey,
      electionId,
      STAGING_ITEM_TYPE_CANDIDATE_PROFILE,
      input.runId ?? "",
      displayName,
      input.rosterParty ?? "",
      input.rosterIsIncumbent === undefined ? "" : input.rosterIsIncumbent ? "true" : "false",
      JSON.stringify(normalizeSeedUrls(input.seedUrls)),
      String(input.rosterIndex),
      input.disambiguationHint ?? "",
      input.skipPerElectionNameDedupe ? "true" : "false",
      JSON.stringify(normalizeStringArray(input.fecIds, (value) => value.toUpperCase())),
      JSON.stringify(normalizeStringArray(input.stateFilingIdsHint, (value) => value.toUpperCase())),
      emittedAt,
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
