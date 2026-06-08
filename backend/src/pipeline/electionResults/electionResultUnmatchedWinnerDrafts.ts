import type { ElectionResultPayload } from "../../contracts/electionResultPayloadContract.js";
import type { CandidateProfileDraftEmitInput } from "../candidates/candidateProfileDraftEmitter.js";
import type { ElectionResultContext } from "./electionResultContextLoader.js";
import { normalizeCandidateName } from "../../utils/candidateIdentity.js";

const RESULT_WINNER_ROSTER_INDEX_OFFSET = 100_000;

function contextByElectionId(contexts: readonly ElectionResultContext[]): Map<string, ElectionResultContext> {
  return new Map(contexts.map((context) => [context.electionId, context]));
}

export function buildCandidateProfileDraftsForUnmatchedElectionResultWinners(input: {
  contexts: readonly ElectionResultContext[];
  payload: ElectionResultPayload;
  runId: string | null;
}): CandidateProfileDraftEmitInput[] {
  const contextsById = contextByElectionId(input.contexts);
  const drafts: CandidateProfileDraftEmitInput[] = [];
  const seen = new Set<string>();

  for (const [rowIndex, row] of input.payload.results.entries()) {
    const context = contextsById.get(row.election_id);
    if (!context || context.raceType !== "office") {
      continue;
    }
    if (row.match_status !== "unmatched" && row.match_status !== "partial") {
      continue;
    }

    for (const [winnerIndex, winner] of row.winners.entries()) {
      if (winner.candidate_election_id) {
        continue;
      }
      const displayName = winner.candidate_name?.trim() ?? "";
      const normalizedName = normalizeCandidateName(displayName);
      if (normalizedName.length === 0) {
        continue;
      }

      const key = `${row.election_id}:${normalizedName}`;
      if (seen.has(key)) {
        continue;
      }
      seen.add(key);

      drafts.push({
        electionId: row.election_id,
        runId: input.runId,
        displayName,
        rosterIndex: RESULT_WINNER_ROSTER_INDEX_OFFSET + rowIndex * 100 + winnerIndex,
        rosterParty: winner.party,
        disambiguationHint: `Winner/advancer reported by election result source for ${context.officialBallotTitle}.`,
        skipPerElectionNameDedupe: false,
        seedUrls: [row.source_url],
        dedupeKey: `election_result_winner:${row.election_id}:${normalizedName}`,
      });
    }
  }

  return drafts;
}
