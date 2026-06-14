import type { Pool, PoolClient } from "pg";

import {
  buildPresidentialNomineeAiConfigFromEnv,
  enrichPresidentialNominee,
  type PresidentialNomineeAiConfig,
  type PresidentialNomineeAiInput,
  type PresidentialNomineeAiResult,
} from "../../ai/enrichPresidentialNominee.js";
import type { AiCandidate } from "../../ai/aiCandidates.js";
import {
  loadActivePresidentialCycleCandidatesForNomineeResolution,
  resolvePresidentialNomineeCandidate,
  type PresidentialNomineeCandidateForResolution,
  type PresidentialNomineeResolutionResult,
} from "../presidential/presidentialNomineeResolver.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type PresidentialNomineeEnricherInput = {
  db: Queryable;
  cycleId: string;
  electionYear: number;
  party: string;
  aiConfig?: PresidentialNomineeAiConfig;
  aiCandidates?: readonly AiCandidate[];
  enrichNominee?: (
    input: PresidentialNomineeAiInput,
    config: PresidentialNomineeAiConfig,
    candidates?: readonly AiCandidate[]
  ) => Promise<PresidentialNomineeAiResult>;
  loadCandidatesForResolution?: (
    db: Queryable,
    cycleId: string
  ) => Promise<PresidentialNomineeCandidateForResolution[]>;
};

export type PresidentialNomineeEnricherResult =
  | {
      ok: true;
      cycleId: string;
      electionYear: number;
      party: string;
      provider: string;
      model: string;
      candidateCount: number;
      resolution: PresidentialNomineeResolutionResult;
      aiRawDebug: Record<string, unknown> | null;
    }
  | {
      ok: false;
      cycleId: string;
      electionYear: number;
      party: string;
      error: string;
      retryable: boolean;
      errorCode: string;
      failureDebug?: Record<string, unknown>;
    };

function normalizeNonEmpty(value: string, fieldName: string): string {
  const normalized = value.trim();
  if (normalized.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return normalized;
}

function assertPresidentialElectionYear(electionYear: number): void {
  if (!Number.isInteger(electionYear) || electionYear < 2000 || electionYear > 2100 || electionYear % 4 !== 0) {
    throw new Error(`Invalid presidential nominee election year: ${electionYear}`);
  }
}

export async function enrichPresidentialNomineeForCycle(
  input: PresidentialNomineeEnricherInput
): Promise<PresidentialNomineeEnricherResult> {
  const cycleId = normalizeNonEmpty(input.cycleId, "presidential cycle id");
  const party = normalizeNonEmpty(input.party, "presidential nominee party");
  assertPresidentialElectionYear(input.electionYear);

  const loadCandidates =
    input.loadCandidatesForResolution ?? loadActivePresidentialCycleCandidatesForNomineeResolution;
  const candidates = await loadCandidates(input.db, cycleId);
  if (candidates.length === 0) {
    return {
      ok: false,
      cycleId,
      electionYear: input.electionYear,
      party,
      error: "No active presidential primary candidates are available for nominee research",
      retryable: false,
      errorCode: "NO_ACTIVE_CANDIDATES",
    };
  }

  const aiConfig = input.aiConfig ?? buildPresidentialNomineeAiConfigFromEnv();
  const enrichNominee = input.enrichNominee ?? enrichPresidentialNominee;
  const aiResult = await enrichNominee(
    {
      cycleId,
      electionYear: input.electionYear,
      party,
      candidates,
    },
    aiConfig,
    input.aiCandidates
  );

  if (!aiResult.ok) {
    return {
      ok: false,
      cycleId,
      electionYear: input.electionYear,
      party,
      error: aiResult.reason,
      retryable: aiResult.retryable,
      errorCode: aiResult.errorCode,
      failureDebug: aiResult.failureDebug,
    };
  }

  return {
    ok: true,
    cycleId,
    electionYear: input.electionYear,
    party,
    provider: aiResult.provider,
    model: aiResult.model,
    candidateCount: candidates.length,
    resolution: resolvePresidentialNomineeCandidate({
      payload: aiResult.payload,
      candidates,
    }),
    aiRawDebug: aiResult.aiRawDebug,
  };
}
