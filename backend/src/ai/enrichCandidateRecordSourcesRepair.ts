import { FRONTIER_AI_CANDIDATES, type AiCandidate } from "./aiCandidates.js";
import { getPipelineEnv } from "../config/env.js";
import {
  callResearchProvider,
  trimDebugText,
  type ResearchErrorCode,
} from "./researchProviderClient.js";
import {
  parseCandidateRecordSourceRepairPayload,
  type CandidateRecordSourceRepair,
} from "../contracts/candidateRecordSourceRepairPayloadContract.js";
import {
  buildCandidateRecordSourceRepairPrompt,
  type CandidateRecordSourceRepairPromptBadRecord,
} from "./providers/candidateRecordSourceRepairPrompt.js";
import type { AiProvider } from "./types.js";

type CandidateRecordSourceRepairErrorCode = ResearchErrorCode | "SCHEMA_MISMATCH";

type CandidateRecordSourceRepairFailure = {
  ok: false;
  retryable: boolean;
  errorCode: CandidateRecordSourceRepairErrorCode;
  reason: string;
  failureDebug?: Record<string, unknown>;
};

type ProviderFailureAttempt = {
  provider: string;
  model: string;
  reason: string;
  errorCode: CandidateRecordSourceRepairErrorCode;
  retryable: boolean;
  failureDebug?: Record<string, unknown>;
};

export type EnrichCandidateRecordSourcesRepairInput = {
  candidateDisplayName: string;
  districtName: string;
  districtType: string;
  state: string;
  electionDate: string;
  officialBallotTitle: string;
  electionStage?: string | null;
  senateClass?: string | null;
  termEndYear?: string | null;
  blockedUrls: readonly string[];
  badRecords: readonly CandidateRecordSourceRepairPromptBadRecord[];
};

export type EnrichCandidateRecordSourcesRepairConfig = {
  timeoutMs: number;
  anthropicWebSearchMaxUses?: number;
  openAiApiKey?: string;
  anthropicApiKey?: string;
  geminiApiKey?: string;
};

export type EnrichCandidateRecordSourcesRepairResult =
  | {
      ok: true;
      provider: AiProvider;
      model: string;
      repairs: CandidateRecordSourceRepair[];
      noReplacementIndexes: number[];
      aiRawDebug: Record<string, unknown> | null;
    }
  | CandidateRecordSourceRepairFailure;

const CLAUDE_INTER_CALL_DELAY_MS = 20_000;
const CLAUDE_RETRY_AFTER_BUFFER_MS = 10_000;

async function callProvider(
  candidate: AiCandidate,
  prompt: string,
  config: EnrichCandidateRecordSourcesRepairConfig
): Promise<
  { ok: true; parsed: unknown; rawText: string; debugMeta?: Record<string, unknown> } | CandidateRecordSourceRepairFailure
> {
  const providerResult = await callResearchProvider(candidate, prompt, {
    timeoutMs: config.timeoutMs,
    anthropicWebSearchMaxUses: config.anthropicWebSearchMaxUses,
    claudeInterCallDelayMs: CLAUDE_INTER_CALL_DELAY_MS,
    claudeRetryAfterBufferMs: CLAUDE_RETRY_AFTER_BUFFER_MS,
    openAiApiKey: config.openAiApiKey,
    anthropicApiKey: config.anthropicApiKey,
    geminiApiKey: config.geminiApiKey,
    geminiApiVersion: "v1",
  });
  if (providerResult.ok) {
    return {
      ok: true,
      parsed: providerResult.parsed,
      rawText: providerResult.rawText,
      debugMeta: providerResult.debugMeta,
    };
  }

  return {
    ok: false,
    retryable: providerResult.retryable,
    errorCode: providerResult.errorCode,
    reason: providerResult.reason,
    failureDebug: providerResult.failureDebug,
  };
}

export function buildCandidateRecordSourcesRepairConfigFromEnv(): EnrichCandidateRecordSourcesRepairConfig {
  const env = getPipelineEnv();
  return {
    timeoutMs: env.AI_TIMEOUT_MS,
    anthropicWebSearchMaxUses: env.ANTHROPIC_WEB_SEARCH_MAX_USES,
    openAiApiKey: env.OPENAI_API_KEY,
    anthropicApiKey: env.ANTHROPIC_API_KEY,
    geminiApiKey: env.GEMINI_API_KEY,
  };
}

export async function enrichCandidateRecordSourcesRepair(
  input: EnrichCandidateRecordSourcesRepairInput,
  config: EnrichCandidateRecordSourcesRepairConfig,
  candidates: readonly AiCandidate[] = FRONTIER_AI_CANDIDATES
): Promise<EnrichCandidateRecordSourcesRepairResult> {
  if (input.badRecords.length === 0) {
    return {
      ok: true,
      provider: "openai",
      model: "none",
      repairs: [],
      noReplacementIndexes: [],
      aiRawDebug: { skipped: true, reason: "no bad records provided" },
    };
  }

  const failures: ProviderFailureAttempt[] = [];
  const reviewFeedbackLines = new Set<string>();

  for (const candidate of candidates) {
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const prompt = buildCandidateRecordSourceRepairPrompt({
        ...input,
        reviewFeedbackLines: [...reviewFeedbackLines],
      });

      const generated = await callProvider(candidate, prompt, config);
      if (!generated.ok) {
        failures.push({
          provider: candidate.provider,
          model: candidate.model,
          reason: generated.reason,
          errorCode: generated.errorCode,
          retryable: generated.retryable,
          failureDebug: generated.failureDebug,
        });
        break;
      }

      const parsed = parseCandidateRecordSourceRepairPayload(generated.parsed, {
        badRecordCount: input.badRecords.length,
      });
      if (!parsed.ok) {
        const feedbackLine = `Fix payload schema: ${parsed.reason}.`;
        reviewFeedbackLines.add(feedbackLine);
        failures.push({
          provider: candidate.provider,
          model: candidate.model,
          reason: parsed.reason,
          errorCode: "SCHEMA_MISMATCH",
          retryable: false,
          failureDebug: {
            parser_reason: parsed.reason,
            prompt_feedback_line: feedbackLine,
          },
        });
        if (attempt === 0) {
          continue;
        }
        break;
      }

      return {
        ok: true,
        provider: candidate.provider,
        model: candidate.model,
        repairs: parsed.payload.repairs,
        noReplacementIndexes: parsed.payload.no_replacement_indexes,
        aiRawDebug: {
          raw_response_preview: trimDebugText(generated.rawText),
          debug_meta: generated.debugMeta ?? null,
        },
      };
    }
  }

  const selected = failures.find((failure) => failure.retryable) ?? failures[failures.length - 1];
  if (selected) {
    return {
      ok: false,
      retryable: selected.retryable,
      errorCode: selected.errorCode,
      reason: selected.reason,
      failureDebug: {
        attempted_candidates: failures.map((failure) => ({
          provider: failure.provider,
          model: failure.model,
          reason: failure.reason,
          error_code: failure.errorCode,
          retryable: failure.retryable,
          failure_debug: failure.failureDebug ?? null,
        })),
      },
    };
  }

  return {
    ok: false,
    retryable: false,
    errorCode: "CONFIGURATION_ERROR",
    reason: "No AI candidates available for candidate record source repair",
  };
}
