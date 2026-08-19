import { FRONTIER_AI_CANDIDATES, type AiCandidate } from "./aiCandidates.js";
import { getPipelineEnv } from "../config/env.js";
import {
  callResearchProvider,
  trimDebugText,
  type ResearchErrorCode,
} from "./researchProviderClient.js";
import {
  parseCandidateRecordAreaLabelPayload,
  type CandidateRecordAreaLabel,
} from "../contracts/candidateRecordAreaLabelPayloadContract.js";
import {
  buildCandidateRecordAreaLabelPrompt,
  type CandidateRecordAreaLabelPromptGoal,
  type CandidateRecordAreaLabelPromptRecord,
} from "./providers/candidateRecordAreaLabelPrompt.js";
import type { AiProvider } from "./types.js";

type CandidateRecordAreaLabelErrorCode = ResearchErrorCode | "SCHEMA_MISMATCH";

type CandidateRecordAreaLabelFailure = {
  ok: false;
  retryable: boolean;
  errorCode: CandidateRecordAreaLabelErrorCode;
  reason: string;
  failureDebug?: Record<string, unknown>;
};

type ProviderFailureAttempt = {
  provider: string;
  model: string;
  reason: string;
  errorCode: CandidateRecordAreaLabelErrorCode;
  retryable: boolean;
  failureDebug?: Record<string, unknown>;
};

export type EnrichCandidateRecordAreasInput = {
  candidateDisplayName: string;
  districtName: string;
  districtType: string;
  state: string;
  electionDate: string;
  officialBallotTitle: string;
  electionStage?: string | null;
  senateClass?: string | null;
  termEndYear?: string | null;
  allowedResearchAreaSlugs: readonly string[];
  allowedResearchAreaGoals?: readonly CandidateRecordAreaLabelPromptGoal[];
  records: readonly CandidateRecordAreaLabelPromptRecord[];
};

export type EnrichCandidateRecordAreasConfig = {
  timeoutMs: number;
  anthropicWebSearchMaxUses?: number;
  openAiApiKey?: string;
  anthropicApiKey?: string;
  geminiApiKey?: string;
};

export type EnrichCandidateRecordAreasResult =
  | {
      ok: true;
      provider: AiProvider;
      model: string;
      labels: CandidateRecordAreaLabel[];
      aiRawDebug: Record<string, unknown> | null;
    }
  | CandidateRecordAreaLabelFailure;

const CLAUDE_INTER_CALL_DELAY_MS = 20_000;
const CLAUDE_RETRY_AFTER_BUFFER_MS = 10_000;

async function callProvider(
  candidate: AiCandidate,
  prompt: string,
  config: EnrichCandidateRecordAreasConfig
): Promise<{ ok: true; parsed: unknown; rawText: string; debugMeta?: Record<string, unknown> } | CandidateRecordAreaLabelFailure> {
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

export function buildCandidateRecordAreasConfigFromEnv(): EnrichCandidateRecordAreasConfig {
  const env = getPipelineEnv();
  return {
    timeoutMs: env.AI_TIMEOUT_MS,
    anthropicWebSearchMaxUses: env.ANTHROPIC_WEB_SEARCH_MAX_USES,
    openAiApiKey: env.OPENAI_API_KEY,
    anthropicApiKey: env.ANTHROPIC_API_KEY,
    geminiApiKey: env.GEMINI_API_KEY,
  };
}

export async function enrichCandidateRecordAreas(
  input: EnrichCandidateRecordAreasInput,
  config: EnrichCandidateRecordAreasConfig,
  candidates: readonly AiCandidate[] = FRONTIER_AI_CANDIDATES
): Promise<EnrichCandidateRecordAreasResult> {
  const failures: ProviderFailureAttempt[] = [];
  const reviewFeedbackLines = new Set<string>();
  const allowedSlugSet = new Set(input.allowedResearchAreaSlugs.map((slug) => slug.trim().toLowerCase()));

  for (const candidate of candidates) {
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const prompt = buildCandidateRecordAreaLabelPrompt({
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

      const parsed = parseCandidateRecordAreaLabelPayload(generated.parsed, {
        allowedResearchAreaSlugs: allowedSlugSet,
        recordCount: input.records.length,
        requireLabelForEveryRecord: true,
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
        labels: parsed.payload.labels,
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
    reason: "No AI candidates available for candidate record area labeling",
  };
}
