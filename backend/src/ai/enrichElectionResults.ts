import {
  FRONTIER_AI_CANDIDATES,
  type AiCandidate,
} from "./aiCandidates.js";
import { getPipelineEnv } from "../config/env.js";
import { parseElectionResultPayload, type ElectionResultPayload } from "../contracts/electionResultPayloadContract.js";
import type { ElectionResultContext } from "../pipeline/electionResults/electionResultContextLoader.js";
import {
  validateElectionResultSourceUrls,
  type ElectionResultSourceVerification,
} from "../pipeline/electionResults/electionResultSourceValidation.js";
import type { ElectionResultPassType } from "../types/electionResults.js";
import { buildElectionResultPrompt } from "./providers/electionResultPrompt.js";
import {
  callResearchProvider,
  trimDebugText,
  type ResearchErrorCode,
} from "./researchProviderClient.js";
import type { AiProvider } from "./types.js";

type ElectionResultErrorCode = ResearchErrorCode | "SCHEMA_MISMATCH";

type ElectionResultFailure = {
  ok: false;
  retryable: boolean;
  errorCode: ElectionResultErrorCode;
  reason: string;
  failureDebug?: Record<string, unknown>;
};

type ProviderFailureAttempt = {
  provider: string;
  model: string;
  reason: string;
  errorCode: ElectionResultErrorCode;
  retryable: boolean;
  failureDebug?: Record<string, unknown>;
};

export type ElectionResultAiInput = {
  passType: ElectionResultPassType;
  scheduledFor: string;
  contexts: readonly ElectionResultContext[];
};

export type ElectionResultAiConfig = {
  timeoutMs: number;
  anthropicWebSearchMaxUses?: number;
  openAiApiKey?: string;
  anthropicApiKey?: string;
  geminiApiKey?: string;
};

export type ElectionResultAiResult =
  | {
      ok: true;
      provider: AiProvider;
      model: string;
      payload: ElectionResultPayload;
      sourceVerifications: ElectionResultSourceVerification[];
      aiRawDebug: Record<string, unknown> | null;
    }
  | ElectionResultFailure;

const CLAUDE_INTER_CALL_DELAY_MS = 20_000;

async function callProvider(
  candidate: AiCandidate,
  prompt: string,
  config: ElectionResultAiConfig
): Promise<{ ok: true; parsed: unknown; rawText: string; debugMeta?: Record<string, unknown> } | ElectionResultFailure> {
  const providerResult = await callResearchProvider(candidate, prompt, {
    timeoutMs: config.timeoutMs,
    anthropicWebSearchMaxUses: config.anthropicWebSearchMaxUses,
    claudeInterCallDelayMs: CLAUDE_INTER_CALL_DELAY_MS,
    claudeRetryAfterBufferMs: 10_000,
    openAiApiKey: config.openAiApiKey,
    anthropicApiKey: config.anthropicApiKey,
    geminiApiKey: config.geminiApiKey,
    geminiApiVersion: "v1beta",
    geminiResponseMimeTypeJson: true,
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

export function buildElectionResultAiConfigFromEnv(): ElectionResultAiConfig {
  const env = getPipelineEnv();
  return {
    timeoutMs: env.AI_TIMEOUT_MS,
    anthropicWebSearchMaxUses: env.ANTHROPIC_WEB_SEARCH_MAX_USES,
    openAiApiKey: env.OPENAI_API_KEY,
    anthropicApiKey: env.ANTHROPIC_API_KEY,
    geminiApiKey: env.GEMINI_API_KEY,
  };
}

export async function enrichElectionResults(
  input: ElectionResultAiInput,
  config: ElectionResultAiConfig,
  candidates: readonly AiCandidate[] = FRONTIER_AI_CANDIDATES
): Promise<ElectionResultAiResult> {
  const failures: ProviderFailureAttempt[] = [];
  const cumulativeFeedback = new Set<string>();

  for (const candidate of candidates) {
    let reviewFeedbackLines = [...cumulativeFeedback];

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const prompt = buildElectionResultPrompt({
        ...input,
        reviewFeedbackLines,
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

      const parsed = parseElectionResultPayload(generated.parsed, {
        passType: input.passType,
        contexts: input.contexts,
      });
      if (!parsed.ok) {
        failures.push({
          provider: candidate.provider,
          model: candidate.model,
          reason: parsed.reason,
          errorCode: "SCHEMA_MISMATCH",
          retryable: false,
          failureDebug: {
            provider_response_text: trimDebugText(generated.rawText),
          },
        });
        if (attempt === 0) {
          cumulativeFeedback.add(`Fix this payload/schema issue: ${parsed.reason}`);
          reviewFeedbackLines = [...cumulativeFeedback].slice(0, 20);
          continue;
        }
        break;
      }

      const sourceValidation = await validateElectionResultSourceUrls(parsed.payload, {
        timeoutMs: config.timeoutMs,
      });
      if (!sourceValidation.ok) {
        failures.push({
          provider: candidate.provider,
          model: candidate.model,
          reason: sourceValidation.reason,
          errorCode: "SCHEMA_MISMATCH",
          retryable: false,
          failureDebug: {
            provider_response_text: trimDebugText(generated.rawText),
            ...(sourceValidation.failureDebug ?? {}),
          },
        });
        if (attempt === 0) {
          for (const line of sourceValidation.reviewFeedbackLines) {
            cumulativeFeedback.add(line);
          }
          reviewFeedbackLines = [...cumulativeFeedback].slice(0, 20);
          continue;
        }
        break;
      }

      return {
        ok: true,
        provider: candidate.provider,
        model: candidate.model,
        payload: sourceValidation.payload,
        sourceVerifications: sourceValidation.sourceVerifications,
        aiRawDebug: {
          provider_response_text: trimDebugText(generated.rawText),
          source_verifications: sourceValidation.sourceVerifications,
          ...(failures.length > 0 ? { prior_failed_attempts: failures } : {}),
          ...(generated.debugMeta ?? {}),
        },
      };
    }
  }

  const finalFailure = failures[failures.length - 1];
  const anyRetryable = failures.some((failure) => failure.retryable);
  const firstRetryable = failures.find((failure) => failure.retryable);
  const firstPermanent = failures.find((failure) => !failure.retryable);
  const selected = anyRetryable ? (firstRetryable ?? finalFailure) : (firstPermanent ?? finalFailure);

  return {
    ok: false,
    retryable: selected?.retryable ?? false,
    errorCode: selected?.errorCode ?? "TEMP_PROVIDER_ERROR",
    reason: selected?.reason ?? "No AI candidates available for election-result enrichment",
    failureDebug: {
      attempts: failures,
      prompt_preview: trimDebugText(
        buildElectionResultPrompt({
          ...input,
          reviewFeedbackLines: [],
        }),
        6000
      ),
      ...(selected?.failureDebug ?? {}),
    },
  };
}
