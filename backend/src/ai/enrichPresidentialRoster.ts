import { CANDIDATES_AI_CANDIDATES, type AiCandidate } from "./aiCandidates.js";
import { getPipelineEnv } from "../config/env.js";
import {
  parsePresidentialRosterPayload,
  type PresidentialRosterCandidate,
} from "../contracts/presidentialRosterPayloadContract.js";
import { buildPresidentialRosterPrompt, type PresidentialRosterPromptStage } from "./providers/presidentialRosterPrompt.js";
import {
  callResearchProvider,
  trimDebugText,
  type ResearchErrorCode,
} from "./researchProviderClient.js";
import type { AiProvider } from "./types.js";

type PresidentialRosterErrorCode = ResearchErrorCode | "SCHEMA_MISMATCH";

type PresidentialRosterFailure = {
  ok: false;
  retryable: boolean;
  errorCode: PresidentialRosterErrorCode;
  reason: string;
  failureDebug?: Record<string, unknown>;
};

type ProviderFailureAttempt = {
  provider: string;
  model: string;
  reason: string;
  errorCode: PresidentialRosterErrorCode;
  retryable: boolean;
  failureDebug?: Record<string, unknown>;
};

export type PresidentialRosterAiInput = {
  cycleId: string;
  electionYear: number;
  stage: PresidentialRosterPromptStage;
  party: string | null;
};

export type PresidentialRosterAiConfig = {
  timeoutMs: number;
  anthropicWebSearchMaxUses?: number;
  openAiApiKey?: string;
  anthropicApiKey?: string;
  geminiApiKey?: string;
};

export type PresidentialRosterAiResult =
  | {
      ok: true;
      provider: AiProvider;
      model: string;
      candidates: PresidentialRosterCandidate[];
      aiRawDebug: Record<string, unknown> | null;
    }
  | PresidentialRosterFailure;

const CLAUDE_INTER_CALL_DELAY_MS = 20_000;
const CLAUDE_RETRY_AFTER_BUFFER_MS = 10_000;

async function callProvider(
  candidate: AiCandidate,
  prompt: string,
  config: PresidentialRosterAiConfig
): Promise<
  | { ok: true; parsed: unknown; rawText: string; debugMeta?: Record<string, unknown> }
  | PresidentialRosterFailure
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

export function buildPresidentialRosterAiConfigFromEnv(): PresidentialRosterAiConfig {
  const env = getPipelineEnv();
  return {
    timeoutMs: env.AI_TIMEOUT_MS,
    anthropicWebSearchMaxUses: env.ANTHROPIC_WEB_SEARCH_MAX_USES,
    openAiApiKey: env.OPENAI_API_KEY,
    anthropicApiKey: env.ANTHROPIC_API_KEY,
    geminiApiKey: env.GEMINI_API_KEY,
  };
}

export async function enrichPresidentialRoster(
  input: PresidentialRosterAiInput,
  config: PresidentialRosterAiConfig,
  candidates: readonly AiCandidate[] = CANDIDATES_AI_CANDIDATES
): Promise<PresidentialRosterAiResult> {
  const failures: ProviderFailureAttempt[] = [];
  const expectedParty = input.stage === "primary" ? input.party?.trim() || null : null;

  for (const candidate of candidates) {
    let reviewFeedbackLines: string[] = [];

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const prompt = buildPresidentialRosterPrompt({
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

      const parsed = parsePresidentialRosterPayload(generated.parsed, {
        expectedParty,
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
          reviewFeedbackLines = [`Fix validation issue: ${parsed.reason}`];
          continue;
        }
        break;
      }

      return {
        ok: true,
        provider: candidate.provider,
        model: candidate.model,
        candidates: parsed.payload.candidates,
        aiRawDebug: {
          provider_response_text: trimDebugText(generated.rawText),
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
    reason: selected?.reason ?? "No AI candidates available for presidential roster research",
    failureDebug: {
      attempts: failures,
      prompt_preview: trimDebugText(
        buildPresidentialRosterPrompt({
          ...input,
          reviewFeedbackLines: [],
        }),
        6000
      ),
      ...(selected?.failureDebug ?? {}),
    },
  };
}
