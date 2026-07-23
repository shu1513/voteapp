import {
  FRONTIER_AI_CANDIDATES,
  type AiCandidate,
} from "./aiCandidates.js";
import { getPipelineEnv } from "../config/env.js";
import {
  parsePresidentialPrimaryDatePayloadPartial,
  type PresidentialPrimaryDatePayload,
  type PresidentialPrimaryDatePayloadRowFailure,
} from "../contracts/presidentialPrimaryDatePayloadContract.js";
import {
  validatePresidentialPrimaryDateSourceUrlsPartial,
  type PresidentialPrimaryDateSourceVerification,
} from "../pipeline/presidential/presidentialPrimaryDateSourceValidation.js";
import { buildPresidentialPrimaryDatePrompt } from "./providers/presidentialPrimaryDatePrompt.js";
import {
  callResearchProvider,
  trimDebugText,
  type ResearchErrorCode,
} from "./researchProviderClient.js";
import type { AiProvider } from "./types.js";

type PresidentialPrimaryDateErrorCode = ResearchErrorCode | "SCHEMA_MISMATCH";

type PresidentialPrimaryDateFailure = {
  ok: false;
  retryable: boolean;
  errorCode: PresidentialPrimaryDateErrorCode;
  reason: string;
  failureDebug?: Record<string, unknown>;
};

type ProviderFailureAttempt = {
  provider: string;
  model: string;
  reason: string;
  errorCode: PresidentialPrimaryDateErrorCode;
  retryable: boolean;
  failureDebug?: Record<string, unknown>;
};

export type PresidentialPrimaryDateAiInput = {
  cycleId: string;
  electionName: string;
  electionYear: number;
  party: string;
  stateFipsList: readonly string[];
  scheduledFor: string;
};

export type PresidentialPrimaryDateAiConfig = {
  timeoutMs: number;
  anthropicWebSearchMaxUses?: number;
  openAiApiKey?: string;
  anthropicApiKey?: string;
  geminiApiKey?: string;
};

export type PresidentialPrimaryDateAiResult =
  | {
      ok: true;
      provider: AiProvider;
      model: string;
      payload: PresidentialPrimaryDatePayload;
      failedRows: PresidentialPrimaryDatePayloadRowFailure[];
      sourceVerifications: PresidentialPrimaryDateSourceVerification[];
      aiRawDebug: Record<string, unknown> | null;
    }
  | PresidentialPrimaryDateFailure;

const CLAUDE_INTER_CALL_DELAY_MS = 20_000;

async function callProvider(
  candidate: AiCandidate,
  prompt: string,
  config: PresidentialPrimaryDateAiConfig
): Promise<
  | { ok: true; parsed: unknown; rawText: string; debugMeta?: Record<string, unknown> }
  | PresidentialPrimaryDateFailure
> {
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

export function buildPresidentialPrimaryDateAiConfigFromEnv(): PresidentialPrimaryDateAiConfig {
  const env = getPipelineEnv();
  return {
    timeoutMs: env.AI_TIMEOUT_MS,
    anthropicWebSearchMaxUses: env.ANTHROPIC_WEB_SEARCH_MAX_USES,
    openAiApiKey: env.OPENAI_API_KEY,
    anthropicApiKey: env.ANTHROPIC_API_KEY,
    geminiApiKey: env.GEMINI_API_KEY,
  };
}

export async function enrichPresidentialPrimaryDates(
  input: PresidentialPrimaryDateAiInput,
  config: PresidentialPrimaryDateAiConfig,
  candidates: readonly AiCandidate[] = FRONTIER_AI_CANDIDATES
): Promise<PresidentialPrimaryDateAiResult> {
  const failures: ProviderFailureAttempt[] = [];
  const cumulativeFeedback = new Set<string>();

  for (const candidate of candidates) {
    let reviewFeedbackLines = [...cumulativeFeedback];

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const prompt = buildPresidentialPrimaryDatePrompt({
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

      const parsed = parsePresidentialPrimaryDatePayloadPartial(generated.parsed, {
        electionYear: input.electionYear,
        expectedStateFips: input.stateFipsList,
      });
      if (parsed.payload.results.length === 0) {
        failures.push({
          provider: candidate.provider,
          model: candidate.model,
          reason: parsed.reason ?? "presidential primary date payload had no usable rows",
          errorCode: "SCHEMA_MISMATCH",
          retryable: false,
          failureDebug: {
            provider_response_text: trimDebugText(generated.rawText),
            failed_rows: parsed.failedRows,
            ignored_rows: parsed.ignoredRowReasons,
          },
        });
        if (attempt === 0) {
          for (const line of parsed.reviewFeedbackLines) {
            cumulativeFeedback.add(line);
          }
          reviewFeedbackLines = [...cumulativeFeedback].slice(0, 20);
          continue;
        }
        break;
      }

      const sourceValidation = await validatePresidentialPrimaryDateSourceUrlsPartial(parsed.payload, {
        timeoutMs: config.timeoutMs,
      });
      const failedRows = [
        ...parsed.failedRows,
        ...sourceValidation.failedRows.map((failure) => ({
          state_fips: failure.state_fips,
          reason: failure.reason,
        })),
      ].sort((a, b) => a.state_fips.localeCompare(b.state_fips));

      if (sourceValidation.payload.results.length === 0) {
        failures.push({
          provider: candidate.provider,
          model: candidate.model,
          reason:
            sourceValidation.failedRows[0]?.reason ??
            parsed.reason ??
            "presidential primary date source validation had no usable rows",
          errorCode: "SCHEMA_MISMATCH",
          retryable: false,
          failureDebug: {
            provider_response_text: trimDebugText(generated.rawText),
            ...(sourceValidation.failureDebug ?? {}),
            failed_rows: failedRows,
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

      if (failedRows.length > 0 && attempt === 0) {
        failures.push({
          provider: candidate.provider,
          model: candidate.model,
          reason: `presidential primary date payload partially failed for state_fips: ${failedRows
            .map((failure) => failure.state_fips)
            .join(", ")}`,
          errorCode: "SCHEMA_MISMATCH",
          retryable: false,
          failureDebug: {
            provider_response_text: trimDebugText(generated.rawText),
            failed_rows: failedRows,
            ...(sourceValidation.failureDebug ?? {}),
          },
        });
        for (const line of parsed.reviewFeedbackLines) {
          cumulativeFeedback.add(line);
        }
        for (const line of sourceValidation.reviewFeedbackLines) {
          cumulativeFeedback.add(line);
        }
        reviewFeedbackLines = [...cumulativeFeedback].slice(0, 20);
        continue;
      }

      return {
        ok: true,
        provider: candidate.provider,
        model: candidate.model,
        payload: sourceValidation.payload,
        failedRows,
        sourceVerifications: sourceValidation.sourceVerifications,
        aiRawDebug: {
          provider_response_text: trimDebugText(generated.rawText),
          source_verifications: sourceValidation.sourceVerifications,
          ...(failedRows.length > 0 ? { failed_rows: failedRows } : {}),
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
    reason: selected?.reason ?? "No AI candidates available for presidential primary-date research",
    failureDebug: {
      attempts: failures,
      prompt_preview: trimDebugText(
        buildPresidentialPrimaryDatePrompt({
          ...input,
          reviewFeedbackLines: [],
        }),
        6000
      ),
      ...(selected?.failureDebug ?? {}),
    },
  };
}
