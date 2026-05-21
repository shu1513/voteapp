import { BALLOT_MEASURES_AI_CANDIDATES, type AiCandidate } from "./aiCandidates.js";
import { getPipelineEnv } from "../config/env.js";
import {
  callResearchProvider,
  trimDebugText,
  type ResearchErrorCode,
} from "./researchProviderClient.js";
import { normalizeHttpUrl } from "../utils/normalizeHttpUrl.js";
import type { AiProvider } from "./types.js";
import { buildBallotMeasuresPrompt } from "./providers/ballotMeasuresPrompt.js";
import { verifyHttpUrlReachability } from "./urlReachability.js";

type BallotMeasureErrorCode = ResearchErrorCode | "SCHEMA_MISMATCH";

type EnrichmentFailure = {
  ok: false;
  retryable: boolean;
  errorCode: BallotMeasureErrorCode;
  reason: string;
  failureDebug?: Record<string, unknown>;
};

type ProviderFailureAttempt = {
  provider: string;
  model: string;
  reason: string;
  errorCode: BallotMeasureErrorCode;
  retryable: boolean;
  failureDebug?: Record<string, unknown>;
};

type BallotMeasureValidationResult =
  | {
      ok: true;
      officialMeasureUrl: string;
      whatYesMeans: string;
      whatNoMeans: string;
      sources: string[];
    }
  | {
      ok: false;
      reason: string;
      blockedUrls: string[];
      failureDebug?: Record<string, unknown>;
    };

export type BallotMeasureAiInput = {
  districtName: string;
  districtType: string;
  state: string;
  electionDate: string;
  officialBallotTitle: string;
  seedUrls: readonly string[];
};

export type BallotMeasureAiConfig = {
  timeoutMs: number;
  anthropicWebSearchMaxUses?: number;
  openAiApiKey?: string;
  anthropicApiKey?: string;
  geminiApiKey?: string;
};

export type BallotMeasureAiResult =
  | {
      ok: true;
      provider: AiProvider;
      model: string;
      officialMeasureUrl: string;
      whatYesMeans: string;
      whatNoMeans: string;
      researchUrls: string[];
      aiRawDebug: Record<string, unknown> | null;
    }
  | EnrichmentFailure;

const MAX_SOURCE_URLS_TO_VERIFY = 20;
const SOURCE_URL_VERIFY_CONCURRENCY = 4;
const CLAUDE_INTER_CALL_DELAY_MS = 20_000;

function parseBallotMeasureAiPayload(payload: unknown): {
  ok: true;
  officialMeasureUrl: string;
  whatYesMeans: string;
  whatNoMeans: string;
  sources: string[];
} | {
  ok: false;
  reason: string;
} {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return { ok: false, reason: "payload must be an object" };
  }

  const input = payload as Record<string, unknown>;
  if (typeof input.official_measure_url !== "string") {
    return { ok: false, reason: "official_measure_url must be string" };
  }
  if (typeof input.what_yes_means !== "string") {
    return { ok: false, reason: "what_yes_means must be string" };
  }
  if (typeof input.what_no_means !== "string") {
    return { ok: false, reason: "what_no_means must be string" };
  }
  if (!Array.isArray(input.sources)) {
    return { ok: false, reason: "sources must be array" };
  }

  const officialMeasureUrl = normalizeHttpUrl(input.official_measure_url);
  if (!officialMeasureUrl) {
    return { ok: false, reason: "official_measure_url must be valid http(s) URL" };
  }

  const whatYesMeans = input.what_yes_means.trim();
  const whatNoMeans = input.what_no_means.trim();
  if (whatYesMeans.length === 0 || whatNoMeans.length === 0) {
    return { ok: false, reason: "what_yes_means/what_no_means must be non-empty" };
  }

  const sources: string[] = [];
  const seenSourceUrls = new Set<string>();
  for (const raw of input.sources) {
    if (typeof raw !== "string") {
      return { ok: false, reason: "sources must contain URL strings" };
    }
    const normalized = normalizeHttpUrl(raw);
    if (!normalized) {
      return { ok: false, reason: "sources must contain valid http(s) URLs" };
    }
    if (seenSourceUrls.has(normalized)) {
      continue;
    }
    seenSourceUrls.add(normalized);
    sources.push(normalized);
  }
  if (sources.length === 0) {
    return { ok: false, reason: "sources must contain at least one URL" };
  }

  return {
    ok: true,
    officialMeasureUrl,
    whatYesMeans,
    whatNoMeans,
    sources,
  };
}

async function validateBallotMeasurePayload(
  payload: unknown,
  timeoutMs: number
): Promise<BallotMeasureValidationResult> {
  const parsed = parseBallotMeasureAiPayload(payload);
  if (!parsed.ok) {
    return { ok: false, reason: parsed.reason, blockedUrls: [] };
  }

  const verificationTimeoutMs = Math.min(timeoutMs, 15_000);
  const officialVerification = await verifyHttpUrlReachability(parsed.officialMeasureUrl, {
    timeoutMs: verificationTimeoutMs,
    allowStatusCodes: [403],
  });
  if (!officialVerification.ok) {
    return {
      ok: false,
      reason: `official_measure_url is not reachable: ${officialVerification.reason}`,
      blockedUrls: [parsed.officialMeasureUrl],
      failureDebug: {
        official_measure_url: parsed.officialMeasureUrl,
        official_measure_url_verification_reason: officialVerification.reason,
      },
    };
  }

  const uniqueSourceUrls = [...new Set(parsed.sources)].slice(0, MAX_SOURCE_URLS_TO_VERIFY);
  type SourceCheck = {
    url: string;
    verification: Awaited<ReturnType<typeof verifyHttpUrlReachability>>;
  };
  const sourceChecks: SourceCheck[] = new Array(uniqueSourceUrls.length);
  const concurrency = Math.max(
    1,
    Math.min(SOURCE_URL_VERIFY_CONCURRENCY, uniqueSourceUrls.length)
  );
  let cursor = 0;
  await Promise.all(
    Array.from({ length: concurrency }, async () => {
      while (true) {
        const index = cursor;
        cursor += 1;
        if (index >= uniqueSourceUrls.length) {
          return;
        }

        const url = uniqueSourceUrls[index];
        sourceChecks[index] = {
          url,
          verification: await verifyHttpUrlReachability(url, {
            timeoutMs: Math.min(timeoutMs, 8_000),
            allowStatusCodes: [403],
          }),
        };
      }
    })
  );

  const badSourceChecks = sourceChecks.flatMap((check) =>
    check.verification.ok
      ? []
      : [
          {
            url: check.url,
            reason: check.verification.reason,
          },
        ]
  );
  if (badSourceChecks.length > 0) {
    const firstBad = badSourceChecks[0];
    const badUrls = badSourceChecks.map((check) => check.url);
    return {
      ok: false,
      reason: `source URL is not reachable: ${firstBad.url} (${firstBad.reason})`,
      blockedUrls: badUrls,
      failureDebug: {
        bad_source_urls: badSourceChecks.map((check) => ({
          url: check.url,
          reason: check.reason,
        })),
      },
    };
  }

  const normalizedSources = sourceChecks
    .map((check) => (check.verification.ok ? check.verification.finalUrl : null))
    .filter((url): url is string => typeof url === "string");

  return {
    ok: true,
    officialMeasureUrl: officialVerification.finalUrl,
    whatYesMeans: parsed.whatYesMeans,
    whatNoMeans: parsed.whatNoMeans,
    sources: [...new Set(normalizedSources)],
  };
}

async function callProvider(
  candidate: AiCandidate,
  prompt: string,
  config: BallotMeasureAiConfig
): Promise<{ ok: true; parsed: unknown; rawText: string; debugMeta?: Record<string, unknown> } | EnrichmentFailure> {
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

export function buildBallotMeasureAiConfigFromEnv(): BallotMeasureAiConfig {
  const env = getPipelineEnv();
  return {
    timeoutMs: env.AI_TIMEOUT_MS,
    anthropicWebSearchMaxUses: env.ANTHROPIC_WEB_SEARCH_MAX_USES,
    openAiApiKey: env.OPENAI_API_KEY,
    anthropicApiKey: env.ANTHROPIC_API_KEY,
    geminiApiKey: env.GEMINI_API_KEY,
  };
}

export async function enrichBallotMeasure(
  input: BallotMeasureAiInput,
  config: BallotMeasureAiConfig,
  candidates: readonly AiCandidate[] = BALLOT_MEASURES_AI_CANDIDATES
): Promise<BallotMeasureAiResult> {
  const failures: ProviderFailureAttempt[] = [];
  const cumulativeBlockedUrlFeedback = new Set<string>();

  for (const candidate of candidates) {
    let reviewFeedbackLines: string[] = [...cumulativeBlockedUrlFeedback];

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const prompt = buildBallotMeasuresPrompt({
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

      const validation = await validateBallotMeasurePayload(generated.parsed, config.timeoutMs);
      if (!validation.ok) {
        failures.push({
          provider: candidate.provider,
          model: candidate.model,
          reason: validation.reason,
          errorCode: "SCHEMA_MISMATCH",
          retryable: false,
          failureDebug: {
            provider_response_text: trimDebugText(generated.rawText),
            ...(validation.failureDebug ?? {}),
          },
        });
        const canRetrySameModel = attempt === 0;
        if (canRetrySameModel) {
          for (const blockedUrl of validation.blockedUrls) {
            cumulativeBlockedUrlFeedback.add(`Do not use or cite this URL: ${blockedUrl}`);
          }
          cumulativeBlockedUrlFeedback.add(`Fix this validation issue: ${validation.reason}`);
          reviewFeedbackLines = [...cumulativeBlockedUrlFeedback].slice(0, 20);
          continue;
        }
        break;
      }

      return {
        ok: true,
        provider: candidate.provider,
        model: candidate.model,
        officialMeasureUrl: validation.officialMeasureUrl,
        whatYesMeans: validation.whatYesMeans,
        whatNoMeans: validation.whatNoMeans,
        researchUrls: (() => {
          const urls = new Set<string>();
          for (const url of validation.sources) {
            urls.add(url);
          }
          if (generated.debugMeta && Array.isArray(generated.debugMeta.web_search_urls)) {
            for (const raw of generated.debugMeta.web_search_urls) {
              if (typeof raw !== "string") {
                continue;
              }
              const normalized = normalizeHttpUrl(raw);
              if (normalized) {
                urls.add(normalized);
              }
            }
          }
          urls.add(validation.officialMeasureUrl);
          return [...urls];
        })(),
        aiRawDebug: {
          provider_response_text: trimDebugText(generated.rawText),
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
    reason: selected?.reason ?? "No AI candidates available for ballot-measure enrichment",
    failureDebug: {
      attempts: failures,
      prompt_preview: trimDebugText(
        buildBallotMeasuresPrompt({
          ...input,
          reviewFeedbackLines: [],
        }),
        6000
      ),
      ...(selected?.failureDebug ?? {}),
    },
  };
}
