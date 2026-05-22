import { CANDIDATES_AI_CANDIDATES, type AiCandidate } from "./aiCandidates.js";
import { getPipelineEnv } from "../config/env.js";
import {
  callResearchProvider,
  trimDebugText,
  type ResearchErrorCode,
} from "./researchProviderClient.js";
import { verifyHttpUrlReachability } from "./urlReachability.js";
import {
  type CandidateRosterEntry,
  parseCandidateRosterPayload,
} from "../contracts/candidateRosterPayloadContract.js";
import { buildCandidateRosterPrompt } from "./providers/candidateRosterPrompt.js";
import { normalizeCandidateName } from "../utils/candidateIdentity.js";
import type { AiProvider } from "./types.js";

type CandidateRosterErrorCode = ResearchErrorCode | "SCHEMA_MISMATCH";

type CandidateRosterFailure = {
  ok: false;
  retryable: boolean;
  errorCode: CandidateRosterErrorCode;
  reason: string;
  failureDebug?: Record<string, unknown>;
};

type ProviderFailureAttempt = {
  provider: string;
  model: string;
  reason: string;
  errorCode: CandidateRosterErrorCode;
  retryable: boolean;
  failureDebug?: Record<string, unknown>;
};

type CitationVerificationFailure = {
  candidate_display_name: string;
  url: string;
  reason: string;
  failureType: "transient" | "permanent";
};

export type EnrichCandidateRosterInput = {
  districtName: string;
  districtType: string;
  state: string;
  electionDate: string;
  officialBallotTitle: string;
  seedUrls: readonly string[];
};

export type EnrichCandidateRosterConfig = {
  timeoutMs: number;
  anthropicWebSearchMaxUses?: number;
  openAiApiKey?: string;
  anthropicApiKey?: string;
  geminiApiKey?: string;
};

export type EnrichCandidateRosterResult =
  | {
      ok: true;
      provider: AiProvider;
      model: string;
      candidates: CandidateRosterEntry[];
      aiRawDebug: Record<string, unknown> | null;
    }
  | CandidateRosterFailure;

const CLAUDE_INTER_CALL_DELAY_MS = 20_000;
const CLAUDE_RETRY_AFTER_BUFFER_MS = 10_000;

function classifyCitationVerificationFailure(reason: string): "transient" | "permanent" {
  const normalized = reason.toLowerCase();
  if (
    normalized.includes("timed out") ||
    normalized.includes("fetch failed") ||
    normalized.includes("status 429") ||
    normalized.includes("status 500") ||
    normalized.includes("status 502") ||
    normalized.includes("status 503") ||
    normalized.includes("status 504")
  ) {
    return "transient";
  }
  return "permanent";
}

async function verifyUniqueCandidateSourceUrls(
  urls: string[],
  timeoutMs: number
): Promise<Map<string, Awaited<ReturnType<typeof verifyHttpUrlReachability>>>> {
  const results = new Map<string, Awaited<ReturnType<typeof verifyHttpUrlReachability>>>();
  if (urls.length === 0) {
    return results;
  }

  const maxConcurrency = 6;
  const workerCount = Math.min(maxConcurrency, urls.length);
  let nextIndex = 0;

  const workers = Array.from({ length: workerCount }, async () => {
    while (true) {
      const currentIndex = nextIndex;
      nextIndex += 1;
      if (currentIndex >= urls.length) {
        return;
      }

      const sourceUrl = urls[currentIndex];
      if (!sourceUrl) {
        continue;
      }

      const verification = await verifyHttpUrlReachability(sourceUrl, {
        timeoutMs: Math.min(timeoutMs, 8_000),
        allowStatusCodes: [403],
      });
      results.set(sourceUrl, verification);
    }
  });

  await Promise.all(workers);
  return results;
}

async function verifyCandidateRosterSources(
  candidates: CandidateRosterEntry[],
  timeoutMs: number
): Promise<
  | { ok: true }
  | {
      ok: false;
      reason: string;
      retryable: boolean;
      failedCitationUrls: string[];
      failures: CitationVerificationFailure[];
      permanentFailures: CitationVerificationFailure[];
      transientFailures: CitationVerificationFailure[];
    }
> {
  const uniqueUrls = [...new Set(candidates.flatMap((candidate) => candidate.sources))];
  const verificationByUrl = await verifyUniqueCandidateSourceUrls(uniqueUrls, timeoutMs);
  const failures: CitationVerificationFailure[] = [];

  for (const candidate of candidates) {
    for (const sourceUrl of candidate.sources) {
      const verification = verificationByUrl.get(sourceUrl);
      if (!verification) {
        failures.push({
          candidate_display_name: candidate.display_name,
          url: sourceUrl,
          reason: "citation URL verification did not return a result",
          failureType: "transient",
        });
        continue;
      }
      if (!verification.ok) {
        failures.push({
          candidate_display_name: candidate.display_name,
          url: sourceUrl,
          reason: verification.reason,
          failureType: classifyCitationVerificationFailure(verification.reason),
        });
      }
    }
  }

  if (failures.length === 0) {
    return { ok: true };
  }

  const permanentFailures = failures.filter((failure) => failure.failureType === "permanent");
  const transientFailures = failures.filter((failure) => failure.failureType === "transient");
  const retryable = permanentFailures.length === 0 && transientFailures.length > 0;
  const reasonFailures = retryable ? transientFailures : permanentFailures;
  const failedCitationUrls = [...new Set(permanentFailures.map((failure) => failure.url))].slice(0, 100);
  const reasonPreview = reasonFailures
    .slice(0, 3)
    .map((failure) => `${failure.candidate_display_name} (${failure.url}): ${failure.reason}`)
    .join("; ");
  const extraCount = reasonFailures.length > 3 ? ` (+${reasonFailures.length - 3} more)` : "";
  const reasonPrefix = retryable
    ? `citation URL verification had transient failures for ${transientFailures.length} citation(s)`
    : `citation URL(s) could not be verified for ${permanentFailures.length} citation(s)`;

  return {
    ok: false,
    reason: `${reasonPrefix}: ${reasonPreview}${extraCount}`,
    retryable,
    failedCitationUrls,
    failures,
    permanentFailures,
    transientFailures,
  };
}

async function callProvider(
  candidate: AiCandidate,
  prompt: string,
  config: EnrichCandidateRosterConfig
): Promise<{ ok: true; parsed: unknown; rawText: string; debugMeta?: Record<string, unknown> } | CandidateRosterFailure> {
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

function dedupeCandidatesByDisplayName(candidates: CandidateRosterEntry[]): CandidateRosterEntry[] {
  const seen = new Set<string>();
  const output: CandidateRosterEntry[] = [];

  for (const candidate of candidates) {
    const key = normalizeCandidateName(candidate.display_name);
    if (key.length === 0 || seen.has(key)) {
      continue;
    }
    seen.add(key);
    output.push(candidate);
  }

  return output;
}

export function buildCandidateRosterConfigFromEnv(): EnrichCandidateRosterConfig {
  const env = getPipelineEnv();
  return {
    timeoutMs: env.AI_TIMEOUT_MS,
    anthropicWebSearchMaxUses: env.ANTHROPIC_WEB_SEARCH_MAX_USES,
    openAiApiKey: env.OPENAI_API_KEY,
    anthropicApiKey: env.ANTHROPIC_API_KEY,
    geminiApiKey: env.GEMINI_API_KEY,
  };
}

export async function enrichCandidateRoster(
  input: EnrichCandidateRosterInput,
  config: EnrichCandidateRosterConfig,
  candidates: readonly AiCandidate[] = CANDIDATES_AI_CANDIDATES
): Promise<EnrichCandidateRosterResult> {
  const failures: ProviderFailureAttempt[] = [];
  const cumulativeBlockedUrlFeedback = new Set<string>();

  for (const candidate of candidates) {
    let reviewFeedbackLines: string[] = [...cumulativeBlockedUrlFeedback];

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const prompt = buildCandidateRosterPrompt({
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

      const parsed = parseCandidateRosterPayload(generated.parsed);
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
          reviewFeedbackLines = [...cumulativeBlockedUrlFeedback, `Fix validation issue: ${parsed.reason}`].slice(
            0,
            20
          );
          continue;
        }
        break;
      }

      const dedupedCandidates = dedupeCandidatesByDisplayName(parsed.payload.candidates);
      const citationVerification = await verifyCandidateRosterSources(dedupedCandidates, config.timeoutMs);
      if (!citationVerification.ok) {
        failures.push({
          provider: candidate.provider,
          model: candidate.model,
          reason: citationVerification.reason,
          errorCode: citationVerification.retryable ? "TEMP_PROVIDER_ERROR" : "SCHEMA_MISMATCH",
          retryable: citationVerification.retryable,
          failureDebug: {
            failed_citation_urls: citationVerification.failedCitationUrls,
            citation_verification_failures: citationVerification.failures,
            permanent_citation_verification_failures: citationVerification.permanentFailures,
            transient_citation_verification_failures: citationVerification.transientFailures,
          },
        });

        const canRetrySameModel = attempt === 0;
        if (canRetrySameModel && !citationVerification.retryable) {
          const newFeedbackLines = citationVerification.permanentFailures.slice(0, 10).map(
            (failure) =>
              `Do not use or cite this URL for "${failure.candidate_display_name}": ${failure.url} (${failure.reason})`
          );
          for (const line of newFeedbackLines) {
            cumulativeBlockedUrlFeedback.add(line);
          }
          reviewFeedbackLines = [...cumulativeBlockedUrlFeedback].slice(0, 20);
          continue;
        }
        break;
      }

      return {
        ok: true,
        provider: candidate.provider,
        model: candidate.model,
        candidates: dedupedCandidates,
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
    reason: selected?.reason ?? "No AI candidates available for candidate roster enrichment",
    failureDebug: {
      attempts: failures,
      prompt_preview: trimDebugText(
        buildCandidateRosterPrompt({
          ...input,
          reviewFeedbackLines: [],
        }),
        6000
      ),
      ...(selected?.failureDebug ?? {}),
    },
  };
}
