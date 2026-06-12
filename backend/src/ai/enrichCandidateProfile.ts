import { CANDIDATES_AI_CANDIDATES, type AiCandidate } from "./aiCandidates.js";
import { getPipelineEnv } from "../config/env.js";
import {
  callResearchProvider,
  trimDebugText,
  type ResearchErrorCode,
} from "./researchProviderClient.js";
import { verifyHttpUrlReachability } from "./urlReachability.js";
import {
  type CandidateProfilePayload,
  parseCandidateProfilePayload,
} from "../contracts/candidateProfilePayloadContract.js";
import { buildCandidateProfilePrompt } from "./providers/candidateProfilePrompt.js";
import { resolveCandidateResearchMode } from "./candidateResearchMode.js";
import type { AiProvider } from "./types.js";

type CandidateProfileErrorCode = ResearchErrorCode | "SCHEMA_MISMATCH";

type CandidateProfileFailure = {
  ok: false;
  retryable: boolean;
  errorCode: CandidateProfileErrorCode;
  reason: string;
  failureDebug?: Record<string, unknown>;
};

type ProviderFailureAttempt = {
  provider: string;
  model: string;
  reason: string;
  errorCode: CandidateProfileErrorCode;
  retryable: boolean;
  failureDebug?: Record<string, unknown>;
};

type CitationVerificationFailure = {
  candidate_display_name: string;
  url: string;
  reason: string;
  failureType: "transient" | "permanent";
};

export type EnrichCandidateProfileInput = {
  candidateDisplayName: string;
  districtName: string;
  districtType: string;
  state: string;
  electionDate?: string | null;
  officialBallotTitle: string;
  electionStage?: string | null;
  senateClass?: string | null;
  termEndYear?: string | null;
  electionIsPartisan?: boolean | null;
  rosterParty?: string;
  rosterIncumbent?: boolean;
  rosterFecIds?: readonly string[];
  rosterStateFilingIds?: readonly string[];
  disambiguationHint?: string;
  seedUrls: readonly string[];
};

export type EnrichCandidateProfileConfig = {
  timeoutMs: number;
  anthropicWebSearchMaxUses?: number;
  openAiApiKey?: string;
  anthropicApiKey?: string;
  geminiApiKey?: string;
};

export type EnrichCandidateProfileResult =
  | {
      ok: true;
      provider: AiProvider;
      model: string;
      profile: CandidateProfilePayload;
      aiRawDebug: Record<string, unknown> | null;
    }
  | CandidateProfileFailure;

const CLAUDE_INTER_CALL_DELAY_MS = 20_000;
const CLAUDE_RETRY_AFTER_BUFFER_MS = 10_000;

function removePartyFromProfile(profile: CandidateProfilePayload): CandidateProfilePayload {
  const { party: _party, ...rest } = profile;
  return rest;
}

function removeDateOfBirthFromProfile(profile: CandidateProfilePayload): CandidateProfilePayload {
  const { date_of_birth: _dateOfBirth, ...rest } = profile;
  return rest;
}

function removeStateFilingIdsFromProfile(profile: CandidateProfilePayload): CandidateProfilePayload {
  const { state_filing_ids: _stateFilingIds, ...rest } = profile;
  return rest;
}

function normalizeFecIds(values: readonly string[] | undefined): string[] {
  return [...new Set((values ?? []).map((value) => value.trim().toUpperCase()).filter((value) => value.length > 0))];
}

function normalizeStateFilingIds(values: readonly string[] | undefined): string[] {
  return [...new Set((values ?? []).map((value) => value.trim().toUpperCase()).filter((value) => value.length > 0))];
}

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

async function verifyCandidateProfileSources(
  profile: CandidateProfilePayload,
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
  const uniqueUrls = [...new Set(profile.sources)];
  const verificationByUrl = await verifyUniqueCandidateSourceUrls(uniqueUrls, timeoutMs);
  const failures: CitationVerificationFailure[] = [];

  for (const sourceUrl of profile.sources) {
    const verification = verificationByUrl.get(sourceUrl);
    if (!verification) {
      failures.push({
        candidate_display_name: profile.display_name,
        url: sourceUrl,
        reason: "citation URL verification did not return a result",
        failureType: "transient",
      });
      continue;
    }
    if (!verification.ok) {
      failures.push({
        candidate_display_name: profile.display_name,
        url: sourceUrl,
        reason: verification.reason,
        failureType: classifyCitationVerificationFailure(verification.reason),
      });
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
  config: EnrichCandidateProfileConfig
): Promise<{ ok: true; parsed: unknown; rawText: string; debugMeta?: Record<string, unknown> } | CandidateProfileFailure> {
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

export function buildCandidateProfileConfigFromEnv(): EnrichCandidateProfileConfig {
  const env = getPipelineEnv();
  return {
    timeoutMs: env.AI_TIMEOUT_MS,
    anthropicWebSearchMaxUses: env.ANTHROPIC_WEB_SEARCH_MAX_USES,
    openAiApiKey: env.OPENAI_API_KEY,
    anthropicApiKey: env.ANTHROPIC_API_KEY,
    geminiApiKey: env.GEMINI_API_KEY,
  };
}

export async function enrichCandidateProfile(
  input: EnrichCandidateProfileInput,
  config: EnrichCandidateProfileConfig,
  candidates: readonly AiCandidate[] = CANDIDATES_AI_CANDIDATES
): Promise<EnrichCandidateProfileResult> {
  const researchMode = resolveCandidateResearchMode({
    districtType: input.districtType,
    officialBallotTitle: input.officialBallotTitle,
  });
  const includeFecIds = researchMode !== "state_level";
  const backendCandidateFecIds = normalizeFecIds(input.rosterFecIds);
  const backendCandidateStateFilingIds = normalizeStateFilingIds(input.rosterStateFilingIds);
  if (includeFecIds && backendCandidateFecIds.length === 0) {
    return {
      ok: false,
      retryable: false,
      errorCode: "SCHEMA_MISMATCH",
      reason: "candidate_fec_ids is required in context for federal profile enrichment",
      failureDebug: {
        research_mode: researchMode,
        candidate_display_name: input.candidateDisplayName,
      },
    };
  }
  const failures: ProviderFailureAttempt[] = [];
  const cumulativeBlockedUrlFeedback = new Set<string>();

  for (const candidate of candidates) {
    let reviewFeedbackLines: string[] = [...cumulativeBlockedUrlFeedback];

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const prompt = buildCandidateProfilePrompt({
        ...input,
        researchMode,
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

      const parsed = parseCandidateProfilePayload(generated.parsed, {
        allowFecIds: false,
        requireFecIds: false,
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
          reviewFeedbackLines = [...cumulativeBlockedUrlFeedback, `Fix validation issue: ${parsed.reason}`].slice(
            0,
            20
          );
          continue;
        }
        break;
      }

      const normalizedProfile = removePartyFromProfile(parsed.payload);
      const normalizedProfileWithFederalDobPolicy = includeFecIds
        ? removeStateFilingIdsFromProfile(removeDateOfBirthFromProfile(normalizedProfile))
        : normalizedProfile;
      const profileWithBackendFecIds = includeFecIds
        ? { ...normalizedProfileWithFederalDobPolicy, fec_ids: backendCandidateFecIds }
        : normalizedProfileWithFederalDobPolicy;
      const profileWithBackendIds =
        !includeFecIds && backendCandidateStateFilingIds.length > 0
          ? { ...profileWithBackendFecIds, state_filing_ids: backendCandidateStateFilingIds }
          : profileWithBackendFecIds;
      const citationVerification = await verifyCandidateProfileSources(profileWithBackendIds, config.timeoutMs);
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
        profile: profileWithBackendIds,
        aiRawDebug: {
          provider_response_text: trimDebugText(generated.rawText),
          profile_prompt_variant: researchMode,
          profile_research_mode: researchMode,
          profile_backend_fec_ids: includeFecIds ? backendCandidateFecIds : undefined,
          profile_backend_state_filing_ids:
            !includeFecIds && backendCandidateStateFilingIds.length > 0
              ? backendCandidateStateFilingIds
              : undefined,
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
    reason: selected?.reason ?? "No AI candidates available for candidate profile enrichment",
    failureDebug: {
      attempts: failures,
      prompt_preview: trimDebugText(
        buildCandidateProfilePrompt({
          ...input,
          researchMode,
          reviewFeedbackLines: [],
        }),
        6000
      ),
      ...(selected?.failureDebug ?? {}),
    },
  };
}
