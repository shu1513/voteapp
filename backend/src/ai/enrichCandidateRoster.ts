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
import { buildCandidateRosterDisambiguationPrompt } from "./providers/candidateRosterDisambiguationPrompt.js";
import { resolveIncludePartyForCandidateContest } from "./candidatePartisanship.js";
import { resolveCandidateResearchMode } from "./candidateResearchMode.js";
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
  electionStage?: string | null;
  senateClass?: string | null;
  termEndYear?: string | null;
  electionIsPartisan?: boolean | null;
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

export type CandidateDuplicateOption = {
  roster_index: number;
  display_name: string;
  party?: string;
  is_incumbent?: boolean;
  sources: string[];
};

export type CandidateDuplicateDisambiguationInput = {
  districtName: string;
  districtType: string;
  state: string;
  electionDate: string;
  officialBallotTitle: string;
  electionStage?: string | null;
  senateClass?: string | null;
  termEndYear?: string | null;
  electionIsPartisan?: boolean | null;
  duplicateDisplayName: string;
  options: CandidateDuplicateOption[];
  seedUrls: readonly string[];
};

export type CandidateDuplicateDisambiguationPerson = {
  roster_index: number;
  status: "clear" | "ambiguous" | "same_as_other";
  disambiguation_hint?: string;
  same_as_roster_index?: number;
  fec_ids?: string[];
  sources: string[];
};

export type CandidateDuplicateDisambiguationResult =
  | {
      ok: true;
      provider: AiProvider;
      model: string;
      people: CandidateDuplicateDisambiguationPerson[];
      aiRawDebug: Record<string, unknown> | null;
    }
  | CandidateRosterFailure;

const CLAUDE_INTER_CALL_DELAY_MS = 20_000;
const CLAUDE_RETRY_AFTER_BUFFER_MS = 10_000;

function shouldIncludePartyInRosterOutput(input: EnrichCandidateRosterInput): boolean {
  return resolveIncludePartyForCandidateContest({
    districtType: input.districtType,
    state: input.state,
    officialBallotTitle: input.officialBallotTitle,
    electionIsPartisan: input.electionIsPartisan,
  });
}

function removePartyFromCandidates(candidates: CandidateRosterEntry[]): CandidateRosterEntry[] {
  return candidates.map((candidate) => {
    const { party: _party, ...rest } = candidate;
    return rest;
  });
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

function normalizeOptionalStringArray(value: unknown): string[] | null | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  if (!Array.isArray(value)) {
    return null;
  }

  const normalized: string[] = [];
  const seen = new Set<string>();
  for (const item of value) {
    if (typeof item !== "string" || item.trim().length === 0) {
      return null;
    }
    const text = item.trim();
    if (seen.has(text)) {
      continue;
    }
    seen.add(text);
    normalized.push(text);
  }
  return normalized;
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

function parseDuplicateDisambiguationPayload(
  payload: unknown,
  expectedOptionIndexes: ReadonlySet<number>,
  options: { allowFecIds: boolean; requireFecIds: boolean }
):
  | {
      ok: true;
      people: CandidateDuplicateDisambiguationPerson[];
    }
  | { ok: false; reason: string } {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return { ok: false, reason: "payload must be object" };
  }
  const input = payload as Record<string, unknown>;
  if (!Array.isArray(input.people)) {
    return { ok: false, reason: "payload.people must be array" };
  }

  const people: CandidateDuplicateDisambiguationPerson[] = [];
  const seenRosterIndexes = new Set<number>();
  for (const row of input.people) {
    if (typeof row !== "object" || row === null || Array.isArray(row)) {
      return { ok: false, reason: "payload.people contains invalid row" };
    }
    const person = row as Record<string, unknown>;
    if (!Number.isInteger(person.roster_index) || Number(person.roster_index) < 0) {
      return { ok: false, reason: "payload.people[].roster_index must be non-negative integer" };
    }
    if (person.status !== "clear" && person.status !== "ambiguous" && person.status !== "same_as_other") {
      return { ok: false, reason: "payload.people[].status must be clear|ambiguous|same_as_other" };
    }
    const rosterIndex = Number(person.roster_index);
    if (!expectedOptionIndexes.has(rosterIndex)) {
      return { ok: false, reason: "payload.people[].roster_index must reference provided option index" };
    }
    if (seenRosterIndexes.has(rosterIndex)) {
      return { ok: false, reason: "payload.people[].roster_index must be unique within response" };
    }
    seenRosterIndexes.add(rosterIndex);

    if (
      person.status === "clear" &&
      (typeof person.disambiguation_hint !== "string" || person.disambiguation_hint.trim().length === 0)
    ) {
      return { ok: false, reason: "payload.people[].disambiguation_hint must be non-empty when status=clear" };
    }
    if (
      (person.status === "ambiguous" || person.status === "same_as_other") &&
      person.disambiguation_hint !== undefined &&
      person.disambiguation_hint !== null &&
      String(person.disambiguation_hint).trim().length > 0
    ) {
      return {
        ok: false,
        reason: "payload.people[].disambiguation_hint must be omitted when status=ambiguous|same_as_other",
      };
    }
    if (
      person.status !== "same_as_other" &&
      person.same_as_roster_index !== undefined &&
      person.same_as_roster_index !== null
    ) {
      return { ok: false, reason: "payload.people[].same_as_roster_index only allowed when status=same_as_other" };
    }
    if (
      person.status === "same_as_other" &&
      (!Number.isInteger(person.same_as_roster_index) || Number(person.same_as_roster_index) < 0)
    ) {
      return { ok: false, reason: "payload.people[].same_as_roster_index must be non-negative integer when status=same_as_other" };
    }
    if (!Array.isArray(person.sources) || person.sources.length === 0) {
      return { ok: false, reason: "payload.people[].sources must be non-empty array" };
    }

    const normalizedSources: string[] = [];
    const seen = new Set<string>();
    for (const item of person.sources) {
      if (typeof item !== "string" || item.trim().length === 0) {
        return { ok: false, reason: "payload.people[].sources must contain non-empty strings" };
      }
      const source = item.trim();
      if (seen.has(source)) {
        continue;
      }
      seen.add(source);
      normalizedSources.push(source);
    }
    if (normalizedSources.length === 0) {
      return { ok: false, reason: "payload.people[].sources must contain at least one URL" };
    }

    const clearHint =
      person.status === "clear" && typeof person.disambiguation_hint === "string"
        ? person.disambiguation_hint.trim()
        : undefined;
    const mergeTargetIndex =
      person.status === "same_as_other" && Number.isInteger(person.same_as_roster_index)
        ? Number(person.same_as_roster_index)
        : undefined;
    const parsedFecIds = options.allowFecIds ? normalizeOptionalStringArray(person.fec_ids) : undefined;
    if (options.allowFecIds && parsedFecIds === null) {
      return { ok: false, reason: "payload.people[].fec_ids must be string array when present" };
    }
    const normalizedFecIds = parsedFecIds ?? undefined;
    if (options.requireFecIds && (!normalizedFecIds || normalizedFecIds.length === 0)) {
      return {
        ok: false,
        reason: "payload.people[].fec_ids must contain at least one FEC ID for federal contests",
      };
    }

    people.push({
      roster_index: rosterIndex,
      status: person.status,
      ...(clearHint ? { disambiguation_hint: clearHint } : {}),
      ...(mergeTargetIndex !== undefined ? { same_as_roster_index: mergeTargetIndex } : {}),
      ...(normalizedFecIds !== undefined ? { fec_ids: normalizedFecIds } : {}),
      sources: normalizedSources,
    });
  }

  if (seenRosterIndexes.size !== expectedOptionIndexes.size) {
    return { ok: false, reason: "payload.people must include exactly one row for each provided option index" };
  }

  const statusByRosterIndex = new Map<number, CandidateDuplicateDisambiguationPerson["status"]>();
  for (const person of people) {
    statusByRosterIndex.set(person.roster_index, person.status);
  }

  let clearCount = 0;
  let hasSameAsOther = false;
  for (const person of people) {
    if (person.status === "clear") {
      clearCount += 1;
      continue;
    }
    if (person.status !== "same_as_other") {
      continue;
    }
    hasSameAsOther = true;
    const targetIndex = person.same_as_roster_index;
    if (targetIndex === undefined) {
      return { ok: false, reason: "payload.people[].same_as_roster_index required when status=same_as_other" };
    }
    if (!expectedOptionIndexes.has(targetIndex)) {
      return { ok: false, reason: "payload.people[].same_as_roster_index must reference provided option index" };
    }
    if (targetIndex === person.roster_index) {
      return { ok: false, reason: "payload.people[].same_as_roster_index must not point to same roster_index" };
    }
    if (statusByRosterIndex.get(targetIndex) !== "clear") {
      return { ok: false, reason: "payload.people[].same_as_roster_index must point to row with status=clear" };
    }
  }

  if (hasSameAsOther && clearCount === 0) {
    return { ok: false, reason: "payload.people with status=same_as_other require at least one clear row" };
  }

  return {
    ok: true,
    people,
  };
}

async function verifyCandidateDisambiguationSources(
  people: CandidateDuplicateDisambiguationPerson[],
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
  const uniqueUrls = [...new Set(people.flatMap((person) => person.sources))];
  const verificationByUrl = await verifyUniqueCandidateSourceUrls(uniqueUrls, timeoutMs);
  const failures: CitationVerificationFailure[] = [];

  for (const person of people) {
    const label = `roster_index ${person.roster_index}`;
    for (const sourceUrl of person.sources) {
      const verification = verificationByUrl.get(sourceUrl);
      if (!verification) {
        failures.push({
          candidate_display_name: label,
          url: sourceUrl,
          reason: "citation URL verification did not return a result",
          failureType: "transient",
        });
        continue;
      }
      if (!verification.ok) {
        failures.push({
          candidate_display_name: label,
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

export async function disambiguateCandidateDuplicateGroup(
  input: CandidateDuplicateDisambiguationInput,
  config: EnrichCandidateRosterConfig,
  candidates: readonly AiCandidate[] = CANDIDATES_AI_CANDIDATES
): Promise<CandidateDuplicateDisambiguationResult> {
  const optionIndexes = input.options.map((option) => option.roster_index);
  const expectedOptionIndexes = new Set(optionIndexes);
  if (expectedOptionIndexes.size !== optionIndexes.length) {
    return {
      ok: false,
      retryable: false,
      errorCode: "SCHEMA_MISMATCH",
      reason: "options[].roster_index must be unique",
      failureDebug: { option_indexes: optionIndexes },
    };
  }

  const failures: ProviderFailureAttempt[] = [];
  const cumulativeBlockedUrlFeedback = new Set<string>();
  const researchMode = resolveCandidateResearchMode({
    districtType: input.districtType,
    officialBallotTitle: input.officialBallotTitle,
  });
  const includeFecIds = researchMode !== "state_level";

  for (const candidate of candidates) {
    let reviewFeedbackLines: string[] = [...cumulativeBlockedUrlFeedback];

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const prompt = buildCandidateRosterDisambiguationPrompt({
        districtName: input.districtName,
        districtType: input.districtType,
        state: input.state,
        electionDate: input.electionDate,
        officialBallotTitle: input.officialBallotTitle,
        electionStage: input.electionStage,
        senateClass: input.senateClass,
        termEndYear: input.termEndYear,
        researchMode,
        electionIsPartisan: input.electionIsPartisan,
        duplicateDisplayName: input.duplicateDisplayName,
        options: input.options.map((option) => ({
          roster_index: option.roster_index,
          party: option.party,
          is_incumbent: option.is_incumbent,
          sources: option.sources,
        })),
        seedUrls: input.seedUrls,
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

      const parsed = parseDuplicateDisambiguationPayload(
        generated.parsed,
        expectedOptionIndexes,
        { allowFecIds: includeFecIds, requireFecIds: includeFecIds }
      );
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

      const invalidIndexes = parsed.people.some(
        (person) => !input.options.some((option) => option.roster_index === person.roster_index)
      );
      if (invalidIndexes) {
        failures.push({
          provider: candidate.provider,
          model: candidate.model,
          reason: "payload.people includes roster_index that is not present in options",
          errorCode: "SCHEMA_MISMATCH",
          retryable: false,
          failureDebug: {
            provider_response_text: trimDebugText(generated.rawText),
          },
        });
        if (attempt === 0) {
          reviewFeedbackLines = [...cumulativeBlockedUrlFeedback, "Use only roster_index values provided in options."].slice(
            0,
            20
          );
          continue;
        }
        break;
      }

      const citationVerification = await verifyCandidateDisambiguationSources(parsed.people, config.timeoutMs);
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
        people: parsed.people,
        aiRawDebug: {
          provider_response_text: trimDebugText(generated.rawText),
          disambiguation_prompt_variant: "duplicate_name_group",
          disambiguation_research_mode: researchMode,
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
    reason: selected?.reason ?? "No AI candidates available for candidate roster duplicate disambiguation",
    failureDebug: {
      attempts: failures,
      prompt_preview: trimDebugText(
        buildCandidateRosterDisambiguationPrompt({
          districtName: input.districtName,
          districtType: input.districtType,
          state: input.state,
          electionDate: input.electionDate,
          officialBallotTitle: input.officialBallotTitle,
          electionStage: input.electionStage,
          senateClass: input.senateClass,
          termEndYear: input.termEndYear,
          researchMode,
          electionIsPartisan: input.electionIsPartisan,
          duplicateDisplayName: input.duplicateDisplayName,
          options: input.options.map((option) => ({
            roster_index: option.roster_index,
            party: option.party,
            is_incumbent: option.is_incumbent,
            sources: option.sources,
          })),
          seedUrls: input.seedUrls,
          reviewFeedbackLines: [],
        }),
        6000
      ),
      ...(selected?.failureDebug ?? {}),
    },
  };
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
  const includeParty = shouldIncludePartyInRosterOutput(input);
  const researchMode = resolveCandidateResearchMode({
    districtType: input.districtType,
    officialBallotTitle: input.officialBallotTitle,
  });
  const includeFecIds = researchMode !== "state_level";
  const failures: ProviderFailureAttempt[] = [];
  const cumulativeBlockedUrlFeedback = new Set<string>();

  for (const candidate of candidates) {
    let reviewFeedbackLines: string[] = [...cumulativeBlockedUrlFeedback];

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const prompt = buildCandidateRosterPrompt({
        ...input,
        researchMode,
        includeParty,
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

      const parsed = parseCandidateRosterPayload(generated.parsed, {
        allowFecIds: includeFecIds,
        requireFecIds: includeFecIds,
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

      const normalizedCandidates = includeParty
        ? parsed.payload.candidates
        : removePartyFromCandidates(parsed.payload.candidates);
      const citationVerification = await verifyCandidateRosterSources(normalizedCandidates, config.timeoutMs);
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
        candidates: normalizedCandidates,
        aiRawDebug: {
          provider_response_text: trimDebugText(generated.rawText),
          roster_prompt_variant: includeParty ? "standard" : "nonpartisan",
          roster_research_mode: researchMode,
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
          researchMode,
          includeParty,
          reviewFeedbackLines: [],
        }),
        6000
      ),
      ...(selected?.failureDebug ?? {}),
    },
  };
}
