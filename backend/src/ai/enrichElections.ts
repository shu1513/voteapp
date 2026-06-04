import { ELECTIONS_AI_CANDIDATES, type AiCandidate } from "./aiCandidates.js";
import { getPipelineEnv } from "../config/env.js";
import { buildElectionsPrompt } from "./providers/electionsPrompt.js";
import {
  callResearchProvider,
  trimDebugText,
  type ResearchErrorCode,
} from "./researchProviderClient.js";
import {
  ELECTION_ENRICHMENT_SCHEMA_VERSION,
} from "../contracts/electionEnrichmentContract.js";
import { parseAiElectionEntriesPayload } from "../contracts/electionPayloadContract.js";
import { resolveElectionIsPartisan } from "./electionPartisanshipPolicy.js";
import type { AiProvider } from "./types.js";
import type {
  ElectionContestFamily,
  ElectionContestScope,
  ElectionDraftPayload,
  ElectionEnrichedPayload,
  ElectionEntryPayload,
} from "../types/election.js";
import { verifyHttpUrlReachability } from "./urlReachability.js";
import { normalizeElectionTitleKey } from "../utils/normalizeElectionTitleKey.js";
import { hasSpecialSeatMarker, isUsSenateOfficeTitle } from "../utils/senateOffice.js";

const CLAUDE_INTER_CALL_DELAY_MS = 20_000;
const CLAUDE_RETRY_AFTER_BUFFER_MS = 10_000;

type ElectionErrorCode = ResearchErrorCode | "SCHEMA_MISMATCH" | "MISSING_REQUIRED_FIELDS";

type ElectionEnrichmentFailure = {
  ok: false;
  retryable: boolean;
  errorCode: ElectionErrorCode;
  reason: string;
  failureDebug?: Record<string, unknown>;
};

type ProviderFailureAttempt = {
  provider: string;
  model: string;
  reason: string;
  errorCode: ElectionErrorCode;
  retryable: boolean;
  failureDebug?: Record<string, unknown>;
};

function normalizeGeneratedPayloadForContestFamily(
  draft: ElectionDraftPayload,
  contestFamily: ElectionContestScope,
  payload: unknown
): unknown {
  const forcedRaceType =
    contestFamily === "ballot_measure"
      ? "ballot_measure"
      : contestFamily === "non_judicial_office" || contestFamily === "judicial_office" || contestFamily === "us_senate"
        ? "office"
        : null;

  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return payload;
  }

  const record = payload as Record<string, unknown>;
  if (!Array.isArray(record.entries)) {
    return payload;
  }

  const normalizedEntries = record.entries.map((entry) => {
    if (typeof entry !== "object" || entry === null || Array.isArray(entry)) {
      return entry;
    }
    const row = entry as Record<string, unknown>;
    const nextRow: Record<string, unknown> = forcedRaceType
      ? { ...row, race_type: forcedRaceType }
      : { ...row };
    const raceType = nextRow.race_type === "office" || nextRow.race_type === "ballot_measure"
      ? nextRow.race_type
      : null;
    const officialBallotTitle =
      typeof nextRow.official_ballot_title === "string" ? nextRow.official_ballot_title : "";
    const aiIsPartisan =
      typeof nextRow.is_partisan === "boolean" ? nextRow.is_partisan : undefined;

    if (!raceType) {
      return nextRow;
    }

    const resolvedIsPartisan = resolveElectionIsPartisan({
      draft,
      contestFamily,
      raceType,
      officialBallotTitle,
      aiValue: aiIsPartisan,
    });

    if (resolvedIsPartisan === undefined) {
      const { is_partisan: _ignored, ...withoutIsPartisan } = nextRow;
      return withoutIsPartisan;
    }

    return { ...nextRow, is_partisan: resolvedIsPartisan };
  });

  return { ...record, entries: normalizedEntries };
}

type ElectionEnrichmentSuccess = {
  ok: true;
  payload: ElectionEnrichedPayload;
  provider: AiProvider;
  model: string;
  schemaVersion: typeof ELECTION_ENRICHMENT_SCHEMA_VERSION;
  promptVersion: string;
  aiRawDebug: Record<string, unknown> | null;
};

export type EnrichElectionsResult = ElectionEnrichmentSuccess | ElectionEnrichmentFailure;

export type EnrichElectionsInput = {
  ingestKey: string;
  draft: ElectionDraftPayload;
  promptVersion: string;
  softRetryCount: number;
  reviewFeedback: string[];
  seedUrls?: readonly string[];
  seedUrlsByFamily?: Partial<Record<ElectionContestScope, readonly string[]>>;
};

export type EnrichElectionsConfig = {
  timeoutMs: number;
  anthropicWebSearchMaxUses?: number;
  openAiApiKey?: string;
  anthropicApiKey?: string;
  geminiApiKey?: string;
};

function needsContestFamilySplit(districtType: ElectionDraftPayload["district_type"]): boolean {
  return districtType === "statewide" || districtType === "county" || districtType === "place";
}

function toDiscoveryContestFamily(scope: ElectionContestScope): ElectionContestFamily | undefined {
  return scope === "all" ? undefined : scope;
}

export function dedupeMergedEntries(entries: ElectionEntryPayload[]): ElectionEntryPayload[] {
  const byKey = new Map<string, ElectionEntryPayload>();
  const mergeSources = (left: string[], right: string[]): string[] => {
    const seen = new Set<string>();
    const combined: string[] = [];
    for (const url of [...left, ...right]) {
      if (seen.has(url)) {
        continue;
      }
      seen.add(url);
      combined.push(url);
    }
    return combined;
  };
  const mergeDiscoveryContestFamily = (
    existing: ElectionEntryPayload["discovery_contest_family"],
    incoming: ElectionEntryPayload["discovery_contest_family"],
    key: string
  ): ElectionEntryPayload["discovery_contest_family"] => {
    if (!existing) {
      return incoming;
    }
    if (!incoming || existing === incoming) {
      return existing;
    }
    if (
      (existing === "us_senate" && incoming === "non_judicial_office") ||
      (existing === "non_judicial_office" && incoming === "us_senate")
    ) {
      return "us_senate";
    }
    console.warn(
      `election family provenance conflict key=${key} existing=${existing} incoming=${incoming}; keeping existing`
    );
    return existing;
  };

  for (const entry of entries) {
    const key = `${entry.election_date}::${normalizeElectionTitleKey(entry.official_ballot_title)}`;
    const prior = byKey.get(key);
    if (!prior) {
      byKey.set(key, entry);
      continue;
    }
    byKey.set(key, {
      ...prior,
      discovery_contest_family: mergeDiscoveryContestFamily(
        prior.discovery_contest_family,
        entry.discovery_contest_family,
        key
      ),
      sources: mergeSources(prior.sources, entry.sources),
    });
  }
  return [...byKey.values()];
}

type UsSenateResolutionNote = {
  election_date: string;
  action: "collapsed_to_one";
  reason: string;
  kept_official_ballot_title: string;
  dropped_official_ballot_titles: string[];
};

function scoreUsSenateEntry(entry: ElectionEntryPayload): number {
  let score = 0;
  if (entry.term_end_year) {
    score += 100;
  }
  if (entry.senate_class) {
    score += 50;
  }
  if (hasSpecialSeatMarker(entry)) {
    score += 10;
  }
  score += Math.min(entry.sources.length, 5);
  return score;
}

function pickCanonicalUsSenateEntry(entries: readonly ElectionEntryPayload[]): ElectionEntryPayload {
  let best = entries[0]!;
  let bestScore = scoreUsSenateEntry(best);
  for (let i = 1; i < entries.length; i += 1) {
    const candidate = entries[i]!;
    const candidateScore = scoreUsSenateEntry(candidate);
    if (candidateScore > bestScore) {
      best = candidate;
      bestScore = candidateScore;
    }
  }
  return best;
}

type UsSenatePairValidation =
  | { status: "valid" }
  | { status: "invalid"; reason: string }
  | { status: "indistinguishable"; reason: string };

function validateTwoEntryUsSenatePair(
  electionDate: string,
  left: ElectionEntryPayload,
  right: ElectionEntryPayload
): UsSenatePairValidation {
  if (left.senate_class && right.senate_class && left.senate_class === right.senate_class) {
    return {
      status: "invalid",
      reason: `Two U.S. Senate entries on ${electionDate} have identical senate_class (${left.senate_class}); these cannot be distinct seats.`,
    };
  }
  if (left.term_end_year && right.term_end_year && left.term_end_year === right.term_end_year) {
    return {
      status: "invalid",
      reason: `Two U.S. Senate entries on ${electionDate} have identical term_end_year (${left.term_end_year}); these cannot be distinct seats.`,
    };
  }

  const leftTitleKey = normalizeElectionTitleKey(left.official_ballot_title);
  const rightTitleKey = normalizeElectionTitleKey(right.official_ballot_title);
  if (leftTitleKey === rightTitleKey) {
    return {
      status: "indistinguishable",
      reason:
        `Two U.S. Senate entries on ${electionDate} normalize to the same official_ballot_title key (${leftTitleKey}); provide seat-distinguishing official ballot labels (for example, regular vs unexpired/special).`,
    };
  }

  if (left.term_end_year && right.term_end_year && left.term_end_year !== right.term_end_year) {
    return { status: "valid" };
  }
  if (left.senate_class && right.senate_class && left.senate_class !== right.senate_class) {
    return { status: "valid" };
  }

  const leftSpecial = hasSpecialSeatMarker(left);
  const rightSpecial = hasSpecialSeatMarker(right);
  if (leftSpecial !== rightSpecial) {
    return { status: "valid" };
  }

  return {
    status: "indistinguishable",
    reason:
      `Could not distinguish two U.S. Senate entries on ${electionDate} by class, term end year, or special-vs-regular evidence.`,
  };
}

function resolveUsSenateEntries(
  entries: ElectionEntryPayload[],
  attempt: number
): {
  entries: ElectionEntryPayload[];
  shouldRetry: boolean;
  retryFeedback?: string;
  notes: UsSenateResolutionNote[];
} {
  const notes: UsSenateResolutionNote[] = [];
  const groups = new Map<string, ElectionEntryPayload[]>();
  for (const entry of entries) {
    const key = entry.election_date;
    const prior = groups.get(key);
    if (!prior) {
      groups.set(key, [entry]);
      continue;
    }
    prior.push(entry);
  }

  for (const [electionDate, group] of groups.entries()) {
    if (group.length <= 1) {
      continue;
    }
    if (group.length > 2) {
      const reason = `U.S. Senate entries on ${electionDate} must contain at most two seats, but ${group.length} were returned.`;
      if (attempt === 0) {
        return { entries, shouldRetry: true, retryFeedback: reason, notes: [] };
      }
      const canonical = pickCanonicalUsSenateEntry(group);
      const dropped = group.filter((entry) => entry !== canonical);
      groups.set(electionDate, [canonical]);
      notes.push({
        election_date: electionDate,
        action: "collapsed_to_one",
        reason,
        kept_official_ballot_title: canonical.official_ballot_title,
        dropped_official_ballot_titles: dropped.map((entry) => entry.official_ballot_title),
      });
      continue;
    }

    const [left, right] = group;
    if (!left || !right) {
      continue;
    }
    const validation = validateTwoEntryUsSenatePair(electionDate, left, right);
    if (validation.status === "valid") {
      continue;
    }

    if (attempt === 0) {
      return { entries, shouldRetry: true, retryFeedback: validation.reason, notes: [] };
    }

    const canonical = pickCanonicalUsSenateEntry(group);
    const dropped = canonical === left ? right : left;
    groups.set(electionDate, [canonical]);
    notes.push({
      election_date: electionDate,
      action: "collapsed_to_one",
      reason: validation.reason,
      kept_official_ballot_title: canonical.official_ballot_title,
      dropped_official_ballot_titles: [dropped.official_ballot_title],
    });
  }

  const resolved: ElectionEntryPayload[] = [];
  for (const group of groups.values()) {
    resolved.push(...group);
  }

  return { entries: resolved, shouldRetry: false, notes };
}

function containsJudicialMarker(entry: ElectionEntryPayload): boolean {
  const text = entry.official_ballot_title.toLowerCase();
  return /\b(judge|justice|judicial|superior court|court of appeals|supreme court|retention)\b/.test(text);
}

function containsBallotMeasureMarker(entry: ElectionEntryPayload): boolean {
  const text = entry.official_ballot_title.toLowerCase();
  return /\b(proposition|measure|amendment|referendum|initiative|bond|question)\b/.test(text);
}

function containsOfficeMarker(entry: ElectionEntryPayload): boolean {
  const text = entry.official_ballot_title.toLowerCase();
  return /\b(member, board|board of|sheriff|assessor|clerk|treasurer|controller|attorney|superintendent|mayor|council|governor|secretary|judge|justice|commissioner|auditor)\b/.test(
    text
  );
}

function containsNonJudicialOfficeMarker(entry: ElectionEntryPayload): boolean {
  const text = entry.official_ballot_title.toLowerCase();
  return /\b(member, board|board of|sheriff|assessor|clerk|treasurer|controller|attorney|superintendent|mayor|council|governor|secretary|commissioner|auditor)\b/.test(
    text
  );
}

function containsUsSenateMarker(entry: ElectionEntryPayload): boolean {
  return isUsSenateOfficeTitle(entry.official_ballot_title);
}

function validateContestFamilySoft(
  family: ElectionContestScope,
  entries: ElectionEntryPayload[]
): { ok: true } | { ok: false; reason: string } {
  if (family === "all" || entries.length === 0) {
    return { ok: true };
  }

  if (family === "ballot_measure") {
    const hasOffice = entries.some(
      (entry) =>
        entry.race_type === "office" ||
        (containsOfficeMarker(entry) && !containsBallotMeasureMarker(entry))
    );
    if (hasOffice) {
      return { ok: false, reason: "ballot_measure family returned office entries" };
    }
    return { ok: true };
  }

  if (family === "judicial_office") {
    const hasBallotMeasure = entries.some(
      (entry) => entry.race_type === "ballot_measure" || containsBallotMeasureMarker(entry)
    );
    if (hasBallotMeasure) {
      return { ok: false, reason: "judicial_office family returned ballot_measure entries" };
    }
    const hasNonJudicialOffice = entries.some(
      (entry) => containsNonJudicialOfficeMarker(entry) && !containsJudicialMarker(entry)
    );
    if (hasNonJudicialOffice) {
      return { ok: false, reason: "judicial_office family returned non-judicial office entries" };
    }
    const hasJudicial = entries.some((entry) => containsJudicialMarker(entry));
    if (!hasJudicial) {
      return { ok: false, reason: "judicial_office family has no clear judicial markers" };
    }
    return { ok: true };
  }

  if (family === "us_senate") {
    const hasBallotMeasure = entries.some(
      (entry) => entry.race_type === "ballot_measure" || containsBallotMeasureMarker(entry)
    );
    if (hasBallotMeasure) {
      return { ok: false, reason: "us_senate family returned ballot_measure entries" };
    }
    const hasNonSenate = entries.some((entry) => !containsUsSenateMarker(entry));
    if (hasNonSenate) {
      return { ok: false, reason: "us_senate family returned non-Senate office entries" };
    }
    return { ok: true };
  }

  // non_judicial_office: keep this light-touch and only reject obvious mismatches.
  const hasBallotMeasure = entries.some(
    (entry) => entry.race_type === "ballot_measure" || containsBallotMeasureMarker(entry)
  );
  if (hasBallotMeasure) {
    return { ok: false, reason: "non_judicial_office family returned ballot_measure entries" };
  }
  const allJudicial = entries.every((entry) => containsJudicialMarker(entry));
  if (allJudicial) {
    return { ok: false, reason: "non_judicial_office family appears fully judicial" };
  }
  const hasUsSenate = entries.some((entry) => containsUsSenateMarker(entry));
  if (hasUsSenate) {
    return { ok: false, reason: "non_judicial_office family returned U.S. Senate entries" };
  }
  return { ok: true };
}

type CitationVerificationFailure = {
  entry_title: string;
  url: string;
  reason: string;
  failureType: "transient" | "permanent";
};

function classifyCitationVerificationFailure(reason: string): "transient" | "permanent" {
  const normalized = reason.toLowerCase();

  if (
    normalized.includes("timed out") ||
    normalized.includes("fetch failed") ||
    normalized.includes("status 500") ||
    normalized.includes("status 502") ||
    normalized.includes("status 503") ||
    normalized.includes("status 504") ||
    normalized.includes("status 429")
  ) {
    return "transient";
  }

  return "permanent";
}

async function verifyUniqueElectionSourceUrls(
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

async function verifyElectionEntrySources(
  entries: ElectionEntryPayload[],
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
  const uniqueUrls = [...new Set(entries.flatMap((entry) => entry.sources))];
  const verificationByUrl = await verifyUniqueElectionSourceUrls(uniqueUrls, timeoutMs);
  const failures: CitationVerificationFailure[] = [];

  for (const entry of entries) {
    for (const sourceUrl of entry.sources) {
      const verification = verificationByUrl.get(sourceUrl);
      if (!verification) {
        failures.push({
          entry_title: entry.official_ballot_title,
          url: sourceUrl,
          reason: "citation URL verification did not return a result",
          failureType: "transient",
        });
        continue;
      }
      if (!verification.ok) {
        const failureType = classifyCitationVerificationFailure(verification.reason);
        failures.push({
          entry_title: entry.official_ballot_title,
          url: sourceUrl,
          reason: verification.reason,
          failureType,
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
    .map((failure) => `${failure.entry_title} (${failure.url}): ${failure.reason}`)
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
  config: EnrichElectionsConfig
): Promise<{ ok: true; parsed: unknown; rawText: string; debugMeta?: Record<string, unknown> } | ElectionEnrichmentFailure> {
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

export function buildEnrichElectionsConfigFromEnv(): EnrichElectionsConfig {
  const env = getPipelineEnv();
  return {
    timeoutMs: env.AI_TIMEOUT_MS,
    anthropicWebSearchMaxUses: env.ANTHROPIC_WEB_SEARCH_MAX_USES,
    openAiApiKey: env.OPENAI_API_KEY,
    anthropicApiKey: env.ANTHROPIC_API_KEY,
    geminiApiKey: env.GEMINI_API_KEY,
  };
}

async function runPromptWithCandidates(
  draft: ElectionDraftPayload,
  prompt: string,
  contestFamily: ElectionContestScope,
  config: EnrichElectionsConfig,
  candidates: readonly AiCandidate[]
): Promise<
  | {
      ok: true;
      entries: ElectionEntryPayload[];
      reviewDecision?: "approve" | "reject";
      reviewReason?: string;
      provider: AiProvider;
      model: string;
      aiRawDebug: Record<string, unknown> | null;
    }
  | {
      ok: false;
      reason: string;
      errorCode: ElectionErrorCode;
      retryable: boolean;
      attempts: ProviderFailureAttempt[];
    }
> {
  const failures: ProviderFailureAttempt[] = [];
  const cumulativeRetryFeedback = new Set<string>();

  for (const candidate of candidates) {
    let retryFeedbackLines: string[] = [...cumulativeRetryFeedback];

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const promptWithFeedback =
        retryFeedbackLines.length > 0
          ? [
              prompt,
              "",
              "Previous feedback to address:",
              ...retryFeedbackLines.map((line, index) => `${index + 1}. ${line}`),
              "Fix only the issues above and keep valid content.",
            ].join("\n")
          : prompt;
      const generated = await callProvider(candidate, promptWithFeedback, config);
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

      const normalizedGeneratedPayload = normalizeGeneratedPayloadForContestFamily(
        draft,
        contestFamily,
        generated.parsed
      );
      const parsed = parseAiElectionEntriesPayload(normalizedGeneratedPayload);
      if (!parsed.ok) {
        failures.push({
          provider: candidate.provider,
          model: candidate.model,
          reason: parsed.reason,
          errorCode: "SCHEMA_MISMATCH",
          retryable: false,
        });
        break;
      }

      const familyValidation = validateContestFamilySoft(contestFamily, parsed.payload.entries);
      if (!familyValidation.ok) {
        failures.push({
          provider: candidate.provider,
          model: candidate.model,
          reason: familyValidation.reason,
          errorCode: "SCHEMA_MISMATCH",
          retryable: false,
        });
        break;
      }

      let resolvedEntries = parsed.payload.entries;
      const usSenateResolutionNotes: UsSenateResolutionNote[] = [];
      if (contestFamily === "us_senate") {
        const resolution = resolveUsSenateEntries(parsed.payload.entries, attempt);
        if (resolution.shouldRetry) {
          const feedbackLine = resolution.retryFeedback
            ? `${resolution.retryFeedback} Differentiate seats (not parties) and provide class, term_end_year, or special/unexpired evidence.`
            : "Could not distinguish U.S. Senate seats. Differentiate seats (not parties) and provide class, term_end_year, or special/unexpired evidence.";
          cumulativeRetryFeedback.add(feedbackLine);
          retryFeedbackLines = [...cumulativeRetryFeedback].slice(0, 20);
          continue;
        }
        resolvedEntries = resolution.entries;
        usSenateResolutionNotes.push(...resolution.notes);
      }

      const citationVerification = await verifyElectionEntrySources(resolvedEntries, config.timeoutMs);
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
              `Do not use or cite this URL for "${failure.entry_title}": ${failure.url} (${failure.reason})`
          );
          for (const line of newFeedbackLines) {
            cumulativeRetryFeedback.add(line);
          }
          retryFeedbackLines = [...cumulativeRetryFeedback].slice(0, 20);
          continue;
        }
        break;
      }

      return {
        ok: true,
        entries: resolvedEntries,
        ...(parsed.payload.review_decision
          ? { reviewDecision: parsed.payload.review_decision }
          : {}),
        ...(parsed.payload.review_reason ? { reviewReason: parsed.payload.review_reason } : {}),
        provider: candidate.provider,
        model: candidate.model,
        aiRawDebug: {
          provider_response_text: trimDebugText(generated.rawText),
          ...(usSenateResolutionNotes.length > 0
            ? { us_senate_resolution_notes: usSenateResolutionNotes }
            : {}),
          ...(generated.debugMeta ?? {}),
        },
      };
    }
  }

  const finalFailure = failures[failures.length - 1];
  const anyRetryable = failures.some((failure) => failure.retryable);
  const firstPermanentFailure = failures.find((failure) => !failure.retryable && failure.errorCode);
  const firstRetryableFailure = failures.find((failure) => failure.retryable && failure.errorCode);
  const selectedFailure = anyRetryable
    ? (firstRetryableFailure ?? finalFailure)
    : (firstPermanentFailure ?? finalFailure);

  return {
    ok: false,
    retryable: selectedFailure?.retryable ?? false,
    errorCode: selectedFailure?.errorCode ?? "TEMP_PROVIDER_ERROR",
    reason: selectedFailure?.reason ?? "No AI candidates available for election enrichment",
    attempts: failures,
  };
}

export async function enrichElections(
  input: EnrichElectionsInput,
  config: EnrichElectionsConfig,
  candidates: readonly AiCandidate[] = ELECTIONS_AI_CANDIDATES
): Promise<EnrichElectionsResult> {
  const familyPlan: ElectionContestScope[] =
    input.draft.district_type === "statewide"
      ? ["non_judicial_office", "judicial_office", "ballot_measure", "us_senate"]
      : needsContestFamilySplit(input.draft.district_type)
        ? ["non_judicial_office", "judicial_office", "ballot_measure"]
        : ["all"];

  const mergedEntries: ElectionEntryPayload[] = [];
  const mergedDebug: Record<string, unknown> = {};
  const reviewDecisions: Array<"approve" | "reject"> = [];
  const reviewReasons: string[] = [];
  const providerModelLabels: string[] = [];
  const providerFamilyLabels: string[] = [];
  const familySourceUrls: Partial<Record<ElectionContestScope, string[]>> = {};
  let primaryProvider: AiProvider | null = null;
  let primaryModel: string | null = null;

  for (const family of familyPlan) {
    const familySeedUrls =
      input.seedUrlsByFamily?.[family] ??
      (family === "all" ? input.seedUrls : undefined) ??
      [];
    const prompt = buildElectionsPrompt({
      draft: input.draft,
      softRetryCount: input.softRetryCount,
      reviewFeedbackLines: input.reviewFeedback,
      contestFamily: family,
      seedUrls: familySeedUrls,
    });
    const outcome = await runPromptWithCandidates(input.draft, prompt, family, config, candidates);
    if (!outcome.ok) {
      return {
        ok: false,
        retryable: outcome.retryable,
        errorCode: outcome.errorCode,
        reason: `${family}: ${outcome.reason}`,
        failureDebug: {
          contest_family: family,
          attempts: outcome.attempts,
          prompt_preview: trimDebugText(prompt, 6000),
        },
      };
    }

    const discoveryContestFamily = toDiscoveryContestFamily(family);
    mergedEntries.push(
      ...outcome.entries.map((entry) => ({
        ...entry,
        ...(discoveryContestFamily ? { discovery_contest_family: discoveryContestFamily } : {}),
      }))
    );
    providerModelLabels.push(`${outcome.provider}:${outcome.model}`);
    providerFamilyLabels.push(`${outcome.provider}:${outcome.model}:${family}`);
    const dedupedFamilySources = [...new Set(outcome.entries.flatMap((entry) => entry.sources))];
    familySourceUrls[family] = dedupedFamilySources;
    if (!primaryProvider) {
      primaryProvider = outcome.provider;
    }
    if (!primaryModel) {
      primaryModel = outcome.model;
    }
    if (outcome.reviewDecision) {
      reviewDecisions.push(outcome.reviewDecision);
    }
    if (outcome.reviewReason && outcome.reviewReason.trim().length > 0) {
      reviewReasons.push(`${family}: ${outcome.reviewReason.trim()}`);
    }
    mergedDebug[family] = outcome.aiRawDebug;
  }

  const canonicalPayload: ElectionEnrichedPayload = {
    district_id: input.draft.district_id,
    district_name: input.draft.district_name,
    district_type: input.draft.district_type,
    state: input.draft.state,
    entries: dedupeMergedEntries(mergedEntries),
    ...(reviewDecisions.includes("reject")
      ? { review_decision: "reject" as const }
      : { review_decision: "approve" as const }),
    ...(reviewReasons.length > 0 ? { review_reason: reviewReasons.join(" | ") } : {}),
  };

  return {
    ok: true,
    payload: canonicalPayload,
    provider: primaryProvider ?? (providerModelLabels[0]?.split(":")[0] as AiProvider),
    model: primaryModel ?? "unknown",
    schemaVersion: ELECTION_ENRICHMENT_SCHEMA_VERSION,
    promptVersion: input.promptVersion,
    aiRawDebug: {
      contest_families: familyPlan,
      providers: providerFamilyLabels,
      provider_models: providerModelLabels,
      family_debug: mergedDebug,
      family_source_urls: familySourceUrls,
    },
  };
}
