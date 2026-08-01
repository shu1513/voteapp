import { FRONTIER_AI_CANDIDATES, type AiCandidate } from "./aiCandidates.js";
import { getPipelineEnv } from "../config/env.js";
import {
  callResearchProvider,
  trimDebugText,
  type ResearchErrorCode,
} from "./researchProviderClient.js";
import {
  parseCandidateRecordDiscoveryPayloadPartial,
  type CandidateDiscoveredRecord,
} from "../contracts/candidateRecordDiscoveryPayloadContract.js";
import { buildCandidateRecordDiscoveryPrompt } from "./providers/candidateRecordDiscoveryPrompt.js";
import type { AiProvider } from "./types.js";
import { classifyCitationVerificationFailure, verifyHttpUrlReachability } from "./urlReachability.js";
import type { ElectionContestFamily } from "../types/election.js";
import { classifyCandidateRecordQuality } from "../pipeline/candidates/candidateRecordQuality.js";
import { evaluateCandidateRecordSourcePolicy } from "../pipeline/candidates/candidateRecordSourcePolicy.js";

type CandidateRecordDiscoveryErrorCode = ResearchErrorCode | "SCHEMA_MISMATCH";

type CandidateRecordDiscoveryFailure = {
  ok: false;
  retryable: boolean;
  errorCode: CandidateRecordDiscoveryErrorCode;
  reason: string;
  failureDebug?: Record<string, unknown>;
};

type ProviderFailureAttempt = {
  provider: string;
  model: string;
  reason: string;
  errorCode: CandidateRecordDiscoveryErrorCode;
  retryable: boolean;
  failureDebug?: Record<string, unknown>;
};

export type CandidateRecordDroppedRecord = {
  record: {
    description: string;
    source_url: string;
    event_date: string;
  };
  reason: string;
  failureType: "transient" | "permanent";
  failureKind: "schema" | "source_url" | "quality_gap";
};

export type CandidateRecordDiscoveryPayloadValidationResult =
  | {
      ok: true;
      records: CandidateDiscoveredRecord[];
      droppedRecords: CandidateRecordDroppedRecord[];
      validationDebug: Record<string, unknown>;
    }
  | {
      ok: false;
      reason: string;
      failureDebug?: Record<string, unknown>;
    };

export type EnrichCandidateRecordsInput = {
  candidateDisplayName: string;
  knownCurrentOffice?: string | null;
  // candidates.has_held_public_office — when set, the discovery prompt states
  // officeholder status as fact instead of asking the model to self-decide.
  hasHeldPublicOffice?: boolean | null;
  districtName: string;
  districtType: string;
  state: string;
  electionDate: string;
  officialBallotTitle: string;
  electionStage?: string | null;
  senateClass?: string | null;
  termEndYear?: string | null;
  discoveryContestFamily?: ElectionContestFamily | null;
  sinceDate?: string | null;
};

export type EnrichCandidateRecordsConfig = {
  timeoutMs: number;
  anthropicWebSearchMaxUses?: number;
  openAiApiKey?: string;
  anthropicApiKey?: string;
  geminiApiKey?: string;
};

export type EnrichCandidateRecordsResult =
  | {
      ok: true;
      provider: AiProvider;
      model: string;
      records: CandidateDiscoveredRecord[];
      droppedRecords: CandidateRecordDroppedRecord[];
      aiRawDebug: Record<string, unknown> | null;
    }
  | CandidateRecordDiscoveryFailure;

const CLAUDE_INTER_CALL_DELAY_MS = 20_000;
const CLAUDE_RETRY_AFTER_BUFFER_MS = 10_000;

async function verifyUniqueCandidateRecordSourceUrls(
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

async function verifyCandidateRecordSources(
  records: readonly CandidateDiscoveredRecord[],
  timeoutMs: number,
  candidateDisplayName?: string | null
): Promise<{
  verifiedRecords: CandidateDiscoveredRecord[];
  droppedRecords: Array<{
    record: CandidateDiscoveredRecord;
    reason: string;
    failureType: "transient" | "permanent";
    failureKind: "source_url";
  }>;
}> {
  const uniqueUrls = [...new Set(records.map((record) => record.source_url))];
  const verificationByUrl = await verifyUniqueCandidateRecordSourceUrls(uniqueUrls, timeoutMs);
  const verifiedRecords: CandidateDiscoveredRecord[] = [];
  const droppedRecords: Array<{
    record: CandidateDiscoveredRecord;
    reason: string;
    failureType: "transient" | "permanent";
    failureKind: "source_url";
  }> = [];

  for (const record of records) {
    const verification = verificationByUrl.get(record.source_url);
    if (!verification) {
      droppedRecords.push({
        record,
        reason: "citation URL verification did not return a result",
        failureType: "transient",
        failureKind: "source_url",
      });
      continue;
    }

    if (!verification.ok) {
      droppedRecords.push({
        record,
        reason: verification.reason,
        failureType: classifyCitationVerificationFailure(verification.reason),
        failureKind: "source_url",
      });
      continue;
    }

    // The stored citation is the post-redirect finalUrl, so the source policy
    // must hold for it too — otherwise a shortener or open redirect that
    // passes the pre-fetch policy check could land on a blocked platform, or
    // on the candidate's own campaign site.
    const finalUrlPolicy = evaluateCandidateRecordSourcePolicy({
      description: record.description,
      sourceUrl: verification.finalUrl,
      ...(candidateDisplayName ? { candidateDisplayName } : {}),
    });
    if (!finalUrlPolicy.ok) {
      droppedRecords.push({
        record,
        reason: `candidate record source policy rejected resolved citation URL ${verification.finalUrl}: ${finalUrlPolicy.reason}`,
        failureType: "permanent",
        failureKind: "source_url",
      });
      continue;
    }

    verifiedRecords.push({
      ...record,
      source_url: verification.finalUrl,
    });
  }

  return { verifiedRecords, droppedRecords };
}

export async function validateCandidateRecordDiscoveryPayload(
  payload: unknown,
  timeoutMs: number,
  options: { sinceDate?: string | null; candidateDisplayName?: string | null } = {}
): Promise<CandidateRecordDiscoveryPayloadValidationResult> {
  const parsed = parseCandidateRecordDiscoveryPayloadPartial(payload);
  if (!parsed.ok) {
    return {
      ok: false,
      reason: parsed.reason,
      failureDebug: {
        parser_reason: parsed.reason,
      },
    };
  }

  // Incremental refreshes are window-scoped: the prompt asks for records with
  // event_date >= since_date only, but the model may still return older
  // career records (e.g. to "balance" coverage). Enforce the window here so
  // out-of-window rows never reach source verification or the writer.
  const sinceDate = options.sinceDate?.trim() || null;
  const recordsBeforeSinceDate: CandidateDiscoveredRecord[] = [];
  const windowedRecords = sinceDate
    ? parsed.payload.records.filter((record) => {
        if (record.event_date >= sinceDate) {
          return true;
        }
        recordsBeforeSinceDate.push(record);
        return false;
      })
    : parsed.payload.records;

  const qualityDroppedRecords: CandidateRecordDroppedRecord[] = [];
  const qualityAcceptedRecords: CandidateDiscoveredRecord[] = [];
  for (const record of windowedRecords) {
    const quality = classifyCandidateRecordQuality({
      description: record.description,
      sourceUrl: record.source_url,
    });
    if (quality.classification === "disallowed_thin") {
      qualityDroppedRecords.push({
        record,
        reason: `candidate record quality rejected row: ${quality.reason}`,
        failureType: "permanent",
        failureKind: "quality_gap",
      });
      continue;
    }
    qualityAcceptedRecords.push(record);
  }

  // Source-domain policy runs before reachability: it is a pure string check
  // (no network), and blocked domains should never even be fetched. Policy
  // drops reuse failureKind "source_url" so the existing repair machinery
  // treats them like any other bad citation — the AI repair pass is asked for
  // a replacement URL with the policy reason attached, and the manual writers
  // surface them in the repair report.
  const policyDroppedRecords: CandidateRecordDroppedRecord[] = [];
  const policyAcceptedRecords: CandidateDiscoveredRecord[] = [];
  for (const record of qualityAcceptedRecords) {
    const policy = evaluateCandidateRecordSourcePolicy({
      description: record.description,
      sourceUrl: record.source_url,
      ...(options.candidateDisplayName ? { candidateDisplayName: options.candidateDisplayName } : {}),
    });
    if (!policy.ok) {
      policyDroppedRecords.push({
        record,
        reason: `candidate record source policy rejected row: ${policy.reason}`,
        failureType: "permanent",
        failureKind: "source_url",
      });
      continue;
    }
    policyAcceptedRecords.push(record);
  }

  const sourceVerification = await verifyCandidateRecordSources(
    policyAcceptedRecords,
    timeoutMs,
    options.candidateDisplayName
  );
  const schemaDroppedRecords: CandidateRecordDroppedRecord[] = parsed.invalid_rows.map((row) => ({
    record: {
      description: row.raw_record.description,
      source_url: row.raw_record.source_url,
      event_date: row.raw_record.event_date,
    },
    reason: `schema invalid row index=${row.index}: ${row.reason}`,
    failureType: "permanent",
    failureKind: "schema",
  }));
  const droppedRecords: CandidateRecordDroppedRecord[] = [
    ...sourceVerification.droppedRecords,
    ...schemaDroppedRecords,
    ...qualityDroppedRecords,
    ...policyDroppedRecords,
  ];

  return {
    ok: true,
    records: sourceVerification.verifiedRecords,
    droppedRecords,
    validationDebug: {
      dropped_records_count: droppedRecords.length,
      dropped_records_source_url_count: sourceVerification.droppedRecords.length,
      dropped_records_schema_count: schemaDroppedRecords.length,
      dropped_records_quality_count: qualityDroppedRecords.length,
      dropped_records_source_policy_count: policyDroppedRecords.length,
      ...(sinceDate
        ? { records_filtered_before_since_date_count: recordsBeforeSinceDate.length }
        : {}),
      verified_records_count: sourceVerification.verifiedRecords.length,
      parsed_valid_row_count: parsed.payload.records.length,
      parsed_invalid_row_count: parsed.invalid_rows.length,
      quality_accepted_row_count: qualityAcceptedRecords.length,
    },
  };
}

async function callProvider(
  candidate: AiCandidate,
  prompt: string,
  config: EnrichCandidateRecordsConfig
): Promise<{ ok: true; parsed: unknown; rawText: string; debugMeta?: Record<string, unknown> } | CandidateRecordDiscoveryFailure> {
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

export function buildCandidateRecordsConfigFromEnv(): EnrichCandidateRecordsConfig {
  const env = getPipelineEnv();
  return {
    timeoutMs: env.AI_TIMEOUT_MS,
    anthropicWebSearchMaxUses: env.ANTHROPIC_WEB_SEARCH_MAX_USES,
    openAiApiKey: env.OPENAI_API_KEY,
    anthropicApiKey: env.ANTHROPIC_API_KEY,
    geminiApiKey: env.GEMINI_API_KEY,
  };
}

export async function enrichCandidateRecords(
  input: EnrichCandidateRecordsInput,
  config: EnrichCandidateRecordsConfig,
  candidates: readonly AiCandidate[] = FRONTIER_AI_CANDIDATES
): Promise<EnrichCandidateRecordsResult> {
  const failures: ProviderFailureAttempt[] = [];
  const reviewFeedbackLines = new Set<string>();

  for (const candidate of candidates) {
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const prompt = buildCandidateRecordDiscoveryPrompt({
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

      const validation = await validateCandidateRecordDiscoveryPayload(
        generated.parsed,
        config.timeoutMs,
        { sinceDate: input.sinceDate, candidateDisplayName: input.candidateDisplayName }
      );
      if (!validation.ok) {
        const feedbackLine = `Fix payload schema: ${validation.reason}.`;
        reviewFeedbackLines.add(feedbackLine);
        failures.push({
          provider: candidate.provider,
          model: candidate.model,
          reason: validation.reason,
          errorCode: "SCHEMA_MISMATCH",
          retryable: false,
          failureDebug: {
            ...(validation.failureDebug ?? {}),
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
        records: validation.records,
        droppedRecords: validation.droppedRecords,
        aiRawDebug: {
          raw_response_preview: trimDebugText(generated.rawText),
          ...validation.validationDebug,
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
    reason: "No AI candidates available for candidate record discovery",
  };
}
