import { CANDIDATE_RECORD_DISCOVERY_AI_CANDIDATES, type AiCandidate } from "./aiCandidates.js";
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
import { verifyHttpUrlReachability } from "./urlReachability.js";

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

export type EnrichCandidateRecordsInput = {
  candidateDisplayName: string;
  districtName: string;
  districtType: string;
  state: string;
  electionDate: string;
  officialBallotTitle: string;
  electionStage?: string | null;
  senateClass?: string | null;
  termEndYear?: string | null;
  sinceDate?: string | null;
  seedUrls: readonly string[];
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
      droppedRecords: Array<{
        record: {
          title: string;
          description: string;
          source_url: string;
          event_date: string;
        };
        reason: string;
        failureType: "transient" | "permanent";
        failureKind: "schema" | "source_url";
      }>;
      aiRawDebug: Record<string, unknown> | null;
    }
  | CandidateRecordDiscoveryFailure;

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
  timeoutMs: number
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

    verifiedRecords.push({
      ...record,
      source_url: verification.finalUrl,
    });
  }

  return { verifiedRecords, droppedRecords };
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
  candidates: readonly AiCandidate[] = CANDIDATE_RECORD_DISCOVERY_AI_CANDIDATES
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

      const parsed = parseCandidateRecordDiscoveryPayloadPartial(generated.parsed);
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

      const sourceVerification = await verifyCandidateRecordSources(
        parsed.payload.records,
        config.timeoutMs
      );
      const schemaDroppedRecords = parsed.invalid_rows.map((row) => ({
        record: {
          title: row.raw_record.title,
          description: row.raw_record.description,
          source_url: row.raw_record.source_url,
          event_date: row.raw_record.event_date,
        },
        reason: `schema invalid row index=${row.index}: ${row.reason}`,
        failureType: "permanent" as const,
        failureKind: "schema" as const,
      }));
      const droppedRecords = [...sourceVerification.droppedRecords, ...schemaDroppedRecords];

      return {
        ok: true,
        provider: candidate.provider,
        model: candidate.model,
        records: sourceVerification.verifiedRecords,
        droppedRecords,
        aiRawDebug: {
          raw_response_preview: trimDebugText(generated.rawText),
          dropped_records_count: droppedRecords.length,
          dropped_records_source_url_count: sourceVerification.droppedRecords.length,
          dropped_records_schema_count: schemaDroppedRecords.length,
          verified_records_count: sourceVerification.verifiedRecords.length,
          parsed_valid_row_count: parsed.payload.records.length,
          parsed_invalid_row_count: parsed.invalid_rows.length,
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
