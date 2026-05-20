import { ELECTIONS_AI_CANDIDATES, type AiCandidate } from "./aiCandidates.js";
import { getPipelineEnv } from "../config/env.js";
import {
  buildElectionsPrompt,
  type ElectionContestFamily,
} from "./providers/electionsPrompt.js";
import {
  extractProviderRateLimitDebugHeaders,
  updateProviderModelCooldownFromHeaders,
  waitForProviderModelCooldown,
} from "./providerRateLimitGate.js";
import {
  ELECTION_ENRICHMENT_SCHEMA_VERSION,
} from "../contracts/electionEnrichmentContract.js";
import { parseAiElectionEntriesPayload } from "../contracts/electionPayloadContract.js";
import type { AiProvider } from "./types.js";
import type {
  ElectionDraftPayload,
  ElectionEnrichedPayload,
  ElectionEntryPayload,
} from "../types/election.js";

const OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses";
const ANTHROPIC_MESSAGES_URL = "https://api.anthropic.com/v1/messages";
const CLAUDE_INTER_CALL_DELAY_MS = 20_000;
const CLAUDE_RETRY_AFTER_BUFFER_MS = 10_000;

let claudeLane: Promise<void> = Promise.resolve();
let lastClaudeCallStartedAt = 0;

type RetryableErrorCode = "RATE_LIMIT" | "TIMEOUT" | "TEMP_PROVIDER_ERROR";
type PermanentErrorCode = "INVALID_JSON" | "SCHEMA_MISMATCH" | "MISSING_REQUIRED_FIELDS" | "CONFIGURATION_ERROR";

type ElectionEnrichmentFailure = {
  ok: false;
  retryable: boolean;
  errorCode: RetryableErrorCode | PermanentErrorCode;
  reason: string;
  failureDebug?: Record<string, unknown>;
};

type ProviderFailureAttempt = {
  provider: string;
  model: string;
  reason: string;
  errorCode: string;
  retryable: boolean;
  failureDebug?: Record<string, unknown>;
};

function normalizeGeneratedPayloadForContestFamily(
  contestFamily: ElectionContestFamily,
  payload: unknown
): unknown {
  const forcedRaceType =
    contestFamily === "ballot_measure"
      ? "ballot_measure"
      : contestFamily === "non_judicial_office" || contestFamily === "judicial_office"
        ? "office"
        : null;

  if (!forcedRaceType) {
    return payload;
  }
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
    return { ...row, race_type: forcedRaceType };
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
  seedUrlsByFamily?: Partial<Record<ElectionContestFamily, readonly string[]>>;
};

export type EnrichElectionsConfig = {
  timeoutMs: number;
  anthropicWebSearchMaxUses?: number;
  openAiApiKey?: string;
  anthropicApiKey?: string;
  geminiApiKey?: string;
};

function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runInClaudeLane<T>(work: () => Promise<T>): Promise<T> {
  const run = claudeLane.catch(() => undefined).then(async () => {
    const nextAllowedStart = lastClaudeCallStartedAt + CLAUDE_INTER_CALL_DELAY_MS;
    const waitMs = nextAllowedStart - Date.now();
    if (waitMs > 0) {
      await sleep(waitMs);
    }
    lastClaudeCallStartedAt = Date.now();
    return work();
  });

  claudeLane = run.then(() => undefined, () => undefined);
  return run;
}

function trimDebugText(input: string, maxChars = 20_000): string {
  return input.length <= maxChars ? input : `${input.slice(0, maxChars)}...`;
}

function extractJsonCandidate(text: string): string {
  const trimmed = text.trim();

  const fenced = /```(?:json)?\s*([\s\S]*?)\s*```/i.exec(trimmed);
  if (fenced?.[1]) {
    return fenced[1].trim();
  }

  return trimmed;
}

function extractResponsesOutputText(responsePayload: unknown): string | null {
  if (typeof responsePayload !== "object" || responsePayload === null) {
    return null;
  }
  const input = responsePayload as Record<string, unknown>;

  if (typeof input.output_text === "string" && input.output_text.trim().length > 0) {
    return input.output_text;
  }

  const output = input.output;
  if (!Array.isArray(output)) {
    return null;
  }

  const parts: string[] = [];
  for (const item of output) {
    if (typeof item !== "object" || item === null || Array.isArray(item)) {
      continue;
    }
    const outputItem = item as Record<string, unknown>;
    if (outputItem.type !== "message") {
      continue;
    }

    const content = outputItem.content;
    if (!Array.isArray(content)) {
      continue;
    }

    for (const contentPart of content) {
      if (typeof contentPart !== "object" || contentPart === null || Array.isArray(contentPart)) {
        continue;
      }
      const outputPart = contentPart as Record<string, unknown>;
      if (outputPart.type !== "output_text" || typeof outputPart.text !== "string") {
        continue;
      }
      const text = outputPart.text.trim();
      if (text.length > 0) {
        parts.push(text);
      }
    }
  }

  if (parts.length === 0) {
    return null;
  }
  return parts.join("\n");
}

function extractOpenAiWebSearchSources(responsePayload: unknown): Array<{ url?: string; title?: string }> {
  if (typeof responsePayload !== "object" || responsePayload === null) {
    return [];
  }
  const input = responsePayload as Record<string, unknown>;
  const output = input.output;
  if (!Array.isArray(output)) {
    return [];
  }

  const sources: Array<{ url?: string; title?: string }> = [];
  for (const item of output) {
    if (typeof item !== "object" || item === null || Array.isArray(item)) {
      continue;
    }
    const outputItem = item as Record<string, unknown>;
    if (outputItem.type !== "web_search_call") {
      continue;
    }
    const action = outputItem.action;
    if (typeof action !== "object" || action === null || Array.isArray(action)) {
      continue;
    }
    const actionRecord = action as Record<string, unknown>;
    const rawSources = actionRecord.sources;
    if (!Array.isArray(rawSources)) {
      continue;
    }
    for (const source of rawSources) {
      if (typeof source !== "object" || source === null || Array.isArray(source)) {
        continue;
      }
      const sourceRecord = source as Record<string, unknown>;
      sources.push({
        ...(typeof sourceRecord.url === "string" ? { url: sourceRecord.url } : {}),
        ...(typeof sourceRecord.title === "string" ? { title: sourceRecord.title } : {}),
      });
    }
  }

  return sources;
}

function shouldSetExplicitTemperature(model: string): boolean {
  return !model.toLowerCase().startsWith("gpt-5");
}

function needsContestFamilySplit(districtType: ElectionDraftPayload["district_type"]): boolean {
  return districtType === "statewide" || districtType === "county" || districtType === "place";
}

function dedupeMergedEntries(entries: ElectionEntryPayload[]): ElectionEntryPayload[] {
  const byKey = new Map<string, ElectionEntryPayload>();
  const normalizeKey = (value: string): string => value.toLowerCase().replace(/\s+/g, " ").trim();
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

  for (const entry of entries) {
    const key = `${entry.election_date}::${normalizeKey(entry.official_ballot_title)}`;
    const prior = byKey.get(key);
    if (!prior) {
      byKey.set(key, entry);
      continue;
    }
    byKey.set(key, {
      ...prior,
      sources: mergeSources(prior.sources, entry.sources),
      // Prefer a longer explanation when two entries collide.
      description:
        entry.description.length > prior.description.length ? entry.description : prior.description,
    });
  }
  return [...byKey.values()];
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

function validateContestFamilySoft(
  family: ElectionContestFamily,
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
  return { ok: true };
}

async function callOpenAiResponsesWithWebSearch(
  prompt: string,
  model: string,
  apiKey: string,
  timeoutMs: number
): Promise<
  | { ok: true; parsed: unknown; rawText: string; responsesDebug?: Record<string, unknown> }
  | ElectionEnrichmentFailure
> {
  let controller: AbortController | null = null;
  let timeout: ReturnType<typeof setTimeout> | null = null;

  try {
    await waitForProviderModelCooldown("openai", model);
    const requestController = new AbortController();
    controller = requestController;
    timeout = setTimeout(() => requestController.abort(), timeoutMs);

    const requestBody: Record<string, unknown> = {
      model,
      input: [
        {
          role: "system",
          content: [{ type: "input_text", text: "Return strict JSON only." }],
        },
        {
          role: "user",
          content: [{ type: "input_text", text: prompt }],
        },
      ],
      tools: [{ type: "web_search" }],
      tool_choice: "auto",
      include: ["web_search_call.action.sources"],
    };
    if (shouldSetExplicitTemperature(model)) {
      requestBody.temperature = 0;
    }

    const response = await fetch(OPENAI_RESPONSES_URL, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(requestBody),
      signal: controller!.signal,
    });

    if (!response.ok) {
      const rateLimitHeaders = extractProviderRateLimitDebugHeaders(response.headers);
      updateProviderModelCooldownFromHeaders("openai", model, response.headers, {
        onRateLimitedResponse: response.status === 429,
      });
      const bodyText = await response.text();
      if (response.status === 429) {
        return {
          ok: false,
          retryable: true,
          errorCode: "RATE_LIMIT",
          reason: `OpenAI responses rate limit: ${bodyText}`,
          failureDebug: {
            provider_response_text: trimDebugText(bodyText),
            provider_rate_limit_headers: rateLimitHeaders,
          },
        };
      }
      if (response.status >= 500) {
        return {
          ok: false,
          retryable: true,
          errorCode: "TEMP_PROVIDER_ERROR",
          reason: `OpenAI responses temporary error ${response.status}: ${bodyText}`,
          failureDebug: {
            provider_response_text: trimDebugText(bodyText),
            provider_rate_limit_headers: rateLimitHeaders,
          },
        };
      }
      return {
        ok: false,
        retryable: false,
        errorCode: "CONFIGURATION_ERROR",
        reason: `OpenAI responses request failed ${response.status}: ${bodyText}`,
        failureDebug: {
          provider_response_text: trimDebugText(bodyText),
          provider_rate_limit_headers: rateLimitHeaders,
        },
      };
    }

    updateProviderModelCooldownFromHeaders("openai", model, response.headers);

    const data = (await response.json()) as Record<string, unknown>;
    const text = extractResponsesOutputText(data);
    if (!text || text.trim().length === 0) {
      return {
        ok: false,
        retryable: false,
        errorCode: "INVALID_JSON",
        reason: "OpenAI responses returned empty assistant text",
        failureDebug: {
          provider_response_payload: data,
        },
      };
    }

    const webSearchSources = extractOpenAiWebSearchSources(data);
    try {
      return {
        ok: true,
        parsed: JSON.parse(extractJsonCandidate(text)),
        rawText: text,
        responsesDebug: {
          web_search_sources: webSearchSources,
          web_search_sources_count: webSearchSources.length,
        },
      };
    } catch (error) {
      return {
        ok: false,
        retryable: false,
        errorCode: "INVALID_JSON",
        reason: `OpenAI responses returned invalid JSON: ${toReason(error)}`,
        failureDebug: {
          provider_response_text: trimDebugText(text),
          web_search_sources: webSearchSources,
          web_search_sources_count: webSearchSources.length,
        },
      };
    }
  } catch (error) {
    const reason = toReason(error);
    if (reason.toLowerCase().includes("aborted")) {
      return {
        ok: false,
        retryable: true,
        errorCode: "TIMEOUT",
        reason: `OpenAI responses request timed out after ${timeoutMs}ms`,
      };
    }
    return {
      ok: false,
      retryable: true,
      errorCode: "TEMP_PROVIDER_ERROR",
      reason: `OpenAI responses request error: ${reason}`,
    };
  } finally {
    if (timeout) {
      clearTimeout(timeout);
    }
  }
}

async function callClaude(
  prompt: string,
  model: string,
  apiKey: string,
  timeoutMs: number,
  webSearchMaxUses = 3
): Promise<{ ok: true; parsed: unknown; rawText: string } | ElectionEnrichmentFailure> {
  return runInClaudeLane(async () => {
    let controller: AbortController | null = null;
    let timeout: ReturnType<typeof setTimeout> | null = null;

    try {
      await waitForProviderModelCooldown("claude", model);
      const requestController = new AbortController();
      controller = requestController;
      timeout = setTimeout(() => requestController.abort(), timeoutMs);

    const requestBody: Record<string, unknown> = {
      model,
      max_tokens: 4000,
      temperature: 0,
      system: "Return strict JSON only.",
      messages: [{ role: "user", content: prompt }],
    };
    const headers: Record<string, string> = {
      "x-api-key": apiKey,
      "anthropic-version": "2023-06-01",
      "content-type": "application/json",
    };
    requestBody.tools = [
      {
        type: "web_search_20250305",
        name: "web_search",
        max_uses: Math.max(1, Math.floor(webSearchMaxUses)),
      },
    ];
    headers["anthropic-beta"] = "web-search-2025-03-05";

      const response = await fetch(ANTHROPIC_MESSAGES_URL, {
        method: "POST",
        headers,
        body: JSON.stringify(requestBody),
        signal: controller!.signal,
      });

      if (!response.ok) {
        const rateLimitHeaders = extractProviderRateLimitDebugHeaders(response.headers);
        updateProviderModelCooldownFromHeaders("claude", model, response.headers, {
          onRateLimitedResponse: response.status === 429,
          retryAfterBufferMs: CLAUDE_RETRY_AFTER_BUFFER_MS,
        });
        const bodyText = await response.text();
        if (response.status === 429) {
          return {
            ok: false,
            retryable: true,
            errorCode: "RATE_LIMIT",
            reason: `Claude rate limit: ${bodyText}`,
            failureDebug: {
              provider_response_text: trimDebugText(bodyText),
              provider_rate_limit_headers: rateLimitHeaders,
            },
          };
        }
        if (response.status >= 500) {
          return {
            ok: false,
            retryable: true,
            errorCode: "TEMP_PROVIDER_ERROR",
            reason: `Claude temporary error ${response.status}: ${bodyText}`,
            failureDebug: {
              provider_response_text: trimDebugText(bodyText),
              provider_rate_limit_headers: rateLimitHeaders,
            },
          };
        }
        return {
          ok: false,
          retryable: false,
          errorCode: "CONFIGURATION_ERROR",
          reason: `Claude request failed ${response.status}: ${bodyText}`,
          failureDebug: {
            provider_response_text: trimDebugText(bodyText),
            provider_rate_limit_headers: rateLimitHeaders,
          },
        };
      }

      updateProviderModelCooldownFromHeaders("claude", model, response.headers);

      const data = (await response.json()) as {
        content?: Array<{ type?: string; text?: string }>;
      };
      const text = data.content?.find((part) => part.type === "text")?.text;
      if (!text || text.trim().length === 0) {
        return { ok: false, retryable: false, errorCode: "INVALID_JSON", reason: "Claude returned empty content" };
      }
      try {
        return { ok: true, parsed: JSON.parse(extractJsonCandidate(text)), rawText: text };
      } catch (error) {
        return {
          ok: false,
          retryable: false,
          errorCode: "INVALID_JSON",
          reason: `Claude returned invalid JSON: ${toReason(error)}`,
          failureDebug: { provider_response_text: trimDebugText(text) },
        };
      }
    } catch (error) {
      const reason = toReason(error);
      if (reason.toLowerCase().includes("aborted")) {
        return { ok: false, retryable: true, errorCode: "TIMEOUT", reason: `Claude request timed out after ${timeoutMs}ms` };
      }
      return { ok: false, retryable: true, errorCode: "TEMP_PROVIDER_ERROR", reason: `Claude request error: ${reason}` };
    } finally {
      if (timeout) {
        clearTimeout(timeout);
      }
    }
  });
}

async function callGemini(
  prompt: string,
  model: string,
  apiKey: string,
  timeoutMs: number
): Promise<{ ok: true; parsed: unknown; rawText: string } | ElectionEnrichmentFailure> {
  let controller: AbortController | null = null;
  let timeout: ReturnType<typeof setTimeout> | null = null;
  const url = `https://generativelanguage.googleapis.com/v1/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(apiKey)}`;

  try {
    await waitForProviderModelCooldown("gemini", model);
    const requestController = new AbortController();
    controller = requestController;
    timeout = setTimeout(() => requestController.abort(), timeoutMs);

    const response = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        generationConfig: { temperature: 0 },
        contents: [{ role: "user", parts: [{ text: prompt }] }],
      }),
      signal: controller!.signal,
    });

    if (!response.ok) {
      const rateLimitHeaders = extractProviderRateLimitDebugHeaders(response.headers);
      updateProviderModelCooldownFromHeaders("gemini", model, response.headers, {
        onRateLimitedResponse: response.status === 429,
      });
      const bodyText = await response.text();
      if (response.status === 429) {
        return {
          ok: false,
          retryable: true,
          errorCode: "RATE_LIMIT",
          reason: `Gemini rate limit: ${bodyText}`,
          failureDebug: {
            provider_rate_limit_headers: rateLimitHeaders,
          },
        };
      }
      if (response.status >= 500) {
        return {
          ok: false,
          retryable: true,
          errorCode: "TEMP_PROVIDER_ERROR",
          reason: `Gemini temporary error ${response.status}: ${bodyText}`,
          failureDebug: {
            provider_rate_limit_headers: rateLimitHeaders,
          },
        };
      }
      return {
        ok: false,
        retryable: false,
        errorCode: "CONFIGURATION_ERROR",
        reason: `Gemini request failed ${response.status}: ${bodyText}`,
        failureDebug: {
          provider_rate_limit_headers: rateLimitHeaders,
        },
      };
    }

    updateProviderModelCooldownFromHeaders("gemini", model, response.headers);

    const data = (await response.json()) as {
      candidates?: Array<{ content?: { parts?: Array<{ text?: string }> } }>;
    };
    const text = data.candidates?.[0]?.content?.parts?.[0]?.text;
    if (!text || text.trim().length === 0) {
      return { ok: false, retryable: false, errorCode: "INVALID_JSON", reason: "Gemini returned empty content" };
    }
    try {
      return { ok: true, parsed: JSON.parse(extractJsonCandidate(text)), rawText: text };
    } catch (error) {
      return {
        ok: false,
        retryable: false,
        errorCode: "INVALID_JSON",
        reason: `Gemini returned invalid JSON: ${toReason(error)}`,
        failureDebug: { provider_response_text: trimDebugText(text) },
      };
    }
  } catch (error) {
    const reason = toReason(error);
    if (reason.toLowerCase().includes("aborted")) {
      return { ok: false, retryable: true, errorCode: "TIMEOUT", reason: `Gemini request timed out after ${timeoutMs}ms` };
    }
    return { ok: false, retryable: true, errorCode: "TEMP_PROVIDER_ERROR", reason: `Gemini request error: ${reason}` };
  } finally {
    if (timeout) {
      clearTimeout(timeout);
    }
  }
}

async function callOpenAi(
  prompt: string,
  model: string,
  apiKey: string,
  timeoutMs: number
): Promise<{ ok: true; parsed: unknown; rawText: string; debugMeta?: Record<string, unknown> } | ElectionEnrichmentFailure> {
  const responsesResult = await callOpenAiResponsesWithWebSearch(
    prompt,
    model,
    apiKey,
    timeoutMs
  );
  if (responsesResult.ok) {
    return {
      ok: true,
      parsed: responsesResult.parsed,
      rawText: responsesResult.rawText,
      debugMeta: {
        openai_api_mode: "responses_web_search",
        ...(responsesResult.responsesDebug ?? {}),
      },
    };
  }
  return responsesResult;
}

async function callProvider(
  candidate: AiCandidate,
  prompt: string,
  config: EnrichElectionsConfig
): Promise<{ ok: true; parsed: unknown; rawText: string; debugMeta?: Record<string, unknown> } | ElectionEnrichmentFailure> {
  if (candidate.provider === "openai") {
    if (!config.openAiApiKey) {
      return { ok: false, retryable: false, errorCode: "CONFIGURATION_ERROR", reason: "OPENAI_API_KEY is missing" };
    }
    return callOpenAi(prompt, candidate.model, config.openAiApiKey, config.timeoutMs);
  }
    if (candidate.provider === "claude") {
      if (!config.anthropicApiKey) {
        return { ok: false, retryable: false, errorCode: "CONFIGURATION_ERROR", reason: "ANTHROPIC_API_KEY is missing" };
      }
      return callClaude(
        prompt,
        candidate.model,
        config.anthropicApiKey,
        config.timeoutMs,
        config.anthropicWebSearchMaxUses
      );
    }
  if (!config.geminiApiKey) {
    return { ok: false, retryable: false, errorCode: "CONFIGURATION_ERROR", reason: "GEMINI_API_KEY is missing" };
  }
  return callGemini(prompt, candidate.model, config.geminiApiKey, config.timeoutMs);
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
  prompt: string,
  contestFamily: ElectionContestFamily,
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
      errorCode: RetryableErrorCode | PermanentErrorCode;
      retryable: boolean;
      attempts: ProviderFailureAttempt[];
    }
> {
  const failures: ProviderFailureAttempt[] = [];

  for (const candidate of candidates) {
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
      continue;
    }

    const normalizedGeneratedPayload = normalizeGeneratedPayloadForContestFamily(
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
      continue;
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
      continue;
    }

    return {
      ok: true,
      entries: parsed.payload.entries,
      ...(parsed.payload.review_decision
        ? { reviewDecision: parsed.payload.review_decision }
        : {}),
      ...(parsed.payload.review_reason ? { reviewReason: parsed.payload.review_reason } : {}),
      provider: candidate.provider,
      model: candidate.model,
      aiRawDebug: {
        provider_response_text: trimDebugText(generated.rawText),
        ...(generated.debugMeta ?? {}),
      },
    };
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
    errorCode:
      (selectedFailure?.errorCode as RetryableErrorCode | PermanentErrorCode | undefined) ??
      "TEMP_PROVIDER_ERROR",
    reason: selectedFailure?.reason ?? "No AI candidates available for election enrichment",
    attempts: failures,
  };
}

export async function enrichElections(
  input: EnrichElectionsInput,
  config: EnrichElectionsConfig,
  candidates: readonly AiCandidate[] = ELECTIONS_AI_CANDIDATES
): Promise<EnrichElectionsResult> {
  const familyPlan: ElectionContestFamily[] = needsContestFamilySplit(input.draft.district_type)
    ? ["non_judicial_office", "judicial_office", "ballot_measure"]
    : ["all"];

  const mergedEntries: ElectionEntryPayload[] = [];
  const mergedDebug: Record<string, unknown> = {};
  const reviewDecisions: Array<"approve" | "reject"> = [];
  const reviewReasons: string[] = [];
  const providerModelLabels: string[] = [];
  const providerFamilyLabels: string[] = [];
  const familySourceUrls: Partial<Record<ElectionContestFamily, string[]>> = {};
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
    const outcome = await runPromptWithCandidates(prompt, family, config, candidates);
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

    mergedEntries.push(...outcome.entries);
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
