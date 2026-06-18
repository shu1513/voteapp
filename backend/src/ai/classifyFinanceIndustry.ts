import {
  FINANCE_INDUSTRY_CLASSIFICATION_AI_CANDIDATES,
  type AiCandidate,
} from "./aiCandidates.js";
import { getPipelineEnv } from "../config/env.js";
import {
  callResearchProvider,
  trimDebugText,
  type ResearchErrorCode,
} from "./researchProviderClient.js";
import type { AiProvider } from "./types.js";
import {
  FINANCE_INDUSTRY_SLUGS,
  type FinanceClassificationConfidence,
  type FinanceIndustrySlug,
  type FinanceLabelClassification,
} from "../pipeline/finance/financeLabelClassifier.js";
import {
  buildFinanceIndustryClassificationPrompt,
  type FinanceIndustryClassificationPromptLabel,
} from "./providers/financeIndustryClassificationPrompt.js";

export type FinanceIndustryClassificationAiConfig = {
  timeoutMs: number;
  openAiApiKey?: string;
  anthropicApiKey?: string;
  geminiApiKey?: string;
};

export type FinanceIndustryClassificationAiResult =
  | {
      ok: true;
      provider: AiProvider;
      model: string;
      classifications: FinanceLabelClassification[];
      aiRawDebug: Record<string, unknown> | null;
    }
  | {
      ok: false;
      retryable: boolean;
      errorCode: ResearchErrorCode | "SCHEMA_MISMATCH";
      reason: string;
      failureDebug?: Record<string, unknown>;
    };

const ALLOWED_INDUSTRY_SLUGS = new Set<string>(FINANCE_INDUSTRY_SLUGS);
const ALLOWED_CONFIDENCES = new Set<FinanceClassificationConfidence>(["high", "medium", "low", "unknown"]);

function toReason(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function parseIndustrySlug(value: unknown): FinanceIndustrySlug | null {
  if (typeof value !== "string") {
    return null;
  }
  const normalized = value.trim().toLowerCase();
  return ALLOWED_INDUSTRY_SLUGS.has(normalized) ? (normalized as FinanceIndustrySlug) : null;
}

function parseConfidence(value: unknown, hasIndustry: boolean): FinanceClassificationConfidence {
  if (typeof value !== "string") {
    return hasIndustry ? "low" : "unknown";
  }
  const normalized = value.trim().toLowerCase();
  return ALLOWED_CONFIDENCES.has(normalized as FinanceClassificationConfidence)
    ? (normalized as FinanceClassificationConfidence)
    : hasIndustry
      ? "low"
      : "unknown";
}

function parseFinanceIndustryPayload(
  payload: unknown,
  labels: readonly FinanceIndustryClassificationPromptLabel[]
): FinanceLabelClassification[] {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    throw new Error("Expected JSON object");
  }
  const classifications = (payload as { classifications?: unknown }).classifications;
  if (!Array.isArray(classifications)) {
    throw new Error("Expected classifications array");
  }

  const expectedById = new Map(labels.map((label, index) => [String(index + 1), label]));
  const parsed: FinanceLabelClassification[] = [];
  const seen = new Set<string>();

  for (const entry of classifications) {
    if (typeof entry !== "object" || entry === null || Array.isArray(entry)) {
      continue;
    }
    const record = entry as Record<string, unknown>;
    const id =
      typeof record.id === "string" || typeof record.id === "number"
        ? String(record.id).trim()
        : "";
    const expected = expectedById.get(id);
    if (!expected || seen.has(id)) {
      continue;
    }
    seen.add(id);

    const industrySlug = parseIndustrySlug(record.industry_slug);
    const confidence = parseConfidence(record.confidence, Boolean(industrySlug));
    parsed.push({
      rawLabel: expected.rawLabel,
      labelType: expected.labelType,
      normalizedLabel: expected.normalizedLabel,
      industrySlug,
      confidence: industrySlug ? confidence : "unknown",
      classificationSource: industrySlug ? "ai" : "unknown",
      matchedRule: null,
    });
  }

  return parsed;
}

async function callProvider(
  candidate: AiCandidate,
  prompt: string,
  config: FinanceIndustryClassificationAiConfig
): Promise<{ ok: true; parsed: unknown; rawText: string; debugMeta?: Record<string, unknown> } | Exclude<FinanceIndustryClassificationAiResult, { ok: true }>> {
  const providerResult = await callResearchProvider(candidate, prompt, {
    timeoutMs: config.timeoutMs,
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

export async function classifyFinanceIndustriesWithAi(input: {
  labels: readonly FinanceIndustryClassificationPromptLabel[];
  aiCandidates?: readonly AiCandidate[];
  config?: FinanceIndustryClassificationAiConfig;
}): Promise<FinanceIndustryClassificationAiResult> {
  const labels = input.labels.filter((label) => label.normalizedLabel.trim().length > 0);
  if (labels.length === 0) {
    return {
      ok: true,
      provider: FINANCE_INDUSTRY_CLASSIFICATION_AI_CANDIDATES[0].provider,
      model: FINANCE_INDUSTRY_CLASSIFICATION_AI_CANDIDATES[0].model,
      classifications: [],
      aiRawDebug: null,
    };
  }

  const env = input.config
    ? null
    : getPipelineEnv();
  const config = input.config ?? {
    timeoutMs: env?.AI_TIMEOUT_MS ?? 90_000,
    openAiApiKey: env?.OPENAI_API_KEY,
    anthropicApiKey: env?.ANTHROPIC_API_KEY,
    geminiApiKey: env?.GEMINI_API_KEY,
  };
  const candidates = input.aiCandidates ?? FINANCE_INDUSTRY_CLASSIFICATION_AI_CANDIDATES;
  const prompt = buildFinanceIndustryClassificationPrompt({ labels });

  let lastFailure: Exclude<FinanceIndustryClassificationAiResult, { ok: true }> | null = null;
  for (const candidate of candidates) {
    const generated = await callProvider(candidate, prompt, config);
    if (!generated.ok) {
      lastFailure = generated;
      // A non-retryable failure is provider-local here (for example, one missing API key).
      // Keep falling through so partially configured deployments can use a later provider.
      continue;
    }

    try {
      return {
        ok: true,
        provider: candidate.provider,
        model: candidate.model,
        classifications: parseFinanceIndustryPayload(generated.parsed, labels),
        aiRawDebug: {
          provider: candidate.provider,
          model: candidate.model,
          raw_text: trimDebugText(generated.rawText),
          ...(generated.debugMeta ? { provider_debug: generated.debugMeta } : {}),
        },
      };
    } catch (error) {
      lastFailure = {
        ok: false,
        retryable: true,
        errorCode: "SCHEMA_MISMATCH",
        reason: `Finance industry classification payload schema mismatch: ${toReason(error)}`,
        failureDebug: {
          provider: candidate.provider,
          model: candidate.model,
          raw_text: trimDebugText(generated.rawText),
        },
      };
    }
  }

  return (
    lastFailure ?? {
      ok: false,
      retryable: true,
      errorCode: "TEMP_PROVIDER_ERROR",
      reason: "No finance industry classification providers were configured",
    }
  );
}

export function createFinanceIndustryClassifierFromEnv(): (input: {
  labels: readonly FinanceIndustryClassificationPromptLabel[];
}) => Promise<FinanceLabelClassification[]> {
  const env = getPipelineEnv();
  return async (input) => {
    const result = await classifyFinanceIndustriesWithAi({
      labels: input.labels,
      config: {
        timeoutMs: env.AI_TIMEOUT_MS,
        openAiApiKey: env.OPENAI_API_KEY,
        anthropicApiKey: env.ANTHROPIC_API_KEY,
        geminiApiKey: env.GEMINI_API_KEY,
      },
    });
    if (!result.ok) {
      throw new Error(result.reason);
    }
    return result.classifications;
  };
}
