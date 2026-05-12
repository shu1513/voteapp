import type { RetryFeedback } from "./types.js";

function isObjectRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function normalizeFailedCitationUrls(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }

  const seen = new Set<string>();
  const normalized: string[] = [];

  for (const item of value) {
    if (!isNonEmptyString(item)) {
      continue;
    }

    const trimmed = item.trim();
    if (seen.has(trimmed)) {
      continue;
    }

    seen.add(trimmed);
    normalized.push(trimmed);
  }

  return normalized;
}

function normalizeFailedCitationDetails(value: unknown): Array<{ url: string; reason: string }> {
  if (!Array.isArray(value)) {
    return [];
  }

  const seen = new Set<string>();
  const normalized: Array<{ url: string; reason: string }> = [];

  for (const item of value) {
    if (!isObjectRecord(item)) {
      continue;
    }
    if (!isNonEmptyString(item.url) || !isNonEmptyString(item.reason)) {
      continue;
    }

    const url = item.url.trim();
    const reason = item.reason.trim();
    const key = `${url}|||${reason}`;
    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    normalized.push({ url, reason });
  }

  return normalized;
}

/**
 * Parses unknown retry feedback payloads into a strict, optional object.
 */
export function normalizeRetryFeedback(value: unknown): RetryFeedback | null {
  if (!isObjectRecord(value)) {
    return null;
  }

  const previousFailureReason = isNonEmptyString(value.previousFailureReason)
    ? value.previousFailureReason.trim()
    : null;

  const retryCount = typeof value.retryCount === "number" && Number.isFinite(value.retryCount)
    ? Math.max(1, Math.floor(value.retryCount))
    : null;

  const failedAt = isNonEmptyString(value.failedAt)
    ? value.failedAt.trim()
    : null;

  const failedCitationUrls = normalizeFailedCitationUrls(value.failedCitationUrls);
  const failedCitationDetails = normalizeFailedCitationDetails(
    value.failedCitationDetails ?? value.failed_citation_details
  );

  if (
    !previousFailureReason &&
    failedCitationUrls.length === 0 &&
    failedCitationDetails.length === 0 &&
    retryCount === null &&
    !failedAt
  ) {
    return null;
  }

  return {
    previousFailureReason,
    failedCitationUrls,
    failedCitationDetails,
    retryCount,
    failedAt,
  };
}

/**
 * Builds concise retry instructions for provider prompts.
 */
export function buildRetryFeedbackPromptLines(retryFeedback: RetryFeedback | null | undefined): string[] {
  const normalized = normalizeRetryFeedback(retryFeedback ?? null);
  if (!normalized) {
    return [];
  }

  // Keep prompt payload keys snake_case for stable JSON prompt format across providers.
  const payload = {
    previous_failure_reason: normalized.previousFailureReason,
    failed_citation_urls: normalized.failedCitationUrls,
    failed_citation_details: normalized.failedCitationDetails,
    retry_count: normalized.retryCount,
    failed_at: normalized.failedAt,
  };

  return [
    "Previous attempt feedback (retry context):",
    JSON.stringify(payload),
    "Strict retry rules:",
    "- Do not reuse any URL listed in failed_citation_urls.",
    "- Use failed_citation_details to avoid the same URL failure patterns (403/404/blocked/fetch-failed).",
    "- Replace failed/blocked/not-found URLs with different verifiable URLs.",
    "- Keep claim-level citations specific and reachable.",
  ];
}
