import * as Sentry from "@sentry/node";

// Error monitoring per plan-error-monitoring.md Phase 2. Everything is dark
// unless SENTRY_DSN is set (local dev and tests never send). Errors only:
// no tracing, no profiling, and defaultIntegrations: false so nothing —
// request data, console breadcrumbs, process context — attaches to events
// except what the explicit capture calls pass in. The scrubber is a second
// layer on top of that.

import { scrubText } from "./scrubText.js";

export { describeError, scrubText } from "./scrubText.js";

/** Recursively masks every string in a structured value (used for
 * event.contexts, which the SDK populates with runtime info worth keeping
 * but future capture calls could load with arbitrary data). */
function scrubDeep(value: unknown): unknown {
  if (typeof value === "string") {
    return scrubText(value);
  }
  if (Array.isArray(value)) {
    return value.map(scrubDeep);
  }
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([key, entry]) => [key, scrubDeep(entry)]));
  }
  return value;
}

/** Exported for tests. Defense in depth: with defaultIntegrations off these
 * fields should not exist, but a future integration change (or a capture
 * call attaching structured data) must fail safe. */
export function scrubSentryEvent<TEvent extends Sentry.ErrorEvent>(event: TEvent): TEvent {
  delete event.request;
  delete event.user;
  delete event.breadcrumbs;
  // Structured attachments can carry whole objects (payloads, upstream
  // request context); nothing legitimate uses extra today, so drop it whole.
  delete event.extra;
  if (event.message) {
    event.message = scrubText(event.message);
  }
  for (const exception of event.exception?.values ?? []) {
    if (exception.value) {
      exception.value = scrubText(exception.value);
    }
  }
  if (event.tags) {
    for (const [key, value] of Object.entries(event.tags)) {
      if (typeof value === "string") {
        event.tags[key] = scrubText(value);
      }
    }
  }
  if (event.contexts) {
    event.contexts = scrubDeep(event.contexts) as typeof event.contexts;
  }
  return event;
}

/**
 * Initializes Sentry when SENTRY_DSN is set; otherwise a no-op (and every
 * capture below is a safe no-op too). `component` distinguishes the API
 * server from workers inside the single backend project.
 */
export function initSentryFromEnv(component: "api" | "worker"): boolean {
  const dsn = process.env.SENTRY_DSN?.trim();
  if (!dsn) {
    return false;
  }
  Sentry.init({
    dsn,
    environment: process.env.DEPLOY_ENV?.trim() || "development",
    release: process.env.DEPLOY_RELEASE?.trim() || undefined,
    sendDefaultPii: false,
    defaultIntegrations: false,
    tracesSampleRate: 0,
    maxBreadcrumbs: 0,
    beforeSend: (event) => scrubSentryEvent(event),
  });
  Sentry.setTag("component", component);
  return true;
}

/** Never throws: monitoring must not break the path it observes. */
export function captureError(error: unknown, tags: Record<string, string> = {}): void {
  try {
    Sentry.captureException(error, { tags });
  } catch {
    // Swallow: an unreachable/misconfigured monitor is not an app failure.
  }
}

/** Flush pending events before a process exit; bounded and non-throwing. */
export async function flushSentry(timeoutMs = 2000): Promise<void> {
  try {
    await Sentry.flush(timeoutMs);
  } catch {
    // Exiting anyway; losing the final event beats hanging the crash path.
  }
}
