import * as Sentry from "@sentry/node";

// Error monitoring per plan-error-monitoring.md Phase 2. Everything is dark
// unless SENTRY_DSN is set (local dev and tests never send). Errors only:
// no tracing, no profiling, and defaultIntegrations: false so nothing —
// request data, console breadcrumbs, process context — attaches to events
// except what the explicit capture calls pass in. The scrubber is a second
// layer on top of that.

const EMAIL_PATTERN = /[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/g;
// Query strings are location-adjacent here (?d=<district-ids>); strip the
// value part of any URL that sneaks into an error message.
const QUERY_STRING_PATTERN = /\?[^\s"']+/g;

export function scrubText(value: string): string {
  return value.replaceAll(EMAIL_PATTERN, "[email]").replaceAll(QUERY_STRING_PATTERN, "?[scrubbed]");
}

/** Exported for tests. Defense in depth: with defaultIntegrations off these
 * fields should not exist, but a future integration change must fail safe. */
export function scrubSentryEvent<TEvent extends Sentry.ErrorEvent>(event: TEvent): TEvent {
  delete event.request;
  delete event.user;
  delete event.breadcrumbs;
  if (event.message) {
    event.message = scrubText(event.message);
  }
  for (const exception of event.exception?.values ?? []) {
    if (exception.value) {
      exception.value = scrubText(exception.value);
    }
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
