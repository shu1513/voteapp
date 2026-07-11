import * as Sentry from "@sentry/react-native";

// Port of frontend/src/lib/errorMonitoring.ts for the mobile app: dark
// unless EXPO_PUBLIC_SENTRY_DSN is set (use a Sentry project separate from
// web — the release/source-map lifecycle differs), errors only, and the same
// scrubber that keeps addresses, emails, and district query strings out of
// events.

const EMAIL_PATTERN = /[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/g;
// Query strings are location-adjacent here (?d=<district-ids>).
const QUERY_STRING_PATTERN = /\?[^\s"']+/g;

export function scrubText(value: string): string {
  return value.replaceAll(EMAIL_PATTERN, "[email]").replaceAll(QUERY_STRING_PATTERN, "?[scrubbed]");
}

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

/** Exported for tests. Same rules as the web scrubber: no request, no user,
 * no breadcrumbs, no extra; emails and query strings masked everywhere
 * else. */
export function scrubSentryEvent<TEvent extends Sentry.ErrorEvent>(event: TEvent): TEvent {
  delete event.request;
  delete event.user;
  delete event.breadcrumbs;
  delete event.extra;
  if (event.message) {
    event.message = scrubText(event.message);
  }
  if (event.transaction) {
    event.transaction = scrubText(event.transaction);
  }
  for (const exception of event.exception?.values ?? []) {
    if (exception.value) {
      exception.value = scrubText(exception.value);
    }
    for (const frame of exception.stacktrace?.frames ?? []) {
      if (frame.filename) {
        frame.filename = scrubText(frame.filename);
      }
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
 * Initializes Sentry when EXPO_PUBLIC_SENTRY_DSN is set; otherwise a no-op
 * (and captureMonitoredError below is a safe no-op too). Errors only — no
 * tracing, no replay, no breadcrumbs.
 */
export function initErrorMonitoring(): boolean {
  const dsn = process.env.EXPO_PUBLIC_SENTRY_DSN;
  if (!dsn) {
    return false;
  }
  Sentry.init({
    dsn,
    environment: process.env.EXPO_PUBLIC_DEPLOY_ENV || "development",
    sendDefaultPii: false,
    tracesSampleRate: 0,
    maxBreadcrumbs: 0,
    beforeSend: (event) => scrubSentryEvent(event),
  });
  return true;
}

/** Never throws: monitoring must not break the path it observes. */
export function captureMonitoredError(error: unknown, tags: Record<string, string> = {}): void {
  try {
    Sentry.captureException(error, { tags });
  } catch {
    // Swallow: an unreachable monitor is not an app failure.
  }
}
