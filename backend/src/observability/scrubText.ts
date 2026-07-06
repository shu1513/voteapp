// Text scrubbing shared by Sentry events AND local log lines — kept free of
// any @sentry/node import so the API server can use it without pulling the
// SDK into its module graph.

const EMAIL_PATTERN = /[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/g;
// Query strings are location-adjacent here (?d=<district-ids>); strip the
// value part of any URL that sneaks into an error message. Stack frames are
// unaffected (no "?" in backend file paths).
const QUERY_STRING_PATTERN = /\?[^\s"']+/g;

export function scrubText(value: string): string {
  return value.replaceAll(EMAIL_PATTERN, "[email]").replaceAll(QUERY_STRING_PATTERN, "?[scrubbed]");
}

/**
 * The one representation of an unknown error that may be logged: the stack
 * string (message + frames) — never the object, whose enumerable custom
 * properties can carry payloads — with emails and query strings masked.
 */
export function describeError(error: unknown): string {
  return scrubText(error instanceof Error ? (error.stack ?? error.message) : String(error));
}
