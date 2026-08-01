/**
 * Validates a post-auth return path (the `?next=` param on /login and
 * /register). Only same-origin paths pass: anything not starting with a
 * single "/" is rejected — "//host" and "/\host" are protocol-relative
 * URLs to a browser, which would turn the redirect into an open redirect.
 */
export function safeInternalPath(raw: string | null): string | null {
  if (!raw || !raw.startsWith("/")) {
    return null;
  }
  if (raw.startsWith("//") || raw.startsWith("/\\")) {
    return null;
  }
  return raw;
}
