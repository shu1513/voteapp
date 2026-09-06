/**
 * A request the API must answer with 400: the caller sent something the
 * handler cannot accept. Thrown from request validators and from service
 * checks on request-derived values (submitted passwords, tokens, session
 * cookies, body fields); mapped by apiErrors to `{ 400, code, message }`
 * with the message shown to the caller.
 *
 * Deliberately dependency-free so auth stores, user writers and usage
 * parsing can throw it without importing the API layer.
 *
 * Anything else — a runtime `TypeError` from a bug, a `SyntaxError` from a
 * stored value that failed to parse, a bad configured value — is NOT a
 * request problem and must stay a plain `Error` so it becomes a captured 500
 * instead of a client-visible 400 that hides the bug.
 */
export class RequestValidationError extends Error {
  constructor(
    message: string,
    readonly code: "invalid_request" | "invalid_json" = "invalid_request"
  ) {
    super(message);
    this.name = "RequestValidationError";
  }
}
