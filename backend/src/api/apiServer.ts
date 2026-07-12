import express, { type Express, type NextFunction, type Request, type Response } from "express";
import {
  AUTH_FORGOT_PASSWORD_PATH,
  AUTH_LOGIN_PATH,
  AUTH_LOGOUT_PATH,
  AUTH_REGISTER_PATH,
  AUTH_RESET_PASSWORD_PATH,
  AUTH_RESEND_VERIFICATION_PATH,
  AUTH_VERIFY_EMAIL_PATH,
  AUTH_VERIFY_EMAIL_CHANGE_PATH,
  AUTH_LOGOUT_ALL_PATH,
  parseAuthForgotPasswordBodyValue,
  parseAuthLoginBodyValue,
  parseAuthRegisterBodyValue,
  parseAuthResetPasswordBodyValue,
  parseAuthResendVerificationBodyValue,
  parseAuthVerifyEmailBodyValue,
  parseAuthVerifyEmailChangeBodyValue,
  parseMeDeleteBodyValue,
  parseMeEmailBodyValue,
  parseMePasswordBodyValue,
  parseMeUpdateBodyValue,
} from "./apiValidation.js";
import type { AddressApiServerOptions } from "./addressApiTypes.js";
import { mapErrorToResponse } from "./apiErrors.js";
import { resolveCorsHeaders } from "./apiCors.js";
import {
  ADDRESS_AUTOCOMPLETE_PATH,
  ADDRESS_AUTOCOMPLETE_RETRIEVE_PATH,
  ADDRESS_RESOLVE_PATH,
  BALLOT_LOOKUP_PATH,
  CONTENT_REPORTS_PATH,
  CANDIDATE_DETAIL_PATH_PREFIX,
  ELECTION_DETAIL_PATH_PREFIX,
  isCandidateDetailPath,
  isCandidateElectionFinancePath,
  isElectionDetailPath,
  MAX_ADDRESS_REQUEST_BODY_BYTES,
  EMAIL_UNSUBSCRIBE_PATH,
  ME_ADDRESS_PATH,
  ME_BALLOT_PATH,
  ME_EMAIL_PATH,
  ME_PASSWORD_PATH,
  ME_PATH,
  ME_BALLOT_PREFERENCES_PATH,
  ME_CANDIDATE_FOLLOWS_PATH,
  ME_DISTRICTS_INITIALIZE_PATH,
  ME_EMAIL_PREFERENCES_PATH,
  ME_PUSH_TOKENS_PATH,
  ME_RESEARCH_AREA_PREFERENCES_PATH,
  parseAuthenticatedAddressBodyValue,
  parseAddressBodyValue,
  parseAutocompleteRetrieveBodyValue,
  parseAutocompleteSuggestBodyValue,
  parseCandidateFollowBodyValue,
  parseContentReportBodyValue,
  parseBallotPreferencesBodyValue,
  parseEmailPreferencesBodyValue,
  parseEmailUnsubscribePreference,
  parsePushTokenDeleteBodyValue,
  parsePushTokenRegisterBodyValue,
  parseBallotSummaryOptions,
  parseCandidateElectionFinancePath,
  parseCandidateId,
  parseDistrictIds,
  parseElectionId,
  parseInitializeUserDistrictsBodyValue,
  parseResearchAreaPreferencesBodyValue,
  RESEARCH_AREAS_PATH,
  SITE_SITEMAP_PATH,
} from "./apiValidation.js";
import { parseBearerAuthorizationValue } from "../auth/authBearer.js";
import {
  AUTH_SESSION_COOKIE_NAME,
  parseCookieHeaderValue,
  serializeAuthSessionCookie,
  serializeClearedAuthSessionCookie,
} from "../auth/authCookies.js";
import { toAddressResolutionDiagnostics, toPublicAddressResolution } from "./addressApiResponses.js";
import { randomUUID } from "node:crypto";
import { describeError } from "../observability/scrubText.js";
import {
  toEmptyResponse,
  toErrorResponse,
  toJsonResponse,
  toXmlResponse,
  type ApiErrorBody,
  type ApiResponse,
} from "./apiResponses.js";
import { CURRENT_TERMS_VERSION } from "../constants/legal.js";

type ApiResponseLocals = {
  clientIp?: string;
  corsHeaders?: Record<string, string>;
};

type ExpressBodyParserError = Error & {
  type?: string;
  status?: number;
  statusCode?: number;
};

const SITE_SITEMAP_CACHE_CONTROL = "public, max-age=3600";

function isKnownApiPath(pathname: string): boolean {
  return (
    pathname === ADDRESS_AUTOCOMPLETE_PATH ||
    pathname === ADDRESS_AUTOCOMPLETE_RETRIEVE_PATH ||
    pathname === ADDRESS_RESOLVE_PATH ||
    pathname === BALLOT_LOOKUP_PATH ||
    pathname === CONTENT_REPORTS_PATH ||
    pathname === AUTH_FORGOT_PASSWORD_PATH ||
    pathname === AUTH_LOGIN_PATH ||
    pathname === AUTH_LOGOUT_PATH ||
    pathname === AUTH_REGISTER_PATH ||
    pathname === AUTH_RESET_PASSWORD_PATH ||
    pathname === AUTH_RESEND_VERIFICATION_PATH ||
    pathname === AUTH_VERIFY_EMAIL_PATH ||
    pathname === AUTH_VERIFY_EMAIL_CHANGE_PATH ||
    pathname === AUTH_LOGOUT_ALL_PATH ||
    pathname === EMAIL_UNSUBSCRIBE_PATH ||
    pathname === ME_PATH ||
    pathname === ME_EMAIL_PATH ||
    pathname === ME_PASSWORD_PATH ||
    pathname === ME_ADDRESS_PATH ||
    pathname === ME_BALLOT_PATH ||
    pathname === ME_BALLOT_PREFERENCES_PATH ||
    pathname === ME_CANDIDATE_FOLLOWS_PATH ||
    pathname === ME_DISTRICTS_INITIALIZE_PATH ||
    pathname === ME_EMAIL_PREFERENCES_PATH ||
    pathname === ME_PUSH_TOKENS_PATH ||
    pathname === ME_RESEARCH_AREA_PREFERENCES_PATH ||
    pathname === RESEARCH_AREAS_PATH ||
    pathname === SITE_SITEMAP_PATH ||
    isCandidateDetailPath(pathname) ||
    // Listed explicitly even though the loose election-detail prefix also
    // matches it today: recognition of the finance route must not depend on
    // a sibling predicate staying loose.
    isCandidateElectionFinancePath(pathname) ||
    isElectionDetailPath(pathname)
  );
}

function getCorsHeaders(response: Response<unknown, ApiResponseLocals>): Record<string, string> {
  return response.locals.corsHeaders ?? {};
}

async function enforceAuthRateLimit(
  options: AddressApiServerOptions,
  request: Request,
  response: Response<unknown, ApiResponseLocals>,
  email: string
): Promise<boolean> {
  if (!options.authRateLimit) {
    return true;
  }

  const rateLimit = options.authRateLimit({
    clientIp: response.locals.clientIp ?? "unknown",
    email,
    method: request.method,
    pathname: request.path,
  });
  if (rateLimit.allowed) {
    return true;
  }

  const retryAfterSeconds = Math.max(1, Math.ceil(rateLimit.retryAfterSeconds ?? 1));
  sendApiResponse(
    response,
    toErrorResponse(429, "rate_limited", "Too many requests. Try again later.", {
      ...getCorsHeaders(response),
      "retry-after": String(retryAfterSeconds),
    })
  );
  return false;
}

async function resolveAuthenticatedUserId(
  options: AddressApiServerOptions,
  request: Request
): Promise<string | null> {
  if (!options.resolveAuthenticatedUserId) {
    return null;
  }
  const resolved = await Promise.resolve(options.resolveAuthenticatedUserId({ headers: request.headers }));
  const trimmed = resolved?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : null;
}

async function requireVerifiedAuthenticatedUser(
  options: AddressApiServerOptions,
  request: Request,
  response: Response<unknown, ApiResponseLocals>
): Promise<string | null> {
  const corsHeaders = getCorsHeaders(response);
  const userId = await resolveAuthenticatedUserId(options, request);
  if (!userId) {
    sendApiResponse(response, toErrorResponse(401, "unauthorized", "Authentication is required", corsHeaders));
    return null;
  }

  // Fail closed: when session auth is live (authService configured), a
  // missing verification lookup is a wiring bug, not permission to skip the
  // gate. Trusted-header-only deployments (no authService) predate email
  // verification and keep the legacy behavior.
  if (!options.lookupAuthenticatedUserEmailVerified) {
    if (options.authService) {
      sendApiResponse(
        response,
        toErrorResponse(500, "internal_error", "Email verification lookup is not configured", corsHeaders)
      );
      return null;
    }
    return userId;
  }

  const emailVerified = await Promise.resolve(options.lookupAuthenticatedUserEmailVerified(userId));
  if (!emailVerified) {
    sendApiResponse(
      response,
      toErrorResponse(403, "forbidden", "Email verification is required", corsHeaders)
    );
    return null;
  }

  return userId;
}

// Only requests that declare themselves as the mobile app receive the session
// id in the response body (for Bearer use); web responses stay cookie-only so
// the id is never readable by browser JS.
const MOBILE_CLIENT_HEADER_NAME = "x-voteapp-client";
const MOBILE_CLIENT_HEADER_VALUE = "mobile";

// The mobile-client header alone is not a trust boundary: browser JS can set
// it, and an XSS wrapping the user's own login request could then read the
// session id from the response — exactly what httpOnly exists to prevent.
// Browsers, however, always attach Origin to POSTs and Sec-Fetch-* to every
// request, and scripts cannot remove these forbidden headers. Native HTTP
// stacks (React Native fetch, OkHttp, NSURLSession) send neither, so any
// browser provenance disqualifies the request from the mobile transport.
function hasBrowserProvenance(request: Request): boolean {
  if (request.headers.origin !== undefined) {
    return true;
  }
  return Object.keys(request.headers).some((name) => name.startsWith("sec-fetch-"));
}

function isMobileClientRequest(request: Request): boolean {
  const rawValue = request.headers[MOBILE_CLIENT_HEADER_NAME];
  const value = Array.isArray(rawValue) ? rawValue[0] : rawValue;
  return value?.trim().toLowerCase() === MOBILE_CLIENT_HEADER_VALUE && !hasBrowserProvenance(request);
}

function getAuthSessionId(request: Request): string | null {
  // Bearer (explicit, attached per-request by the mobile app) wins over the
  // cookie (ambient, replayed by cookie jars): a stale jar cookie must not
  // shadow the session the client actually presented. Web clients never send
  // a Bearer header, so their behavior is unchanged.
  return (
    parseBearerAuthorizationValue(request.headers.authorization) ??
    parseCookieHeaderValue(request.headers.cookie, AUTH_SESSION_COOKIE_NAME)
  );
}

function sendApiResponse(response: Response, apiResponse: ApiResponse): void {
  response.status(apiResponse.statusCode).set(apiResponse.headers);
  if (apiResponse.body === undefined) {
    response.end();
    return;
  }
  if (
    typeof apiResponse.body === "string" &&
    !String(apiResponse.headers["content-type"] ?? "").includes("application/json")
  ) {
    response.send(apiResponse.body);
    return;
  }
  response.json(apiResponse.body);
}

function createClientIpMiddleware(options: AddressApiServerOptions) {
  return (request: Request, response: Response<unknown, ApiResponseLocals>, next: NextFunction): void => {
    const remoteAddress = request.socket.remoteAddress ?? "unknown";
    response.locals.clientIp =
      options.resolveClientIp?.({ headers: request.headers, remoteAddress })?.trim() || remoteAddress;
    next();
  };
}

function createCorsAndPreflightMiddleware(options: AddressApiServerOptions) {
  return (request: Request, response: Response<unknown, ApiResponseLocals>, next: NextFunction): void => {
    const cors = resolveCorsHeaders(request.headers, options.allowedOrigins);
    response.locals.corsHeaders = cors.headers;

    if (!isKnownApiPath(request.path)) {
      sendApiResponse(response, toErrorResponse(404, "not_found", "Not found", cors.headers));
      return;
    }

    if (!cors.ok) {
      sendApiResponse(response, toErrorResponse(403, "invalid_request", "Origin is not allowed", cors.headers));
      return;
    }

    if (request.method === "OPTIONS") {
      sendApiResponse(response, toEmptyResponse(204, cors.headers));
      return;
    }

    next();
  };
}

function createRateLimitMiddleware(options: AddressApiServerOptions) {
  return (request: Request, response: Response<unknown, ApiResponseLocals>, next: NextFunction): void => {
    if (!options.rateLimit) {
      next();
      return;
    }

    const rateLimit = options.rateLimit({
      clientIp: response.locals.clientIp ?? "unknown",
      method: request.method,
      pathname: request.path,
    });
    if (rateLimit.allowed) {
      next();
      return;
    }

    const retryAfterSeconds = Math.max(1, Math.ceil(rateLimit.retryAfterSeconds ?? 1));
    sendApiResponse(
      response,
      toErrorResponse(429, "rate_limited", "Too many requests. Try again later.", {
        ...getCorsHeaders(response),
        "retry-after": String(retryAfterSeconds),
      })
    );
  };
}

function createContentReportRateLimitMiddleware(options: AddressApiServerOptions) {
  return (request: Request, response: Response<unknown, ApiResponseLocals>, next: NextFunction): void => {
    if (!options.contentReportRateLimit || request.method !== "POST" || request.path !== CONTENT_REPORTS_PATH) {
      next();
      return;
    }

    const rateLimit = options.contentReportRateLimit({
      clientIp: response.locals.clientIp ?? "unknown",
      method: request.method,
      pathname: request.path,
    });
    if (rateLimit.allowed) {
      next();
      return;
    }

    const retryAfterSeconds = Math.max(1, Math.ceil(rateLimit.retryAfterSeconds ?? 1));
    sendApiResponse(
      response,
      toErrorResponse(429, "rate_limited", "Too many requests. Try again later.", {
        ...getCorsHeaders(response),
        "retry-after": String(retryAfterSeconds),
      })
    );
  };
}

function createJsonBodyParser() {
  const parseJson = express.json({
    limit: MAX_ADDRESS_REQUEST_BODY_BYTES,
    strict: true,
    type: ["application/json", "application/*+json"],
  });

  function hasJsonContentType(request: Request): boolean {
    const rawContentType = request.headers["content-type"];
    const contentType = Array.isArray(rawContentType) ? rawContentType[0] : rawContentType;
    const mediaType = contentType?.split(";")[0]?.trim().toLowerCase();
    return mediaType === "application/json" || (mediaType?.startsWith("application/") === true && mediaType.endsWith("+json"));
  }

  return (request: Request, response: Response, next: NextFunction): void => {
    const shouldParseJson =
      (request.method === "POST" &&
        (request.path === ADDRESS_AUTOCOMPLETE_PATH ||
          request.path === ADDRESS_AUTOCOMPLETE_RETRIEVE_PATH ||
          request.path === ADDRESS_RESOLVE_PATH ||
          request.path === CONTENT_REPORTS_PATH ||
          request.path === ME_DISTRICTS_INITIALIZE_PATH ||
          request.path === AUTH_FORGOT_PASSWORD_PATH ||
          request.path === AUTH_LOGIN_PATH ||
          // Logout has no meaningful body, but requiring the JSON content
          // type blocks plain cross-site form POSTs from logging users out
          // in SameSite=None deployments (forms cannot send application/json
          // without a CORS preflight).
          request.path === AUTH_LOGOUT_PATH ||
          request.path === AUTH_REGISTER_PATH ||
          request.path === AUTH_RESET_PASSWORD_PATH ||
          request.path === AUTH_RESEND_VERIFICATION_PATH ||
          request.path === AUTH_VERIFY_EMAIL_PATH ||
          request.path === AUTH_VERIFY_EMAIL_CHANGE_PATH ||
          // Like logout: requiring JSON blocks plain cross-site form POSTs.
          request.path === AUTH_LOGOUT_ALL_PATH ||
          request.path === ME_EMAIL_PATH ||
          request.path === ME_PASSWORD_PATH ||
          request.path === ME_PUSH_TOKENS_PATH)) ||
      (request.method === "DELETE" && (request.path === ME_PATH || request.path === ME_PUSH_TOKENS_PATH)) ||
      (request.method === "PUT" &&
        (request.path === ME_PATH ||
          request.path === ME_ADDRESS_PATH ||
          request.path === ME_BALLOT_PREFERENCES_PATH ||
          request.path === ME_CANDIDATE_FOLLOWS_PATH ||
          request.path === ME_EMAIL_PREFERENCES_PATH ||
          request.path === ME_RESEARCH_AREA_PREFERENCES_PATH));
    if (!shouldParseJson) {
      next();
      return;
    }
    if (!hasJsonContentType(request)) {
      sendApiResponse(
        response,
        toErrorResponse(
          415,
          "unsupported_media_type",
          "Content-Type must be application/json",
          getCorsHeaders(response)
        )
      );
      return;
    }
    parseJson(request, response, next);
  };
}

async function dispatchApiRequest(
  request: Request,
  response: Response<unknown, ApiResponseLocals>,
  options: AddressApiServerOptions
): Promise<void> {
  const url = new URL(request.url, "http://localhost");
  const corsHeaders = getCorsHeaders(response);

  if (url.pathname === SITE_SITEMAP_PATH) {
    if (request.method !== "GET") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use GET /sitemap.xml", {
          ...corsHeaders,
          allow: "GET",
        })
      );
      return;
    }
    if (!options.getSitemapXml) {
      sendApiResponse(response, toErrorResponse(404, "not_found", "Sitemap is not configured", corsHeaders));
      return;
    }

    const sitemapXml = await options.getSitemapXml();
    sendApiResponse(
      response,
      toXmlResponse(200, sitemapXml, { ...corsHeaders, "cache-control": SITE_SITEMAP_CACHE_CONTROL })
    );
    return;
  }

  if (url.pathname === RESEARCH_AREAS_PATH) {
    if (request.method !== "GET") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use GET /api/research-areas", {
          ...corsHeaders,
          allow: "GET",
        })
      );
      return;
    }
    if (!options.listResearchAreas) {
      sendApiResponse(
        response,
        toErrorResponse(500, "internal_error", "Research area catalog lookup is not configured", corsHeaders)
      );
      return;
    }

    const result = await options.listResearchAreas();
    sendApiResponse(response, toJsonResponse(200, result, corsHeaders));
    return;
  }

  if (url.pathname === CONTENT_REPORTS_PATH) {
    if (request.method !== "POST") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use POST /api/content-reports", {
          ...corsHeaders,
          allow: "POST",
        })
      );
      return;
    }
    if (!options.createContentReport) {
      sendApiResponse(response, toErrorResponse(500, "internal_error", "Content report storage is not configured", corsHeaders));
      return;
    }

    const payload = parseContentReportBodyValue(request.body);
    const userId = await resolveAuthenticatedUserId(options, request);
    const report = await options.createContentReport({ ...payload, userId });
    sendApiResponse(response, toJsonResponse(201, { report }, corsHeaders));
    return;
  }

  if (url.pathname === AUTH_REGISTER_PATH) {
    if (request.method !== "POST") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use POST /api/auth/register", {
          ...corsHeaders,
          allow: "POST",
        })
      );
      return;
    }
    if (!options.authService) {
      sendApiResponse(response, toErrorResponse(500, "internal_error", "Authentication is not configured", corsHeaders));
      return;
    }

    const payload = parseAuthRegisterBodyValue(request.body);
    // Reject stale terms versions outright: acceptance of superseded terms
    // must never be recorded (a stale frontend re-fetches and re-prompts).
    if (payload.accepted_terms_version !== CURRENT_TERMS_VERSION) {
      sendApiResponse(
        response,
        toErrorResponse(
          400,
          "invalid_request",
          `accepted_terms_version must be the current terms version (${CURRENT_TERMS_VERSION})`,
          corsHeaders
        )
      );
      return;
    }
    if (!(await enforceAuthRateLimit(options, request, response, payload.email))) {
      return;
    }
    await options.authService.register({
      email: payload.email,
      password: payload.password,
      firstName: payload.first_name,
      acceptedTermsVersion: payload.accepted_terms_version,
    });
    sendApiResponse(response, toJsonResponse(200, { status: "ok" }, corsHeaders));
    return;
  }

  if (url.pathname === AUTH_VERIFY_EMAIL_PATH) {
    if (request.method !== "POST") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use POST /api/auth/verify-email", {
          ...corsHeaders,
          allow: "POST",
        })
      );
      return;
    }
    if (!options.authService) {
      sendApiResponse(response, toErrorResponse(500, "internal_error", "Authentication is not configured", corsHeaders));
      return;
    }

    const payload = parseAuthVerifyEmailBodyValue(request.body);
    await options.authService.verifyEmail({
      token: payload.token,
    });
    sendApiResponse(response, toJsonResponse(200, { status: "ok" }, corsHeaders));
    return;
  }

  if (url.pathname === AUTH_VERIFY_EMAIL_CHANGE_PATH) {
    if (request.method !== "POST") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use POST /api/auth/verify-email-change", {
          ...corsHeaders,
          allow: "POST",
        })
      );
      return;
    }
    if (!options.authService) {
      sendApiResponse(response, toErrorResponse(500, "internal_error", "Authentication is not configured", corsHeaders));
      return;
    }

    const payload = parseAuthVerifyEmailChangeBodyValue(request.body);
    await options.authService.verifyEmailChange({
      token: payload.token,
    });
    sendApiResponse(response, toJsonResponse(200, { status: "ok" }, corsHeaders));
    return;
  }

  if (url.pathname === AUTH_LOGOUT_ALL_PATH) {
    if (request.method !== "POST") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use POST /api/auth/logout-all", {
          ...corsHeaders,
          allow: "POST",
        })
      );
      return;
    }
    if (!options.resolveAuthenticatedUserId || !options.authService) {
      sendApiResponse(
        response,
        !options.resolveAuthenticatedUserId
          ? toErrorResponse(401, "unauthorized", "Authentication is required", corsHeaders)
          : toErrorResponse(500, "internal_error", "Authentication is not configured", corsHeaders)
      );
      return;
    }

    // Needs a live session to know whose sessions to destroy; a caller with
    // a dead cookie is already logged out everywhere it matters.
    const userId = await resolveAuthenticatedUserId(options, request);
    if (!userId) {
      sendApiResponse(response, toErrorResponse(401, "unauthorized", "Authentication is required", corsHeaders));
      return;
    }

    await options.authService.logoutAll({ userId });
    sendApiResponse(response, {
      ...toJsonResponse(200, { status: "ok" }, corsHeaders),
      headers: {
        ...corsHeaders,
        "content-type": "application/json; charset=utf-8",
        "set-cookie": serializeClearedAuthSessionCookie({
          ...options.authSessionCookieOptions,
        }),
      },
      body: { status: "ok" },
      statusCode: 200,
    });
    return;
  }

  if (url.pathname === AUTH_RESEND_VERIFICATION_PATH) {
    if (request.method !== "POST") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use POST /api/auth/resend-verification", {
          ...corsHeaders,
          allow: "POST",
        })
      );
      return;
    }
    if (!options.authService) {
      sendApiResponse(response, toErrorResponse(500, "internal_error", "Authentication is not configured", corsHeaders));
      return;
    }

    const payload = parseAuthResendVerificationBodyValue(request.body);
    if (!(await enforceAuthRateLimit(options, request, response, payload.email))) {
      return;
    }
    await options.authService.resendVerification({
      email: payload.email,
    });
    sendApiResponse(response, toJsonResponse(200, { status: "ok" }, corsHeaders));
    return;
  }

  if (url.pathname === AUTH_LOGIN_PATH) {
    if (request.method !== "POST") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use POST /api/auth/login", {
          ...corsHeaders,
          allow: "POST",
        })
      );
      return;
    }
    if (!options.authService) {
      sendApiResponse(response, toErrorResponse(500, "internal_error", "Authentication is not configured", corsHeaders));
      return;
    }

    const payload = parseAuthLoginBodyValue(request.body);
    if (!(await enforceAuthRateLimit(options, request, response, payload.email))) {
      return;
    }
    const currentSessionId = getAuthSessionId(request);
    const result = await options.authService.login({
      email: payload.email,
      password: payload.password,
      currentSessionId,
    });
    // Mobile gets the id in the body and no Set-Cookie: native cookie jars
    // store cookies automatically, and a jar copy of the session would later
    // be replayed alongside the Bearer header and diverge from it.
    const mobileClient = isMobileClientRequest(request);
    sendApiResponse(response, {
      ...toJsonResponse(200, { status: "ok" }, corsHeaders),
      headers: {
        ...corsHeaders,
        "content-type": "application/json; charset=utf-8",
        ...(mobileClient
          ? {}
          : {
              "set-cookie": serializeAuthSessionCookie(result.sessionId, {
                ...options.authSessionCookieOptions,
              }),
            }),
      },
      body: mobileClient ? { status: "ok", session_id: result.sessionId } : { status: "ok" },
      statusCode: 200,
    });
    return;
  }

  if (url.pathname === AUTH_LOGOUT_PATH) {
    if (request.method !== "POST") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use POST /api/auth/logout", {
          ...corsHeaders,
          allow: "POST",
        })
      );
      return;
    }
    if (!options.authService) {
      sendApiResponse(response, toErrorResponse(500, "internal_error", "Authentication is not configured", corsHeaders));
      return;
    }

    const currentSessionId = getAuthSessionId(request);
    await options.authService.logout({
      currentSessionId,
    });
    sendApiResponse(response, {
      ...toJsonResponse(200, { status: "ok" }, corsHeaders),
      headers: {
        ...corsHeaders,
        "content-type": "application/json; charset=utf-8",
        "set-cookie": serializeClearedAuthSessionCookie({
          ...options.authSessionCookieOptions,
        }),
      },
      body: { status: "ok" },
      statusCode: 200,
    });
    return;
  }

  if (url.pathname === AUTH_FORGOT_PASSWORD_PATH) {
    if (request.method !== "POST") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use POST /api/auth/forgot-password", {
          ...corsHeaders,
          allow: "POST",
        })
      );
      return;
    }
    if (!options.authService) {
      sendApiResponse(response, toErrorResponse(500, "internal_error", "Authentication is not configured", corsHeaders));
      return;
    }

    const payload = parseAuthForgotPasswordBodyValue(request.body);
    if (!(await enforceAuthRateLimit(options, request, response, payload.email))) {
      return;
    }
    await options.authService.forgotPassword({
      email: payload.email,
    });
    sendApiResponse(response, toJsonResponse(200, { status: "ok" }, corsHeaders));
    return;
  }

  if (url.pathname === AUTH_RESET_PASSWORD_PATH) {
    if (request.method !== "POST") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use POST /api/auth/reset-password", {
          ...corsHeaders,
          allow: "POST",
        })
      );
      return;
    }
    if (!options.authService) {
      sendApiResponse(response, toErrorResponse(500, "internal_error", "Authentication is not configured", corsHeaders));
      return;
    }

    const payload = parseAuthResetPasswordBodyValue(request.body);
    await options.authService.resetPassword({
      token: payload.token,
      password: payload.password,
    });
    sendApiResponse(response, toJsonResponse(200, { status: "ok" }, corsHeaders));
    return;
  }

  if (url.pathname === BALLOT_LOOKUP_PATH) {
    if (request.method !== "GET") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use GET /api/ballot?district_ids=...", {
          ...corsHeaders,
          allow: "GET",
        })
      );
      return;
    }
    if (!options.lookupBallotSummaries) {
      sendApiResponse(response, toErrorResponse(500, "internal_error", "Ballot lookup is not configured", corsHeaders));
      return;
    }

    const districtIds = parseDistrictIds(url);
    const summaryOptions = parseBallotSummaryOptions(url);
    const result = await options.lookupBallotSummaries(districtIds, summaryOptions);
    sendApiResponse(response, toJsonResponse(200, result, corsHeaders));
    return;
  }

  if (url.pathname === ME_BALLOT_PATH) {
    if (request.method !== "GET") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use GET /api/me/ballot", {
          ...corsHeaders,
          allow: "GET",
        })
      );
      return;
    }
    if (!options.resolveAuthenticatedUserId) {
      sendApiResponse(response, toErrorResponse(401, "unauthorized", "Authentication is required", corsHeaders));
      return;
    }
    if (!options.lookupAuthenticatedBallotSummaries) {
      sendApiResponse(
        response,
        toErrorResponse(500, "internal_error", "Authenticated ballot lookup is not configured", corsHeaders)
      );
      return;
    }

    // Same verified-email gate as every other /api/me route: personalized
    // ballot data must not be readable by unverified accounts.
    const userId = await requireVerifiedAuthenticatedUser(options, request, response);
    if (!userId) {
      return;
    }

    // [ballot-personalized-ordering]
    // Explicit query params win; anything omitted falls back to the user's
    // saved ballot preferences (and the reader defaults below those).
    const summaryOptions = parseBallotSummaryOptions(url);
    if (options.getAuthenticatedBallotPreferences && (summaryOptions.sort === undefined || summaryOptions.followedFirst === undefined)) {
      const saved = await options.getAuthenticatedBallotPreferences(userId);
      summaryOptions.sort = summaryOptions.sort ?? saved.sort;
      summaryOptions.followedFirst = summaryOptions.followedFirst ?? saved.followed_first;
    }
    const result = await options.lookupAuthenticatedBallotSummaries(userId, summaryOptions);
    sendApiResponse(response, toJsonResponse(200, result, corsHeaders));
    return;
  }

  if (url.pathname === ME_PATH) {
    if (request.method !== "GET" && request.method !== "PUT" && request.method !== "DELETE") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use GET, PUT, or DELETE /api/me", {
          ...corsHeaders,
          allow: "GET, PUT, DELETE",
        })
      );
      return;
    }
    if (!options.resolveAuthenticatedUserId) {
      sendApiResponse(response, toErrorResponse(401, "unauthorized", "Authentication is required", corsHeaders));
      return;
    }

    // Deliberately not requireVerifiedAuthenticatedUser: the frontend calls
    // this to find out whether the user is verified, so the unverified state
    // must be readable, not a 403. Same logic for PUT (fix your name) and
    // DELETE (leave) — neither should demand a verified inbox.
    const userId = await resolveAuthenticatedUserId(options, request);
    if (!userId) {
      sendApiResponse(response, toErrorResponse(401, "unauthorized", "Authentication is required", corsHeaders));
      return;
    }

    if (request.method === "GET") {
      if (!options.getAuthenticatedUser) {
        sendApiResponse(
          response,
          toErrorResponse(500, "internal_error", "Authenticated user lookup is not configured", corsHeaders)
        );
        return;
      }

      const user = await options.getAuthenticatedUser(userId);
      sendApiResponse(response, toJsonResponse(200, { user }, corsHeaders));
      return;
    }

    if (request.method === "PUT") {
      if (!options.updateAuthenticatedUserFirstName) {
        sendApiResponse(
          response,
          toErrorResponse(500, "internal_error", "Authenticated user update is not configured", corsHeaders)
        );
        return;
      }

      const payload = parseMeUpdateBodyValue(request.body);
      const user = await options.updateAuthenticatedUserFirstName(userId, payload.first_name);
      sendApiResponse(response, toJsonResponse(200, { user }, corsHeaders));
      return;
    }

    if (!options.authService) {
      sendApiResponse(response, toErrorResponse(500, "internal_error", "Authentication is not configured", corsHeaders));
      return;
    }

    // Password-verifying endpoint: throttle per account (keyed by userId in
    // the shared auth buckets) so a hijacked session cannot brute-force the
    // re-entered password behind the per-IP cap alone.
    if (!(await enforceAuthRateLimit(options, request, response, userId))) {
      return;
    }

    const payload = parseMeDeleteBodyValue(request.body);
    await options.authService.deleteAccount({
      userId,
      password: payload.password,
    });
    sendApiResponse(response, {
      ...toJsonResponse(200, { status: "ok" }, corsHeaders),
      headers: {
        ...corsHeaders,
        "content-type": "application/json; charset=utf-8",
        "set-cookie": serializeClearedAuthSessionCookie({
          ...options.authSessionCookieOptions,
        }),
      },
      body: { status: "ok" },
      statusCode: 200,
    });
    return;
  }

  if (url.pathname === ME_PASSWORD_PATH) {
    if (request.method !== "POST") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use POST /api/me/password", {
          ...corsHeaders,
          allow: "POST",
        })
      );
      return;
    }
    if (!options.resolveAuthenticatedUserId || !options.authService) {
      sendApiResponse(
        response,
        !options.resolveAuthenticatedUserId
          ? toErrorResponse(401, "unauthorized", "Authentication is required", corsHeaders)
          : toErrorResponse(500, "internal_error", "Authentication is not configured", corsHeaders)
      );
      return;
    }

    const userId = await resolveAuthenticatedUserId(options, request);
    if (!userId) {
      sendApiResponse(response, toErrorResponse(401, "unauthorized", "Authentication is required", corsHeaders));
      return;
    }

    // Password-verifying endpoint: per-account throttle, see DELETE /api/me.
    if (!(await enforceAuthRateLimit(options, request, response, userId))) {
      return;
    }

    const payload = parseMePasswordBodyValue(request.body);
    // changePassword rotates every session; hand the fresh one back so the
    // caller stays logged in while any stolen session dies. Mobile callers
    // get it in the body (their Bearer id was just revoked) and no
    // Set-Cookie, so native cookie jars never hold a session copy that could
    // later diverge from the Bearer header.
    const result = await options.authService.changePassword({
      userId,
      currentPassword: payload.current_password,
      newPassword: payload.new_password,
    });
    const mobileClient = isMobileClientRequest(request);
    sendApiResponse(response, {
      ...toJsonResponse(200, { status: "ok" }, corsHeaders),
      headers: {
        ...corsHeaders,
        "content-type": "application/json; charset=utf-8",
        ...(mobileClient
          ? {}
          : {
              "set-cookie": serializeAuthSessionCookie(result.sessionId, {
                ...options.authSessionCookieOptions,
              }),
            }),
      },
      body: mobileClient ? { status: "ok", session_id: result.sessionId } : { status: "ok" },
      statusCode: 200,
    });
    return;
  }

  if (url.pathname === ME_EMAIL_PATH) {
    if (request.method !== "POST") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use POST /api/me/email", {
          ...corsHeaders,
          allow: "POST",
        })
      );
      return;
    }
    if (!options.resolveAuthenticatedUserId || !options.authService) {
      sendApiResponse(
        response,
        !options.resolveAuthenticatedUserId
          ? toErrorResponse(401, "unauthorized", "Authentication is required", corsHeaders)
          : toErrorResponse(500, "internal_error", "Authentication is not configured", corsHeaders)
      );
      return;
    }

    const userId = await resolveAuthenticatedUserId(options, request);
    if (!userId) {
      sendApiResponse(response, toErrorResponse(401, "unauthorized", "Authentication is required", corsHeaders));
      return;
    }

    // Password-verifying endpoint: per-account throttle, see DELETE /api/me.
    if (!(await enforceAuthRateLimit(options, request, response, userId))) {
      return;
    }

    const payload = parseMeEmailBodyValue(request.body);
    await options.authService.requestEmailChange({
      userId,
      newEmail: payload.new_email,
      password: payload.password,
    });
    sendApiResponse(response, toJsonResponse(200, { status: "ok" }, corsHeaders));
    return;
  }

  if (url.pathname === ME_ADDRESS_PATH) {
    if (request.method !== "PUT") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use PUT /api/me/address", {
          ...corsHeaders,
          allow: "PUT",
        })
      );
      return;
    }
    if (!options.resolveAuthenticatedUserId) {
      sendApiResponse(response, toErrorResponse(401, "unauthorized", "Authentication is required", corsHeaders));
      return;
    }
    if (!options.updateAuthenticatedAddressDistricts) {
      sendApiResponse(
        response,
        toErrorResponse(500, "internal_error", "Authenticated address update is not configured", corsHeaders)
      );
      return;
    }

    const userId = await requireVerifiedAuthenticatedUser(options, request, response);
    if (!userId) {
      return;
    }

    const payload = parseAuthenticatedAddressBodyValue(request.body);
    const result = await options.updateAuthenticatedAddressDistricts(userId, payload.address);
    sendApiResponse(response, toJsonResponse(200, result, corsHeaders));
    return;
  }

  if (url.pathname === ME_CANDIDATE_FOLLOWS_PATH) {
    if (request.method !== "GET" && request.method !== "PUT") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use GET or PUT /api/me/candidate-follows", {
          ...corsHeaders,
          allow: "GET, PUT",
        })
      );
      return;
    }
    if (!options.resolveAuthenticatedUserId) {
      sendApiResponse(response, toErrorResponse(401, "unauthorized", "Authentication is required", corsHeaders));
      return;
    }

    const userId = await requireVerifiedAuthenticatedUser(options, request, response);
    if (!userId) {
      return;
    }

    if (request.method === "GET") {
      if (!options.listAuthenticatedCandidateFollows) {
        sendApiResponse(
          response,
          toErrorResponse(500, "internal_error", "Authenticated candidate follow lookup is not configured", corsHeaders)
        );
        return;
      }

      const result = await options.listAuthenticatedCandidateFollows(userId);
      sendApiResponse(response, toJsonResponse(200, result, corsHeaders));
      return;
    }

    if (!options.setAuthenticatedCandidateFollow) {
      sendApiResponse(
        response,
        toErrorResponse(500, "internal_error", "Authenticated candidate follow storage is not configured", corsHeaders)
      );
      return;
    }

    const payload = parseCandidateFollowBodyValue(request.body);
    const result = await options.setAuthenticatedCandidateFollow(userId, payload);
    sendApiResponse(response, toJsonResponse(200, result, corsHeaders));
    return;
  }

  // [ballot-personalized-ordering]
  if (url.pathname === ME_BALLOT_PREFERENCES_PATH) {
    if (request.method !== "GET" && request.method !== "PUT") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use GET or PUT /api/me/ballot-preferences", {
          ...corsHeaders,
          allow: "GET, PUT",
        })
      );
      return;
    }
    if (!options.resolveAuthenticatedUserId) {
      sendApiResponse(response, toErrorResponse(401, "unauthorized", "Authentication is required", corsHeaders));
      return;
    }

    const userId = await requireVerifiedAuthenticatedUser(options, request, response);
    if (!userId) {
      return;
    }

    if (request.method === "GET") {
      if (!options.getAuthenticatedBallotPreferences) {
        sendApiResponse(
          response,
          toErrorResponse(500, "internal_error", "Authenticated ballot preferences lookup is not configured", corsHeaders)
        );
        return;
      }

      const result = await options.getAuthenticatedBallotPreferences(userId);
      sendApiResponse(response, toJsonResponse(200, result, corsHeaders));
      return;
    }

    if (!options.setAuthenticatedBallotPreferences) {
      sendApiResponse(
        response,
        toErrorResponse(500, "internal_error", "Authenticated ballot preference storage is not configured", corsHeaders)
      );
      return;
    }

    const preferences = parseBallotPreferencesBodyValue(request.body);
    const result = await options.setAuthenticatedBallotPreferences(userId, preferences);
    sendApiResponse(response, toJsonResponse(200, result, corsHeaders));
    return;
  }

  if (url.pathname === ME_EMAIL_PREFERENCES_PATH) {
    if (request.method !== "GET" && request.method !== "PUT") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use GET or PUT /api/me/email-preferences", {
          ...corsHeaders,
          allow: "GET, PUT",
        })
      );
      return;
    }
    if (!options.resolveAuthenticatedUserId) {
      sendApiResponse(response, toErrorResponse(401, "unauthorized", "Authentication is required", corsHeaders));
      return;
    }

    const userId = await requireVerifiedAuthenticatedUser(options, request, response);
    if (!userId) {
      return;
    }

    if (request.method === "GET") {
      if (!options.getAuthenticatedEmailPreferences) {
        sendApiResponse(
          response,
          toErrorResponse(500, "internal_error", "Authenticated email preferences lookup is not configured", corsHeaders)
        );
        return;
      }

      const result = await options.getAuthenticatedEmailPreferences(userId);
      sendApiResponse(response, toJsonResponse(200, result, corsHeaders));
      return;
    }

    if (!options.setAuthenticatedEmailPreferences) {
      sendApiResponse(
        response,
        toErrorResponse(500, "internal_error", "Authenticated email preference storage is not configured", corsHeaders)
      );
      return;
    }

    const preferences = parseEmailPreferencesBodyValue(request.body);
    const result = await options.setAuthenticatedEmailPreferences(userId, preferences);
    sendApiResponse(response, toJsonResponse(200, result, corsHeaders));
    return;
  }

  if (url.pathname === ME_PUSH_TOKENS_PATH) {
    if (request.method !== "POST" && request.method !== "DELETE") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use POST or DELETE /api/me/push-tokens", {
          ...corsHeaders,
          allow: "POST, DELETE",
        })
      );
      return;
    }
    if (!options.resolveAuthenticatedUserId) {
      sendApiResponse(response, toErrorResponse(401, "unauthorized", "Authentication is required", corsHeaders));
      return;
    }

    // Same verified-email gate as the other notification preferences: the
    // senders only ever deliver to verified accounts, so an unverified
    // registration would be a dead row.
    const userId = await requireVerifiedAuthenticatedUser(options, request, response);
    if (!userId) {
      return;
    }

    if (request.method === "POST") {
      if (!options.registerAuthenticatedPushToken) {
        sendApiResponse(
          response,
          toErrorResponse(500, "internal_error", "Push token registration is not configured", corsHeaders)
        );
        return;
      }

      const input = parsePushTokenRegisterBodyValue(request.body);
      await options.registerAuthenticatedPushToken(userId, input);
      sendApiResponse(response, toJsonResponse(200, { status: "registered" }, corsHeaders));
      return;
    }

    if (!options.revokeAuthenticatedPushToken) {
      sendApiResponse(
        response,
        toErrorResponse(500, "internal_error", "Push token revocation is not configured", corsHeaders)
      );
      return;
    }

    const { expoPushToken } = parsePushTokenDeleteBodyValue(request.body);
    await options.revokeAuthenticatedPushToken(userId, expoPushToken);
    sendApiResponse(response, toJsonResponse(200, { status: "revoked" }, corsHeaders));
    return;
  }

  if (url.pathname === EMAIL_UNSUBSCRIBE_PATH) {
    // GET serves the human click from the email footer; POST serves RFC 8058
    // one-click unsubscribes from mailbox providers. Both are token-authorized
    // and session-free, and answer with a tiny standalone HTML page.
    if (request.method !== "GET" && request.method !== "POST") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use GET or POST /api/email/unsubscribe", {
          ...corsHeaders,
          allow: "GET, POST",
        })
      );
      return;
    }
    if (!options.unsubscribeFromEmailNotifications) {
      sendApiResponse(
        response,
        toErrorResponse(500, "internal_error", "Email unsubscribe is not configured", corsHeaders)
      );
      return;
    }

    const token = url.searchParams.get("token")?.trim() ?? "";
    // Unknown pref values 400 rather than falling back: a mangled link must
    // not silently flip a different opt-in than the email advertised.
    const preference = parseEmailUnsubscribePreference(url.searchParams.get("pref"));
    // GET must not mutate: mail security gateways, previewers, and prefetchers
    // GET every link in an email body, which would silently unsubscribe users.
    // GET renders a confirmation form that POSTs back here; POST (the form and
    // RFC 8058 one-click) performs the unsubscribe.
    const mode = request.method === "GET" ? ("confirm" as const) : ("execute" as const);
    const outcome =
      token && preference
        ? await options.unsubscribeFromEmailNotifications(token, mode, preference)
        : "invalid_token";
    const invalidPage =
      "<!doctype html><html lang=\"en\"><head><meta charset=\"UTF-8\"><title>Invalid link</title></head><body><p>This unsubscribe link is invalid or incomplete.</p><p>You can manage email settings in your account settings.</p></body></html>";
    if (outcome !== "ok" || !preference) {
      response
        .status(400)
        .set({ ...corsHeaders, "content-type": "text/html; charset=utf-8" })
        .send(invalidPage);
      return;
    }
    const preferenceLabel =
      preference === "new_election_alerts"
        ? "new election alert emails"
        : preference === "election_reminders"
          ? "election reminder emails"
          : preference === "issue_updates"
            ? "emails about your saved issues"
            : "candidate update digest emails";
    const formAction = `${EMAIL_UNSUBSCRIBE_PATH}?token=${encodeURIComponent(token)}&pref=${encodeURIComponent(preference)}`;
    const confirmPage =
      "<!doctype html><html lang=\"en\"><head><meta charset=\"UTF-8\"><title>Unsubscribe</title></head><body>" +
      `<p>Unsubscribe from ${preferenceLabel}?</p>` +
      `<form method="post" action="${formAction}">` +
      "<button type=\"submit\">Unsubscribe</button></form>" +
      "</body></html>";
    const donePage =
      `<!doctype html><html lang="en"><head><meta charset="UTF-8"><title>Unsubscribed</title></head><body><p>You have been unsubscribed from ${preferenceLabel}.</p><p>You can turn them back on any time in your account settings.</p></body></html>`;
    response
      .status(200)
      .set({ ...corsHeaders, "content-type": "text/html; charset=utf-8" })
      .send(mode === "confirm" ? confirmPage : donePage);
    return;
  }

  if (url.pathname === ME_RESEARCH_AREA_PREFERENCES_PATH) {
    if (request.method !== "GET" && request.method !== "PUT") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use GET or PUT /api/me/research-area-preferences", {
          ...corsHeaders,
          allow: "GET, PUT",
        })
      );
      return;
    }
    if (!options.resolveAuthenticatedUserId) {
      sendApiResponse(response, toErrorResponse(401, "unauthorized", "Authentication is required", corsHeaders));
      return;
    }

    const userId = await requireVerifiedAuthenticatedUser(options, request, response);
    if (!userId) {
      return;
    }

    if (request.method === "GET") {
      if (!options.listAuthenticatedResearchAreaPreferences) {
        sendApiResponse(
          response,
          toErrorResponse(
            500,
            "internal_error",
            "Authenticated research area preferences lookup is not configured",
            corsHeaders
          )
        );
        return;
      }

      const result = await options.listAuthenticatedResearchAreaPreferences(userId);
      sendApiResponse(response, toJsonResponse(200, result, corsHeaders));
      return;
    }

    if (!options.replaceAuthenticatedResearchAreaPreferences) {
      sendApiResponse(
        response,
        toErrorResponse(
          500,
          "internal_error",
          "Authenticated research area preference storage is not configured",
          corsHeaders
        )
      );
      return;
    }

    const payload = parseResearchAreaPreferencesBodyValue(request.body);
    const result = await options.replaceAuthenticatedResearchAreaPreferences(userId, payload.preferences);
    sendApiResponse(response, toJsonResponse(200, result, corsHeaders));
    return;
  }

  if (isCandidateDetailPath(url.pathname)) {
    if (request.method !== "GET") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use GET /api/candidates/:candidate_id", {
          ...corsHeaders,
          allow: "GET",
        })
      );
      return;
    }
    if (!options.lookupCandidateDetail) {
      sendApiResponse(
        response,
        toErrorResponse(500, "internal_error", "Candidate detail lookup is not configured", corsHeaders)
      );
      return;
    }

    const candidateId = parseCandidateId(url);
    const userId = await resolveAuthenticatedUserId(options, request);
    const result = await options.lookupCandidateDetail(candidateId, userId);
    if (!result) {
      sendApiResponse(response, toErrorResponse(404, "not_found", "Candidate not found", corsHeaders));
      return;
    }
    sendApiResponse(response, toJsonResponse(200, result, corsHeaders));
    return;
  }

  // Before the election-detail branch: the finance path shares its prefix,
  // and parseElectionId rejects any path with extra segments.
  if (isCandidateElectionFinancePath(url.pathname)) {
    if (request.method !== "GET") {
      sendApiResponse(
        response,
        toErrorResponse(
          405,
          "method_not_allowed",
          "Use GET /api/elections/:election_id/candidates/:candidate_id/finance",
          {
            ...corsHeaders,
            allow: "GET",
          }
        )
      );
      return;
    }
    if (!options.lookupCandidateElectionFinance) {
      sendApiResponse(
        response,
        toErrorResponse(500, "internal_error", "Candidate election finance lookup is not configured", corsHeaders)
      );
      return;
    }

    const { electionId, candidateId } = parseCandidateElectionFinancePath(url);
    const result = await options.lookupCandidateElectionFinance(electionId, candidateId);
    if (!result) {
      sendApiResponse(response, toErrorResponse(404, "not_found", "Candidate election not found", corsHeaders));
      return;
    }
    sendApiResponse(response, toJsonResponse(200, result, corsHeaders));
    return;
  }

  if (isElectionDetailPath(url.pathname)) {
    if (request.method !== "GET") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use GET /api/elections/:election_id", {
          ...corsHeaders,
          allow: "GET",
        })
      );
      return;
    }
    if (!options.lookupElectionDetail) {
      sendApiResponse(response, toErrorResponse(500, "internal_error", "Election detail lookup is not configured", corsHeaders));
      return;
    }

    const electionId = parseElectionId(url);
    const result = await options.lookupElectionDetail(electionId);
    if (!result) {
      sendApiResponse(response, toErrorResponse(404, "not_found", "Election not found", corsHeaders));
      return;
    }
    sendApiResponse(response, toJsonResponse(200, result, corsHeaders));
    return;
  }

  if (url.pathname === ME_DISTRICTS_INITIALIZE_PATH) {
    if (request.method !== "POST") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use POST /api/me/districts/initialize", {
          ...corsHeaders,
          allow: "POST",
        })
      );
      return;
    }
    if (!options.resolveAuthenticatedUserId) {
      sendApiResponse(response, toErrorResponse(401, "unauthorized", "Authentication is required", corsHeaders));
      return;
    }
    if (!options.initializeUserDistricts) {
      sendApiResponse(
        response,
        toErrorResponse(500, "internal_error", "User district initialization is not configured", corsHeaders)
      );
      return;
    }

    const userId = await requireVerifiedAuthenticatedUser(options, request, response);
    if (!userId) {
      return;
    }

    const payload = parseInitializeUserDistrictsBodyValue(request.body);
    const result = await options.initializeUserDistricts({
      userId,
      districtIds: payload.district_ids,
    });
    sendApiResponse(
      response,
      toJsonResponse(
        200,
        {
          status: result.status,
          district_count: result.districtCount,
        },
        corsHeaders
      )
    );
    return;
  }

  if (url.pathname === ADDRESS_AUTOCOMPLETE_PATH) {
    if (request.method !== "POST") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use POST /api/address/autocomplete", {
          ...corsHeaders,
          allow: "POST",
        })
      );
      return;
    }
    if (!options.suggestAddresses) {
      sendApiResponse(
        response,
        toErrorResponse(500, "internal_error", "Address autocomplete is not configured", corsHeaders)
      );
      return;
    }

    const payload = parseAutocompleteSuggestBodyValue(request.body);
    const suggestions = await options.suggestAddresses({
      input: payload.input,
      sessionToken: payload.session_token,
    });
    sendApiResponse(response, toJsonResponse(200, { suggestions }, corsHeaders));
    return;
  }

  if (url.pathname === ADDRESS_AUTOCOMPLETE_RETRIEVE_PATH) {
    if (request.method !== "POST") {
      sendApiResponse(
        response,
        toErrorResponse(405, "method_not_allowed", "Use POST /api/address/autocomplete/retrieve", {
          ...corsHeaders,
          allow: "POST",
        })
      );
      return;
    }
    if (!options.retrieveSuggestedAddress) {
      sendApiResponse(
        response,
        toErrorResponse(500, "internal_error", "Address autocomplete is not configured", corsHeaders)
      );
      return;
    }

    const payload = parseAutocompleteRetrieveBodyValue(request.body);
    const result = await options.retrieveSuggestedAddress({
      placeId: payload.place_id,
      sessionToken: payload.session_token,
    });
    sendApiResponse(response, toJsonResponse(200, { address: result.address }, corsHeaders));
    return;
  }

  if (request.method !== "POST") {
    sendApiResponse(
      response,
      toErrorResponse(405, "method_not_allowed", "Use POST /api/address/resolve", {
        ...corsHeaders,
        allow: "POST",
      })
    );
    return;
  }

  const payload = parseAddressBodyValue(request.body);
  const result = await options.resolveAddress(payload.address);
  if (options.logDiagnostics) {
    try {
      options.logDiagnostics(toAddressResolutionDiagnostics(result));
    } catch {
      // Diagnostics are best-effort; do not fail an otherwise successful request.
    }
  }

  sendApiResponse(response, toJsonResponse(200, toPublicAddressResolution(result), corsHeaders));
}

function mapExpressErrorToResponse(error: unknown): ApiResponse {
  const bodyParserError = error as ExpressBodyParserError;
  if (bodyParserError?.type === "entity.too.large" || bodyParserError?.status === 413 || bodyParserError?.statusCode === 413) {
    return toErrorResponse(413, "invalid_request", `request body exceeds ${MAX_ADDRESS_REQUEST_BODY_BYTES} bytes`);
  }
  if (bodyParserError?.type === "entity.parse.failed") {
    return toErrorResponse(400, "invalid_json", "Request body must be valid JSON");
  }

  const mapped = mapErrorToResponse(error);
  return toErrorResponse(mapped.statusCode, mapped.code, mapped.message);
}

function createApiErrorMiddleware(options: AddressApiServerOptions) {
  return (
    error: unknown,
    request: Request,
    response: Response<unknown, ApiResponseLocals>,
    next: NextFunction
  ): void => {
    if (response.headersSent) {
      next(error);
      return;
    }

    let mapped = mapExpressErrorToResponse(error);
    // 500 only: 502/503 are recognized upstream failures with meaningful
    // bodies (and would flood the log during an outage), but a mapped 500
    // means "we don't know what this is".
    if (mapped.statusCode === 500) {
      // Unexpected failure: without this line the error vanishes — the
      // response body is a generic "Internal error" by design. The id ties
      // a user report ("I saw an error") to this log entry. Method + path
      // only; request bodies can carry addresses and credentials.
      const requestId = randomUUID();
      // describeError: stack string only (never the object, whose enumerable
      // custom properties can carry payloads), with emails and query strings
      // masked — local logs get the same scrubbing as Sentry events.
      console.error(
        `[api] unexpected error request_id=${requestId} ${request.method} ${request.path}`,
        describeError(error)
      );
      // Error-monitoring hook (Sentry in production). Failure here must
      // never break the response.
      try {
        options.captureUnexpectedError?.(error, {
          requestId,
          method: request.method,
          path: request.path,
        });
      } catch {
        // Monitoring is best-effort by definition.
      }
      const body = mapped.body as ApiErrorBody;
      mapped = {
        ...mapped,
        body: { error: { ...body.error, request_id: requestId } } satisfies ApiErrorBody,
      };
    }
    sendApiResponse(response, {
      ...mapped,
      headers: {
        ...mapped.headers,
        ...getCorsHeaders(response),
      },
    });
  };
}

export function createApiApp(options: AddressApiServerOptions): Express {
  const app = express();
  app.disable("x-powered-by");

  // Ordering is load-bearing:
  // - CORS/preflight and unknown-path handling run before rate limiting.
  // - Rate limiting runs before JSON body parsing so oversized POSTs cannot bypass it.
  // - Method checks happen in dispatch so known-path wrong methods can still be rate limited.
  app.use(createClientIpMiddleware(options));
  app.use(createCorsAndPreflightMiddleware(options));
  app.use(createRateLimitMiddleware(options));
  app.use(createContentReportRateLimitMiddleware(options));
  app.use(createJsonBodyParser());
  app.use((request, response, next) => {
    void dispatchApiRequest(request, response, options).catch(next);
  });
  app.use(createApiErrorMiddleware(options));

  return app;
}
