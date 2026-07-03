import express, { type Express, type NextFunction, type Request, type Response } from "express";
import {
  AUTH_FORGOT_PASSWORD_PATH,
  AUTH_LOGIN_PATH,
  AUTH_LOGOUT_PATH,
  AUTH_REGISTER_PATH,
  AUTH_RESET_PASSWORD_PATH,
  AUTH_RESEND_VERIFICATION_PATH,
  AUTH_VERIFY_EMAIL_PATH,
  parseAuthForgotPasswordBodyValue,
  parseAuthLoginBodyValue,
  parseAuthRegisterBodyValue,
  parseAuthResetPasswordBodyValue,
  parseAuthResendVerificationBodyValue,
  parseAuthVerifyEmailBodyValue,
} from "./apiValidation.js";
import type { AddressApiServerOptions } from "./addressApiTypes.js";
import { mapErrorToResponse } from "./apiErrors.js";
import { resolveCorsHeaders } from "./apiCors.js";
import {
  ADDRESS_AUTOCOMPLETE_PATH,
  ADDRESS_AUTOCOMPLETE_RETRIEVE_PATH,
  ADDRESS_RESOLVE_PATH,
  BALLOT_LOOKUP_PATH,
  CANDIDATE_DETAIL_PATH_PREFIX,
  ELECTION_DETAIL_PATH_PREFIX,
  isCandidateDetailPath,
  isElectionDetailPath,
  MAX_ADDRESS_REQUEST_BODY_BYTES,
  ME_ADDRESS_PATH,
  ME_BALLOT_PATH,
  ME_BALLOT_PREFERENCES_PATH,
  ME_CANDIDATE_FOLLOWS_PATH,
  ME_DISTRICTS_INITIALIZE_PATH,
  ME_RESEARCH_AREA_PREFERENCES_PATH,
  parseAuthenticatedAddressBodyValue,
  parseAddressBodyValue,
  parseAutocompleteRetrieveBodyValue,
  parseAutocompleteSuggestBodyValue,
  parseCandidateFollowBodyValue,
  parseBallotPreferencesBodyValue,
  parseBallotSummaryOptions,
  parseCandidateId,
  parseDistrictIds,
  parseElectionId,
  parseInitializeUserDistrictsBodyValue,
  parseResearchAreaPreferencesBodyValue,
  RESEARCH_AREAS_PATH,
} from "./apiValidation.js";
import {
  AUTH_SESSION_COOKIE_NAME,
  parseCookieHeaderValue,
  serializeAuthSessionCookie,
  serializeClearedAuthSessionCookie,
} from "../auth/authCookies.js";
import { toAddressResolutionDiagnostics, toPublicAddressResolution } from "./addressApiResponses.js";
import { toEmptyResponse, toErrorResponse, toJsonResponse, type ApiResponse } from "./apiResponses.js";

type ApiResponseLocals = {
  clientIp?: string;
  corsHeaders?: Record<string, string>;
};

type ExpressBodyParserError = Error & {
  type?: string;
  status?: number;
  statusCode?: number;
};

function isKnownApiPath(pathname: string): boolean {
  return (
    pathname === ADDRESS_AUTOCOMPLETE_PATH ||
    pathname === ADDRESS_AUTOCOMPLETE_RETRIEVE_PATH ||
    pathname === ADDRESS_RESOLVE_PATH ||
    pathname === BALLOT_LOOKUP_PATH ||
    pathname === AUTH_FORGOT_PASSWORD_PATH ||
    pathname === AUTH_LOGIN_PATH ||
    pathname === AUTH_LOGOUT_PATH ||
    pathname === AUTH_REGISTER_PATH ||
    pathname === AUTH_RESET_PASSWORD_PATH ||
    pathname === AUTH_RESEND_VERIFICATION_PATH ||
    pathname === AUTH_VERIFY_EMAIL_PATH ||
    pathname === ME_ADDRESS_PATH ||
    pathname === ME_BALLOT_PATH ||
    pathname === ME_BALLOT_PREFERENCES_PATH ||
    pathname === ME_CANDIDATE_FOLLOWS_PATH ||
    pathname === ME_DISTRICTS_INITIALIZE_PATH ||
    pathname === ME_RESEARCH_AREA_PREFERENCES_PATH ||
    pathname === RESEARCH_AREAS_PATH ||
    isCandidateDetailPath(pathname) ||
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

function getAuthSessionId(request: Request): string | null {
  return parseCookieHeaderValue(request.headers.cookie, AUTH_SESSION_COOKIE_NAME);
}

function sendApiResponse(response: Response, apiResponse: ApiResponse): void {
  response.status(apiResponse.statusCode).set(apiResponse.headers);
  if (apiResponse.body === undefined) {
    response.end();
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
          request.path === AUTH_VERIFY_EMAIL_PATH)) ||
      (request.method === "PUT" &&
        (request.path === ME_ADDRESS_PATH ||
          request.path === ME_BALLOT_PREFERENCES_PATH ||
          request.path === ME_CANDIDATE_FOLLOWS_PATH ||
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
    if (!(await enforceAuthRateLimit(options, request, response, payload.email))) {
      return;
    }
    await options.authService.register({
      email: payload.email,
      password: payload.password,
      firstName: payload.first_name,
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
    sendApiResponse(response, {
      ...toJsonResponse(200, { status: "ok" }, corsHeaders),
      headers: {
        ...corsHeaders,
        "content-type": "application/json; charset=utf-8",
        "set-cookie": serializeAuthSessionCookie(result.sessionId, {
          ...options.authSessionCookieOptions,
        }),
      },
      body: { status: "ok" },
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

function createApiErrorMiddleware() {
  return (
    error: unknown,
    _request: Request,
    response: Response<unknown, ApiResponseLocals>,
    next: NextFunction
  ): void => {
    if (response.headersSent) {
      next(error);
      return;
    }

    const mapped = mapExpressErrorToResponse(error);
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
  app.use(createJsonBodyParser());
  app.use((request, response, next) => {
    void dispatchApiRequest(request, response, options).catch(next);
  });
  app.use(createApiErrorMiddleware());

  return app;
}
