import express, { type Express, type NextFunction, type Request, type Response } from "express";
import type { AddressApiServerOptions } from "./addressApiTypes.js";
import { mapErrorToResponse } from "./apiErrors.js";
import { resolveCorsHeaders } from "./apiCors.js";
import {
  ADDRESS_RESOLVE_PATH,
  BALLOT_LOOKUP_PATH,
  ELECTION_DETAIL_PATH_PREFIX,
  isElectionDetailPath,
  MAX_ADDRESS_REQUEST_BODY_BYTES,
  parseAddressBodyValue,
  parseDistrictIds,
  parseElectionId,
} from "./apiValidation.js";
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
  return pathname === ADDRESS_RESOLVE_PATH || pathname === BALLOT_LOOKUP_PATH || isElectionDetailPath(pathname);
}

function getCorsHeaders(response: Response<unknown, ApiResponseLocals>): Record<string, string> {
  return response.locals.corsHeaders ?? {};
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

function createAddressJsonParser() {
  const parseJson = express.json({
    limit: MAX_ADDRESS_REQUEST_BODY_BYTES,
    strict: true,
    type: "*/*",
  });

  return (request: Request, response: Response, next: NextFunction): void => {
    if (request.path !== ADDRESS_RESOLVE_PATH || request.method !== "POST") {
      next();
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
  const url = new URL(request.originalUrl || request.url, "http://localhost");
  const corsHeaders = getCorsHeaders(response);

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
    const result = await options.lookupBallotSummaries(districtIds);
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
  options.logDiagnostics?.(toAddressResolutionDiagnostics(result));

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
  app.use(createAddressJsonParser());
  app.use((request, response, next) => {
    void dispatchApiRequest(request, response, options).catch(next);
  });
  app.use(createApiErrorMiddleware());

  return app;
}
