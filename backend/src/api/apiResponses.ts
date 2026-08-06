export type ApiErrorCode =
  | "not_found"
  | "method_not_allowed"
  | "unauthorized"
  | "forbidden"
  | "invalid_json"
  | "invalid_request"
  | "unsupported_media_type"
  | "rate_limited"
  | "follow_limit_reached"
  | "address_not_found"
  | "districts_unavailable"
  | "upstream_unavailable"
  | "bad_upstream_response"
  | "internal_error";

export type ApiErrorBody = {
  error: {
    code: ApiErrorCode;
    message: string;
    /** Present on unexpected (5xx) failures: correlates the response with
     * the server-side error log line for that request. */
    request_id?: string;
  };
};

export type ApiResponse = {
  statusCode: number;
  headers: Record<string, string>;
  body?: unknown;
};

export function toJsonResponse(
  statusCode: number,
  body: unknown,
  extraHeaders: Record<string, string> = {}
): ApiResponse {
  return {
    statusCode,
    headers: {
      ...extraHeaders,
      "content-type": "application/json; charset=utf-8",
    },
    body,
  };
}

export function toXmlResponse(
  statusCode: number,
  body: string,
  extraHeaders: Record<string, string> = {}
): ApiResponse {
  return {
    statusCode,
    headers: {
      ...extraHeaders,
      "content-type": "application/xml; charset=utf-8",
    },
    body,
  };
}

export function toEmptyResponse(statusCode: number, extraHeaders: Record<string, string> = {}): ApiResponse {
  return {
    statusCode,
    headers: extraHeaders,
  };
}

export function toErrorResponse(
  statusCode: number,
  code: ApiErrorCode,
  message: string,
  extraHeaders: Record<string, string> = {}
): ApiResponse {
  return toJsonResponse(
    statusCode,
    {
      error: {
        code,
        message,
      },
    } satisfies ApiErrorBody,
    extraHeaders
  );
}
