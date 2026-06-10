export type ApiErrorCode =
  | "not_found"
  | "method_not_allowed"
  | "invalid_json"
  | "invalid_request"
  | "rate_limited"
  | "address_not_found"
  | "upstream_unavailable"
  | "bad_upstream_response"
  | "internal_error";

export type ApiErrorBody = {
  error: {
    code: ApiErrorCode;
    message: string;
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
      "content-type": "application/json; charset=utf-8",
      ...extraHeaders,
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
