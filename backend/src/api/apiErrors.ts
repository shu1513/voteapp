import { CensusAddressGeocoderError } from "../pipeline/address/censusAddressGeocoder.js";
import type { ApiErrorCode } from "./apiResponses.js";

export type MappedApiError = {
  statusCode: number;
  code: ApiErrorCode;
  message: string;
};

export function mapErrorToResponse(error: unknown): MappedApiError {
  if (error instanceof SyntaxError) {
    return { statusCode: 400, code: "invalid_json", message: error.message };
  }
  if (error instanceof TypeError) {
    return { statusCode: 400, code: "invalid_request", message: error.message };
  }
  if (error instanceof CensusAddressGeocoderError) {
    if (error.code === "invalid_address") {
      return { statusCode: 400, code: "invalid_request", message: error.message };
    }
    if (error.code === "not_found") {
      return { statusCode: 422, code: "address_not_found", message: error.message };
    }
    if (error.code === "bad_response") {
      return { statusCode: 502, code: "bad_upstream_response", message: error.message };
    }
    if (error.code === "timeout" || error.code === "http_error") {
      return { statusCode: 503, code: "upstream_unavailable", message: error.message };
    }
  }
  if (error instanceof Error && error.message.startsWith("request body exceeds")) {
    return { statusCode: 413, code: "invalid_request", message: error.message };
  }
  const message = error instanceof Error ? error.message : String(error);
  return { statusCode: 500, code: "internal_error", message };
}
