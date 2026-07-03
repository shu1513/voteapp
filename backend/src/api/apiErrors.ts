import { CensusAddressGeocoderError } from "../pipeline/address/censusAddressGeocoder.js";
import { GooglePlacesAutocompleteError } from "../pipeline/address/googlePlacesAutocomplete.js";
import { CandidateDetailReaderError } from "../pipeline/candidates/candidateDetailReader.js";
import { AuthenticatedAddressDistrictUpdateError } from "../pipeline/users/userAddressDistrictUpdater.js";
import { UserCandidateFollowsError } from "../pipeline/users/userCandidateFollows.js";
import { InitializeUserDistrictsError } from "../pipeline/users/userDistrictInitializer.js";
import { UserDistrictReaderError } from "../pipeline/users/userDistrictReader.js";
import { ReplaceUserDistrictsError } from "../pipeline/users/userDistrictReplacer.js";
import { UserResearchAreaPreferencesError } from "../pipeline/users/userResearchAreaPreferences.js";
import { UserBallotPreferencesError } from "../pipeline/users/userBallotPreferences.js";
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
    if (error.code === "timeout" || error.code === "http_error" || error.code === "network_error") {
      return { statusCode: 503, code: "upstream_unavailable", message: error.message };
    }
  }
  if (error instanceof GooglePlacesAutocompleteError) {
    if (error.code === "invalid_input") {
      return { statusCode: 400, code: "invalid_request", message: error.message };
    }
    if (error.code === "bad_response") {
      return { statusCode: 502, code: "bad_upstream_response", message: error.message };
    }
    if (error.code === "timeout" || error.code === "http_error" || error.code === "network_error") {
      return { statusCode: 503, code: "upstream_unavailable", message: error.message };
    }
  }
  if (error instanceof InitializeUserDistrictsError) {
    if (error.code === "invalid_user_id" || error.code === "user_not_found") {
      return { statusCode: 401, code: "unauthorized", message: "Authentication is required" };
    }
    return { statusCode: 400, code: "invalid_request", message: error.message };
  }
  if (error instanceof UserDistrictReaderError) {
    return { statusCode: 401, code: "unauthorized", message: "Authentication is required" };
  }
  if (error instanceof AuthenticatedAddressDistrictUpdateError) {
    return { statusCode: 422, code: "address_not_found", message: error.message };
  }
  if (error instanceof ReplaceUserDistrictsError) {
    if (error.code === "invalid_user_id" || error.code === "user_not_found") {
      return { statusCode: 401, code: "unauthorized", message: "Authentication is required" };
    }
    if (error.code === "unknown_district_ids") {
      return {
        statusCode: 400,
        code: "invalid_request",
        message: "Address could not be matched to saved districts",
      };
    }
    return { statusCode: 400, code: "invalid_request", message: error.message };
  }
  if (error instanceof CandidateDetailReaderError) {
    if (error.code === "invalid_user_id") {
      return { statusCode: 401, code: "unauthorized", message: "Authentication is required" };
    }
    return { statusCode: 400, code: "invalid_request", message: error.message };
  }
  if (error instanceof UserCandidateFollowsError) {
    if (error.code === "invalid_user_id" || error.code === "user_not_found") {
      return { statusCode: 401, code: "unauthorized", message: "Authentication is required" };
    }
    if (error.code === "candidate_not_found") {
      return { statusCode: 404, code: "not_found", message: "Candidate not found" };
    }
    return { statusCode: 400, code: "invalid_request", message: error.message };
  }
  // [ballot-personalized-ordering]
  if (error instanceof UserBallotPreferencesError) {
    if (error.code === "invalid_user_id" || error.code === "user_not_found") {
      return { statusCode: 401, code: "unauthorized", message: "Authentication is required" };
    }
    return { statusCode: 400, code: "invalid_request", message: error.message };
  }
  if (error instanceof UserResearchAreaPreferencesError) {
    if (error.code === "invalid_user_id" || error.code === "user_not_found") {
      return { statusCode: 401, code: "unauthorized", message: "Authentication is required" };
    }
    return { statusCode: 400, code: "invalid_request", message: error.message };
  }
  if (error instanceof Error && error.message.startsWith("request body exceeds")) {
    return { statusCode: 413, code: "invalid_request", message: error.message };
  }
  return { statusCode: 500, code: "internal_error", message: "Internal error" };
}
