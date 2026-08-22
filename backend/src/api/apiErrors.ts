import { CensusAddressGeocoderError } from "../pipeline/address/censusAddressGeocoder.js";
import { GooglePlacesAutocompleteError } from "../pipeline/address/googlePlacesAutocomplete.js";
import { CandidateDetailReaderError } from "../pipeline/candidates/candidateDetailReader.js";
import { AuthenticatedAddressDistrictUpdateError } from "../pipeline/users/userAddressDistrictUpdater.js";
import { ContentReportError } from "../pipeline/reports/contentReports.js";
import { UserCandidateFollowsError } from "../pipeline/users/userCandidateFollows.js";
import { AutoPickError } from "../pipeline/users/autoPick.js";
import { UserElectionChoicesError } from "../pipeline/users/userElectionChoices.js";
import { UserPickCardSharesError } from "../pipeline/users/userPickCardShares.js";
import { InitializeUserDistrictsError } from "../pipeline/users/userDistrictInitializer.js";
import { UserDistrictReaderError } from "../pipeline/users/userDistrictReader.js";
import { ReplaceUserDistrictsError } from "../pipeline/users/userDistrictReplacer.js";
import { UserResearchAreaPreferencesError } from "../pipeline/users/userResearchAreaPreferences.js";
import { UserBallotPreferencesError } from "../pipeline/users/userBallotPreferences.js";
import { UserEmailPreferencesError } from "../pipeline/users/userEmailPreferences.js";
import { UserPushTokensError } from "../pipeline/users/userPushTokens.js";
import { UserIdentityError } from "../pipeline/users/userIdentity.js";
import { AuthGoogleSignInError } from "../auth/authService.js";
import { MembershipServiceError, MembershipWebhookRetryError } from "./membership/membershipService.js";
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
  // Distinct code (not a generic 400) so the login page can route the user
  // to the register page instead of showing a dead-end error.
  if (error instanceof AuthGoogleSignInError) {
    return { statusCode: 400, code: "needs_signup", message: error.message };
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
    // Server-side data gap, not a problem with the submitted address: the
    // saved districts were preserved and a later retry can succeed.
    if (error.code === "partial_district_resolution") {
      return { statusCode: 503, code: "districts_unavailable", message: error.message };
    }
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
    if (error.code === "follow_limit_reached") {
      return { statusCode: 409, code: "follow_limit_reached", message: error.message };
    }
    return { statusCode: 400, code: "invalid_request", message: error.message };
  }
  if (error instanceof UserElectionChoicesError) {
    if (error.code === "invalid_user_id" || error.code === "user_not_found") {
      return { statusCode: 401, code: "unauthorized", message: "Authentication is required" };
    }
    if (error.code === "election_not_found") {
      return { statusCode: 404, code: "not_found", message: "Election not found" };
    }
    if (error.code === "candidacy_not_available") {
      return { statusCode: 404, code: "not_found", message: "Candidate is not an active candidate in this election" };
    }
    return { statusCode: 400, code: "invalid_request", message: error.message };
  }
  if (error instanceof AutoPickError) {
    if (error.code === "invalid_user_id" || error.code === "user_not_found") {
      return { statusCode: 401, code: "unauthorized", message: "Authentication is required" };
    }
    if (error.code === "election_not_found") {
      return { statusCode: 404, code: "not_found", message: "Election not found" };
    }
    return { statusCode: 400, code: "invalid_request", message: error.message };
  }
  if (error instanceof UserPickCardSharesError) {
    if (error.code === "invalid_user_id" || error.code === "user_not_found") {
      return { statusCode: 401, code: "unauthorized", message: "Authentication is required" };
    }
    // invalid_election_date and no_picks_to_share are both caller problems
    // with self-explanatory writer messages.
    return { statusCode: 400, code: "invalid_request", message: error.message };
  }
  // [ballot-personalized-ordering]
  if (error instanceof UserBallotPreferencesError) {
    if (error.code === "invalid_user_id" || error.code === "user_not_found") {
      return { statusCode: 401, code: "unauthorized", message: "Authentication is required" };
    }
    return { statusCode: 400, code: "invalid_request", message: error.message };
  }
  if (error instanceof UserEmailPreferencesError) {
    if (error.code === "invalid_user_id" || error.code === "user_not_found") {
      return { statusCode: 401, code: "unauthorized", message: "Authentication is required" };
    }
    return { statusCode: 400, code: "invalid_request", message: error.message };
  }
  if (error instanceof UserPushTokensError) {
    if (error.code === "invalid_user_id" || error.code === "user_not_found") {
      return { statusCode: 401, code: "unauthorized", message: "Authentication is required" };
    }
    return { statusCode: 400, code: "invalid_request", message: error.message };
  }
  // A valid session pointing at a missing or deleted user row is an
  // authentication failure, not a server bug.
  if (error instanceof UserIdentityError) {
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
  if (error instanceof ContentReportError) {
    if (error.code === "entity_not_found") {
      return { statusCode: 404, code: "not_found", message: "Reported content not found" };
    }
    if (error.code === "invalid_user_id") {
      return { statusCode: 401, code: "unauthorized", message: "Authentication is required" };
    }
    return { statusCode: 400, code: "invalid_request", message: error.message };
  }
  if (error instanceof MembershipServiceError) {
    if (error.code === "membership_exists") {
      return { statusCode: 409, code: "membership_exists", message: error.message };
    }
    if (error.code === "no_billing_account") {
      return { statusCode: 404, code: "not_found", message: error.message };
    }
    if (error.code === "user_not_found") {
      return { statusCode: 401, code: "unauthorized", message: "Authentication is required" };
    }
    // subscription_cancel_failed: Stripe was unreachable during the
    // account-deletion precondition; nothing was deleted and a retry is safe.
    return { statusCode: 503, code: "upstream_unavailable", message: error.message };
  }
  // A webhook event that cannot be applied yet (refund before its ledger row,
  // invoice mid-settlement). Any non-2xx makes Stripe redeliver; 503 keeps it
  // out of the unexpected-error capture path, and the handler already logged.
  if (error instanceof MembershipWebhookRetryError) {
    return { statusCode: 503, code: "upstream_unavailable", message: error.message };
  }
  if (error instanceof Error && error.message.startsWith("request body exceeds")) {
    return { statusCode: 413, code: "invalid_request", message: error.message };
  }
  return { statusCode: 500, code: "internal_error", message: "Internal error" };
}
