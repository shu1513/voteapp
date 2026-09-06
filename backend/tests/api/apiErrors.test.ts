import { describe, expect, it } from "vitest";

import { mapErrorToResponse } from "../../src/api/apiErrors.js";
import { CandidateDetailReaderError } from "../../src/pipeline/candidates/candidateDetailReader.js";
import { AuthenticatedAddressDistrictUpdateError } from "../../src/pipeline/users/userAddressDistrictUpdater.js";
import { UserCandidateFollowsError } from "../../src/pipeline/users/userCandidateFollows.js";
import { UserElectionChoicesError } from "../../src/pipeline/users/userElectionChoices.js";
import { UserDistrictReaderError } from "../../src/pipeline/users/userDistrictReader.js";
import { ReplaceUserDistrictsError } from "../../src/pipeline/users/userDistrictReplacer.js";
import { UserResearchAreaPreferencesError } from "../../src/pipeline/users/userResearchAreaPreferences.js";
import { UserEmailPreferencesError } from "../../src/pipeline/users/userEmailPreferences.js";
import { UserIdentityError } from "../../src/pipeline/users/userIdentity.js";
import { RequestValidationError } from "../../src/utils/requestValidationError.js";

describe("mapErrorToResponse", () => {
  it("maps invalid candidate detail IDs to invalid_request", () => {
    expect(mapErrorToResponse(new CandidateDetailReaderError("invalid_candidate_id", "Candidate ID must be a valid UUID"))).toEqual({
      statusCode: 400,
      code: "invalid_request",
      message: "Candidate ID must be a valid UUID",
    });
  });

  it("maps invalid candidate detail user IDs to unauthorized", () => {
    expect(mapErrorToResponse(new CandidateDetailReaderError("invalid_user_id", "User ID must be a valid UUID"))).toEqual({
      statusCode: 401,
      code: "unauthorized",
      message: "Authentication is required",
    });
  });

  it.each([
    ["invalid_user_id", "User ID must be a valid UUID"],
    ["user_not_found", "User not found"],
  ] as const)("maps user district reader %s errors to unauthorized", (code, message) => {
    expect(mapErrorToResponse(new UserDistrictReaderError(code, message))).toEqual({
      statusCode: 401,
      code: "unauthorized",
      message: "Authentication is required",
    });
  });

  it.each([
    ["invalid_user_id", "User ID must be a valid UUID"],
    ["user_not_found", "User not found"],
  ] as const)("maps user research area preference %s errors to unauthorized", (code, message) => {
    expect(mapErrorToResponse(new UserResearchAreaPreferencesError(code, message))).toEqual({
      statusCode: 401,
      code: "unauthorized",
      message: "Authentication is required",
    });
  });

  it("maps user research area preference validation errors to invalid_request", () => {
    expect(
      mapErrorToResponse(
        new UserResearchAreaPreferencesError("unselectable_research_area_ids", "Research area cannot be selected")
      )
    ).toEqual({
      statusCode: 400,
      code: "invalid_request",
      message: "Research area cannot be selected",
    });
  });

  it.each([
    ["invalid_user_id", "User ID must be a valid UUID"],
    ["user_not_found", "User not found"],
  ] as const)("maps user district replacement %s errors to unauthorized", (code, message) => {
    expect(mapErrorToResponse(new ReplaceUserDistrictsError(code, message))).toEqual({
      statusCode: 401,
      code: "unauthorized",
      message: "Authentication is required",
    });
  });

  it("maps user district replacement validation errors to invalid_request", () => {
    expect(mapErrorToResponse(new ReplaceUserDistrictsError("unknown_district_ids", "Unknown district IDs: abc"))).toEqual({
      statusCode: 400,
      code: "invalid_request",
      message: "Address could not be matched to saved districts",
    });
  });

  it("maps authenticated address updates with no supported districts to address_not_found", () => {
    expect(
      mapErrorToResponse(
        new AuthenticatedAddressDistrictUpdateError(
          "no_supported_districts",
          "Resolved address did not match any supported districts"
        )
      )
    ).toEqual({
      statusCode: 422,
      code: "address_not_found",
      message: "Resolved address did not match any supported districts",
    });
  });

  it("maps partial district resolution to a retryable 503, not an address error", () => {
    expect(
      mapErrorToResponse(new AuthenticatedAddressDistrictUpdateError("partial_district_resolution", "Try again later."))
    ).toEqual({
      statusCode: 503,
      code: "districts_unavailable",
      message: "Try again later.",
    });
  });

  it.each([
    ["invalid_user_id", "User ID must be a valid UUID"],
    ["user_not_found", "User not found"],
  ] as const)("maps user candidate follow %s errors to unauthorized", (code, message) => {
    expect(mapErrorToResponse(new UserCandidateFollowsError(code, message))).toEqual({
      statusCode: 401,
      code: "unauthorized",
      message: "Authentication is required",
    });
  });

  it.each([
    ["invalid_candidate_id", "Candidate ID must be a valid UUID"],
    ["invalid_follow_input", "following must be a boolean"],
  ] as const)("maps user candidate follow %s errors to invalid_request", (code, message) => {
    expect(mapErrorToResponse(new UserCandidateFollowsError(code, message))).toEqual({
      statusCode: 400,
      code: "invalid_request",
      message,
    });
  });

  it("maps missing/deleted/merged followed candidates to not_found", () => {
    expect(mapErrorToResponse(new UserCandidateFollowsError("candidate_not_found", "Candidate not found"))).toEqual({
      statusCode: 404,
      code: "not_found",
      message: "Candidate not found",
    });
  });

  it.each([
    ["invalid_user_id", "User ID must be a valid UUID"],
    ["user_not_found", "User not found"],
  ] as const)("maps user election choice %s errors to unauthorized", (code, message) => {
    expect(mapErrorToResponse(new UserElectionChoicesError(code, message))).toEqual({
      statusCode: 401,
      code: "unauthorized",
      message: "Authentication is required",
    });
  });

  it.each([
    ["election_not_found", "Election not found"],
    ["candidacy_not_available", "Candidate is not an active candidate in this election"],
  ] as const)("maps user election choice %s errors to not_found", (code) => {
    const mapped = mapErrorToResponse(new UserElectionChoicesError(code, "ignored: mapping supplies the message"));
    expect(mapped.statusCode).toBe(404);
    expect(mapped.code).toBe("not_found");
  });

  it.each([
    ["invalid_election_id", "Election ID must be a valid UUID"],
    ["invalid_candidate_id", "Candidate ID must be a valid UUID"],
    ["invalid_choice_input", "chosen must be a boolean"],
    ["election_closed", "Choices can only be changed for upcoming elections"],
    ["choice_limit_reached", "This election fills 3 seats; remove a pick before adding another"],
  ] as const)("maps user election choice %s errors to invalid_request with the writer's message", (code, message) => {
    expect(mapErrorToResponse(new UserElectionChoicesError(code, message))).toEqual({
      statusCode: 400,
      code: "invalid_request",
      message,
    });
  });

  it("maps email preference identity errors to 401 and other codes to 400", () => {
    expect(mapErrorToResponse(new UserEmailPreferencesError("user_not_found", "User not found"))).toEqual({
      statusCode: 401,
      code: "unauthorized",
      message: "Authentication is required",
    });
    expect(mapErrorToResponse(new UserEmailPreferencesError("invalid_user_id", "userId must be a UUID"))).toEqual({
      statusCode: 401,
      code: "unauthorized",
      message: "Authentication is required",
    });
  });

  it("maps user identity errors to 401", () => {
    expect(mapErrorToResponse(new UserIdentityError("user_not_found", "User not found"))).toEqual({
      statusCode: 401,
      code: "unauthorized",
      message: "Authentication is required",
    });
    expect(mapErrorToResponse(new UserIdentityError("invalid_user_id", "userId must be a UUID"))).toEqual({
      statusCode: 401,
      code: "unauthorized",
      message: "Authentication is required",
    });
  });
});

describe("mapErrorToResponse request-validation boundary", () => {
  it("maps RequestValidationError to 400 with its code and message", () => {
    expect(mapErrorToResponse(new RequestValidationError("email must be a string"))).toEqual({
      statusCode: 400,
      code: "invalid_request",
      message: "email must be a string",
    });
    expect(mapErrorToResponse(new RequestValidationError("Request body must be valid JSON", "invalid_json"))).toEqual({
      statusCode: 400,
      code: "invalid_json",
      message: "Request body must be valid JSON",
    });
  });

  it("treats a bare TypeError or SyntaxError as an internal failure, not a client error", () => {
    // A runtime TypeError (undefined.foo) or a JSON.parse failure on a stored
    // value is a bug; surfacing it as a 400 with the internal message would
    // hide it from the logs and from monitoring.
    for (const error of [new TypeError("Cannot read properties of undefined (reading 'id')"), new SyntaxError("Unexpected token")]) {
      expect(mapErrorToResponse(error)).toEqual({ statusCode: 500, code: "internal_error", message: "Internal error" });
    }
  });
});
