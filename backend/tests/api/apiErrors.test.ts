import { describe, expect, it } from "vitest";

import { mapErrorToResponse } from "../../src/api/apiErrors.js";
import { AuthenticatedAddressDistrictUpdateError } from "../../src/pipeline/users/userAddressDistrictUpdater.js";
import { UserCandidateFollowsError } from "../../src/pipeline/users/userCandidateFollows.js";
import { UserDistrictReaderError } from "../../src/pipeline/users/userDistrictReader.js";
import { ReplaceUserDistrictsError } from "../../src/pipeline/users/userDistrictReplacer.js";
import { UserResearchAreaPreferencesError } from "../../src/pipeline/users/userResearchAreaPreferences.js";

describe("mapErrorToResponse", () => {
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
});
