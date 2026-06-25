import { describe, expect, it } from "vitest";

import { mapErrorToResponse } from "../../src/api/apiErrors.js";
import { UserDistrictReaderError } from "../../src/pipeline/users/userDistrictReader.js";
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
});
