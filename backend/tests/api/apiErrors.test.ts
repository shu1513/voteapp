import { describe, expect, it } from "vitest";

import { mapErrorToResponse } from "../../src/api/apiErrors.js";
import { UserDistrictReaderError } from "../../src/pipeline/users/userDistrictReader.js";

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
});
