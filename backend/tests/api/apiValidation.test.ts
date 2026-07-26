import { describe, expect, it } from "vitest";

import {
  AUTH_FORGOT_PASSWORD_PATH,
  AUTH_LOGIN_PATH,
  AUTH_LOGOUT_PATH,
  AUTH_REGISTER_PATH,
  AUTH_RESET_PASSWORD_PATH,
  AUTH_RESEND_VERIFICATION_PATH,
  AUTH_VERIFY_EMAIL_PATH,
  CANDIDATE_DETAIL_PATH_PREFIX,
  ME_CANDIDATE_FOLLOWS_PATH,
  MAX_USER_RESEARCH_AREA_PREFERENCES,
  MAX_ADDRESS_INPUT_LENGTH,
  MAX_AUTH_EMAIL_LENGTH,
  MAX_FIRST_NAME_LENGTH,
  ME_ADDRESS_PATH,
  ME_RESEARCH_AREA_PREFERENCES_PATH,
  parseAuthForgotPasswordBodyValue,
  parseAuthLoginBodyValue,
  parseAuthRegisterBodyValue,
  parseAuthResetPasswordBodyValue,
  parseAuthResendVerificationBodyValue,
  parseAuthVerifyEmailBodyValue,
  parseAuthenticatedAddressBodyValue,
  parseCandidateFollowBodyValue,
  parseCandidateId,
  parseMeEmailBodyValue,
  parseMeUpdateBodyValue,
  parseResearchAreaPreferencesBodyValue,
  RESEARCH_AREAS_PATH,
} from "../../src/api/apiValidation.js";

describe("candidate detail API contract constants", () => {
  it("defines and parses the public candidate detail path", () => {
    expect(CANDIDATE_DETAIL_PATH_PREFIX).toBe("/api/candidates/");
    expect(parseCandidateId(new URL("http://localhost/api/candidates/22222222-2222-4222-8222-222222222222"))).toBe(
      "22222222-2222-4222-8222-222222222222"
    );
  });

  it.each([
    ["http://localhost/api/candidates/", "Candidate detail path must be /api/candidates/:candidate_id"],
    [
      "http://localhost/api/candidates/22222222-2222-4222-8222-222222222222/extra",
      "Candidate detail path must be /api/candidates/:candidate_id",
    ],
    ["http://localhost/api/candidates/not-a-uuid", "Candidate detail path contains invalid UUID: not-a-uuid"],
  ])("rejects invalid candidate detail path %#", (url, message) => {
    expect(() => parseCandidateId(new URL(url))).toThrow(message);
  });
});

describe("authenticated address API contract constants", () => {
  it("defines the authenticated address replacement path", () => {
    expect(ME_ADDRESS_PATH).toBe("/api/me/address");
  });

  it("parses authenticated address payloads like normal address lookups", () => {
    expect(parseAuthenticatedAddressBodyValue({ address: "  123 Main St Denver CO 80203  " })).toEqual({
      address: "123 Main St Denver CO 80203",
    });
  });

  it.each([
    [null, "Request body must be a JSON object"],
    [{}, "Request body must include non-empty string field: address"],
    [{ address: "   " }, "Request body must include non-empty string field: address"],
    [{ address: 123 }, "Request body must include non-empty string field: address"],
    [
      { address: "a".repeat(MAX_ADDRESS_INPUT_LENGTH + 1) },
      `address must be at most ${MAX_ADDRESS_INPUT_LENGTH} characters`,
    ],
  ])("rejects invalid authenticated address payload %#", (payload, message) => {
    expect(() => parseAuthenticatedAddressBodyValue(payload)).toThrow(message);
  });

  it("accepts an address at exactly the length cap", () => {
    const address = "a".repeat(MAX_ADDRESS_INPUT_LENGTH);
    expect(parseAuthenticatedAddressBodyValue({ address })).toEqual({ address });
  });
});

describe("candidate follow API contract constants", () => {
  it("defines the authenticated candidate-follow path", () => {
    expect(ME_CANDIDATE_FOLLOWS_PATH).toBe("/api/me/candidate-follows");
  });

  it("parses candidate follow payloads into pipeline inputs", () => {
    expect(
      parseCandidateFollowBodyValue({
        candidate_id: "  22222222-2222-4222-8222-222222222222  ",
        following: true,
        notify_elections: false,
        notify_updates: true,
      })
    ).toEqual({
      candidateId: "22222222-2222-4222-8222-222222222222",
      following: true,
      notifyElections: false,
      notifyUpdates: true,
    });
  });

  it("allows minimal unfollow payloads", () => {
    expect(
      parseCandidateFollowBodyValue({
        candidate_id: "22222222-2222-4222-8222-222222222222",
        following: false,
      })
    ).toEqual({
      candidateId: "22222222-2222-4222-8222-222222222222",
      following: false,
    });
  });

  it.each([
    [null, "Request body must be a JSON object"],
    [{}, "Request body must include UUID string field: candidate_id"],
    [{ candidate_id: 123, following: true }, "Request body must include UUID string field: candidate_id"],
    [{ candidate_id: "not-a-uuid", following: true }, "candidate_id must be a valid UUID: not-a-uuid"],
    [
      { candidate_id: "22222222-2222-4222-8222-222222222222" },
      "Request body must include boolean field: following",
    ],
    [
      { candidate_id: "22222222-2222-4222-8222-222222222222", following: "yes" },
      "Request body must include boolean field: following",
    ],
    [
      {
        candidate_id: "22222222-2222-4222-8222-222222222222",
        following: true,
        notify_elections: "yes",
      },
      "notify_elections must be a boolean",
    ],
    [
      {
        candidate_id: "22222222-2222-4222-8222-222222222222",
        following: true,
        notify_updates: "yes",
      },
      "notify_updates must be a boolean",
    ],
  ])("rejects invalid candidate follow payload %#", (payload, message) => {
    expect(() => parseCandidateFollowBodyValue(payload)).toThrow(message);
  });
});

describe("public auth API contract constants", () => {
  it("defines the public auth endpoints", () => {
    expect(AUTH_REGISTER_PATH).toBe("/api/auth/register");
    expect(AUTH_VERIFY_EMAIL_PATH).toBe("/api/auth/verify-email");
    expect(AUTH_LOGIN_PATH).toBe("/api/auth/login");
    expect(AUTH_LOGOUT_PATH).toBe("/api/auth/logout");
    expect(AUTH_FORGOT_PASSWORD_PATH).toBe("/api/auth/forgot-password");
    expect(AUTH_RESEND_VERIFICATION_PATH).toBe("/api/auth/resend-verification");
    expect(AUTH_RESET_PASSWORD_PATH).toBe("/api/auth/reset-password");
  });

  it("parses public auth payloads", () => {
    expect(
      parseAuthRegisterBodyValue({
        email: "  user@example.com ",
        password: "correct horse battery staple",
        first_name: "  Alice  ",
        accepted_terms_version: " 1.0 ",
      })
    ).toEqual({
      email: "user@example.com",
      password: "correct horse battery staple",
      first_name: "Alice",
      accepted_terms_version: "1.0",
    });
    // Clickwrap: registration without terms acceptance must not parse.
    expect(() =>
      parseAuthRegisterBodyValue({ email: "user@example.com", password: "correct horse battery staple" })
    ).toThrow("accepted_terms_version");
    expect(parseAuthLoginBodyValue({ email: " user@example.com ", password: " secret123 " })).toEqual({
      email: "user@example.com",
      password: "secret123",
    });
    expect(parseAuthForgotPasswordBodyValue({ email: " user@example.com " })).toEqual({
      email: "user@example.com",
    });
    expect(parseAuthResendVerificationBodyValue({ email: " user@example.com " })).toEqual({
      email: "user@example.com",
    });
    expect(parseAuthVerifyEmailBodyValue({ token: " abc " })).toEqual({ token: "abc" });
    expect(parseAuthResetPasswordBodyValue({ token: " abc ", password: " password123 " })).toEqual({
      token: "abc",
      password: "password123",
    });
  });

  it("caps auth email, first name, and change-email fields at practical lengths", () => {
    // 320 is the RFC 5321 mailbox ceiling; longer values are junk, not mail.
    const longEmail = `${"a".repeat(MAX_AUTH_EMAIL_LENGTH)}@example.com`;
    const emailCapMessage = `must be at most ${MAX_AUTH_EMAIL_LENGTH} characters`;
    expect(() =>
      parseAuthRegisterBodyValue({
        email: longEmail,
        password: "correct horse battery staple",
        accepted_terms_version: "1.0",
      })
    ).toThrow(emailCapMessage);
    expect(() => parseAuthLoginBodyValue({ email: longEmail, password: "secret123" })).toThrow(emailCapMessage);
    expect(() => parseAuthForgotPasswordBodyValue({ email: longEmail })).toThrow(emailCapMessage);
    expect(() => parseMeEmailBodyValue({ new_email: longEmail, password: "secret123" })).toThrow(emailCapMessage);

    const longFirstName = "b".repeat(MAX_FIRST_NAME_LENGTH + 1);
    const firstNameCapMessage = `first_name must be at most ${MAX_FIRST_NAME_LENGTH} characters`;
    expect(() =>
      parseAuthRegisterBodyValue({
        email: "user@example.com",
        password: "correct horse battery staple",
        accepted_terms_version: "1.0",
        first_name: longFirstName,
      })
    ).toThrow(firstNameCapMessage);
    expect(() => parseMeUpdateBodyValue({ first_name: longFirstName })).toThrow(firstNameCapMessage);
    expect(parseMeUpdateBodyValue({ first_name: "c".repeat(MAX_FIRST_NAME_LENGTH) })).toEqual({
      first_name: "c".repeat(MAX_FIRST_NAME_LENGTH),
    });
  });
});

describe("research area API contract constants", () => {
  it("defines the public research-area catalog and authenticated preference paths", () => {
    expect(RESEARCH_AREAS_PATH).toBe("/api/research-areas");
    expect(ME_RESEARCH_AREA_PREFERENCES_PATH).toBe("/api/me/research-area-preferences");
  });

  it("exposes the shared maximum user preference count", () => {
    expect(MAX_USER_RESEARCH_AREA_PREFERENCES).toBe(7);
  });

  it("parses research area preference payloads into pipeline inputs", () => {
    expect(
      parseResearchAreaPreferencesBodyValue({
        preferences: [
          { research_area_id: "22222222-2222-4222-8222-222222222222", rank: 1 },
          { research_area_id: "33333333-3333-4333-8333-333333333333", rank: null },
        ],
      })
    ).toEqual({
      preferences: [
        { researchAreaId: "22222222-2222-4222-8222-222222222222", rank: 1 },
        { researchAreaId: "33333333-3333-4333-8333-333333333333", rank: null },
      ],
    });
  });

  it("allows clearing all research area preferences", () => {
    expect(parseResearchAreaPreferencesBodyValue({ preferences: [] })).toEqual({ preferences: [] });
  });

  it.each([
    [{}, "Request body must include array field: preferences"],
    [{ preferences: ["bad"] }, "preferences must contain only JSON objects"],
    [
      { preferences: [{ research_area_id: "not-a-uuid" }] },
      "preferences contains invalid research_area_id: not-a-uuid",
    ],
    [
      {
        preferences: [
          { research_area_id: "22222222-2222-4222-8222-222222222222", rank: 1 },
          { research_area_id: "22222222-2222-4222-8222-222222222222", rank: 2 },
        ],
      },
      "preferences contains duplicate research_area_id: 22222222-2222-4222-8222-222222222222",
    ],
    [
      { preferences: [{ research_area_id: "22222222-2222-4222-8222-222222222222", rank: 8 }] },
      "preferences[].rank must be an integer from 1 to 7",
    ],
    [
      {
        preferences: Array.from({ length: MAX_USER_RESEARCH_AREA_PREFERENCES + 1 }, (_value, index) => ({
          research_area_id: `22222222-2222-4222-8222-${(index + 1).toString().padStart(12, "0")}`,
        })),
      },
      "preferences supports at most 7 research areas",
    ],
    [
      {
        preferences: [
          { research_area_id: "22222222-2222-4222-8222-222222222222", rank: 1 },
          { research_area_id: "33333333-3333-4333-8333-333333333333", rank: 1 },
        ],
      },
      "preferences contains duplicate rank: 1",
    ],
  ])("rejects invalid research area preference payload %#", (payload, message) => {
    expect(() => parseResearchAreaPreferencesBodyValue(payload)).toThrow(message);
  });
});

describe("content report API contract constants", () => {
  it("defines and parses the public content report path", async () => {
    const { CONTENT_REPORTS_PATH, parseContentReportBodyValue } = await import("../../src/api/apiValidation.js");
    expect(CONTENT_REPORTS_PATH).toBe("/api/content-reports");
    expect(
      parseContentReportBodyValue({
        entity_type: "candidate_record",
        entity_id: "22222222-2222-4222-8222-222222222222",
        message: "  This record looks wrong.  ",
        suggested_source_url: " https://example.org/source ",
        reporter_email: " reader@example.com ",
      })
    ).toEqual({
      entityType: "candidate_record",
      entityId: "22222222-2222-4222-8222-222222222222",
      message: "This record looks wrong.",
      suggestedSourceUrl: "https://example.org/source",
      reporterEmail: "reader@example.com",
    });
  });

  it.each([
    [null, "Request body must be a JSON object"],
    [{}, "Request body must include string field: entity_type"],
    [
      {
        entity_type: "bad",
        entity_id: "22222222-2222-4222-8222-222222222222",
        message: "wrong",
      },
      "entity_type must be one of",
    ],
    [
      {
        entity_type: "candidate",
        entity_id: "not-a-uuid",
        message: "wrong",
      },
      "entity_id must be a valid UUID: not-a-uuid",
    ],
    [
      {
        entity_type: "candidate",
        entity_id: "22222222-2222-4222-8222-222222222222",
        message: " ",
      },
      "Request body must include non-empty string field: message",
    ],
    [
      {
        entity_type: "candidate",
        entity_id: "22222222-2222-4222-8222-222222222222",
        message: "wrong",
        suggested_source_url: "ftp://example.org/file",
      },
      "suggested_source_url must be a valid http(s) URL",
    ],
    [
      {
        entity_type: "candidate",
        entity_id: "22222222-2222-4222-8222-222222222222",
        message: "wrong",
        reporter_email: "not-an-email",
      },
      "reporter_email must be a valid email address when provided",
    ],
    [
      {
        entity_type: "candidate",
        entity_id: "22222222-2222-4222-8222-222222222222",
        message: "wrong",
        entity_label_snapshot: "client supplied",
      },
      "Request body contains unknown field: entity_label_snapshot",
    ],
  ])("rejects invalid content report payload %#", async (payload, message) => {
    const { parseContentReportBodyValue } = await import("../../src/api/apiValidation.js");
    expect(() => parseContentReportBodyValue(payload)).toThrow(message);
  });

  it.each([
    [
      {
        entity_type: "candidate",
        entity_id: "22222222-2222-4222-8222-222222222222",
        message: "a".repeat(2001),
      },
      "message must be at most 2000 characters",
    ],
    [
      {
        entity_type: "candidate",
        entity_id: "22222222-2222-4222-8222-222222222222",
        message: "wrong",
        suggested_source_url: `https://example.org/${"a".repeat(2040)}`,
      },
      "suggested_source_url must be at most 2048 characters",
    ],
    [
      {
        entity_type: "candidate",
        entity_id: "22222222-2222-4222-8222-222222222222",
        message: "wrong",
        reporter_email: `${"a".repeat(310)}@example.org`,
      },
      "reporter_email must be at most 320 characters",
    ],
  ])("rejects oversized content report payload %#", async (payload, message) => {
    const { parseContentReportBodyValue } = await import("../../src/api/apiValidation.js");
    expect(() => parseContentReportBodyValue(payload)).toThrow(message);
  });
});
