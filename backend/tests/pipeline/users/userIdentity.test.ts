import { describe, expect, it, vi } from "vitest";

import {
  acceptUserTerms,
  getUserIdentity,
  MAX_FIRST_NAME_LENGTH,
  setUserFirstName,
  UserIdentityError,
} from "../../../src/pipeline/users/userIdentity.js";
import { CURRENT_LEGAL_PRESENTATION_VERSION, CURRENT_TERMS_VERSION } from "../../../src/constants/legal.js";

const USER_ID = "11111111-1111-4111-8111-111111111111";
const IDENTITY = {
  email: "voter@example.com",
  first_name: "Val",
  email_verified: true,
  accepted_terms_version: CURRENT_TERMS_VERSION,
};
const ACCEPTANCE_EVIDENCE = {
  eventId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  anonymousSubjectId: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
  termsVersion: CURRENT_TERMS_VERSION,
  presentationVersion: CURRENT_LEGAL_PRESENTATION_VERSION,
};

describe("userIdentity", () => {
  it("getUserIdentity returns email, first_name, email_verified, and accepted_terms_version", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [IDENTITY], rowCount: 1 });

    await expect(getUserIdentity({ query } as never, USER_ID)).resolves.toEqual(IDENTITY);
    expect(String(query.mock.calls[0][0])).toContain("deleted_at IS NULL");
    expect(String(query.mock.calls[0][0])).toContain("accepted_terms_version");
    expect(query.mock.calls[0][1]).toEqual([USER_ID]);
  });

  it("throws user_not_found for unknown or deleted users", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 0 });

    await expect(getUserIdentity({ query } as never, USER_ID)).rejects.toMatchObject({
      code: "user_not_found",
    });
  });

  it("rejects a non-UUID userId without querying", async () => {
    const query = vi.fn();

    await expect(getUserIdentity({ query } as never, "bob")).rejects.toBeInstanceOf(UserIdentityError);
    expect(query).not.toHaveBeenCalled();
  });

  it("setUserFirstName trims, updates, and returns the identity", async () => {
    const updated = { ...IDENTITY, first_name: "Valerie" };
    const query = vi.fn().mockResolvedValue({ rows: [updated], rowCount: 1 });

    await expect(setUserFirstName({ query } as never, USER_ID, "  Valerie  ")).resolves.toEqual(updated);
    expect(query.mock.calls[0][1]).toEqual([USER_ID, "Valerie"]);
  });

  it("setUserFirstName rejects empty and over-long names without querying", async () => {
    const query = vi.fn();

    await expect(setUserFirstName({ query } as never, USER_ID, "   ")).rejects.toThrow(
      "first_name must be a non-empty string"
    );
    await expect(
      setUserFirstName({ query } as never, USER_ID, "x".repeat(MAX_FIRST_NAME_LENGTH + 1))
    ).rejects.toThrow(`at most ${MAX_FIRST_NAME_LENGTH} characters`);
    expect(query).not.toHaveBeenCalled();
  });

  it("setUserFirstName throws user_not_found for unknown or deleted users", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 0 });

    await expect(setUserFirstName({ query } as never, USER_ID, "Val")).rejects.toMatchObject({
      code: "user_not_found",
    });
  });

  it("acceptUserTerms stamps the version and acceptance timestamp", async () => {
    const query = vi.fn()
      .mockResolvedValueOnce({ rows: [IDENTITY], rowCount: 1 })
      .mockResolvedValueOnce({ rows: [{ id: ACCEPTANCE_EVIDENCE.eventId, accepted_at: new Date() }] });

    await expect(acceptUserTerms({ query } as never, USER_ID, ` ${CURRENT_TERMS_VERSION} `, ACCEPTANCE_EVIDENCE)).resolves.toEqual(IDENTITY);
    const sql = String(query.mock.calls[0][0]);
    expect(sql).toContain("accepted_terms_version = $2");
    expect(sql).toContain("accepted_terms_at = now()");
    expect(sql).toContain("deleted_at IS NULL");
    expect(query.mock.calls[0][1]).toEqual([USER_ID, CURRENT_TERMS_VERSION]);
    expect(String(query.mock.calls[1][0])).toContain("legal_acceptance_events");
  });

  it("acceptUserTerms rejects an empty version without querying", async () => {
    const query = vi.fn();

    await expect(acceptUserTerms({ query } as never, USER_ID, "   ", ACCEPTANCE_EVIDENCE)).rejects.toThrow(
      "termsVersion must be a non-empty string"
    );
    expect(query).not.toHaveBeenCalled();
  });

  it("acceptUserTerms throws user_not_found for unknown or deleted users", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 0 });

    await expect(acceptUserTerms({ query } as never, USER_ID, CURRENT_TERMS_VERSION, ACCEPTANCE_EVIDENCE)).rejects.toMatchObject({
      code: "user_not_found",
    });
  });
});
