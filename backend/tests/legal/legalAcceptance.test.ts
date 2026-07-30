import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it, vi } from "vitest";

import {
  LEGAL_ACCEPTANCE_PRESENTATIONS,
  hashLegalAcceptanceEmail,
  recordLegalAcceptance,
} from "../../src/legal/legalAcceptance.js";
import {
  CURRENT_LEGAL_PRESENTATION_VERSION,
  CURRENT_TERMS_VERSION,
} from "../../src/constants/legal.js";

const EVENT_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const SUBJECT_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";

describe("legal acceptance evidence", () => {
  it("writes exact immutable presentation evidence and hashes account email", async () => {
    const acceptedAt = new Date("2026-07-29T20:00:00.000Z");
    const query = vi.fn().mockResolvedValue({
      rows: [{
        id: EVENT_ID,
        account_user_id: null,
        account_email_sha256: null,
        anonymous_subject_id: SUBJECT_ID,
        context: "anonymous_search",
        document_bundle_version: CURRENT_TERMS_VERSION,
        presentation_version: CURRENT_LEGAL_PRESENTATION_VERSION,
        acceptance_text: LEGAL_ACCEPTANCE_PRESENTATIONS.anonymous_search.acceptanceText,
        action_text: "Agree and search",
        accepted_at: acceptedAt,
      }],
    });

    await expect(recordLegalAcceptance({ query } as never, {
      eventId: EVENT_ID,
      anonymousSubjectId: SUBJECT_ID,
      termsVersion: CURRENT_TERMS_VERSION,
      presentationVersion: CURRENT_LEGAL_PRESENTATION_VERSION,
      context: "anonymous_search",
      clientIp: "203.0.113.4",
      userAgent: "Test Browser",
      origin: "https://electionssimplified.com",
    })).resolves.toEqual({ id: EVENT_ID, acceptedAt: acceptedAt.toISOString() });

    const params = query.mock.calls[0][1] as unknown[];
    expect(params).toContain(LEGAL_ACCEPTANCE_PRESENTATIONS.anonymous_search.acceptanceText);
    expect(params).toContain("Agree and search");
    expect(params).toContain("203.0.113.4");
    expect(hashLegalAcceptanceEmail(" Voter@Example.com ")).toBe(
      createHash("sha256").update("voter@example.com").digest("hex")
    );
  });

  it("rejects stale document or presentation versions before touching DB", async () => {
    const query = vi.fn();
    await expect(recordLegalAcceptance({ query } as never, {
      eventId: EVENT_ID,
      anonymousSubjectId: SUBJECT_ID,
      termsVersion: "stale",
      presentationVersion: CURRENT_LEGAL_PRESENTATION_VERSION,
      context: "anonymous_search",
    })).rejects.toThrow("current terms version");
    expect(query).not.toHaveBeenCalled();
  });
});

describe("legal bundle archive integrity", () => {
  it("migration hashes match the exact published legal sources", () => {
    const root = fileURLToPath(new URL("../../../", import.meta.url));
    const migration = readFileSync(`${root}db/migrations/201_add_immutable_legal_acceptance_events.sql`, "utf8");
    for (const relativePath of [
      "docs/legal/terms-of-use.md",
      "docs/legal/privacy-policy.md",
      "docs/legal/disclaimer.md",
      "docs/legal/checkbox-copy.md",
    ]) {
      const hash = createHash("sha256").update(readFileSync(`${root}${relativePath}`)).digest("hex");
      expect(migration, `${relativePath} hash missing from bundle migration`).toContain(hash);
    }
  });
});
