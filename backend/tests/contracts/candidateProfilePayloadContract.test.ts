import { describe, expect, it } from "vitest";

import { parseCandidateProfilePayload } from "../../src/contracts/candidateProfilePayloadContract.js";

describe("parseCandidateProfilePayload", () => {
  it("parses valid candidate profile payload", () => {
    const parsed = parseCandidateProfilePayload({
      display_name: "Jane Doe",
      first_name: "Jane",
      last_name: "Doe",
      date_of_birth: "1980-01-01",
      twitter_handle: "@JaneDoe",
      linkedin_url: "https://www.linkedin.com/in/janedoe/",
      official_website_url: "https://janedoe.example.com/",
      fec_ids: ["H0XX00000"],
      state_filing_ids: ["SF-100"],
      current_office: "  Governor  ",
      sources: ["https://example.org/profile"],
    });

    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }

    expect(parsed.payload.twitter_handle).toBe("janedoe");
    expect(parsed.payload.official_website_url).toBe("https://janedoe.example.com");
    expect(parsed.payload.fec_ids).toEqual(["H0XX00000"]);
    expect(parsed.payload.current_office).toBe("Governor");
  });

  it("rejects blank current office", () => {
    const parsed = parseCandidateProfilePayload({
      display_name: "Jane Doe",
      first_name: "Jane",
      last_name: "Doe",
      current_office: "   ",
      sources: ["https://example.org/profile"],
    });

    expect(parsed.ok).toBe(false);
    if (parsed.ok) {
      return;
    }
    expect(parsed.reason).toBe("payload.current_office must be non-empty string when present");
  });

  it("accepts twitter profile URL and normalizes to handle", () => {
    const parsed = parseCandidateProfilePayload({
      display_name: "Jane Doe",
      first_name: "Jane",
      last_name: "Doe",
      twitter_handle: "https://x.com/Jane_Doe",
      sources: ["https://example.org/profile"],
    });

    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }

    expect(parsed.payload.twitter_handle).toBe("jane_doe");
  });

  it("rejects malformed twitter handle", () => {
    const parsed = parseCandidateProfilePayload({
      display_name: "Jane Doe",
      first_name: "Jane",
      last_name: "Doe",
      twitter_handle: "https://example.org/not-twitter",
      sources: ["https://example.org/profile"],
    });

    expect(parsed.ok).toBe(false);
  });

  it("rejects non-office website URL", () => {
    const parsed = parseCandidateProfilePayload({
      display_name: "Jane Doe",
      first_name: "Jane",
      last_name: "Doe",
      official_website_url: "not-a-url",
      sources: ["https://example.org/profile"],
    });

    expect(parsed.ok).toBe(false);
  });

  it("requires fec_ids in federal mode when configured", () => {
    const parsed = parseCandidateProfilePayload(
      {
        display_name: "Jane Doe",
        first_name: "Jane",
        last_name: "Doe",
        sources: ["https://example.org/profile"],
      },
      { requireFecIds: true, allowFecIds: true }
    );

    expect(parsed.ok).toBe(false);
    if (parsed.ok) {
      return;
    }
    expect(parsed.reason).toContain("fec_ids");
  });

  it("rejects fec_ids when mode disallows it", () => {
    const parsed = parseCandidateProfilePayload(
      {
        display_name: "Jane Doe",
        first_name: "Jane",
        last_name: "Doe",
        fec_ids: ["H0XX00000"],
        sources: ["https://example.org/profile"],
      },
      { allowFecIds: false }
    );

    expect(parsed.ok).toBe(false);
  });
});
