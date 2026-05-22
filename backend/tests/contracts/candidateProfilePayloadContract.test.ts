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
      sources: ["https://example.org/profile"],
    });

    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }

    expect(parsed.payload.twitter_handle).toBe("janedoe");
    expect(parsed.payload.official_website_url).toBe("https://janedoe.example.com");
    expect(parsed.payload.fec_ids).toEqual(["H0XX00000"]);
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
});
