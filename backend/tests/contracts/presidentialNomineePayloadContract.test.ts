import { describe, expect, it } from "vitest";

import { parsePresidentialNomineePayload } from "../../src/contracts/presidentialNomineePayloadContract.js";

describe("parsePresidentialNomineePayload", () => {
  it("parses and normalizes a found nominee claim", () => {
    const parsed = parsePresidentialNomineePayload({
      nominee_found: true,
      candidate_name: " Jane President ",
      fec_candidate_id: " p80000001 ",
      sources: [" https://example.org/nominee ", "https://example.org/nominee"],
    });

    expect(parsed).toEqual({
      ok: true,
      payload: {
        nominee_found: true,
        candidate_name: "Jane President",
        fec_candidate_id: "P80000001",
        sources: ["https://example.org/nominee"],
      },
    });
  });

  it("parses a no-nominee result with checked sources", () => {
    const parsed = parsePresidentialNomineePayload({
      nominee_found: false,
      candidate_name: "Ignored Candidate",
      fec_candidate_id: "P80000001",
      sources: ["https://example.org/primary-status"],
    });

    expect(parsed).toEqual({
      ok: true,
      payload: {
        nominee_found: false,
        sources: ["https://example.org/primary-status"],
      },
    });
  });

  it("allows a found nominee without an FEC ID", () => {
    const parsed = parsePresidentialNomineePayload({
      nominee_found: true,
      candidate_name: "Jane President",
      sources: ["https://example.org/nominee"],
    });

    expect(parsed.ok).toBe(true);
    if (parsed.ok) {
      expect(parsed.payload).not.toHaveProperty("fec_candidate_id");
    }
  });

  it("rejects invalid found nominee rows", () => {
    expect(
      parsePresidentialNomineePayload({
        nominee_found: true,
        sources: ["https://example.org/nominee"],
      }).ok
    ).toBe(false);

    expect(
      parsePresidentialNomineePayload({
        nominee_found: true,
        candidate_name: "Jane President",
        fec_candidate_id: "H0CA00001",
        sources: ["https://example.org/nominee"],
      }).ok
    ).toBe(false);

    expect(
      parsePresidentialNomineePayload({
        nominee_found: true,
        candidate_name: "Jane President",
        fec_candidate_id: "PABCDEFGH",
        sources: ["https://example.org/nominee"],
      }).ok
    ).toBe(false);
  });

  it("rejects invalid base payload shape and sources", () => {
    expect(parsePresidentialNomineePayload(null).ok).toBe(false);
    expect(parsePresidentialNomineePayload({}).ok).toBe(false);
    expect(
      parsePresidentialNomineePayload({
        nominee_found: false,
        sources: [],
      }).ok
    ).toBe(false);
    expect(
      parsePresidentialNomineePayload({
        nominee_found: false,
        sources: ["not a url"],
      }).ok
    ).toBe(false);
  });

  it("rejects a nominee determination sourced from a blocked platform domain", () => {
    const parsed = parsePresidentialNomineePayload({
      nominee_found: true,
      candidate_name: "Jane President",
      sources: ["https://x.com/party/status/789"],
    });

    expect(parsed.ok).toBe(false);
    if (parsed.ok) {
      return;
    }
    expect(parsed.reason).toContain("payload.sources:");
    expect(parsed.reason).toContain("user-generated/social platform");
  });
});
