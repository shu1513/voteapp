import { describe, expect, it } from "vitest";

import { parsePresidentialRosterStatusPayload } from "../../src/contracts/presidentialRosterStatusPayloadContract.js";

describe("parsePresidentialRosterStatusPayload", () => {
  it("parses, normalizes, and orders valid candidate status rows", () => {
    const parsed = parsePresidentialRosterStatusPayload(
      {
        candidates: [
          {
            candidate_id: "candidate-2",
            status: "UNKNOWN",
            sources: ["https://example.org/b", "https://example.org/b"],
            notes: " Could not confirm current status. ",
          },
          {
            candidate_id: "candidate-1",
            status: "withdrawn",
            sources: [" https://example.org/a "],
            notes: "Suspended campaign.",
          },
        ],
      },
      { expectedCandidateIds: ["candidate-1", "candidate-2"] }
    );

    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }

    expect(parsed.payload.candidates).toEqual([
      {
        candidate_id: "candidate-1",
        status: "withdrawn",
        sources: ["https://example.org/a"],
        notes: "Suspended campaign.",
      },
      {
        candidate_id: "candidate-2",
        status: "unknown",
        sources: ["https://example.org/b"],
        notes: "Could not confirm current status.",
      },
    ]);
  });

  it("rejects unknown candidate IDs", () => {
    const parsed = parsePresidentialRosterStatusPayload(
      {
        candidates: [
          {
            candidate_id: "candidate-x",
            status: "active",
            sources: ["https://example.org/a"],
            notes: "Still running.",
          },
        ],
      },
      { expectedCandidateIds: ["candidate-1"] }
    );

    expect(parsed.ok).toBe(false);
    expect(parsed.ok ? "" : parsed.reason).toContain("was not provided for verification");
  });

  it("rejects duplicate and missing candidate IDs", () => {
    const duplicate = parsePresidentialRosterStatusPayload(
      {
        candidates: [
          {
            candidate_id: "candidate-1",
            status: "active",
            sources: ["https://example.org/a"],
            notes: "Still running.",
          },
          {
            candidate_id: "candidate-1",
            status: "withdrawn",
            sources: ["https://example.org/b"],
            notes: "Suspended.",
          },
        ],
      },
      { expectedCandidateIds: ["candidate-1"] }
    );
    expect(duplicate.ok).toBe(false);
    expect(duplicate.ok ? "" : duplicate.reason).toContain("duplicate candidate_id candidate-1");

    const missing = parsePresidentialRosterStatusPayload(
      {
        candidates: [
          {
            candidate_id: "candidate-1",
            status: "active",
            sources: ["https://example.org/a"],
            notes: "Still running.",
          },
        ],
      },
      { expectedCandidateIds: ["candidate-1", "candidate-2"] }
    );
    expect(missing.ok).toBe(false);
    expect(missing.ok ? "" : missing.reason).toContain("payload is missing candidate_id rows: candidate-2");
  });

  it("rejects invalid status, sources, and notes", () => {
    expect(
      parsePresidentialRosterStatusPayload(
        {
          candidates: [
            {
              candidate_id: "candidate-1",
              status: "maybe",
              sources: ["https://example.org/a"],
              notes: "Still running.",
            },
          ],
        },
        { expectedCandidateIds: ["candidate-1"] }
      ).ok
    ).toBe(false);

    expect(
      parsePresidentialRosterStatusPayload(
        {
          candidates: [
            {
              candidate_id: "candidate-1",
              status: "active",
              sources: [],
              notes: "Still running.",
            },
          ],
        },
        { expectedCandidateIds: ["candidate-1"] }
      ).ok
    ).toBe(false);

    expect(
      parsePresidentialRosterStatusPayload(
        {
          candidates: [
            {
              candidate_id: "candidate-1",
              status: "active",
              sources: ["https://example.org/a"],
              notes: " ",
            },
          ],
        },
        { expectedCandidateIds: ["candidate-1"] }
      ).ok
    ).toBe(false);
  });

  it("rejects non-object payloads and empty expected candidate IDs", () => {
    expect(
      parsePresidentialRosterStatusPayload(null, { expectedCandidateIds: ["candidate-1"] }).ok
    ).toBe(false);
    expect(parsePresidentialRosterStatusPayload({}, { expectedCandidateIds: ["candidate-1"] }).ok).toBe(false);
    expect(parsePresidentialRosterStatusPayload({ candidates: [] }, { expectedCandidateIds: [] }).ok).toBe(false);
  });
});
