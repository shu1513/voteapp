import { describe, expect, it } from "vitest";

import { parseCandidateRosterPayload } from "../../src/contracts/candidateRosterPayloadContract.js";

describe("parseCandidateRosterPayload", () => {
  it("parses valid candidate roster payload", () => {
    const parsed = parseCandidateRosterPayload({
      candidates: [
        {
          display_name: "Jane Doe",
          party: "Independent",
          is_incumbent: true,
          state_filing_ids: ["CA-123"],
          sources: ["https://example.org/a", "https://example.org/a"],
        },
      ],
    });

    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }

    expect(parsed.payload.candidates).toEqual([
      {
        display_name: "Jane Doe",
        party: "Independent",
        is_incumbent: true,
        state_filing_ids: ["CA-123"],
        sources: ["https://example.org/a"],
      },
    ]);
  });

  it("rejects candidate row without sources", () => {
    const parsed = parseCandidateRosterPayload({
      candidates: [{ display_name: "Jane Doe", sources: [] }],
    });

    expect(parsed.ok).toBe(false);
    if (parsed.ok) {
      return;
    }
    expect(parsed.reason).toBe("payload.candidates[0]: row.sources must contain at least one valid URL");
  });

  it("preserves duplicate candidate display names within one payload", () => {
    const parsed = parseCandidateRosterPayload({
      candidates: [
        {
          display_name: "Jane Doe",
          party: "Independent",
          sources: ["https://example.org/a"],
        },
        {
          display_name: "  JANE   DOE ",
          party: "Party X",
          sources: ["https://example.org/b"],
        },
      ],
    });

    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }

    expect(parsed.payload.candidates).toEqual([
      {
        display_name: "Jane Doe",
        party: "Independent",
        sources: ["https://example.org/a"],
      },
      {
        display_name: "JANE   DOE",
        party: "Party X",
        sources: ["https://example.org/b"],
      },
    ]);
  });

  it("filters federal roster candidates without fec_ids and reports them", () => {
    const parsed = parseCandidateRosterPayload(
      {
        candidates: [
          {
            display_name: "No Fec Filer",
            sources: ["https://example.org/a"],
          },
          {
            display_name: "Jane Doe",
            fec_ids: ["S0XX00001"],
            sources: ["https://example.org/b"],
          },
          {
            display_name: "Unregistered Runner",
            sources: ["https://example.org/c"],
          },
          {
            display_name: "John Roe",
            fec_ids: ["S0XX00002"],
            sources: ["https://example.org/d"],
          },
        ],
      },
      { requireFecIds: true, allowFecIds: true }
    );

    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }
    expect(parsed.payload.candidates.map((candidate) => candidate.display_name)).toEqual([
      "Jane Doe",
      "John Roe",
    ]);
    expect(parsed.skippedCandidatesWithoutFecIds).toEqual(["No Fec Filer", "Unregistered Runner"]);
    expect(parsed.keptCandidateIndexes).toEqual([1, 3]);
  });

  it("fails a federal roster when no candidate has a fec_id", () => {
    const parsed = parseCandidateRosterPayload(
      {
        candidates: [
          {
            display_name: "Jane Doe",
            sources: ["https://example.org/a"],
          },
        ],
      },
      { requireFecIds: true, allowFecIds: true }
    );

    expect(parsed.ok).toBe(false);
    if (parsed.ok) {
      return;
    }
    expect(parsed.reason).toBe(
      "payload.candidates: no candidate has a FEC ID for this federal contest (skipped: Jane Doe)"
    );
  });

  it("does not filter and reports no skips when fec_ids are not required", () => {
    const parsed = parseCandidateRosterPayload({
      candidates: [
        {
          display_name: "Jane Doe",
          sources: ["https://example.org/a"],
        },
      ],
    });

    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }
    expect(parsed.payload.candidates).toHaveLength(1);
    expect(parsed.skippedCandidatesWithoutFecIds).toEqual([]);
    expect(parsed.keptCandidateIndexes).toEqual([0]);
  });

  it("accepts fec_ids for federal roster mode", () => {
    const parsed = parseCandidateRosterPayload(
      {
        candidates: [
          {
            display_name: "Jane Doe",
            fec_ids: ["H0XX00000"],
            sources: ["https://example.org/a"],
          },
        ],
      },
      { requireFecIds: true, allowFecIds: true }
    );

    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }
    expect(parsed.payload.candidates[0]?.fec_ids).toEqual(["H0XX00000"]);
  });

  it("rejects fec_ids when allowFecIds is disabled (state-level mode)", () => {
    const parsed = parseCandidateRosterPayload(
      {
        candidates: [
          {
            display_name: "Jane Doe",
            fec_ids: ["H0XX00000"],
            sources: ["https://example.org/a"],
          },
        ],
      },
      { allowFecIds: false }
    );

    expect(parsed.ok).toBe(false);
    if (parsed.ok) {
      return;
    }
    expect(parsed.reason).toBe("payload.candidates[0]: row.fec_ids is not allowed for this election context");
  });

  it("parses joint-ticket running mates and rejects malformed ones", () => {
    const parsed = parseCandidateRosterPayload({
      candidates: [
        {
          display_name: "Begich, Tom",
          party: "Democrat",
          running_mate: {
            display_name: "Hnilicka, Julia",
            party: "Democrat",
            sources: ["https://www.elections.alaska.gov/candidates/?election=26prim"],
          },
          sources: ["https://www.elections.alaska.gov/candidates/?election=26prim"],
        },
      ],
    });

    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }
    expect(parsed.payload.candidates[0]?.running_mate).toEqual({
      display_name: "Hnilicka, Julia",
      party: "Democrat",
      sources: ["https://www.elections.alaska.gov/candidates/?election=26prim"],
    });

    const missingSources = parseCandidateRosterPayload({
      candidates: [
        {
          display_name: "Begich, Tom",
          running_mate: { display_name: "Hnilicka, Julia" },
          sources: ["https://example.org/a"],
        },
      ],
    });
    expect(missingSources.ok).toBe(false);
    if (!missingSources.ok) {
      expect(missingSources.reason).toBe(
        "payload.candidates[0]: row.running_mate.sources must contain at least one valid URL"
      );
    }

    const sameName = parseCandidateRosterPayload({
      candidates: [
        {
          display_name: "Begich, Tom",
          running_mate: {
            display_name: "  begich, tom ",
            sources: ["https://example.org/a"],
          },
          sources: ["https://example.org/a"],
        },
      ],
    });
    expect(sameName.ok).toBe(false);
    if (!sameName.ok) {
      expect(sameName.reason).toBe(
        "payload.candidates[0]: row.running_mate.display_name must differ from the ticket lead's display_name"
      );
    }
  });

  it("accepts optional state_filing_ids when present", () => {
    const parsed = parseCandidateRosterPayload({
      candidates: [
        {
          display_name: "Jane Doe",
          state_filing_ids: ["CA-777", "CA-777"],
          sources: ["https://example.org/a"],
        },
      ],
    });

    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }
    expect(parsed.payload.candidates[0]?.state_filing_ids).toEqual(["CA-777"]);
  });
});
