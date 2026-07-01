import { describe, expect, it } from "vitest";

import { buildInjectedCandidateRosterStagingPayload } from "../../src/scripts/injectManualCandidateRoster.js";

describe("buildInjectedCandidateRosterStagingPayload", () => {
  it("preserves raw roster identity hints when staging parsed injected rosters", () => {
    const payload = buildInjectedCandidateRosterStagingPayload({
      electionId: "election-1",
      rawPayload: {
        candidates: [
          {
            display_name: "Jane Candidate",
            roster_index: 7,
            disambiguation_hint: "  incumbent on official filing  ",
            skip_per_election_name_dedupe: true,
            ignored_raw_field: "drop me",
          },
          {
            display_name: "John Candidate",
            roster_index: -1,
            disambiguation_hint: "   ",
            skip_per_election_name_dedupe: "yes",
          },
        ],
      },
      candidates: [
        {
          display_name: "Jane Candidate",
          sources: ["https://example.org/jane"],
        },
        {
          display_name: "John Candidate",
          sources: ["https://example.org/john"],
        },
      ],
    });

    expect(payload).toEqual({
      election_id: "election-1",
      candidates: [
        {
          display_name: "Jane Candidate",
          sources: ["https://example.org/jane"],
          roster_index: 7,
          disambiguation_hint: "incumbent on official filing",
          skip_per_election_name_dedupe: true,
        },
        {
          display_name: "John Candidate",
          sources: ["https://example.org/john"],
        },
      ],
    });
  });
});
