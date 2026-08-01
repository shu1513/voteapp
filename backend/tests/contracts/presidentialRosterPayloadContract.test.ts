import { describe, expect, it } from "vitest";

import { parsePresidentialRosterPayload } from "../../src/contracts/presidentialRosterPayloadContract.js";

describe("parsePresidentialRosterPayload", () => {
  it("parses and normalizes valid presidential roster candidates", () => {
    const parsed = parsePresidentialRosterPayload(
      {
        candidates: [
          {
            display_name: " Jane President ",
            party: "Democrat",
            fec_candidate_id: " p80000001 ",
            sources: ["https://example.org/a", "https://example.org/a"],
            qualification_evidence: [
              {
                kind: "official_campaign_website",
                source_url: "https://jane.example.org",
                description: "Official campaign website",
              },
              {
                kind: "official_campaign_website",
                source_url: "https://jane.example.org",
                description: "duplicate is ignored",
              },
            ],
            status: "ACTIVE",
            running_mate: {
              display_name: " Pat Running Mate ",
              fec_candidate_id: " p80000002 ",
              sources: ["https://example.org/mate", "https://example.org/mate"],
            },
          },
          {
            display_name: "Pat Suspended",
            party: "Democratic",
            fec_candidate_id: "P80000003",
            sources: ["https://example.org/b"],
            qualification_evidence: [
              {
                kind: "public_campaign_launch",
                source_url: "https://example.org/pat-launch",
              },
            ],
            status: "withdrawn",
          },
        ],
      },
      { expectedParty: "Democratic" }
    );

    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }

    expect(parsed.payload.candidates).toEqual([
      {
        display_name: "Jane President",
        party: "Democrat",
        fec_candidate_id: "P80000001",
        sources: ["https://example.org/a"],
        qualification_evidence: [
          {
            kind: "official_campaign_website",
            source_url: "https://jane.example.org",
            description: "Official campaign website",
          },
        ],
        status: "active",
        running_mate: {
          display_name: "Pat Running Mate",
          fec_candidate_id: "P80000002",
          sources: ["https://example.org/mate"],
        },
      },
      {
        display_name: "Pat Suspended",
        party: "Democratic",
        fec_candidate_id: "P80000003",
        sources: ["https://example.org/b"],
        qualification_evidence: [
          {
            kind: "public_campaign_launch",
            source_url: "https://example.org/pat-launch",
          },
        ],
        status: "withdrawn",
      },
    ]);
  });

  it("rejects wrong-party candidates when expected party is configured", () => {
    const parsed = parsePresidentialRosterPayload(
      {
        candidates: [
          {
            display_name: "Wrong Primary",
            party: "Republican",
            fec_candidate_id: "P80000001",
            sources: ["https://example.org/a"],
            qualification_evidence: [
              {
                kind: "official_campaign_website",
                source_url: "https://wrong.example.org",
              },
            ],
            status: "active",
          },
        ],
      },
      { expectedParty: "Democratic" }
    );

    expect(parsed.ok).toBe(false);
    expect(parsed.ok ? "" : parsed.reason).toContain("does not match expected party Democratic");
  });

  it("accepts Republican aliases for Republican primary parsing", () => {
    const parsed = parsePresidentialRosterPayload(
      {
        candidates: [
          {
            display_name: "Jane GOP",
            party: "GOP",
            fec_candidate_id: "P80000001",
            sources: ["https://example.org/a"],
            qualification_evidence: [
              {
                kind: "official_campaign_website",
                source_url: "https://gop.example.org",
              },
            ],
            status: "active",
          },
        ],
      },
      { expectedParty: "Republican" }
    );

    expect(parsed.ok).toBe(true);
  });

  it("rejects invalid rows", () => {
    expect(
      parsePresidentialRosterPayload({
        candidates: [
          {
            display_name: "Jane President",
            party: "Democratic",
            fec_candidate_id: "P80000001",
            sources: [],
            qualification_evidence: [
              {
                kind: "official_campaign_website",
                source_url: "https://jane.example.org",
              },
            ],
            status: "active",
          },
        ],
      }).ok
    ).toBe(false);

    expect(
      parsePresidentialRosterPayload({
        candidates: [
          {
            display_name: "Jane President",
            party: "Democratic",
            fec_candidate_id: "P80000001",
            sources: ["https://example.org/a"],
            qualification_evidence: [
              {
                kind: "official_campaign_website",
                source_url: "https://jane.example.org",
              },
            ],
            status: "maybe",
          },
        ],
      }).ok
    ).toBe(false);

    expect(
      parsePresidentialRosterPayload({
        candidates: [
          {
            display_name: "Jane President",
            party: "Democratic",
            fec_candidate_id: "H0CA00001",
            sources: ["https://example.org/a"],
            qualification_evidence: [
              {
                kind: "official_campaign_website",
                source_url: "https://jane.example.org",
              },
            ],
            status: "active",
          },
        ],
      }).ok
    ).toBe(false);

    expect(
      parsePresidentialRosterPayload({
        candidates: [
          {
            display_name: "Jane President",
            party: "Democratic",
            fec_candidate_id: "PABCDEFGH",
            sources: ["https://example.org/a"],
            qualification_evidence: [
              {
                kind: "official_campaign_website",
                source_url: "https://jane.example.org",
              },
            ],
            status: "active",
          },
        ],
      }).ok
    ).toBe(false);

    expect(
      parsePresidentialRosterPayload({
        candidates: [
          {
            display_name: "Jane President",
            party: "Democratic",
            fec_candidate_id: "P80000001",
            sources: ["https://example.org/a"],
            qualification_evidence: [
              {
                kind: "official_campaign_website",
                source_url: "https://jane.example.org",
              },
            ],
            status: "active",
            running_mate: {
              display_name: "Pat Running Mate",
              fec_candidate_id: "S80000002",
              sources: ["https://example.org/mate"],
            },
          },
        ],
      }).ok
    ).toBe(false);
  });

  it("filters ineligible candidates and reports them when eligible candidates remain", () => {
    const parsed = parsePresidentialRosterPayload({
      candidates: [
        {
          display_name: "Jane President",
          party: "Democratic",
          fec_candidate_id: "P80000001",
          sources: ["https://example.org/a"],
          qualification_evidence: [
            {
              kind: "official_campaign_website",
              source_url: "https://jane.example.org",
            },
          ],
          status: "active",
        },
        {
          display_name: "Never Registered",
          party: "Democratic",
          sources: ["https://example.org/b"],
          qualification_evidence: [
            {
              kind: "official_campaign_website",
              source_url: "https://never.example.org",
            },
          ],
          status: "active",
        },
        {
          display_name: "Fec Only Filer",
          party: "Democratic",
          fec_candidate_id: "P80000002",
          sources: ["https://example.org/c"],
          status: "active",
        },
      ],
    });

    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }
    expect(parsed.payload.candidates.map((candidate) => candidate.display_name)).toEqual(["Jane President"]);
    expect(parsed.skippedIneligibleCandidates).toEqual([
      { display_name: "Never Registered", reason: "missing_fec_candidate_id" },
      { display_name: "Fec Only Filer", reason: "missing_qualification_evidence" },
    ]);
  });

  it("fails when no candidate is FEC-registered with qualification evidence", () => {
    const parsed = parsePresidentialRosterPayload({
      candidates: [
        {
          display_name: "No FEC",
          party: "Democratic",
          sources: ["https://example.org/a"],
          qualification_evidence: [
            {
              kind: "official_campaign_website",
              source_url: "https://nofec.example.org",
            },
          ],
          status: "active",
        },
      ],
    });

    expect(parsed.ok).toBe(false);
    expect(parsed.ok ? "" : parsed.reason).toBe(
      "payload.candidates: no candidate is FEC-registered with qualification evidence (skipped: No FEC (missing_fec_candidate_id))"
    );
  });

  it("keeps rejecting present-but-invalid FEC evidence as hard failures", () => {

    const fecOnly = parsePresidentialRosterPayload({
      candidates: [
        {
          display_name: "FEC Only",
          party: "Democratic",
          fec_candidate_id: "P80000001",
          sources: ["https://www.fec.gov/data/candidate/P80000001"],
          qualification_evidence: [
            {
              kind: "official_campaign_website",
              source_url: "https://www.fec.gov/data/candidate/P80000001",
            },
          ],
          status: "active",
        },
      ],
    });

    expect(fecOnly.ok).toBe(false);
    expect(fecOnly.ok ? "" : fecOnly.reason).toContain("qualification_evidence");
  });

  it("rejects running mates with invalid source URLs", () => {
    const parsed = parsePresidentialRosterPayload({
      candidates: [
        {
          display_name: "Jane President",
          party: "Democratic",
          fec_candidate_id: "P80000001",
          sources: ["https://example.org/a"],
          qualification_evidence: [
            {
              kind: "official_campaign_website",
              source_url: "https://jane.example.org",
            },
          ],
          status: "active",
          running_mate: {
            display_name: "Pat Running Mate",
            sources: ["not a url"],
          },
        },
      ],
    });

    expect(parsed.ok).toBe(false);
    expect(parsed.ok ? "" : parsed.reason).toContain("candidate.running_mate.sources");
  });

  it("rejects non-object payloads and missing candidates array", () => {
    expect(parsePresidentialRosterPayload(null).ok).toBe(false);
    expect(parsePresidentialRosterPayload({}).ok).toBe(false);
  });

  it("rejects a candidate sourced from a blocked platform domain", () => {
    const parsed = parsePresidentialRosterPayload({
      candidates: [
        {
          display_name: "Jane President",
          party: "Democratic",
          fec_candidate_id: "P80000001",
          sources: ["https://x.com/janeprez/status/123"],
          qualification_evidence: [
            { kind: "official_campaign_website", source_url: "https://jane.example.org" },
          ],
          status: "active",
        },
      ],
    });

    expect(parsed.ok).toBe(false);
    expect(parsed.ok ? "" : parsed.reason).toContain("candidate.sources:");
    expect(parsed.ok ? "" : parsed.reason).toContain("user-generated/social platform");
  });

  it("rejects qualification evidence sourced from a blocked platform domain", () => {
    // A campaign launch announced on social media is citable via news
    // coverage, never via the platform post itself.
    const parsed = parsePresidentialRosterPayload({
      candidates: [
        {
          display_name: "Jane President",
          party: "Democratic",
          fec_candidate_id: "P80000001",
          sources: ["https://news.example.org/jane-runs"],
          qualification_evidence: [
            { kind: "public_campaign_launch", source_url: "https://www.youtube.com/watch?v=abc123" },
          ],
          status: "active",
        },
      ],
    });

    expect(parsed.ok).toBe(false);
    expect(parsed.ok ? "" : parsed.reason).toContain("candidate.qualification_evidence:");
    expect(parsed.ok ? "" : parsed.reason).toContain("user-generated/social platform");
  });

  it("rejects a running mate sourced from a blocked platform domain", () => {
    const parsed = parsePresidentialRosterPayload({
      candidates: [
        {
          display_name: "Jane President",
          party: "Democratic",
          fec_candidate_id: "P80000001",
          sources: ["https://news.example.org/jane-runs"],
          qualification_evidence: [
            { kind: "official_campaign_website", source_url: "https://jane.example.org" },
          ],
          status: "active",
          running_mate: {
            display_name: "Pat Running Mate",
            sources: ["https://www.facebook.com/patmate/posts/456"],
          },
        },
      ],
    });

    expect(parsed.ok).toBe(false);
    expect(parsed.ok ? "" : parsed.reason).toContain("candidate.running_mate.sources:");
    expect(parsed.ok ? "" : parsed.reason).toContain("user-generated/social platform");
  });
});
