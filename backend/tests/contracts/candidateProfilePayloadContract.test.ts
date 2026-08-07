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
      has_held_public_office: true,
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
    expect(parsed.payload.has_held_public_office).toBe(true);
  });

  it("strips trailing roster footnote markers from names", () => {
    // State rosters mark incumbency with a trailing asterisk. Scraped
    // verbatim it reached display_name and was served to voters as part of
    // the person's name — 366 stored candidates across CT and MN carry one.
    const parsed = parseCandidateProfilePayload({
      display_name: "Jill Oberlander *",
      first_name: "Jill",
      last_name: "Oberlander *",
      has_held_public_office: true,
      sources: ["https://example.org/profile"],
    });

    expect(parsed.ok).toBe(true);
    if (parsed.ok) {
      expect(parsed.payload.display_name).toBe("Jill Oberlander");
      expect(parsed.payload.last_name).toBe("Oberlander");
    }
  });

  it("leaves legitimate name punctuation and suffixes alone", () => {
    // The stripper must not become a general name cleaner: apostrophes,
    // hyphens, accents, lowercase particles and generational suffixes are all
    // part of real stored names.
    const parsed = parseCandidateProfilePayload({
      display_name: "Paul Cicarella, Jr.",
      first_name: "Paul",
      last_name: "Cicarella, Jr.",
      has_held_public_office: true,
      sources: ["https://example.org/profile"],
    });

    expect(parsed.ok).toBe(true);
    if (parsed.ok) {
      expect(parsed.payload.display_name).toBe("Paul Cicarella, Jr.");
      expect(parsed.payload.last_name).toBe("Cicarella, Jr.");
    }

    for (const name of ["Tom O'Dea", "Cara Pavalock-D'Amato", "Aundr\u00e9 Bumgardner", "Joe de la Cruz"]) {
      const kept = parseCandidateProfilePayload({
        display_name: name,
        first_name: "X",
        last_name: name,
        has_held_public_office: false,
        sources: ["https://example.org/profile"],
      });
      expect(kept.ok, name).toBe(true);
      if (kept.ok) {
        expect(kept.payload.display_name, name).toBe(name);
      }
    }
  });

  it("rejects a name that is only footnote markers", () => {
    const parsed = parseCandidateProfilePayload({
      display_name: "*",
      first_name: "*",
      last_name: "*",
      has_held_public_office: false,
      sources: ["https://example.org/profile"],
    });

    expect(parsed.ok).toBe(false);
    if (!parsed.ok) {
      expect(parsed.reason).toContain("not only footnote markers");
    }
  });

  it("rejects a first_name that is the wreckage of a bad name split", () => {
    // Live rows: first_name "Franks," with last_name "Jr." (the given name is
    // absent from the source), and first_name "(Butch)" where a nickname
    // displaced it. Rejected rather than normalised — the right value cannot
    // be derived from what is present.
    const trailingComma = parseCandidateProfilePayload({
      display_name: "Franks, Jr.",
      first_name: "Franks,",
      last_name: "Jr.",
      has_held_public_office: false,
      sources: ["https://example.org/profile"],
    });
    expect(trailingComma.ok).toBe(false);
    if (!trailingComma.ok) {
      expect(trailingComma.reason).toContain("split wrongly");
    }

    const nickname = parseCandidateProfilePayload({
      display_name: "(Butch) Lawter, Jr.",
      first_name: "(Butch)",
      last_name: "Lawter, Jr.",
      has_held_public_office: false,
      sources: ["https://example.org/profile"],
    });
    expect(nickname.ok).toBe(false);
    if (!nickname.ok) {
      expect(nickname.reason).toContain("parenthetical nickname");
    }
  });

  it("requires has_held_public_office as true, false, or explicit null", () => {
    const base = {
      display_name: "Jane Doe",
      first_name: "Jane",
      last_name: "Doe",
      sources: ["https://example.org/profile"],
    };

    // An omitted key is not the same answer as null: null asserts "I checked
    // and no cited source carries office history", omission asserts nothing.
    const missing = parseCandidateProfilePayload(base);
    expect(missing.ok).toBe(false);
    if (!missing.ok) {
      expect(missing.reason).toContain("has_held_public_office");
    }

    const wrongType = parseCandidateProfilePayload({ ...base, has_held_public_office: "true" });
    expect(wrongType.ok).toBe(false);

    const falseAnswer = parseCandidateProfilePayload({ ...base, has_held_public_office: false });
    expect(falseAnswer.ok).toBe(true);
    if (falseAnswer.ok) {
      expect(falseAnswer.payload.has_held_public_office).toBe(false);
    }

    // Sources without an office-history field cannot support false — a
    // partisan-race aggregator page manufactured false for nonpartisan local
    // officeholders. null is the sanctioned answer for that pass.
    const sourcesSilent = parseCandidateProfilePayload({ ...base, has_held_public_office: null });
    expect(sourcesSilent.ok).toBe(true);
    if (sourcesSilent.ok) {
      expect(sourcesSilent.payload.has_held_public_office).toBeNull();
    }
  });

  it("rejects current_office unless has_held_public_office is true", () => {
    const base = {
      display_name: "Jane Doe",
      first_name: "Jane",
      last_name: "Doe",
      sources: ["https://example.org/profile"],
    };

    const contradiction = parseCandidateProfilePayload({
      ...base,
      current_office: "Mayor",
      has_held_public_office: false,
    });
    expect(contradiction.ok).toBe(false);
    if (!contradiction.ok) {
      expect(contradiction.reason).toContain('current_office ("Mayor") requires has_held_public_office=true');
    }

    // A payload that names a current office has already answered the routing
    // question — null alongside it under-claims the payload's own fact.
    const underClaim = parseCandidateProfilePayload({
      ...base,
      current_office: "Mayor",
      has_held_public_office: null,
    });
    expect(underClaim.ok).toBe(false);
    if (!underClaim.ok) {
      expect(underClaim.reason).toContain("requires has_held_public_office=true (got null)");
    }

    const officeholder = parseCandidateProfilePayload({
      ...base,
      current_office: "Mayor",
      has_held_public_office: true,
    });
    expect(officeholder.ok).toBe(true);

    const formerOfficeholder = parseCandidateProfilePayload({
      ...base,
      has_held_public_office: true,
    });
    expect(formerOfficeholder.ok).toBe(true);
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
      has_held_public_office: false,
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

  it("rejects a blocked platform domain used as a citation source", () => {
    const parsed = parseCandidateProfilePayload({
      display_name: "Jane Doe",
      first_name: "Jane",
      last_name: "Doe",
      has_held_public_office: true,
      sources: ["https://www.linkedin.com/in/janedoe/"],
    });

    expect(parsed.ok).toBe(false);
    if (parsed.ok) {
      return;
    }
    expect(parsed.reason).toContain("payload.sources:");
    expect(parsed.reason).toContain("linkedin.com");
    expect(parsed.reason).toContain("user-generated/social platform");
  });

  it("still accepts linkedin_url as a profile link field", () => {
    // The domain policy gates CITATIONS only. linkedin_url /
    // official_website_url / twitter_handle are profile link fields, not
    // evidence, and must stay importable.
    const parsed = parseCandidateProfilePayload({
      display_name: "Jane Doe",
      first_name: "Jane",
      last_name: "Doe",
      linkedin_url: "https://www.linkedin.com/in/janedoe/",
      has_held_public_office: true,
      sources: ["https://example.org/profile"],
    });

    expect(parsed.ok).toBe(true);
  });

  it("accepts a summary that follows the role/credentials/priorities formula", () => {
    const parsed = parseCandidateProfilePayload({
      display_name: "Jane Doe",
      first_name: "Jane",
      last_name: "Doe",
      has_held_public_office: false,
      summary:
        "Community organizer in South Los Angeles for about 20 years and co-director of ACCE's LA chapter. Priorities: tenant protections and more affordable housing.",
      sources: ["https://example.org/profile"],
    });

    expect(parsed.ok).toBe(true);
    if (parsed.ok) {
      expect(parsed.payload.summary).toContain("Community organizer");
    }
  });

  it("rejects a summary over the length cap", () => {
    const parsed = parseCandidateProfilePayload({
      display_name: "Jane Doe",
      first_name: "Jane",
      last_name: "Doe",
      has_held_public_office: false,
      summary: "A ".repeat(200).trim(),
      sources: ["https://example.org/profile"],
    });

    expect(parsed.ok).toBe(false);
    if (parsed.ok) {
      return;
    }
    expect(parsed.reason).toContain("max 300");
    expect(parsed.reason).toContain("2 sentences");
  });

  it("rejects horse-race content in the summary", () => {
    // The live over-cap example also violated the horse-race ban: office,
    // runoff, and vote share all next to a contest card that already shows
    // the race.
    const horseRaceSummaries = [
      "Longtime organizer running for City Council District 9.",
      "State legislator seeking re-election after two terms.",
      "Advanced to the November runoff against another Democrat.",
      "Won about 26% in the June primary.",
      "Won 26 percent of the vote in June.",
      "She won 52%.",
      "She lost the runoff.",
      "She advanced with 26%.",
      "She won 52% in the U.S. Senate primary.",
      "Community advocate who received nearly 31 percent of the vote.",
    ];
    for (const summary of horseRaceSummaries) {
      const parsed = parseCandidateProfilePayload({
        display_name: "Jane Doe",
        first_name: "Jane",
        last_name: "Doe",
        has_held_public_office: false,
        summary,
        sources: ["https://example.org/profile"],
      });
      expect(parsed.ok, summary).toBe(false);
      if (!parsed.ok) {
        expect(parsed.reason).toContain("horse-race");
      }
    }
  });

  it("does not false-positive the horse-race patterns on biography wording", () => {
    // Percentages and "runoff" are horse-race only inside a result
    // construction: biography statistics (even about voters or elections),
    // policy runoff, and article-carrying stat claims ("secured a 40%
    // increase") must all survive.
    const biographySummaries = [
      "Primary care physician and clinic director. Priorities: lowering the percentage of uninsured residents and expanding rural clinics.",
      "Pediatrician who cut uninsured rates by 15% in her county. Priorities: expanding rural clinics and lowering drug costs.",
      "Farmer and county water-board member. Priorities: reducing agricultural runoff and improving drinking-water quality.",
      "Former election commissioner. Cut clinic costs by 20% as hospital administrator.",
      "Registered 20% more voters as election commissioner.",
      "Primary-care physician who reduced uninsured rates by 15%.",
      "Reduced agricultural runoff by 20% countywide as water-board chair.",
      "Won a state grant to curb stormwater runoff. Advanced legislation to protect wetlands.",
      "Secured a 40% increase in park funding as council aide.",
    ];
    for (const summary of biographySummaries) {
      const parsed = parseCandidateProfilePayload({
        display_name: "Jane Doe",
        first_name: "Jane",
        last_name: "Doe",
        has_held_public_office: false,
        summary,
        sources: ["https://example.org/profile"],
      });
      expect(parsed.ok, summary).toBe(true);
    }
  });

  it("accepts three short sentences under the cap — sentence count is prompt guidance only", () => {
    // Enforcing a sentence count mechanically would false-reject legitimate
    // abbreviations ("St. Paul", "Jr."); the 300-character cap is the
    // enforceable proxy.
    const parsed = parseCandidateProfilePayload({
      display_name: "Jane Doe",
      first_name: "Jane",
      last_name: "Doe",
      has_held_public_office: false,
      summary: "High-school teacher in St. Paul. Former school-board member. Priorities: smaller classes and safer streets.",
      sources: ["https://example.org/profile"],
    });

    expect(parsed.ok).toBe(true);
  });
});
