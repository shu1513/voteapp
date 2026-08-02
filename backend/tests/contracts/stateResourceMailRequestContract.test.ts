import { describe, expect, it } from "vitest";
import { parseCanonicalStateResourcePayload } from "../../src/contracts/stateResourcePayloadContract.ts";

const OFFICIAL_MAIL_PAGE = "https://www.sos.wa.gov/elections/voters/vote-mail";

function validPayload(overrides: Record<string, unknown> = {}) {
  const base: Record<string, unknown> = {
    state_fips: "53",
    state_abbreviation: "WA",
    state_name: "Washington",
    polling_place_url: "https://voter.votewa.gov/portal2023/login.aspx",
    voter_registration_url: "https://vote.gov/register",
    mail_voting_available: true,
    mail_ballot_request_url: OFFICIAL_MAIL_PAGE,
    mail_ballot_request_type: "not_required",
    mail_ballot_request_deadline_rule: null,
    mail_ballot_return_deadline_rule:
      "Washington mail ballots must be postmarked by Election Day or returned to a drop box by 8 p.m.",
    mail_ballot_return_deadline_type: "postmarked_by",
    early_voting_available: true,
    early_voting_start_date_rule:
      "Washington voting begins when ballots are mailed about 18 days before Election Day.",
    early_voting_end_date_rule: "Washington voting ends at 8 p.m. on Election Day.",
    polling_hours: "Voting centers are generally open during business hours and until 8 p.m. on Election Day.",
    id_requirements: "Non-strict, non-photo ID",
    same_day_registration_available: true,
    online_registration_available: true,
    online_registration_deadline_rule:
      "Washington online voter registration closes 8 days before Election Day.",
    in_person_registration_deadline_rule:
      "Washington in-person registration is available through 8 p.m. on Election Day.",
    sources: {} as Record<string, string[]>,
  };

  const sources: Record<string, string[]> = {};
  for (const key of [
    "polling_place_url",
    "mail_voting_available",
    "mail_ballot_request_url",
    "mail_ballot_request_type",
    "mail_ballot_request_deadline_rule",
    "mail_ballot_return_deadline_rule",
    "mail_ballot_return_deadline_type",
    "early_voting_available",
    "early_voting_start_date_rule",
    "early_voting_end_date_rule",
    "polling_hours",
    "id_requirements",
    "same_day_registration_available",
    "online_registration_available",
    "online_registration_deadline_rule",
    "in_person_registration_deadline_rule",
  ]) {
    sources[key] = [OFFICIAL_MAIL_PAGE];
  }
  base.sources = sources;

  return { ...base, ...overrides };
}

describe("parseCanonicalStateResourcePayload mail-ballot request fields", () => {
  it("accepts an automatic vote-by-mail payload (not_required with explanatory URL)", () => {
    const result = parseCanonicalStateResourcePayload(validPayload());
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.payload.mail_ballot_request_url).toBe(OFFICIAL_MAIL_PAGE);
      expect(result.payload.mail_ballot_request_type).toBe("not_required");
    }
  });

  it("rejects a payload missing mail_ballot_request_url", () => {
    const payload = validPayload();
    delete (payload as Record<string, unknown>).mail_ballot_request_url;
    const result = parseCanonicalStateResourcePayload(payload);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("mail_ballot_request_url");
    }
  });

  it("rejects null request URL while mail voting is available", () => {
    const result = parseCanonicalStateResourcePayload(validPayload({ mail_ballot_request_url: null }));
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("mail_ballot_request_url");
    }
  });

  it("rejects an unknown request type", () => {
    const result = parseCanonicalStateResourcePayload(validPayload({ mail_ballot_request_type: "portal" }));
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("mail_ballot_request_type");
    }
  });

  it("rejects a request deadline rule on a not_required jurisdiction", () => {
    const result = parseCanonicalStateResourcePayload(
      validPayload({
        mail_ballot_request_deadline_rule: "Requests must be received 7 days before Election Day.",
      })
    );
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("not_required");
    }
  });

  it("requires request fields to be null when mail voting is unavailable", () => {
    const result = parseCanonicalStateResourcePayload(
      validPayload({
        mail_voting_available: false,
        mail_ballot_request_deadline_rule: null,
        mail_ballot_return_deadline_rule: null,
        mail_ballot_return_deadline_type: null,
      })
    );
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("mail_ballot_request_url must be null");
    }
  });
});
