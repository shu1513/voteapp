import { describe, expect, it } from "vitest";
import { parseStateResourceGroupPayloadFromAi } from "../../src/ai/stateResourcePayloadValidation.ts";

describe("parseStateResourceGroupPayloadFromAi", () => {
  it("parses online_registration group and normalizes deadline to null when unavailable", () => {
    const result = parseStateResourceGroupPayloadFromAi(
      {
        online_registration_available: false,
        online_registration_deadline_rule: "not applicable",
        sources: {
          online_registration_available: ["https://vote.gov/register/california"],
          online_registration_deadline_rule: ["https://vote.gov/register/california"],
        },
      },
      "online_registration"
    );

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.payload.online_registration_available).toBe(false);
      expect(result.payload.online_registration_deadline_rule).toBeNull();
    }
  });

  it("rejects mail group when return type is missing while available is true", () => {
    const result = parseStateResourceGroupPayloadFromAi(
      {
        mail_voting_available: true,
        mail_ballot_request_deadline_rule: "Request by 7 days before Election Day.",
        mail_ballot_return_deadline_rule: "Ballots must be received by Election Day.",
        mail_ballot_return_deadline_type: null,
        sources: {
          mail_voting_available: ["https://vote.gov/register/california"],
          mail_ballot_request_deadline_rule: ["https://vote.gov/register/california"],
          mail_ballot_return_deadline_rule: ["https://vote.gov/register/california"],
          mail_ballot_return_deadline_type: ["https://vote.gov/register/california"],
        },
      },
      "mail"
    );

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("mail_ballot_return_deadline_type");
    }
  });

  it("requires id_requirements to be one of the allowed enum values", () => {
    const result = parseStateResourceGroupPayloadFromAi(
      {
        id_requirements: "ID required",
        sources: {
          id_requirements: ["https://www.ncsl.org/elections-and-campaigns/voter-id"],
        },
      },
      "id_requirements"
    );

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("allowed ID requirement categories");
    }
  });

  it("requires scoped source buckets for the group", () => {
    const result = parseStateResourceGroupPayloadFromAi(
      {
        polling_hours: "Polling places are open 7:00 a.m. to 8:00 p.m.",
        sources: {},
      },
      "polling_hours"
    );

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("sources.polling_hours");
    }
  });
});

