import { describe, expect, it } from "vitest";
import {
  normalizePhoenixTextKey,
  phoenixPersonNameMatchesCandidate,
  resolvePhoenixCandidateCommittees,
  type PhoenixAppCandidate,
} from "../../../src/pipeline/phoenixFinance/phoenixCandidateCommitteeResolver.js";
import type { PhoenixRegistrationRow } from "../../../src/pipeline/phoenixFinance/phoenixEfilingClient.js";

// Fixtures mirror live canonical registrations (2026-08-12).
const hermesRegistration: PhoenixRegistrationRow = {
  copId: "CAN-25-4",
  committeeName: "Ed Hermes for Phoenix",
  committeeType: "Candidate Committee",
  candidateName: "Ed Hermes",
  electionCycle: "2025 Election Cycle",
  officeSoughtElectionCycle: "2026",
  terminated: false,
  approved: true,
  approvedTimestamp: 1_748_632_357_450,
  isStandingCommittee: false,
};

const hermesCandidate: PhoenixAppCandidate = {
  candidateId: "c-hermes",
  displayName: "Ed Hermes",
  officeName: "City Council Member",
  districtNumber: 4,
  electionYear: 2026,
  electionDate: "2026-11-03",
  stateFilingIds: ["CAN-25-4"],
};

function resolveOne(
  candidate: PhoenixAppCandidate,
  committees: PhoenixRegistrationRow[],
  covers?: ReadonlyMap<string, readonly string[]>,
) {
  return resolvePhoenixCandidateCommittees({
    candidates: [candidate],
    committees,
    coverOfficeSoughtByCopId: covers,
  })[0]!;
}

describe("resolvePhoenixCandidateCommittees — COP-id tier", () => {
  it("matches a stored COP id against its confirming registration", () => {
    const resolution = resolveOne(hermesCandidate, [hermesRegistration]);
    expect(resolution).toMatchObject({
      status: "matched",
      copId: "CAN-25-4",
      committeeName: "Ed Hermes for Phoenix",
      portalCycleName: "2025 Election Cycle",
      matchedBy: "cop_id",
    });
  });

  it("accepts a prior-cycle registration whose office-sought year targets this election (Jimenez)", () => {
    // Live: CAN-23-5 is a 2023-cycle registration with
    // OfficeSoughtElectionCycle "2026" — the ElectionCycle display string is
    // never cycle evidence.
    const resolution = resolveOne(
      {
        ...hermesCandidate,
        displayName: "Patricia Jimenez",
        stateFilingIds: ["CAN-23-5"],
      },
      [
        {
          ...hermesRegistration,
          copId: "CAN-23-5",
          committeeName: "Patricia Jimenez for Phoenix City Council District 4",
          candidateName: "Patricia Jimenez",
          electionCycle: "2023 Election Cycle",
        },
      ],
    );
    expect(resolution).toMatchObject({
      status: "matched",
      copId: "CAN-23-5",
      portalCycleName: "2023 Election Cycle",
      matchedBy: "cop_id",
    });
  });

  it("matches a March-runoff election to the same portal cycle's registration", () => {
    const resolution = resolveOne(
      { ...hermesCandidate, electionYear: 2027, electionDate: "2027-03-09" },
      [hermesRegistration],
    );
    expect(resolution).toMatchObject({ status: "matched", copId: "CAN-25-4" });
  });

  it("fails closed when the stored id's registration is terminated — never falls through to the name tier", () => {
    // A name-matching, cover-corroborated sibling exists; a contradicted
    // curated id must still stop resolution for manual review.
    const sibling: PhoenixRegistrationRow = {
      ...hermesRegistration,
      copId: "CAN-27-1",
      committeeName: "Hermes for Phoenix",
    };
    const resolution = resolveOne(
      hermesCandidate,
      [{ ...hermesRegistration, terminated: true }, sibling],
      new Map([["CAN-27-1", ["Council Member District 4"]]]),
    );
    expect(resolution).toMatchObject({
      status: "unmatched",
      reason: expect.stringContaining("CAN-25-4 is terminated"),
    });
  });

  it("fails closed when the stored id points at a PAC registration", () => {
    const resolution = resolveOne(
      { ...hermesCandidate, stateFilingIds: ["PAC-22-14"] },
      [
        {
          ...hermesRegistration,
          copId: "PAC-22-14",
          committeeName: "Phoenix First Super PAC",
          committeeType: "Political Action Committee",
          candidateName: null,
        },
      ],
    );
    expect(resolution).toMatchObject({
      status: "unmatched",
      reason: expect.stringContaining('"Political Action Committee"'),
    });
  });

  it("fails closed when the stored id has no registration in the index", () => {
    const resolution = resolveOne(hermesCandidate, []);
    expect(resolution).toMatchObject({
      status: "unmatched",
      reason: expect.stringContaining(
        "CAN-25-4 has no approved registration",
      ),
    });
  });

  it("fails closed when the registration CandidateName does not match", () => {
    const resolution = resolveOne(hermesCandidate, [
      { ...hermesRegistration, candidateName: "Kate Gallego" },
    ]);
    expect(resolution).toMatchObject({
      status: "unmatched",
      reason: expect.stringContaining('"Kate Gallego" does not match'),
    });
  });

  it("fails closed on a stale office-sought year", () => {
    const resolution = resolveOne(hermesCandidate, [
      { ...hermesRegistration, officeSoughtElectionCycle: "2022" },
    ]);
    expect(resolution).toMatchObject({
      status: "unmatched",
      reason: expect.stringContaining('office-sought cycle "2022"'),
    });
  });

  it("fails closed when the committee name carries a contradicting district", () => {
    const resolution = resolveOne(hermesCandidate, [
      { ...hermesRegistration, committeeName: "Ed Hermes for District 2" },
    ]);
    expect(resolution).toMatchObject({
      status: "unmatched",
      reason: expect.stringContaining("contradictory district/office evidence"),
    });
  });

  it("matches a comma-suffix CandidateName (live Maupin form) and still vetoes a generation swap", () => {
    // CAN-26-3 live: CandidateName "Jarrett Barton Maupin, Jr." — the comma
    // form parses to nothing in the shared parser without the rewrite.
    const maupin: PhoenixAppCandidate = {
      ...hermesCandidate,
      displayName: "Jarrett Barton Maupin Jr.",
      districtNumber: 8,
      stateFilingIds: ["CAN-26-3"],
    };
    const registration: PhoenixRegistrationRow = {
      ...hermesRegistration,
      copId: "CAN-26-3",
      committeeName: "Maupin for Phoenix",
      candidateName: "Jarrett Barton Maupin, Jr.",
    };
    expect(resolveOne(maupin, [registration])).toMatchObject({
      status: "matched",
      copId: "CAN-26-3",
      matchedBy: "cop_id",
    });
    // The rewrite must keep the suffix visible to the generational veto.
    expect(
      resolveOne(maupin, [
        { ...registration, candidateName: "Jarrett Barton Maupin, Sr." },
      ]),
    ).toMatchObject({ status: "unmatched" });
  });

  it("reports ambiguity when two stored ids both confirm", () => {
    const resolution = resolveOne(
      { ...hermesCandidate, stateFilingIds: ["CAN-25-4", "CAN-24-1"] },
      [
        hermesRegistration,
        { ...hermesRegistration, copId: "CAN-24-1" },
      ],
    );
    expect(resolution).toMatchObject({
      status: "ambiguous",
      reason: expect.stringContaining("2 registrations"),
    });
  });

  it("ignores non-COP-shaped filing ids instead of misreading them", () => {
    // An AZ SOS id on the person row is not Phoenix evidence; with no cover
    // data the name tier then fails closed.
    const resolution = resolveOne(
      { ...hermesCandidate, stateFilingIds: ["100512"] },
      [hermesRegistration],
    );
    expect(resolution).toMatchObject({
      status: "unmatched",
      reason: expect.stringContaining("name-only evidence fails closed"),
    });
  });
});

describe("resolvePhoenixCandidateCommittees — name tier", () => {
  const noIdCandidate: PhoenixAppCandidate = {
    ...hermesCandidate,
    stateFilingIds: [],
  };

  it("matches on CandidateName only with a corroborating report cover", () => {
    const resolution = resolveOne(
      noIdCandidate,
      [hermesRegistration],
      new Map([["CAN-25-4", ["Council Member District 4"]]]),
    );
    expect(resolution).toMatchObject({
      status: "matched",
      copId: "CAN-25-4",
      matchedBy: "name",
    });
  });

  it("fails closed without a parsed report cover (plan rule)", () => {
    const resolution = resolveOne(noIdCandidate, [hermesRegistration]);
    expect(resolution).toMatchObject({
      status: "unmatched",
      reason: expect.stringContaining("name-only evidence fails closed"),
    });
  });

  it("fails closed when the cover contradicts the district", () => {
    const resolution = resolveOne(
      noIdCandidate,
      [hermesRegistration],
      new Map([["CAN-25-4", ["Council Member District 5"]]]),
    );
    expect(resolution).toMatchObject({
      status: "unmatched",
      reason: expect.stringContaining("does not corroborate this contest"),
    });
  });

  it("skips the terminated prior-cycle committee of the same person silently", () => {
    // Hermes CAN-23-7 (terminated, 2023 cycle) is the expected sibling of
    // CAN-25-4 — an affirmative "not this contest" signal, not a blocker.
    const resolution = resolveOne(
      noIdCandidate,
      [
        {
          ...hermesRegistration,
          copId: "CAN-23-7",
          electionCycle: "2023 Election Cycle",
          officeSoughtElectionCycle: "2024",
          terminated: true,
        },
        hermesRegistration,
      ],
      new Map([["CAN-25-4", ["Council Member District 4"]]]),
    );
    expect(resolution).toMatchObject({ status: "matched", copId: "CAN-25-4" });
  });

  it("expands roster-side nicknames (Matt ↔ Matthew) through the shared gates", () => {
    const resolution = resolveOne(
      { ...noIdCandidate, displayName: "Matt Evans", districtNumber: 2 },
      [
        {
          ...hermesRegistration,
          copId: "CAN-22-10",
          committeeName: "Matt For District 2",
          candidateName: "Matthew Evans",
        },
      ],
      new Map([["CAN-22-10", ["Council Member District 2"]]]),
    );
    expect(resolution).toMatchObject({ status: "matched", copId: "CAN-22-10" });
  });

  it("vetoes a generational-suffix conflict", () => {
    const resolution = resolveOne(
      { ...noIdCandidate, displayName: "Frank Abasciano Jr.", districtNumber: 8 },
      [
        {
          ...hermesRegistration,
          copId: "CAN-26-1",
          committeeName: "Abasciano for Phoenix",
          candidateName: "Frank Abasciano Sr.",
        },
      ],
      new Map([["CAN-26-1", ["Council Member District 8"]]]),
    );
    expect(resolution).toMatchObject({ status: "unmatched" });
  });

  it("never auto-picks a corroborated match over an unresolved name-matching sibling", () => {
    const resolution = resolveOne(
      noIdCandidate,
      [
        hermesRegistration,
        { ...hermesRegistration, copId: "CAN-25-99", committeeName: "Hermes 2026" },
      ],
      new Map([["CAN-25-4", ["Council Member District 4"]]]),
    );
    expect(resolution).toMatchObject({
      status: "unmatched",
      reason: expect.stringContaining("CAN-25-99"),
    });
  });

  it("corroborates a Mayor candidate only on a mayoral cover", () => {
    const mayorRegistration: PhoenixRegistrationRow = {
      ...hermesRegistration,
      copId: "CAN-25-50",
      committeeName: "Hermes for Phoenix",
    };
    const mayorCandidate: PhoenixAppCandidate = {
      ...noIdCandidate,
      officeName: "Mayor",
      districtNumber: null,
    };
    expect(
      resolveOne(
        mayorCandidate,
        [mayorRegistration],
        new Map([["CAN-25-50", ["Council Member District 4"]]]),
      ),
    ).toMatchObject({ status: "unmatched" });
    expect(
      resolveOne(
        mayorCandidate,
        [mayorRegistration],
        new Map([["CAN-25-50", ["Mayor"]]]),
      ),
    ).toMatchObject({ status: "matched", copId: "CAN-25-50" });
  });

  it("fails a council candidate without a valid district number", () => {
    const resolution = resolveOne(
      { ...noIdCandidate, districtNumber: null },
      [hermesRegistration],
    );
    expect(resolution).toMatchObject({
      status: "unmatched",
      reason: expect.stringContaining("no valid district number"),
    });
  });
});

describe("resolvePhoenixCandidateCommittees — cross-candidate guard", () => {
  it("fails both candidates closed when one committee resolves to both", () => {
    const resolutions = resolvePhoenixCandidateCommittees({
      candidates: [
        hermesCandidate,
        { ...hermesCandidate, candidateId: "c-duplicate" },
      ],
      committees: [hermesRegistration],
    });
    expect(resolutions.map((resolution) => resolution.status)).toEqual([
      "ambiguous",
      "ambiguous",
    ]);
    expect(resolutions[0]).toMatchObject({
      reason: expect.stringContaining("multiple roster candidates"),
    });
  });
});

describe("phoenixPersonNameMatchesCandidate / normalizePhoenixTextKey", () => {
  it("matches token-wise, never substring", () => {
    expect(phoenixPersonNameMatchesCandidate("Ed Hermes", "Ed Hermes")).toBe(true);
    expect(phoenixPersonNameMatchesCandidate("Hermes", "Ed Hermes")).toBe(false);
  });

  it("treats a bare V as a middle initial, not a generational suffix", () => {
    // Bare "V" is a middle initial, not a suffix (the shared
    // GENERATIONAL_SUFFIX_RANK policy deliberately excludes it), so it must
    // stay as middle evidence on either side instead of being stripped.
    expect(
      phoenixPersonNameMatchesCandidate("Smith, John B.", "John V. Smith"),
    ).toBe(false);
    expect(
      phoenixPersonNameMatchesCandidate("Smith, John V", "John B. Smith"),
    ).toBe(false);
    expect(
      phoenixPersonNameMatchesCandidate("Smith, John V", "John V. Smith"),
    ).toBe(true);
    expect(
      phoenixPersonNameMatchesCandidate("Smith, John V", "John Smith"),
    ).toBe(true);
  });

  it("normalizes accents, punctuation, and whitespace", () => {
    expect(normalizePhoenixTextKey("  Jiménez,  Patricia ")).toBe(
      "JIMENEZ PATRICIA",
    );
  });
});
