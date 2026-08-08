import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

import {
  georgiaCandidateNameMatchesRowNames,
  georgiaLastNameSearchToken,
  normalizeGeorgiaCandidateNameForStorage,
  normalizeGeorgiaCandidateNameKeys,
  resolveGeorgiaCandidateCommittee,
  searchAndResolveGeorgiaCandidateCommittee,
} from "../../../src/pipeline/georgiaFinance/georgiaCandidateCommitteeResolver.js";
import type {
  GeorgiaCandidateIndexRow,
  GeorgiaEthicsTransport,
} from "../../../src/pipeline/georgiaFinance/georgiaEthicsClient.js";

const FIXTURES_DIR = join(dirname(fileURLToPath(import.meta.url)), "../../fixtures/georgiaFinance");

// The PeachFile Carr fixture row, as the client parses it.
const CARR_ROW: GeorgiaCandidateIndexRow = {
  filerEntityId: 100035,
  filerRegistrationId: 35,
  guid: "d973ab3b-54c2-416e-81ce-f5b1ee9a6f57",
  filerName: "Carr, Christopher M.",
  committeeName: "Carr for Georgia, Inc.",
  candidateFirstName: "Christopher",
  candidateMiddleName: "Michael",
  candidateLastName: "Carr",
  ballotFullName: null,
  office: "Governor",
  districtName: null,
  filerStatusCode: "FACT",
  filingCycleName: "2026 Candidate/Committee Filing Cycle",
  electionCycleName: "2026 Georgia State Election",
  totalContributions: 5374711.06,
  totalExpenditures: 4168947.51,
  cashOnHand: 1167791.24,
};

const GOVERNOR_INPUT = {
  candidateName: "Christopher Carr",
  officeScope: "statewide",
  officeName: "Governor",
  electionYear: 2026,
  district: null,
};

describe("name normalization", () => {
  it("matches app-format and PeachFile comma-format names through shared keys", () => {
    const appKeys = normalizeGeorgiaCandidateNameKeys("Christopher Carr");
    const indexKeys = normalizeGeorgiaCandidateNameKeys("Carr, Christopher M.");
    expect([...appKeys].some((key) => indexKeys.has(key))).toBe(true);
  });

  it("derives the last-name search token from either name order", () => {
    expect(georgiaLastNameSearchToken("Christopher Carr")).toBe("CARR");
    expect(georgiaLastNameSearchToken("Carr, Christopher M.")).toBe("CARR");
  });

  it("produces a stable storage key", () => {
    expect(normalizeGeorgiaCandidateNameForStorage("Christopher  Carr Jr.")).toBe("CHRISTOPHER CARR");
  });
});

describe("georgiaCandidateNameMatchesRowNames (middle-name evidence)", () => {
  it("rejects conflicting middle initials even when first and last agree", () => {
    expect(georgiaCandidateNameMatchesRowNames("John A. Smith", ["Smith, John B."])).toBe(false);
    expect(georgiaCandidateNameMatchesRowNames("John Anthony Smith", ["Smith, John B."])).toBe(false);
  });

  it("accepts an initial that corroborates the full middle name", () => {
    expect(georgiaCandidateNameMatchesRowNames("John Anthony Smith", ["Smith, John A."])).toBe(true);
    expect(georgiaCandidateNameMatchesRowNames("Christopher Michael Carr", ["Carr, Christopher M."])).toBe(true);
  });

  it("falls back to first+last only when a side lacks middle information", () => {
    expect(georgiaCandidateNameMatchesRowNames("John Smith", ["Smith, John B."])).toBe(true);
    expect(georgiaCandidateNameMatchesRowNames("John A. Smith", ["Smith, John"])).toBe(true);
  });

  it("lets a middle conflict veto a middle-less variant of the same row", () => {
    // The row's ballot name lacks the middle, but its filerName carries a
    // conflicting one — the evidence must win over the weaker variant.
    expect(georgiaCandidateNameMatchesRowNames("John A. Smith", ["Smith, John B.", "John Smith"])).toBe(false);
  });

  it("rejects a conflict past the first middle token", () => {
    // The MICHAEL agreement must not short-circuit before ANDREW-vs-BERNARD
    // is compared — every shared middle position carries evidence.
    expect(
      georgiaCandidateNameMatchesRowNames("John Michael Andrew Smith", ["SMITH, JOHN MICHAEL BERNARD"])
    ).toBe(false);
    expect(georgiaCandidateNameMatchesRowNames("John Michael Andrew Smith", ["SMITH, JOHN MICHAEL A."])).toBe(
      true
    );
  });

  it("still matches multi-word surnames across name orders", () => {
    expect(georgiaCandidateNameMatchesRowNames("Mary Van Dyke", ["Van Dyke, Mary A."])).toBe(true);
  });

  it("judges middle evidence only at the longest aligned surname", () => {
    // Space-form "MARY B VAN DYKE" also emits a bogus DYKE-surname split that
    // reads VAN-vs-B as a middle conflict; the real VAN DYKE alignment wins.
    expect(georgiaCandidateNameMatchesRowNames("Mary Van Dyke", ["MARY B VAN DYKE"])).toBe(true);
  });
});

describe("resolveGeorgiaCandidateCommittee", () => {
  it("rejects a same-race row whose middle name contradicts the candidate", () => {
    // Same office, cycle, and district — only the middle evidence differs.
    // Without the middle gate this row would link as an "exact" match and
    // attach another person's money.
    const wrongPerson: GeorgiaCandidateIndexRow = {
      ...CARR_ROW,
      filerName: "Carr, Christopher B.",
      candidateMiddleName: "Bernard",
    };
    const resolution = resolveGeorgiaCandidateCommittee({
      ...GOVERNOR_INPUT,
      candidateName: "Christopher Michael Carr",
      candidateIndexRows: [wrongPerson],
    });
    expect(resolution).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });
  });

  it("uses the structured middle name even when the app name has no middle", () => {
    const resolution = resolveGeorgiaCandidateCommittee({
      ...GOVERNOR_INPUT,
      candidateName: "Christopher Carr",
      candidateIndexRows: [CARR_ROW],
    });
    expect(resolution).toMatchObject({ status: "matched", filerEntityId: "100035" });
  });

  it("matches Carr to the PeachFile registration on the fixture row", () => {
    const resolution = resolveGeorgiaCandidateCommittee({
      ...GOVERNOR_INPUT,
      candidateIndexRows: [CARR_ROW],
    });
    expect(resolution).toMatchObject({
      status: "matched",
      filerEntityId: "100035",
      registrationGuid: "d973ab3b-54c2-416e-81ce-f5b1ee9a6f57",
      committeeName: "Carr for Georgia, Inc.",
      confidence: "exact",
      source: "peachfile_candidate_index",
    });
  });

  it("rejects rows from another cycle", () => {
    const oldCycle: GeorgiaCandidateIndexRow = {
      ...CARR_ROW,
      filingCycleName: "2022 Candidate/Committee Filing Cycle",
      electionCycleName: "2022 Georgia State Election",
    };
    const resolution = resolveGeorgiaCandidateCommittee({
      ...GOVERNOR_INPUT,
      candidateIndexRows: [oldCycle],
    });
    expect(resolution).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });
  });

  it("rejects rows for a different office and a different person", () => {
    const wrongOffice = resolveGeorgiaCandidateCommittee({
      ...GOVERNOR_INPUT,
      candidateIndexRows: [{ ...CARR_ROW, office: "Attorney General" }],
    });
    expect(wrongOffice.status).toBe("unmatched");

    const wrongPerson = resolveGeorgiaCandidateCommittee({
      ...GOVERNOR_INPUT,
      candidateName: "Geoff Duncan",
      candidateIndexRows: [CARR_ROW],
    });
    expect(wrongPerson.status).toBe("unmatched");
  });

  it("returns ambiguous when two distinct registrations survive every gate", () => {
    const secondRegistration: GeorgiaCandidateIndexRow = {
      ...CARR_ROW,
      guid: "84070958-98cc-4ec5-a7de-de1757d712fc",
      filerEntityId: 200099,
      committeeName: "Another Carr Committee",
    };
    const resolution = resolveGeorgiaCandidateCommittee({
      ...GOVERNOR_INPUT,
      candidateIndexRows: [CARR_ROW, secondRegistration],
    });
    expect(resolution).toMatchObject({ status: "ambiguous", reason: "multiple_matching_registrations" });
    if (resolution.status === "ambiguous") {
      expect(resolution.matches).toHaveLength(2);
    }
  });

  it("accumulates duplicate rows for the same registration into one match", () => {
    const resolution = resolveGeorgiaCandidateCommittee({
      ...GOVERNOR_INPUT,
      candidateIndexRows: [CARR_ROW, { ...CARR_ROW }],
    });
    expect(resolution).toMatchObject({ status: "matched", matchedRowCount: 2 });
  });

  it("gates legislative offices on the district", () => {
    const senateRow: GeorgiaCandidateIndexRow = {
      ...CARR_ROW,
      office: "State Senator",
      districtName: "District 33",
    };
    const senateInput = {
      candidateName: "Christopher Carr",
      officeScope: "state_upper",
      officeName: "State Senator",
      electionYear: 2026,
    };

    expect(
      resolveGeorgiaCandidateCommittee({ ...senateInput, district: "33", candidateIndexRows: [senateRow] })
    ).toMatchObject({ status: "matched" });
    expect(
      resolveGeorgiaCandidateCommittee({ ...senateInput, district: "34", candidateIndexRows: [senateRow] })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });
    expect(
      resolveGeorgiaCandidateCommittee({ ...senateInput, district: null, candidateIndexRows: [senateRow] })
    ).toMatchObject({ status: "unmatched", reason: "missing_legislative_district" });
  });

  it("maps the state_lower app office onto PeachFile's State Representative label", () => {
    const houseRow: GeorgiaCandidateIndexRow = {
      ...CARR_ROW,
      office: "State Representative",
      districtName: "60",
      filerName: "Miller, Tanya",
      candidateFirstName: "Tanya",
      candidateLastName: "Miller",
      committeeName: "Committee to Elect Tanya Miller",
    };
    const resolution = resolveGeorgiaCandidateCommittee({
      candidateName: "Tanya Miller",
      officeScope: "state_lower",
      officeName: "State Lower Chamber Legislator",
      electionYear: 2026,
      district: "60",
      candidateIndexRows: [houseRow],
    });
    expect(resolution).toMatchObject({ status: "matched", committeeName: "Committee to Elect Tanya Miller" });
  });

  it("fails closed on offices outside the eligible set", () => {
    const resolution = resolveGeorgiaCandidateCommittee({
      candidateName: "Christopher Carr",
      officeScope: "county",
      officeName: "County Commissioner",
      electionYear: 2026,
      district: null,
      candidateIndexRows: [CARR_ROW],
    });
    expect(resolution).toMatchObject({ status: "unmatched", reason: "unsupported_office" });
  });
});

describe("searchAndResolveGeorgiaCandidateCommittee", () => {
  it("searches PeachFile by last-name token and resolves from the fixture response", async () => {
    const calls: Array<{ url: string; body: string }> = [];
    const transport: GeorgiaEthicsTransport = {
      postJson: async (url, body) => {
        calls.push({ url, body });
        const fixture = JSON.parse(readFileSync(join(FIXTURES_DIR, "peachfile_candidate_index_carr.json"), "utf-8"));
        return fixture.data;
      },
    };
    const resolution = await searchAndResolveGeorgiaCandidateCommittee(GOVERNOR_INPUT, transport);
    expect(resolution).toMatchObject({ status: "matched", filerEntityId: "100035" });
    expect(calls).toHaveLength(1);
    expect(calls[0]!.url).toContain("api-peachfile.ethics.ga.gov/api/PublicFilerDetails/GetCandidateDetails");
    expect(JSON.parse(calls[0]!.body)).toMatchObject({ filerTypeCode: "RC", filerName: "CARR" });
  });

  it("skips the network entirely for unsupported offices", async () => {
    const transport: GeorgiaEthicsTransport = {
      postJson: async () => {
        throw new Error("should not be called");
      },
    };
    const resolution = await searchAndResolveGeorgiaCandidateCommittee(
      { ...GOVERNOR_INPUT, officeScope: "county", officeName: "Sheriff" },
      transport
    );
    expect(resolution).toMatchObject({ status: "unmatched", reason: "unsupported_office" });
  });
});
