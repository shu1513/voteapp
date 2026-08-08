import { describe, expect, it } from "vitest";
import {
  normalizeSanFranciscoCandidateNameForStorage,
  resolveSanFranciscoContestCandidates,
  sanFranciscoCandidateNameMatches,
  sanFranciscoSyntheticSpenderId,
} from "../../src/pipeline/sanFranciscoFinance/sanFranciscoCandidateCommitteeResolver.js";
import type { SanFranciscoManifestCandidate } from "../../src/pipeline/sanFranciscoFinance/sanFranciscoDashboardManifestClient.js";

const manifestCandidate = (
  overrides: Partial<SanFranciscoManifestCandidate>,
): SanFranciscoManifestCandidate => ({
  filerNid: "216198377",
  fppcId: "1489126",
  committeeName: "ALAN WONG FOR SUPERVISOR 2026 GENERAL",
  candidateName: "ALAN WONG",
  fundsCents: 0,
  expensesCents: 0,
  ...overrides,
});

describe("sanFranciscoCandidateNameMatches", () => {
  // Every pair below is a real November 2026 ballot-name / manifest-name
  // shape (DoE qualified list vs SFEC dashboard frontmatter).
  it("matches the real manifest-vs-ballot skews", () => {
    expect(
      sanFranciscoCandidateNameMatches("Michael T. Nguyen", "MICHAEL NGUYEN"),
    ).toBe(true);
    expect(
      sanFranciscoCandidateNameMatches("Jeremy Julian Greco", "JEREMY GRECO"),
    ).toBe(true);
    expect(
      sanFranciscoCandidateNameMatches(
        'Emanuel "Manny" Yekutiel',
        "EMANUEL YEKUTIEL",
      ),
    ).toBe(true);
    expect(
      sanFranciscoCandidateNameMatches(
        "Dionjay (DJ) Brookter",
        "DION-JAY (DJ) BROOKTER",
      ),
    ).toBe(true);
    expect(sanFranciscoCandidateNameMatches("J.R. Eppler", "J.R. EPPLER")).toBe(
      true,
    );
    expect(
      sanFranciscoCandidateNameMatches(
        'Ellsworth "Ell" M. Jennison, Jr.',
        "ELLSWORTH JENNISON",
      ),
    ).toBe(true);
    expect(
      sanFranciscoCandidateNameMatches(
        'Pearci "PJ" Bastiany III',
        "PEARCI BASTIANY",
      ),
    ).toBe(true);
  });

  it("rejects middle-name conflicts and different people", () => {
    // Middle initial disagreement is another person, not a variant.
    expect(
      sanFranciscoCandidateNameMatches("Michael T. Nguyen", "MICHAEL J NGUYEN"),
    ).toBe(false);
    // A conflict past the first middle token still rejects: agreeing on
    // MICHAEL must not hide the ANDREW-vs-BERNARD disagreement.
    expect(
      sanFranciscoCandidateNameMatches(
        "John Michael Andrew Smith",
        "SMITH, JOHN MICHAEL BERNARD",
      ),
    ).toBe(false);
    // The two McCoys of November 2026 (D2 and D8) must never cross-match.
    expect(sanFranciscoCandidateNameMatches("Gary McCoy", "GUY MCCOY")).toBe(
      false,
    );
    expect(sanFranciscoCandidateNameMatches("Alan Wong", "NATALIE GEE")).toBe(
      false,
    );
  });

  it("trusts the longest surname alignment for compound surnames", () => {
    // The bogus DYKE-surname split reads VAN-vs-B as a middle conflict; the
    // real VAN DYKE alignment is weak and must win.
    expect(
      sanFranciscoCandidateNameMatches("Mary Van Dyke", "MARY B VAN DYKE"),
    ).toBe(true);
    // A genuine conflict at the longest alignment still rejects.
    expect(
      sanFranciscoCandidateNameMatches("Mary C. Van Dyke", "MARY B VAN DYKE"),
    ).toBe(false);
  });
});

describe("normalizeSanFranciscoCandidateNameForStorage", () => {
  it("strips punctuation, nickname quotes, and suffix commas", () => {
    expect(
      normalizeSanFranciscoCandidateNameForStorage(
        'Ellsworth "Ell" M. Jennison, Jr.',
      ),
    ).toBe("ELLSWORTH ELL M JENNISON");
    expect(normalizeSanFranciscoCandidateNameForStorage("J.R. Eppler")).toBe(
      "J R EPPLER",
    );
  });
});

describe("sanFranciscoSyntheticSpenderId", () => {
  it("derives a stable name-based identity", () => {
    expect(
      sanFranciscoSyntheticSpenderId("GrowSF Supporting Alan Wong  2026"),
    ).toBe("name:GROWSF SUPPORTING ALAN WONG 2026");
  });
  it("rejects an unusable committee name", () => {
    expect(() => sanFranciscoSyntheticSpenderId(" -- ")).toThrow(
      /no usable name/,
    );
  });
});

describe("resolveSanFranciscoContestCandidates", () => {
  const wong = {
    candidateId: "cand-wong",
    displayName: "Alan Wong",
    stateFilingIds: ["1489126"],
  };
  const chow = {
    candidateId: "cand-chow",
    displayName: "Albert Chow",
    stateFilingIds: ["1492163", "1485609"],
  };
  const greco = {
    candidateId: "cand-greco",
    displayName: "Jeremy Julian Greco",
    stateFilingIds: [],
  };

  it("resolves the real bos04 contest: id match, name match, expected unmatched", () => {
    const resolutions = resolveSanFranciscoContestCandidates({
      manifestCandidates: [
        manifestCandidate({}),
        manifestCandidate({
          filerNid: "216135683",
          fppcId: "1490199",
          committeeName: "GEE FOR SUPERVISOR 2026",
          candidateName: "NATALIE GEE",
        }),
        manifestCandidate({
          filerNid: "216781160",
          fppcId: "1491969",
          committeeName: "GRECO FOR SUPERVISOR 2026",
          candidateName: "JEREMY GRECO",
        }),
      ],
      appCandidates: [wong, chow, greco],
    });
    expect(resolutions[0]).toMatchObject({
      status: "matched",
      candidateId: "cand-wong",
      matchedBy: "fppc_id",
    });
    // Gee formed a committee but never qualified for the ballot.
    expect(resolutions[1]).toMatchObject({ status: "unmatched" });
    // Greco's roster row carries no id in this fixture — name path.
    expect(resolutions[2]).toMatchObject({
      status: "matched",
      candidateId: "cand-greco",
      matchedBy: "name",
    });
  });

  it("prefers the FPPC id over a contradicting name", () => {
    const resolutions = resolveSanFranciscoContestCandidates({
      manifestCandidates: [
        manifestCandidate({ candidateName: "SOMEONE ELSE ENTIRELY" }),
      ],
      appCandidates: [wong, chow],
    });
    expect(resolutions[0]).toMatchObject({
      status: "matched",
      candidateId: "cand-wong",
      matchedBy: "fppc_id",
    });
  });

  it("fails closed when a manifest name matches two candidates", () => {
    const resolutions = resolveSanFranciscoContestCandidates({
      manifestCandidates: [
        manifestCandidate({ fppcId: "1400000", candidateName: "MICHAEL NGUYEN" }),
      ],
      appCandidates: [
        {
          candidateId: "cand-a",
          displayName: "Michael Nguyen",
          stateFilingIds: [],
        },
        {
          candidateId: "cand-b",
          displayName: "Michael T. Nguyen",
          stateFilingIds: [],
        },
      ],
    });
    expect(resolutions[0]).toMatchObject({ status: "ambiguous" });
  });

  it("fails closed when two manifest committees resolve to one candidate", () => {
    const resolutions = resolveSanFranciscoContestCandidates({
      manifestCandidates: [
        manifestCandidate({ fppcId: "1400001", candidateName: "ALAN WONG" }),
        manifestCandidate({ fppcId: "1400002", candidateName: "ALAN WONG" }),
      ],
      appCandidates: [
        { candidateId: "cand-wong", displayName: "Alan Wong", stateFilingIds: [] },
      ],
    });
    expect(resolutions[0]).toMatchObject({ status: "ambiguous" });
    expect(resolutions[1]).toMatchObject({ status: "ambiguous" });
  });

  it("fails closed when one FPPC id sits on two candidates", () => {
    const resolutions = resolveSanFranciscoContestCandidates({
      manifestCandidates: [manifestCandidate({})],
      appCandidates: [
        wong,
        {
          candidateId: "cand-dupe",
          displayName: "Alan A. Wong",
          stateFilingIds: ["1489126"],
        },
      ],
    });
    expect(resolutions[0]).toMatchObject({ status: "ambiguous" });
  });
});
