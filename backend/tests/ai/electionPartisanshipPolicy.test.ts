import { describe, expect, it } from "vitest";

import {
  isJudicialOfficeTitle,
  isJudicialRetentionTitle,
  resolveCandidateContestPartisanshipByPolicy,
  resolveElectionIsPartisan,
  shouldIncludeCandidatePartyByPolicy,
  shouldAskIsPartisanInPrompt,
} from "../../src/ai/electionPartisanshipPolicy.js";

describe("electionPartisanshipPolicy", () => {
  it("never asks AI for ballot_measure family", () => {
    expect(
      shouldAskIsPartisanInPrompt({
        draft: {
          district_id: "d-1",
          district_name: "Los Angeles County, California",
          district_type: "county",
          state: "CA",
        },
        contestFamily: "ballot_measure",
      })
    ).toBe(false);
  });

  it("asks AI for mixed school states and not for fixed school states", () => {
    expect(
      shouldAskIsPartisanInPrompt({
        draft: {
          district_id: "d-1",
          district_name: "Demo Unified School District",
          district_type: "school_unified",
          state: "NC",
        },
        contestFamily: "all",
      })
    ).toBe(true);

    expect(
      shouldAskIsPartisanInPrompt({
        draft: {
          district_id: "d-1",
          district_name: "Demo Unified School District",
          district_type: "school_unified",
          state: "CA",
        },
        contestFamily: "all",
      })
    ).toBe(false);
  });

  it("never asks AI for judicial_office family", () => {
    expect(
      shouldAskIsPartisanInPrompt({
        draft: {
          district_id: "d-1",
          district_name: "Los Angeles County, California",
          district_type: "county",
          state: "CA",
        },
        contestFamily: "judicial_office",
      })
    ).toBe(false);
  });

  it("never asks AI for us_senate family and forces partisan true", () => {
    expect(
      shouldAskIsPartisanInPrompt({
        draft: {
          district_id: "d-1",
          district_name: "California",
          district_type: "statewide",
          state: "CA",
        },
        contestFamily: "us_senate",
      })
    ).toBe(false);

    const resolved = resolveElectionIsPartisan({
      draft: {
        district_id: "d-1",
        district_name: "California",
        district_type: "statewide",
        state: "CA",
      },
      contestFamily: "us_senate",
      raceType: "office",
      officialBallotTitle: "United States Senator",
      aiValue: undefined,
    });
    expect(resolved).toBe(true);
  });

  it("forces ballot_measure to nonpartisan", () => {
    const resolved = resolveElectionIsPartisan({
      draft: {
        district_id: "d-1",
        district_name: "California",
        district_type: "statewide",
        state: "CA",
      },
      contestFamily: "ballot_measure",
      raceType: "ballot_measure",
      officialBallotTitle: "Proposition 1",
      aiValue: true,
    });
    expect(resolved).toBe(false);
  });

  it("forces judicial contests by state policy", () => {
    const tx = resolveElectionIsPartisan({
      draft: {
        district_id: "d-1",
        district_name: "Texas",
        district_type: "statewide",
        state: "TX",
      },
      contestFamily: "judicial_office",
      raceType: "office",
      officialBallotTitle: "Judge of the Supreme Court",
      aiValue: undefined,
    });
    expect(tx).toBe(true);

    const ca = resolveElectionIsPartisan({
      draft: {
        district_id: "d-1",
        district_name: "California",
        district_type: "statewide",
        state: "CA",
      },
      contestFamily: "judicial_office",
      raceType: "office",
      officialBallotTitle: "Judge of the Superior Court",
      aiValue: true,
    });
    expect(ca).toBe(false);
  });

  it("uses AI value for mixed school states and leaves undefined when missing", () => {
    const fromAi = resolveElectionIsPartisan({
      draft: {
        district_id: "d-1",
        district_name: "Demo Unified School District",
        district_type: "school_unified",
        state: "NC",
      },
      contestFamily: "all",
      raceType: "office",
      officialBallotTitle: "School Board Member",
      aiValue: true,
    });
    expect(fromAi).toBe(true);

    const missingAi = resolveElectionIsPartisan({
      draft: {
        district_id: "d-1",
        district_name: "Demo Unified School District",
        district_type: "school_unified",
        state: "NC",
      },
      contestFamily: "all",
      raceType: "office",
      officialBallotTitle: "School Board Member",
      aiValue: undefined,
    });
    expect(missingAi).toBeUndefined();
  });

  it("supports California Court of Appeal title detection", () => {
    expect(isJudicialOfficeTitle("Justice of the Court of Appeal, District 2")).toBe(true);
  });

  it("treats judicial retention contests as nonpartisan in partisan-judicial states", () => {
    expect(isJudicialRetentionTitle("Justice of the Supreme Court (Retention)")).toBe(true);

    const resolved = resolveElectionIsPartisan({
      draft: {
        district_id: "d-1",
        district_name: "Kansas",
        district_type: "statewide",
        state: "KS",
      },
      contestFamily: "judicial_office",
      raceType: "office",
      officialBallotTitle: "Justice of the Supreme Court (Retention)",
      aiValue: true,
    });
    expect(resolved).toBe(false);
  });

  it("keeps non-retention judicial contests in partisan-judicial states as partisan", () => {
    const resolved = resolveElectionIsPartisan({
      draft: {
        district_id: "d-1",
        district_name: "Kansas",
        district_type: "statewide",
        state: "KS",
      },
      contestFamily: "judicial_office",
      raceType: "office",
      officialBallotTitle: "Judge of the District Court",
      aiValue: undefined,
    });
    expect(resolved).toBe(true);
  });

  it("keeps South Carolina probate judges partisan", () => {
    const resolved = resolveElectionIsPartisan({
      draft: {
        district_id: "d-sc",
        district_name: "Spartanburg County, South Carolina",
        district_type: "county",
        state: "SC",
      },
      contestFamily: "judicial_office",
      raceType: "office",
      officialBallotTitle: "Probate Judge",
      aiValue: false,
    });
    expect(resolved).toBe(true);
  });

  it("candidate policy helper excludes party for retention judicial contests", () => {
    const includeParty = shouldIncludeCandidatePartyByPolicy({
      districtType: "statewide",
      state: "KS",
      officialBallotTitle: "Shall Justice Jane Doe be retained in office?",
    });
    expect(includeParty).toBe(false);
  });

  it("forces Arizona Justice of the Peace contests to partisan", () => {
    // The JP is a precinct office in section one of the ballot, printed with
    // the party in bold-faced letters (A.R.S. § 16-502(C)(5)). Yuma County's
    // 2026 listing shows Precinct 2 as a REP-vs-DEM contest.
    const title = "Justice of the Peace, Precinct 1";

    expect(
      resolveElectionIsPartisan({
        draft: {
          district_id: "d-az",
          district_name: "Yuma County, Arizona",
          district_type: "county",
          state: "AZ",
        },
        contestFamily: "judicial_office",
        raceType: "office",
        officialBallotTitle: title,
        aiValue: false,
      })
    ).toBe(true);

    expect(
      resolveCandidateContestPartisanshipByPolicy({
        districtType: "county",
        state: "AZ",
        officialBallotTitle: title,
      })
    ).toBe(true);

    expect(
      shouldIncludeCandidatePartyByPolicy({
        districtType: "county",
        state: "AZ",
        officialBallotTitle: title,
      })
    ).toBe(true);
  });

  it("keeps elected Arizona Superior Court judges nonpartisan", () => {
    // Counties under 250,000 elect their Superior Court judges instead of
    // retaining them, and those candidates are nominated in a party primary —
    // but Ariz. Const. art. 6 § 12(A) puts their names on the general ballot
    // "without partisan or other designation except the division and title of
    // the office", and A.R.S. § 16-502(J) files them in the nonpartisan
    // section. A party column on a county candidate listing is the primary
    // nomination, not the November ballot classification.
    for (const title of [
      "Judge of Superior Court, Division 2",
      "Judge of the Superior Court, Division 5",
    ]) {
      expect(
        resolveElectionIsPartisan({
          draft: {
            district_id: "d-az",
            district_name: "Yuma County, Arizona",
            district_type: "county",
            state: "AZ",
          },
          contestFamily: "judicial_office",
          raceType: "office",
          officialBallotTitle: title,
          aiValue: true,
        })
      ).toBe(false);

      expect(
        shouldIncludeCandidatePartyByPolicy({
          districtType: "county",
          state: "AZ",
          officialBallotTitle: title,
        })
      ).toBe(false);
    }
  });

  it("keeps Arizona merit-selection retention contests nonpartisan", () => {
    // Maricopa, Pima, and Pinal judges — and every Arizona appellate judge —
    // stand for retention instead. Ariz. Const. art. 6 § 38 fixes the ballot
    // question as "Shall (name) of the (applicable) court be retained in
    // office?", so the retention guard has the keyword in every real title.
    const retentionTitles = [
      "Shall Judge Jane Doe of the Superior Court in Maricopa County be retained in office?",
      "Shall Justice John Roe of the Supreme Court of Arizona be retained in office?",
      "Judge of the Superior Court in Pima County (Retention) - Jane Doe",
      "Arizona Court of Appeals, Division One Retention - John Roe",
    ];

    for (const title of retentionTitles) {
      expect(isJudicialRetentionTitle(title)).toBe(true);

      for (const districtType of ["county", "statewide"] as const) {
        expect(
          resolveElectionIsPartisan({
            draft: {
              district_id: "d-az",
              district_name: "Maricopa County, Arizona",
              district_type: districtType,
              state: "AZ",
            },
            contestFamily: "judicial_office",
            raceType: "office",
            officialBallotTitle: title,
            aiValue: true,
          })
        ).toBe(false);

        expect(
          shouldIncludeCandidatePartyByPolicy({
            districtType,
            state: "AZ",
            officialBallotTitle: title,
          })
        ).toBe(false);
      }
    }
  });

  it("does not force Arizona municipal judges partisan", () => {
    // City magistrates are nonpartisan and their titles carry no retention
    // wording, so only the Justice of the Peace exception may fire in Arizona.
    // A live row already holds this contest as nonpartisan.
    expect(
      resolveCandidateContestPartisanshipByPolicy({
        districtType: "place",
        state: "AZ",
        officialBallotTitle: "Presiding Municipal Judge, City of Yuma",
      })
    ).toBe(false);

    expect(
      resolveElectionIsPartisan({
        draft: {
          district_id: "d-az-place",
          district_name: "Yuma, Arizona",
          district_type: "place",
          state: "AZ",
        },
        contestFamily: "judicial_office",
        raceType: "office",
        officialBallotTitle: "Presiding Municipal Judge, City of Yuma",
        aiValue: false,
      })
    ).toBe(false);
  });

  it("treats Clerk of Superior Court as a non-judicial office", () => {
    expect(isJudicialOfficeTitle("Clerk of Superior Court")).toBe(false);
    expect(isJudicialOfficeTitle("Clerk of the Circuit Court")).toBe(false);
    expect(isJudicialOfficeTitle("Judge of Superior Court, Division 2")).toBe(true);

    // The clerk is an elected partisan county officer in Arizona, so policy
    // must leave the value alone instead of forcing it nonpartisan.
    expect(
      resolveCandidateContestPartisanshipByPolicy({
        districtType: "county",
        state: "AZ",
        officialBallotTitle: "Clerk of Superior Court",
      })
    ).toBeUndefined();

    // California is not a partisan-judicial state; before the fix the clerk
    // was dragged into judicial policy and forced nonpartisan there too.
    expect(
      resolveCandidateContestPartisanshipByPolicy({
        districtType: "county",
        state: "CA",
        officialBallotTitle: "Clerk of Superior Court",
      })
    ).toBeUndefined();
  });

  it("forces Washington state-legislative contests to partisan", () => {
    for (const districtType of ["state_upper", "state_lower"] as const) {
      expect(
        shouldAskIsPartisanInPrompt({
          draft: {
            district_id: "d-wa",
            district_name: "Washington legislative district",
            district_type: districtType,
            state: "WA",
          },
          contestFamily: "all",
        })
      ).toBe(false);

      expect(
        resolveElectionIsPartisan({
          draft: {
            district_id: "d-wa",
            district_name: "Washington legislative district",
            district_type: districtType,
            state: "WA",
          },
          contestFamily: "all",
          raceType: "office",
          officialBallotTitle: "State Representative Position 1",
          aiValue: false,
        })
      ).toBe(true);

      expect(
        shouldIncludeCandidatePartyByPolicy({
          districtType,
          state: "WA",
          officialBallotTitle: "State Representative Position 1",
        })
      ).toBe(true);
    }
  });
});
