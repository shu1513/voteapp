import { describe, expect, it, vi } from "vitest";

import {
  normalizeNewJerseyCandidateNameKeys,
  resolveNewJerseyCandidateCommittee,
  searchAndResolveNewJerseyCandidateCommittee,
} from "../../../src/pipeline/newJerseyFinance/newJerseyCandidateCommitteeResolver.js";
import type { NewJerseyElecEntity } from "../../../src/pipeline/newJerseyFinance/newJerseyElecClient.js";

function entity(overrides: Partial<NewJerseyElecEntity> = {}): NewJerseyElecEntity {
  return {
    entityS: 473742,
    entityName: "SHERRILL, MIKIE",
    firstName: "Mikie",
    middleInitial: null,
    lastName: "Sherrill",
    suffix: null,
    nonIndividualName: null,
    pacName: null,
    electionYear: 2025,
    sequenceNumber: null,
    officeCode: "GOV",
    office: "Governor",
    partyCode: "D",
    party: "Democratic",
    locationCode: 0,
    location: "New Jersey",
    electionTypeCode: "G",
    electionType: "General",
    entityType: "Candidate",
    sourceUrl: "https://www.njelecefilesearch.com/api/VWEntity/GetEntityList?LastName=Sherrill",
    ...overrides,
  };
}

function jsonResponse(payload: unknown): Response {
  return new Response(JSON.stringify(payload), { status: 200, statusText: "OK" });
}

describe("newJerseyCandidateCommitteeResolver", () => {
  it("normalizes direct, comma-form, and parenthetical candidate names", () => {
    expect([...normalizeNewJerseyCandidateNameKeys("SHERRILL, Mikie (Rebecca Michelle Sherrill)")]).toEqual([
      "SHERRILL MIKIE",
      "MIKIE SHERRILL",
      "REBECCA MICHELLE SHERRILL",
      "REBECCA SHERRILL",
    ]);
  });

  it("matches one NJ ELEC candidate entity when the election type is specified", () => {
    expect(
      resolveNewJerseyCandidateCommittee({
        candidateName: "Mikie Sherrill",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2025,
        electionTypeCode: "G",
        entityRows: [
          entity({ entityS: 472129, electionTypeCode: "P", electionType: "Primary" }),
          entity(),
          entity({ entityS: 999999, entityName: "OTHER, PERSON", firstName: "Other", lastName: "Person" }),
        ],
      })
    ).toEqual({
      status: "matched",
      entityS: 473742,
      entityName: "SHERRILL, MIKIE",
      firstName: "Mikie",
      lastName: "Sherrill",
      office: "Governor",
      officeCode: "GOV",
      party: "Democratic",
      partyCode: "D",
      location: "New Jersey",
      locationCode: 0,
      electionType: "General",
      electionTypeCode: "G",
      confidence: "exact",
      source: "elec_api",
      sourceUrl: "https://www.njelecefilesearch.com/api/VWEntity/GetEntityList?LastName=Sherrill",
      matchedEntityRowCount: 1,
    });
  });

  it("does not guess when primary and general entities both match", () => {
    expect(
      resolveNewJerseyCandidateCommittee({
        candidateName: "Mikie Sherrill",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2025,
        entityRows: [entity({ entityS: 472129, electionTypeCode: "P", electionType: "Primary" }), entity()],
      })
    ).toMatchObject({
      status: "ambiguous",
      reason: "multiple_matching_entities",
      candidateNameNormalized: "MIKIE SHERRILL",
      officeNameNormalized: "Governor",
      matches: [{ entityS: 472129 }, { entityS: 473742 }],
    });
  });

  it("returns unmatched for unsupported offices, missing names, typos, and office mismatches", () => {
    expect(
      resolveNewJerseyCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "county",
        officeName: "Sheriff",
        electionYear: 2025,
        entityRows: [entity({ entityName: "DOE, JANE", firstName: "Jane", lastName: "Doe" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "unsupported_office" });

    expect(
      resolveNewJerseyCandidateCommittee({
        candidateName: "   ",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2025,
        entityRows: [entity()],
      })
    ).toEqual({
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized: "",
      officeNameNormalized: "Governor",
    });

    expect(
      resolveNewJerseyCandidateCommittee({
        candidateName: "Mikie Sheridan",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2025,
        entityRows: [entity()],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_entity_match" });

    expect(
      resolveNewJerseyCandidateCommittee({
        candidateName: "Mikie Sherrill",
        officeScope: "state_upper",
        officeName: "State Senator",
        electionYear: 2025,
        entityRows: [entity()],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_entity_match" });
  });

  it("searches by candidate last name and resolves fetched entity rows", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse([
        {
          ENTITY_S: 473742,
          ENTITYNAME: "SHERRILL, MIKIE",
          FIRST_NAME: "Mikie",
          LAST_NAME: "Sherrill",
          ELECTIONYEAR: 2025,
          OFFICE: "Governor",
          ELECTIONTYPECODE: "G",
        },
      ])
    ) as unknown as typeof fetch;

    await expect(
      searchAndResolveNewJerseyCandidateCommittee(
        {
          candidateName: "Mikie Sherrill",
          officeScope: "statewide",
          officeName: "Governor",
          electionYear: 2025,
          electionTypeCode: "G",
        },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toMatchObject({ status: "matched", entityS: 473742 });

    expect(String(vi.mocked(fetchImpl).mock.calls[0]?.[0])).toContain("LastName=SHERRILL");
  });

  it("searches comma-form candidate names by surname instead of first name", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse([
        {
          ENTITY_S: 473742,
          ENTITYNAME: "SHERRILL, MIKIE",
          FIRST_NAME: "Mikie",
          LAST_NAME: "Sherrill",
          ELECTIONYEAR: 2025,
          OFFICE: "Governor",
          ELECTIONTYPECODE: "G",
        },
      ])
    ) as unknown as typeof fetch;

    await expect(
      searchAndResolveNewJerseyCandidateCommittee(
        {
          candidateName: "Sherrill, Mikie",
          officeScope: "statewide",
          officeName: "Governor",
          electionYear: 2025,
          electionTypeCode: "G",
        },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toMatchObject({ status: "matched", entityS: 473742 });

    expect(String(vi.mocked(fetchImpl).mock.calls[0]?.[0])).toContain("LastName=SHERRILL");
    expect(String(vi.mocked(fetchImpl).mock.calls[0]?.[0])).not.toContain("LastName=MIKIE");
  });
});
