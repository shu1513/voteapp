import { describe, expect, it, vi } from "vitest";

import type { IdahoFinanceAutoLinkCandidateElection } from "../../../src/pipeline/idahoFinance/idahoCandidateFinanceAutoLink.js";
import { linkIdahoCandidateFinanceManually } from "../../../src/pipeline/idahoFinance/idahoCandidateFinanceManualLink.js";
import { GUID_A, GUID_B, GUID_C, registration } from "./idahoTestFixtures.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const NOW = new Date("2026-09-03T12:00:00.000Z");
const PROFILE_URL = `https://sunshine.voteidaho.gov/public/cf/candidateprofile?guid=${GUID_A}&tabName=CAN&isLegacy=false`;

// The roster spelling the resolver could not match to the grid's legal name.
const CANDIDATE: IdahoFinanceAutoLinkCandidateElection = {
  candidateId: CANDIDATE_ID,
  electionId: ELECTION_ID,
  candidateNames: ["Marty Kilhefner"],
  electionYear: 2026,
  officeScope: "state_upper",
  officeName: "State Senator",
  district: "State Senate District 16 (2024); Idaho",
  ballotTitle: "State Senator",
  legislativeDistrict: 16,
};
const LINKED = registration({ registrationGuid: GUID_A, filerName: "Rotz-Kilhefner, Martha Louise", totalRaised: 3625.7 });
const PRIOR = registration({ registrationGuid: GUID_B, electionYear: 2024, status: "Terminated" });
const OTHER_DISTRICT = registration({ registrationGuid: GUID_C, district: "Legislative District 17" });
const GRID = [LINKED, PRIOR, OTHER_DISTRICT];

function baseInput(overrides: Partial<Parameters<typeof linkIdahoCandidateFinanceManually>[0]> = {}) {
  return {
    db: { query: vi.fn().mockResolvedValue({ rows: [] }) } as never,
    candidateId: CANDIDATE_ID.toUpperCase(),
    electionId: ELECTION_ID,
    registrationGuid: GUID_A.toUpperCase(),
    now: NOW,
    registrations: GRID,
    listCandidateElectionsFn: vi.fn().mockResolvedValue([CANDIDATE]),
    upsertLinkFn: vi.fn().mockResolvedValue({ linkId: "link-1" }),
    ...overrides,
  };
}

describe("linkIdahoCandidateFinanceManually", () => {
  it("writes a manual link from the grid row's facts, bypassing only the name gate", async () => {
    const input = baseInput();
    const result = await linkIdahoCandidateFinanceManually(input);
    expect(result).toEqual({
      dryRun: false,
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      candidateName: "Marty Kilhefner",
      electionYear: 2026,
      officeName: "State Senator",
      registrationGuid: GUID_A,
      filerName: "Rotz-Kilhefner, Martha Louise",
      district: "16",
      sourceUrl: PROFILE_URL,
      totalRaised: 3625.7,
      linkId: "link-1",
    });
    expect(input.listCandidateElectionsFn).toHaveBeenCalledWith(input.db, {
      now: NOW,
      maxCandidates: null,
      electionLookbackDays: 98,
      electionLookaheadDays: 730,
    });
    expect(input.upsertLinkFn).toHaveBeenCalledWith({
      db: input.db,
      link: {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        electionYear: 2026,
        candidateNameNormalized: "MARTY KILHEFNER",
        officeName: "State Senator",
        district: "16",
        registrationGuid: GUID_A,
        filerName: "Rotz-Kilhefner, Martha Louise",
        linkStatus: "active",
        linkSource: "manual",
        sourceUrl: PROFILE_URL,
        lastVerifiedAt: NOW,
      },
    });
  });

  it("validates without writing in dry-run mode", async () => {
    const input = baseInput({ dryRun: true });
    const result = await linkIdahoCandidateFinanceManually(input);
    expect(result).toMatchObject({ dryRun: true, linkId: null, filerName: "Rotz-Kilhefner, Martha Louise" });
    expect(input.upsertLinkFn).not.toHaveBeenCalled();
  });

  it("refuses a candidate election that is not in the unlinked eligible list", async () => {
    const input = baseInput({ listCandidateElectionsFn: vi.fn().mockResolvedValue([]) });
    await expect(linkIdahoCandidateFinanceManually(input)).rejects.toThrow(
      "is not an unlinked Idaho-finance-eligible race (already linked, ineligible office, or outside the sync window)"
    );
    expect(input.upsertLinkFn).not.toHaveBeenCalled();
  });

  it("refuses a registration that is missing, on another cycle, on another district, or not Active", async () => {
    await expect(
      linkIdahoCandidateFinanceManually(baseInput({ registrationGuid: "99999999-9999-4999-8999-999999999999" }))
    ).rejects.toThrow("is not in the candidate grid");
    await expect(linkIdahoCandidateFinanceManually(baseInput({ registrationGuid: GUID_B }))).rejects.toThrow(
      `Idaho registration ${GUID_B} (Achilles, Todd Baker, State Senator, Legislative District 16, 2024, Terminated) is not on Marty Kilhefner's State Senator 2026 race`
    );
    await expect(linkIdahoCandidateFinanceManually(baseInput({ registrationGuid: GUID_C }))).rejects.toThrow(
      "is not on Marty Kilhefner's State Senator 2026 race"
    );
    await expect(
      linkIdahoCandidateFinanceManually(
        baseInput({ registrations: [registration({ registrationGuid: GUID_A, status: "Terminated" })] })
      )
    ).rejects.toThrow(`Idaho registration ${GUID_A} (Achilles, Todd Baker, State Senator, Legislative District 16, 2026, Terminated) is not Active`);
    await expect(linkIdahoCandidateFinanceManually(baseInput({ registrationGuid: "not-a-guid" }))).rejects.toThrow(
      "Invalid Idaho registration guid"
    );
  });

  it("refuses a registration already linked to another candidate, in dry-run too", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({
        rows: [{ candidate_id: "99999999-9999-4999-8999-999999999999", candidate_name_normalized: "TODD ACHILLES" }],
      }),
    } as never;
    const input = baseInput({ db, dryRun: true });
    await expect(linkIdahoCandidateFinanceManually(input)).rejects.toThrow(
      `Idaho registration ${GUID_A} (Rotz-Kilhefner, Martha Louise) is already linked to another candidate: TODD ACHILLES (99999999-9999-4999-8999-999999999999)`
    );
    expect(vi.mocked((db as { query: ReturnType<typeof vi.fn> }).query).mock.calls[0]![1]).toEqual([GUID_A, CANDIDATE_ID]);
    expect(input.upsertLinkFn).not.toHaveBeenCalled();
  });

  it("pulls the grid itself when the caller did not supply it", async () => {
    const fetchImpl = vi.fn().mockRejectedValue(new Error("offline"));
    await expect(
      linkIdahoCandidateFinanceManually(
        baseInput({ registrations: undefined, registrationGuid: GUID_A, dryRun: true, clientOptions: { fetchImpl } })
      )
    ).rejects.toThrow();
    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });
});
