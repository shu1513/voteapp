import { describe, expect, it, vi } from "vitest";

import {
  autoLinkMissingIdahoCandidateFinanceLinks,
  type IdahoFinanceAutoLinkCandidateElection,
} from "../../../src/pipeline/idahoFinance/idahoCandidateFinanceAutoLink.js";
import { GUID_A, GUID_B, registration } from "./idahoTestFixtures.js";

const LINK_ID = "33333333-3333-4333-8333-333333333333";
const NOW = new Date("2026-09-01T00:00:00.000Z");

function candidate(overrides: Partial<IdahoFinanceAutoLinkCandidateElection> = {}): IdahoFinanceAutoLinkCandidateElection {
  return {
    candidateId: "11111111-1111-4111-8111-111111111111",
    electionId: "22222222-2222-4222-8222-222222222222",
    candidateNames: ["Todd Achilles"],
    electionYear: 2026,
    officeScope: "state_upper",
    officeName: "State Senator",
    district: "State Senate District 16 (2024); Idaho",
    ballotTitle: "State Senator District 16",
    legislativeDistrict: 16,
    ...overrides,
  };
}

function fakeDb(onInsert?: () => Promise<never>) {
  return {
    query: vi.fn((sql: unknown) => {
      if (String(sql).includes("INSERT INTO public.id_candidate_finance_links")) {
        return onInsert ? onInsert() : Promise.resolve({ rows: [{ id: LINK_ID }], rowCount: 1 });
      }
      return Promise.resolve({ rows: [], rowCount: 0 });
    }),
  };
}

const baseInput = {
  now: NOW,
  maxCandidates: 25,
  electionLookbackDays: 98,
  electionLookaheadDays: 730,
};

describe("autoLinkMissingIdahoCandidateFinanceLinks", () => {
  it("writes a sunshine_grid link for a resolved candidate", async () => {
    const db = fakeDb();
    const results = await autoLinkMissingIdahoCandidateFinanceLinks({
      ...baseInput,
      db,
      candidateElections: [candidate()],
      registrations: [registration({ registrationGuid: GUID_A })],
    });
    expect(results).toEqual([
      {
        candidateId: "11111111-1111-4111-8111-111111111111",
        electionId: "22222222-2222-4222-8222-222222222222",
        status: "linked",
        registrationGuid: GUID_A,
        filerName: "Achilles, Todd Baker",
        district: "16",
        confidence: "name_exact",
      },
    ]);
    const insert = db.query.mock.calls.find((call) => String(call[0]).includes("INSERT INTO public.id_candidate_finance_links"));
    expect(insert?.[1]).toEqual([
      "11111111-1111-4111-8111-111111111111",
      "22222222-2222-4222-8222-222222222222",
      2026,
      "TODD ACHILLES",
      "State Senator",
      "16",
      GUID_A,
      "Achilles, Todd Baker",
      "active",
      "sunshine_grid",
      `https://sunshine.voteidaho.gov/public/cf/candidateprofile?guid=${GUID_A}&tabName=CAN&isLegacy=false`,
      NOW.toISOString(),
    ]);
  });

  it("reports without writing in dry-run mode and for ambiguous or unmatched candidates", async () => {
    const db = fakeDb();
    const results = await autoLinkMissingIdahoCandidateFinanceLinks({
      ...baseInput,
      db,
      dryRun: true,
      candidateElections: [
        candidate(),
        candidate({ candidateId: "11111111-1111-4111-8111-111111111112", candidateNames: ["Nobody Here"] }),
        candidate({ candidateId: "11111111-1111-4111-8111-111111111113", officeScope: "county", officeName: "Sheriff", district: "Ada County, Idaho" }),
      ],
      registrations: [
        registration({ registrationGuid: GUID_A }),
        registration({ registrationGuid: GUID_B, filerRegistrationId: 2 }),
      ],
    });
    expect(results.map((result) => [result.status, result.reason])).toEqual([
      ["ambiguous", "multiple_active_registrations"],
      ["unmatched", "no_registration_match"],
      ["unmatched", "no_registration_match"],
    ]);
    expect(results[0]?.matches).toEqual([
      { registrationGuid: GUID_B, filerName: "Achilles, Todd Baker", status: "Active" },
      { registrationGuid: GUID_A, filerName: "Achilles, Todd Baker", status: "Active" },
    ]);
    expect(db.query).not.toHaveBeenCalled();
  });

  it("captures write failures per candidate and skips the grid when nothing is due", async () => {
    const failing = fakeDb(() => Promise.reject(new Error("db down")));
    const results = await autoLinkMissingIdahoCandidateFinanceLinks({
      ...baseInput,
      db: failing,
      candidateElections: [candidate()],
      registrations: [registration({ registrationGuid: GUID_A })],
    });
    expect(results[0]).toMatchObject({ status: "error", reason: "auto_link_failed", error: "db down" });

    // No candidates → no grid fetch (registrations deliberately omitted).
    await expect(
      autoLinkMissingIdahoCandidateFinanceLinks({ ...baseInput, db: fakeDb(), candidateElections: [] })
    ).resolves.toEqual([]);
  });
});
