import { describe, expect, it, vi } from "vitest";

import {
  normalizeDistrictOfColumbiaCandidateNameKeys,
  resolveDistrictOfColumbiaCandidateCommittee,
  searchAndResolveDistrictOfColumbiaCandidateCommittee,
} from "../../../src/pipeline/districtOfColumbiaFinance/districtOfColumbiaCandidateCommitteeResolver.js";
import type { DistrictOfColumbiaOcfContributionRecord } from "../../../src/pipeline/districtOfColumbiaFinance/districtOfColumbiaOcfClient.js";

function record(
  overrides: Partial<DistrictOfColumbiaOcfContributionRecord> = {}
): DistrictOfColumbiaOcfContributionRecord {
  return {
    committeeName: "Committee To Elect Jane Doe",
    committeeKey: "COMMITTEE TO ELECT JANE DOE",
    candidateName: "Jane Doe",
    office: "Mayor",
    electionYear: 2026,
    contributorName: "John Smith",
    occupation: "Attorney",
    amount: 100,
    date: "03/01/2026",
    ...overrides,
  };
}

function utf16LeCsv(csv: string): Uint8Array {
  return Buffer.concat([Buffer.from([0xff, 0xfe]), Buffer.from(csv, "utf16le")]);
}

describe("districtOfColumbiaCandidateCommitteeResolver", () => {
  it("normalizes direct, comma-form, and suffix candidate names", () => {
    expect([...normalizeDistrictOfColumbiaCandidateNameKeys("DOE, Jane Q. Jr.")]).toEqual([
      "DOE JANE Q",
      "JANE Q DOE",
      "JANE DOE",
    ]);
  });

  it("matches exactly one D.C. candidate committee by candidate, office, and cycle", () => {
    expect(
      resolveDistrictOfColumbiaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "place",
        officeName: "Mayor",
        electionYear: 2026,
        contributionRecords: [
          record(),
          record({ committeeName: "Other Committee", committeeKey: "OTHER", candidateName: "Other Person" }),
          record({ committeeName: "Old Committee", committeeKey: "OLD", electionYear: 2022 }),
        ],
        sourceUrl: "https://efiling.ocf.dc.gov/DataDownload",
      })
    ).toEqual({
      status: "matched",
      committeeKey: "COMMITTEE TO ELECT JANE DOE",
      committeeName: "Committee To Elect Jane Doe",
      confidence: "exact",
      source: "ocf_export",
      sourceUrl: "https://efiling.ocf.dc.gov/DataDownload",
      matchedContributionRowCount: 1,
    });
  });

  it("can match when candidate name is only supported by the committee name", () => {
    expect(
      resolveDistrictOfColumbiaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "place",
        officeName: "Mayor",
        electionYear: 2026,
        contributionRecords: [
          record({
            candidateName: undefined,
            office: undefined,
            committeeName: "Friends of Jane Doe 2026",
            committeeKey: "FRIENDS OF JANE DOE 2026",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      committeeKey: "FRIENDS OF JANE DOE 2026",
    });
  });

  it("requires and enforces D.C. ward or at-large seats for seat-scoped offices", () => {
    expect(
      resolveDistrictOfColumbiaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "place",
        officeName: "City Council Member",
        electionYear: 2026,
        contributionRecords: [record({ office: "Councilmember", seat: "Ward 4" })],
      })
    ).toEqual({
      status: "unmatched",
      reason: "missing_seat",
      candidateNameNormalized: "JANE DOE",
      officeNameNormalized: "CITY COUNCIL MEMBER",
    });

    expect(
      resolveDistrictOfColumbiaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "place",
        officeName: "City Council Member",
        seat: "Ward 5",
        electionYear: 2026,
        contributionRecords: [record({ office: "Councilmember", seat: "Ward 4" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });

    expect(
      resolveDistrictOfColumbiaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "place",
        officeName: "City Council Member",
        seat: "Ward 4",
        electionYear: 2026,
        contributionRecords: [record({ office: "Councilmember", seat: "Ward 4" })],
      })
    ).toMatchObject({
      status: "matched",
      committeeKey: "COMMITTEE TO ELECT JANE DOE",
    });
  });

  it("does not guess when multiple D.C. committees match", () => {
    expect(
      resolveDistrictOfColumbiaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "place",
        officeName: "Mayor",
        electionYear: 2026,
        contributionRecords: [
          record({ committeeName: "Jane Doe 2026", committeeKey: "JANE DOE 2026" }),
          record({ committeeName: "Friends of Jane Doe", committeeKey: "FRIENDS OF JANE DOE" }),
        ],
      })
    ).toMatchObject({
      status: "ambiguous",
      reason: "multiple_matching_committees",
      candidateNameNormalized: "JANE DOE",
      officeNameNormalized: "Mayor",
      matches: [
        { committeeKey: "FRIENDS OF JANE DOE" },
        { committeeKey: "JANE DOE 2026" },
      ],
    });
  });

  it("returns unmatched for unsupported offices, missing names, wrong offices, and invalid years", () => {
    expect(
      resolveDistrictOfColumbiaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "place",
        officeName: "City Treasurer",
        electionYear: 2026,
        contributionRecords: [record()],
      })
    ).toMatchObject({ status: "unmatched", reason: "unsupported_office" });

    expect(
      resolveDistrictOfColumbiaCandidateCommittee({
        candidateName: "   ",
        officeScope: "place",
        officeName: "Mayor",
        electionYear: 2026,
        contributionRecords: [record()],
      })
    ).toEqual({
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized: "",
      officeNameNormalized: "Mayor",
    });

    expect(
      resolveDistrictOfColumbiaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "Attorney General",
        electionYear: 2026,
        contributionRecords: [record({ office: "Mayor" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });

    expect(() =>
      resolveDistrictOfColumbiaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "place",
        officeName: "Mayor",
        electionYear: 1999,
        contributionRecords: [],
      })
    ).toThrow("Invalid D.C. candidate committee election year");
  });

  it("searches OCF exports and resolves them through the async wrapper", async () => {
    const csv = [
      "Committee Name,Candidate Name,Office,Seat,Election Year,Contributor Name,Occupation,Amount",
      "Committee To Elect Jane Doe,Jane Doe,Mayor,,2026,John Smith,Attorney,$100.00",
    ].join("\n");
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(new Response("<html></html>", { status: 200, statusText: "OK" }))
      .mockResolvedValueOnce(new Response("<div>results</div>", { status: 200, statusText: "OK" }))
      .mockResolvedValueOnce(new Response(utf16LeCsv(csv), { status: 200, statusText: "OK" })) as unknown as typeof fetch;

    await expect(
      searchAndResolveDistrictOfColumbiaCandidateCommittee(
        {
          candidateName: "Jane Doe",
          officeScope: "place",
          officeName: "Mayor",
          electionYear: 2026,
        },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toMatchObject({
      status: "matched",
      committeeKey: "COMMITTEE TO ELECT JANE DOE",
      committeeName: "Committee To Elect Jane Doe",
    });

    expect(fetchImpl).toHaveBeenCalledTimes(3);
    const submitInit = vi.mocked(fetchImpl).mock.calls[1]?.[1];
    expect(String(submitInit?.body)).toContain("FilerTypeId=2");
    expect(String(submitInit?.body)).toContain("FromDate=01%2F01%2F2025");
    expect(String(submitInit?.body)).toContain("ToDate=12%2F31%2F2026");
  });
});
