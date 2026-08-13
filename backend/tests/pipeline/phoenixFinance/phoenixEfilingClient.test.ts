import { describe, expect, it, vi } from "vitest";
import {
  canonicalPhoenixRegistration,
  fetchPhoenixCanonicalRegistrations,
  parsePhoenixGridEnvelope,
  phoenixCandidateCycleForDate,
  phoenixGridAll,
  toPhoenixRegistrationRow,
} from "../../../src/pipeline/phoenixFinance/phoenixEfilingClient.js";

function jsonResponse(body: unknown) {
  return { status: 200, text: () => Promise.resolve(JSON.stringify(body)) };
}

describe("parsePhoenixGridEnvelope", () => {
  it("rejects the WAF maintenance page (HTTP 200 HTML)", () => {
    expect(() =>
      parsePhoenixGridEnvelope(
        "<!DOCTYPE html><html><body><h2>Sorry! The requested service is currently undergoing maintenance.</h2></body></html>",
        "self-test",
      ),
    ).toThrow(/maintenance page/);
  });

  it("rejects a JSON body missing the Data/Total envelope", () => {
    expect(() => parsePhoenixGridEnvelope('{"Data": []}', "self-test")).toThrow(
      /missing Data\/Total/,
    );
  });
});

describe("phoenixGridAll", () => {
  it("pages until Total rows are collected", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ Data: [{ n: 1 }, { n: 2 }], Total: 3 }))
      .mockResolvedValueOnce(jsonResponse({ Data: [{ n: 3 }], Total: 3 }));
    const rows = await phoenixGridAll({
      path: "/CampaignFinance/Search/_SearchCommittees",
      filters: { COPID: "CAN-25-4" },
      fetchImpl,
    });
    expect(rows).toEqual([{ n: 1 }, { n: 2 }, { n: 3 }]);
    // The wire format: form body with sort/page/pageSize/group/filter + the
    // caller's filters, XHR marker header, browser User-Agent.
    const [url, init] = fetchImpl.mock.calls[0]!;
    expect(url).toBe(
      "https://apps-secure.phoenix.gov/CampaignFinance/Search/_SearchCommittees",
    );
    expect(String(init?.body)).toContain("COPID=CAN-25-4");
    expect(String(init?.body)).toContain("page=1");
    expect(
      (init?.headers as Record<string, string>)["X-Requested-With"],
    ).toBe("XMLHttpRequest");
  });

  it("rejects a non-2xx status before parsing the body", async () => {
    const fetchImpl = vi.fn().mockResolvedValue({
      status: 500,
      text: () => Promise.resolve('{"Data": [], "Total": 0}'),
    });
    await expect(
      phoenixGridAll({
        path: "/CampaignFinance/Search/_SearchCommittees",
        filters: {},
        fetchImpl,
      }),
    ).rejects.toThrow(/HTTP 500/);
  });

  it("aborts a stalled request via the timeout", async () => {
    const fetchImpl = vi.fn().mockImplementation(
      (_url: string, init?: RequestInit) =>
        new Promise((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () =>
            reject(new DOMException("aborted", "AbortError")),
          );
        }),
    );
    await expect(
      phoenixGridAll({
        path: "/CampaignFinance/Search/_SearchCommittees",
        filters: {},
        timeoutMs: 10,
        fetchImpl,
      }),
    ).rejects.toThrow(/timed out after 10ms/);
  });

  it("throws on premature pagination exhaustion instead of truncating", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ Data: [{ n: 1 }], Total: 5 }))
      .mockResolvedValueOnce(jsonResponse({ Data: [], Total: 5 }));
    await expect(
      phoenixGridAll({
        path: "/CampaignFinance/Search/_SearchCommittees",
        filters: {},
        fetchImpl,
      }),
    ).rejects.toThrow(/exhausted at 1\/5 rows/);
  });
});

describe("toPhoenixRegistrationRow / canonicalPhoenixRegistration", () => {
  // Live field shapes, 2026-08-12 (CAN-25-4).
  const rawHermes = {
    COPID: "can-25-4",
    CommitteeName: " Ed Hermes for Phoenix ",
    CommitteeType: "Candidate Committee",
    CandidateName: "Ed Hermes",
    ElectionCycle: "2025 Election Cycle",
    OfficeSoughtElectionCycle: "2026",
    Approved: true,
    AppovedTimestamp: "/Date(1748632357450)/",
    Terminated: false,
    IsStandingCommittee: false,
  };

  it("maps and normalizes a live registration row", () => {
    expect(toPhoenixRegistrationRow(rawHermes)).toEqual({
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
    });
  });

  it("maps blank optional fields to null/defaults", () => {
    const row = toPhoenixRegistrationRow({ COPID: "PAC-21-15" });
    expect(row.candidateName).toBeNull();
    expect(row.officeSoughtElectionCycle).toBeNull();
    expect(row.approved).toBe(false);
    expect(row.approvedTimestamp).toBe(0);
  });

  it("keeps the latest approved document version and drops unapproved-only groups", () => {
    const versions = [
      toPhoenixRegistrationRow({
        ...rawHermes,
        ElectionCycle: "2021 Election Cycle",
        OfficeSoughtElectionCycle: "2022",
        AppovedTimestamp: "/Date(1633553307983)/",
      }),
      toPhoenixRegistrationRow(rawHermes),
    ];
    expect(canonicalPhoenixRegistration(versions)?.electionCycle).toBe(
      "2025 Election Cycle",
    );
    expect(
      canonicalPhoenixRegistration([
        toPhoenixRegistrationRow({ ...rawHermes, Approved: false }),
      ]),
    ).toBeNull();
  });

  it("fetchPhoenixCanonicalRegistrations collapses versions per COP ID", async () => {
    // Robinson's live pattern: one COP ID re-registered for a new cycle via
    // an amended Statement of Organization.
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse({
        Total: 3,
        Data: [
          {
            ...rawHermes,
            COPID: "CAN-21-16",
            ElectionCycle: "2021 Election Cycle",
            OfficeSoughtElectionCycle: "2022",
            AppovedTimestamp: "/Date(1633553307983)/",
          },
          {
            ...rawHermes,
            COPID: "CAN-21-16",
            ElectionCycle: "2025 Election Cycle",
            OfficeSoughtElectionCycle: "2026",
            AppovedTimestamp: "/Date(1753746294360)/",
          },
          rawHermes,
        ],
      }),
    );
    const canonical = await fetchPhoenixCanonicalRegistrations({ fetchImpl });
    expect(canonical).toHaveLength(2);
    expect(canonical.map((row) => [row.copId, row.officeSoughtElectionCycle])).toEqual([
      ["CAN-21-16", "2026"],
      ["CAN-25-4", "2026"],
    ]);
  });
});

describe("phoenixCandidateCycleForDate", () => {
  it("derives the Apr-1-odd-year cycle from a date inside it", () => {
    expect(phoenixCandidateCycleForDate("2026-11-03")).toEqual({
      startYear: 2025,
      cycleStart: "2025-04-01",
      cycleEnd: "2027-03-31",
    });
    // A March runoff belongs to the SAME cycle as its November election.
    expect(phoenixCandidateCycleForDate("2027-03-09").startYear).toBe(2025);
    // Cycle boundary: Mar 31 closes the old cycle, Apr 1 opens the new one.
    expect(phoenixCandidateCycleForDate("2025-03-31").startYear).toBe(2023);
    expect(phoenixCandidateCycleForDate("2025-04-01").startYear).toBe(2025);
  });

  it("rejects non-ISO input", () => {
    expect(() => phoenixCandidateCycleForDate("11/03/2026")).toThrow(
      /Not an ISO date/,
    );
  });
});
