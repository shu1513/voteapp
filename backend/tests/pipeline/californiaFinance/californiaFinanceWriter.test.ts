import { describe, expect, it, vi } from "vitest";

import {
  replaceCaliforniaCandidateFinanceSnapshot,
  upsertCaliforniaFinanceLink,
} from "../../../src/pipeline/californiaFinance/californiaFinanceWriter.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";

function createMockDb() {
  return {
    query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
  };
}

function baseLink() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    electionYear: 2026,
    candidateNameNormalized: "GAVIN NEWSOM",
    officeName: "Governor",
    controlledCommitteeId: "1456045",
    controlledCommitteeName: "Newsom for California Governor 2026",
    linkSource: "power_search" as const,
    sourceUrl: "https://powersearch.sos.ca.gov/advanced.php",
    lastVerifiedAt: new Date("2026-01-01T00:00:00.000Z"),
  };
}

describe("californiaFinanceWriter", () => {
  it("upserts California finance links and returns the link id", async () => {
    const db = createMockDb();

    await expect(upsertCaliforniaFinanceLink({ db, link: baseLink() })).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.ca_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT (candidate_id, election_id, controlled_committee_id)");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("RETURNING id");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "GAVIN NEWSOM",
      "Governor",
      "1456045",
      "Newsom for California Governor 2026",
      "active",
      "power_search",
      "https://powersearch.sos.ca.gov/advanced.php",
      "2026-01-01T00:00:00.000Z",
    ]);
  });

  it("replaces a full California finance snapshot inside a transaction", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };
    const db = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    };

    const result = await replaceCaliforniaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 1000,
        totalDisbursements: 400,
        cashOnHand: 600,
        debtsOwed: 0,
        outsideSupportTotal: 300,
        outsideOpposeTotal: 50,
        sourceUrl: "https://powersearch.sos.ca.gov/advanced.php",
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 700,
          contributorCount: 3,
          sourceUrl: "https://powersearch.sos.ca.gov/advanced.php",
        },
        {
          categoryType: "employer",
          categoryName: "Google",
          amount: 300,
          contributorCount: 2,
          sourceUrl: "https://powersearch.sos.ca.gov/advanced.php",
        },
      ],
      outsideGroups: [
        {
          committeeId: "1267335",
          committeeName: "Democratic Club of Ventura",
          supportOppose: "support",
          amount: 300,
          sourceUrl: "https://powersearch.sos.ca.gov:3000/ie/search?candidatename=Newsom%2C+Gavin&electioncycle=2025",
        },
      ],
      outsideGroupBreakdowns: [
        {
          committeeId: "1267335",
          supportOppose: "support",
          categoryType: "employer",
          categoryName: "Example Employer",
          amount: 200,
          contributorCount: 1,
          sourceUrl: "https://powersearch.sos.ca.gov/advanced.php",
        },
      ],
    });

    expect(result).toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 2,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 1,
    });
    expect(db.connect).toHaveBeenCalledTimes(1);
    expect(db.query).not.toHaveBeenCalled();
    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
    expect(client.release).toHaveBeenCalledTimes(1);

    const sql = client.query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("INSERT INTO public.ca_candidate_finance_summaries"))).toBe(true);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.ca_candidate_finance_direct_breakdowns"))).toHaveLength(2);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.ca_candidate_finance_outside_groups"))).toHaveLength(1);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.ca_candidate_finance_outside_group_breakdowns"))).toHaveLength(1);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ca_candidate_finance_direct_breakdowns"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ca_candidate_finance_outside_group_breakdowns"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ca_candidate_finance_outside_groups"))).toBe(true);
  });

  it("does not delete omitted direct or outside sections", async () => {
    const db = createMockDb();

    const result = await replaceCaliforniaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 1000,
        sourceUrl: "https://powersearch.sos.ca.gov/advanced.php",
      },
    });

    expect(result).toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
    });
    const sql = db.query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ca_candidate_finance_direct_breakdowns"))).toBe(false);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ca_candidate_finance_outside_groups"))).toBe(false);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ca_candidate_finance_outside_group_breakdowns"))).toBe(
      false
    );
  });

  it("rolls back and releases the client when a transactional write fails", async () => {
    const client = {
      query: vi
        .fn()
        .mockResolvedValueOnce({ rows: [], rowCount: 0 })
        .mockRejectedValueOnce(new Error("write failed"))
        .mockResolvedValueOnce({ rows: [], rowCount: 0 }),
      release: vi.fn(),
    };
    const db = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    };

    await expect(
      replaceCaliforniaCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        summary: { totalReceipts: 1000 },
      })
    ).rejects.toThrow("write failed");

    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("ROLLBACK");
    expect(client.release).toHaveBeenCalledTimes(1);
  });

  it("validates core inputs before writing", async () => {
    const db = createMockDb();

    await expect(
      replaceCaliforniaCandidateFinanceSnapshot({
        db,
        link: {
          ...baseLink(),
          controlledCommitteeId: " ",
        },
      })
    ).rejects.toThrow("California controlled committee id is required");

    expect(db.query).not.toHaveBeenCalled();
  });
});
