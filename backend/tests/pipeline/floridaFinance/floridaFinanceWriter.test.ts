import { describe, expect, it, vi } from "vitest";

import {
  listFloridaOutsideGroupSupportLinks,
  replaceFloridaCandidateFinanceSnapshot,
  upsertFloridaFinanceLink,
  upsertFloridaOutsideGroupSupportLink,
} from "../../../src/pipeline/floridaFinance/floridaFinanceWriter.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const CANDIDATE_ELECTION_ID = "44444444-4444-4444-4444-444444444444";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";
const SUPPORT_LINK_ID = "55555555-5555-5555-5555-555555555555";

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
    candidateNameNormalized: "JANE DOE",
    officeName: "Governor",
    committeeId: "FRIENDS_OF_JANE_DOE",
    committeeName: "Friends of Jane Doe",
    linkSource: "dos_export" as const,
    sourceUrl: "https://dos.elections.myflorida.com/cgi-bin/contrib.exe",
    lastVerifiedAt: new Date("2026-01-01T00:00:00.000Z"),
  };
}

describe("floridaFinanceWriter", () => {
  it("upserts Florida finance links and returns the link id", async () => {
    const db = createMockDb();

    await expect(upsertFloridaFinanceLink({ db, link: baseLink() })).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.fl_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT (candidate_id, election_id, committee_id)");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("RETURNING id");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "JANE DOE",
      "Governor",
      null,
      "FRIENDS_OF_JANE_DOE",
      "Friends of Jane Doe",
      "active",
      "dos_export",
      "https://dos.elections.myflorida.com/cgi-bin/contrib.exe",
      "2026-01-01T00:00:00.000Z",
    ]);
  });

  it("upserts trusted Florida outside group support links", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: SUPPORT_LINK_ID }], rowCount: 1 }),
    };

    await expect(
      upsertFloridaOutsideGroupSupportLink({
        db,
        link: {
          candidateElectionId: CANDIDATE_ELECTION_ID,
          committeeId: "FLORIDIANS_FOR_JANE_DOE",
          committeeName: "Floridians for Jane Doe",
          supportOppose: "support",
          confidence: "high",
          amount: 1200,
          evidenceUrl: "https://example.test/evidence",
          evidenceNote: "Trusted report says the PAC supports Jane Doe.",
          linkSource: "manual",
        },
      })
    ).resolves.toEqual({ id: SUPPORT_LINK_ID });

    expect(String(db.query.mock.calls[0]?.[0])).toContain(
      "INSERT INTO public.fl_candidate_finance_outside_group_links"
    );
    expect(String(db.query.mock.calls[0]?.[0])).toContain(
      "ON CONFLICT (candidate_election_id, committee_name, support_oppose, link_source)"
    );
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ELECTION_ID,
      "FLORIDIANS_FOR_JANE_DOE",
      "Floridians for Jane Doe",
      "support",
      "high",
      1200,
      "https://example.test/evidence",
      "Trusted report says the PAC supports Jane Doe.",
      "manual",
    ]);
  });

  it("lists trusted Florida outside group support links for a candidate election", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({
        rows: [
          {
            id: SUPPORT_LINK_ID,
            candidate_election_id: CANDIDATE_ELECTION_ID,
            committee_id: "FLORIDIANS_FOR_JANE_DOE",
            committee_name: "Floridians for Jane Doe",
            support_oppose: "support",
            confidence: "medium",
            amount: "1200.00",
            evidence_url: "https://example.test/evidence",
            evidence_note: "Trusted report says the PAC supports Jane Doe.",
            link_source: "manual",
          },
        ],
        rowCount: 1,
      }),
    };

    await expect(
      listFloridaOutsideGroupSupportLinks({
        db,
        candidateElectionId: CANDIDATE_ELECTION_ID,
      })
    ).resolves.toEqual([
      {
        id: SUPPORT_LINK_ID,
        candidateElectionId: CANDIDATE_ELECTION_ID,
        committeeId: "FLORIDIANS_FOR_JANE_DOE",
        committeeName: "Floridians for Jane Doe",
        supportOppose: "support",
        confidence: "medium",
        amount: 1200,
        evidenceUrl: "https://example.test/evidence",
        evidenceNote: "Trusted report says the PAC supports Jane Doe.",
        linkSource: "manual",
      },
    ]);
    expect(String(db.query.mock.calls[0]?.[0])).toContain(
      "FROM public.fl_candidate_finance_outside_group_links"
    );
    expect(db.query.mock.calls[0]?.[1]).toEqual([CANDIDATE_ELECTION_ID]);
  });

  it("replaces a Florida finance snapshot inside a transaction", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };
    const db = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    };

    const result = await replaceFloridaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 5350,
        directContributionTotal: 5350,
        outsideSupportTotal: 100000.25,
        outsideOpposeTotal: 5000,
        sourceUrl: "https://dos.elections.myflorida.com/cgi-bin/contrib.exe",
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 350,
          contributorCount: 2,
          sourceUrl: "https://dos.elections.myflorida.com/cgi-bin/contrib.exe",
        },
        {
          categoryType: "contribution_size",
          categoryName: "$250-$499",
          amount: 250,
          contributorCount: 1,
        },
      ],
      outsideGroups: [
        {
          committeeId: "FLORIDIANS_FOR_JANE_DOE",
          committeeName: "Floridians for Jane Doe",
          supportOppose: "support",
          amount: 100000.25,
        },
        {
          committeeId: "FLORIDA_ACCOUNTABILITY_PAC",
          committeeName: "Florida Accountability PAC",
          supportOppose: "oppose",
          amount: 5000,
        },
      ],
      outsideGroupBreakdowns: [
        {
          committeeId: "FLORIDIANS_FOR_JANE_DOE",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Energy Transfer LLC",
          amount: 35000,
          contributorCount: 1,
        },
        {
          committeeId: "FLORIDIANS_FOR_JANE_DOE",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "oil_gas_energy",
          amount: 35000,
          contributorCount: 1,
        },
      ],
    });

    expect(result).toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 2,
      outsideGroupsWritten: 2,
      outsideGroupBreakdownsWritten: 2,
      outsideGroupSupportLinksWritten: 0,
    });
    expect(db.connect).toHaveBeenCalledTimes(1);
    expect(db.query).not.toHaveBeenCalled();
    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
    expect(client.release).toHaveBeenCalledTimes(1);

    const sql = client.query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("INSERT INTO public.fl_candidate_finance_summaries"))).toBe(true);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.fl_candidate_finance_direct_breakdowns"))).toHaveLength(2);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.fl_candidate_finance_outside_groups"))).toHaveLength(2);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.fl_candidate_finance_outside_group_breakdowns"))).toHaveLength(2);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.fl_candidate_finance_direct_breakdowns"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.fl_candidate_finance_outside_groups"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.fl_candidate_finance_outside_group_breakdowns"))).toBe(true);
  });

  it("wraps a supplied queryable in a transaction", async () => {
    const db = createMockDb();

    const result = await replaceFloridaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 1000,
      },
    });

    expect(result.summaryWritten).toBe(true);
    expect(db.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(db.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
  });

  it("rejects a supplied PoolClient so it cannot commit an outer transaction", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };

    await expect(
      replaceFloridaCandidateFinanceSnapshot({
        db: client,
        link: baseLink(),
        syncedAt: new Date("2026-02-03T04:05:06.000Z"),
        summary: {
          totalReceipts: 1000,
        },
      })
    ).rejects.toThrow("Florida finance snapshot writes must receive a Pool, not a PoolClient");
    expect(client.query).not.toHaveBeenCalled();
    expect(client.release).not.toHaveBeenCalled();
  });

  it("uses current snapshot keys when cleaning repeated writes", async () => {
    const db = createMockDb();
    const syncedAt = new Date("2026-02-03T04:05:06.000Z");

    await replaceFloridaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt,
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 700,
        },
      ],
      outsideGroups: [
        {
          committeeId: "FLORIDIANS_FOR_JANE_DOE",
          committeeName: "Floridians for Jane Doe",
          supportOppose: "support",
          amount: 1000,
        },
      ],
      outsideGroupBreakdowns: [
        {
          committeeId: "FLORIDIANS_FOR_JANE_DOE",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Energy Transfer LLC",
          amount: 1000,
        },
      ],
    });
    await replaceFloridaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt,
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Teacher",
          amount: 500,
        },
      ],
      outsideGroups: [
        {
          committeeId: "FLORIDA_ACCOUNTABILITY_PAC",
          committeeName: "Florida Accountability PAC",
          supportOppose: "oppose",
          amount: 2000,
        },
      ],
      outsideGroupBreakdowns: [
        {
          committeeId: "FLORIDA_ACCOUNTABILITY_PAC",
          supportOppose: "oppose",
          categoryType: "industry",
          categoryName: "oil_gas_energy",
          amount: 2000,
        },
      ],
    });

    const directDeleteCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("DELETE FROM public.fl_candidate_finance_direct_breakdowns")
    );
    expect(directDeleteCalls.at(-1)?.[1]).toEqual([
      LINK_ID,
      2026,
      JSON.stringify([{ category_type: "occupation", category_name: "Teacher" }]),
    ]);

    const groupDeleteCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("DELETE FROM public.fl_candidate_finance_outside_groups")
    );
    expect(groupDeleteCalls.at(-1)?.[1]).toEqual([
      LINK_ID,
      2026,
      JSON.stringify([{ committee_id: "FLORIDA_ACCOUNTABILITY_PAC", support_oppose: "oppose" }]),
    ]);

    const outsideBreakdownDeleteCalls = db.query.mock.calls.filter((call) =>
      String(call[0]).includes("DELETE FROM public.fl_candidate_finance_outside_group_breakdowns")
    );
    expect(outsideBreakdownDeleteCalls.at(-1)?.[1]).toEqual([
      LINK_ID,
      2026,
      JSON.stringify([
        {
          committee_id: "FLORIDA_ACCOUNTABILITY_PAC",
          support_oppose: "oppose",
          category_type: "industry",
          category_name: "oil_gas_energy",
        },
      ]),
    ]);
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
      replaceFloridaCandidateFinanceSnapshot({
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
      replaceFloridaCandidateFinanceSnapshot({
        db,
        link: {
          ...baseLink(),
          committeeId: " ",
        },
      })
    ).rejects.toThrow("Florida committee id is required");

    expect(db.query).not.toHaveBeenCalled();
  });

  it("rejects invalid election years", async () => {
    const db = createMockDb();

    await expect(
      upsertFloridaFinanceLink({
        db,
        link: {
          ...baseLink(),
          electionYear: 1995,
        },
      })
    ).rejects.toThrow("Invalid Florida finance election year");

    expect(db.query).not.toHaveBeenCalled();
  });
});
