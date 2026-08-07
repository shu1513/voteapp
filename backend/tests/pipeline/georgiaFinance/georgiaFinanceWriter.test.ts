import { describe, expect, it, vi } from "vitest";

import {
  replaceGeorgiaCandidateFinanceSnapshot,
  upsertGeorgiaFinanceLink,
} from "../../../src/pipeline/georgiaFinance/georgiaFinanceWriter.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";
const LINK_ID = "33333333-3333-3333-3333-333333333333";
const SOURCE_URL = "https://peachfile.ethics.ga.gov/public/cf/publiccandidate";

function createMockDb() {
  return {
    query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
  };
}

function createMockPool() {
  const client = {
    query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
    release: vi.fn(),
  };
  return {
    client,
    db: {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    },
  };
}

function baseLink() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    electionYear: 2026,
    candidateNameNormalized: "JANE EXAMPLE",
    officeName: "Governor",
    committeeId: "100035",
    committeeName: "Jane Example for Georgia, Inc.",
    linkSource: "peachfile_api" as const,
    sourceUrl: SOURCE_URL,
    lastVerifiedAt: new Date("2026-01-01T00:00:00.000Z"),
  };
}

describe("georgiaFinanceWriter", () => {
  it("upserts Georgia finance links and returns the link id", async () => {
    const db = createMockDb();

    await expect(upsertGeorgiaFinanceLink({ db, link: baseLink() })).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.ga_candidate_finance_links");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ON CONFLICT (candidate_id, election_id, committee_id)");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("RETURNING id");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "JANE EXAMPLE",
      "Governor",
      null,
      "100035",
      "Jane Example for Georgia, Inc.",
      "active",
      "peachfile_api",
      SOURCE_URL,
      "2026-01-01T00:00:00.000Z",
    ]);
  });

  it("trims committee ids before writing", async () => {
    const db = createMockDb();

    await upsertGeorgiaFinanceLink({
      db,
      link: { ...baseLink(), committeeId: " 100035 " },
    });

    expect(db.query.mock.calls[0]?.[1]).toContain("100035");
  });

  it("replaces a Georgia finance snapshot inside a transaction", async () => {
    const { db, client } = createMockPool();

    const result = await replaceGeorgiaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 5374711.06,
        directContributionTotal: null,
        totalDisbursements: 4168947.51,
        cashOnHand: 1167791.24,
        outsideSupportTotal: 75000,
        outsideOpposeTotal: 21900,
        sourceUrl: SOURCE_URL,
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 5000,
          contributorCount: 4,
          sourceUrl: SOURCE_URL,
        },
        {
          categoryType: "contribution_size",
          categoryName: "$250-$499",
          amount: 750,
          contributorCount: 3,
          sourceUrl: SOURCE_URL,
        },
      ],
      outsideGroups: [
        {
          committeeId: "101418",
          committeeName: "Peach State Example Fund",
          supportOppose: "support",
          amount: 75000,
          sourceUrl: SOURCE_URL,
        },
        {
          committeeId: "101500",
          committeeName: "Georgia Accountability Example PAC",
          supportOppose: "oppose",
          amount: 21900,
          sourceUrl: SOURCE_URL,
        },
      ],
      outsideGroupBreakdowns: [
        {
          committeeId: "101418",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Rolling Sea Fund",
          amount: 24506,
          contributorCount: 1,
          sourceUrl: SOURCE_URL,
        },
      ],
    });

    expect(result).toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 2,
      outsideGroupsWritten: 2,
      outsideGroupBreakdownsWritten: 1,
    });
    expect(db.connect).toHaveBeenCalledTimes(1);
    expect(db.query).not.toHaveBeenCalled();
    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
    expect(client.release).toHaveBeenCalledTimes(1);

    const sql = client.query.mock.calls.map((call) => String(call[0]));
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.ga_candidate_finance_summaries"))).toHaveLength(1);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.ga_candidate_finance_direct_breakdowns"))).toHaveLength(2);
    expect(sql.filter((statement) => statement.includes("INSERT INTO public.ga_candidate_finance_outside_groups"))).toHaveLength(2);
    expect(
      sql.filter((statement) => statement.includes("INSERT INTO public.ga_candidate_finance_outside_group_breakdowns"))
    ).toHaveLength(1);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ga_candidate_finance_direct_breakdowns"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ga_candidate_finance_outside_groups"))).toBe(true);
    expect(sql.some((statement) => statement.includes("DELETE FROM public.ga_candidate_finance_outside_group_breakdowns"))).toBe(
      true
    );

    const summaryCall = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ga_candidate_finance_summaries")
    );
    expect(String(summaryCall?.[0])).toContain("total_receipts = EXCLUDED.total_receipts");
    expect(String(summaryCall?.[0])).toContain("direct_contribution_total = EXCLUDED.direct_contribution_total");
    expect(String(summaryCall?.[0])).toContain("cash_on_hand = EXCLUDED.cash_on_hand");
    expect(String(summaryCall?.[0])).toContain("outside_support_total = COALESCE(EXCLUDED.outside_support_total");
    expect(String(summaryCall?.[0])).toContain("outside_oppose_total = COALESCE(EXCLUDED.outside_oppose_total");
    expect(summaryCall?.[1]).toEqual([
      LINK_ID,
      2026,
      5374711.06,
      null,
      4168947.51,
      1167791.24,
      75000,
      21900,
      SOURCE_URL,
      "2026-02-03T04:05:06.000Z",
    ]);
  });

  it("deactivates other active peachfile links for the same candidate election", async () => {
    const { db, client } = createMockPool();

    await replaceGeorgiaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: { totalReceipts: 1000 },
    });

    const deactivationCall = client.query.mock.calls.find((call) =>
      String(call[0]).includes("UPDATE public.ga_candidate_finance_links")
    );
    expect(String(deactivationCall?.[0])).toContain("link_source = 'peachfile_api'");
    expect(deactivationCall?.[1]).toEqual([CANDIDATE_ID, ELECTION_ID, LINK_ID]);
  });

  it("does not deactivate peachfile links when the incoming link is manual", async () => {
    const { db, client } = createMockPool();

    await replaceGeorgiaCandidateFinanceSnapshot({
      db,
      link: { ...baseLink(), linkSource: "manual" as const },
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: { totalReceipts: 1000 },
    });

    const sql = client.query.mock.calls.map((call) => String(call[0]));
    expect(sql.some((statement) => statement.includes("UPDATE public.ga_candidate_finance_links"))).toBe(false);
  });

  it("preserves prior outside totals when a direct-only refresh has no IE data", async () => {
    const { db, client } = createMockPool();

    await replaceGeorgiaCandidateFinanceSnapshot({
      db,
      link: baseLink(),
      syncedAt: new Date("2026-02-03T04:05:06.000Z"),
      summary: {
        totalReceipts: 5374711.06,
        directContributionTotal: null,
        outsideSupportTotal: null,
        outsideOpposeTotal: null,
      },
    });

    const summaryCall = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ga_candidate_finance_summaries")
    );
    expect(String(summaryCall?.[0])).toContain(
      "outside_support_total = COALESCE(EXCLUDED.outside_support_total, ga_candidate_finance_summaries.outside_support_total)"
    );
    expect(String(summaryCall?.[0])).toContain("total_disbursements = EXCLUDED.total_disbursements");
  });

  it("requires each outside group breakdown to pair with an outside group in the snapshot", async () => {
    const { db, client } = createMockPool();

    await expect(
      replaceGeorgiaCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        outsideGroups: [
          {
            committeeId: "101418",
            committeeName: "Peach State Example Fund",
            supportOppose: "support",
            amount: 1000,
          },
        ],
        outsideGroupBreakdowns: [
          {
            committeeId: "101418",
            supportOppose: "oppose",
            categoryType: "donor",
            categoryName: "Rolling Sea Fund",
            amount: 1000,
          },
        ],
      })
    ).rejects.toThrow(/outside group/i);
    expect(client.query).not.toHaveBeenCalled();
  });

  it("rejects empty committee ids before writing", async () => {
    const db = createMockDb();

    await expect(
      upsertGeorgiaFinanceLink({
        db,
        link: { ...baseLink(), committeeId: "   " },
      })
    ).rejects.toThrow(/committee id/i);

    expect(db.query).not.toHaveBeenCalled();
  });

  it("rejects a supplied PoolClient so it cannot commit an outer transaction", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }),
      release: vi.fn(),
    };

    await expect(
      replaceGeorgiaCandidateFinanceSnapshot({
        db: client as never,
        link: baseLink(),
        summary: { totalReceipts: 1000 },
      })
    ).rejects.toThrow("Georgia finance snapshot writes must receive a Pool, not a PoolClient");
    expect(client.query).not.toHaveBeenCalled();
    expect(client.release).not.toHaveBeenCalled();
  });

  it("rolls back and releases the client when a transactional write fails", async () => {
    const client = {
      query: vi
        .fn()
        .mockResolvedValueOnce({ rows: [], rowCount: 0 })
        .mockRejectedValueOnce(new Error("write failed")),
      release: vi.fn(),
    };
    const db = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    };

    await expect(
      replaceGeorgiaCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        summary: { totalReceipts: 1000 },
      })
    ).rejects.toThrow("write failed");

    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("ROLLBACK");
    expect(client.release).toHaveBeenCalledTimes(1);
  });

  it("rejects election years below the Georgia floor", async () => {
    const db = createMockDb();

    // Archive-only 2022–2025 cycles are out of v1 link scope because the
    // PeachFile filerEntityId identity does not exist for them
    // (georgia_plan.md D7).
    await expect(
      upsertGeorgiaFinanceLink({
        db,
        link: { ...baseLink(), electionYear: 2025 },
      })
    ).rejects.toThrow("Invalid Georgia finance election year");

    expect(db.query).not.toHaveBeenCalled();
  });
});
