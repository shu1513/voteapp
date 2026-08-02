import { describe, expect, it, vi } from "vitest";

import { createStandardStateFinanceSnapshotWriter } from "../../../src/pipeline/finance/standardStateFinanceSnapshotWriter.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const NOW = new Date("2026-06-01T00:00:00.000Z");

const TABLES = {
  links: "zz_candidate_finance_links",
  summaries: "zz_candidate_finance_summaries",
  directBreakdowns: "zz_candidate_finance_direct_breakdowns",
  outsideGroups: "zz_candidate_finance_outside_groups",
  outsideGroupBreakdowns: "zz_candidate_finance_outside_group_breakdowns",
} as const;

function makeWriter(overrides?: Partial<Parameters<typeof createStandardStateFinanceSnapshotWriter>[0]>) {
  return createStandardStateFinanceSnapshotWriter({
    label: "Zetaland",
    minElectionYear: 2000,
    tables: TABLES,
    ...overrides,
  });
}

function linkInput() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    electionYear: 2026,
    candidateNameNormalized: "PAT DOE",
    officeName: "Governor",
    district: null,
    committeeId: "C-1",
    committeeName: "Doe for Zetaland",
    linkStatus: "active" as const,
    linkSource: "manual" as const,
    sourceUrl: "https://example.test/committee/C-1",
    lastVerifiedAt: NOW,
  };
}

function poolWithClient() {
  const client = {
    query: vi
      .fn()
      .mockResolvedValueOnce({ rows: [], rowCount: 0 })
      .mockResolvedValueOnce({ rows: [{ id: "link-1" }], rowCount: 1 })
      .mockResolvedValue({ rows: [], rowCount: 0 }),
    release: vi.fn(),
  };
  const db = { connect: vi.fn().mockResolvedValue(client), query: vi.fn() };
  return { db, client };
}

describe("createStandardStateFinanceSnapshotWriter config options", () => {
  it("enforces the configured election-year floor", async () => {
    const writer = makeWriter({ minElectionYear: 2008 });
    const db = { query: vi.fn() };

    await expect(
      writer.upsertLink({ db, link: { ...linkInput(), electionYear: 2007 } })
    ).rejects.toThrow("Invalid Zetaland finance election year: 2007");
    expect(db.query).not.toHaveBeenCalled();

    db.query.mockResolvedValue({ rows: [{ id: "link-1" }], rowCount: 1 });
    await expect(writer.upsertLink({ db, link: { ...linkInput(), electionYear: 2008 } })).resolves.toEqual({
      linkId: "link-1",
    });
  });

  it("rejects a nonsensical configured floor", () => {
    expect(() => makeWriter({ minElectionYear: 199 })).toThrow(
      "Invalid Zetaland finance minimum election year: 199"
    );
  });

  it("COALESCEs every summary column by default", async () => {
    const { db, client } = poolWithClient();

    await makeWriter().replaceSnapshot({
      db,
      link: linkInput(),
      syncedAt: NOW,
      summary: { totalReceipts: 100 },
    });

    const summarySql = String(client.query.mock.calls[2]?.[0]);
    expect(summarySql).toContain("total_receipts = COALESCE(EXCLUDED.total_receipts, zz_candidate_finance_summaries.total_receipts)");
    expect(summarySql).toContain("cash_on_hand = COALESCE(EXCLUDED.cash_on_hand, zz_candidate_finance_summaries.cash_on_hand)");
    expect(summarySql).toContain("last_synced_at = EXCLUDED.last_synced_at");
  });

  it("replaces the columns configured as replace, leaving the rest preserved", async () => {
    const { db, client } = poolWithClient();

    await makeWriter({
      summaryUpdatePolicy: {
        total_receipts: "replace",
        direct_contribution_total: "replace",
        total_disbursements: "replace",
        cash_on_hand: "replace",
        source_url: "replace",
      },
    }).replaceSnapshot({
      db,
      link: linkInput(),
      syncedAt: NOW,
      summary: { totalReceipts: 100 },
    });

    const summarySql = String(client.query.mock.calls[2]?.[0]);
    expect(summarySql).toContain("total_receipts = EXCLUDED.total_receipts,");
    expect(summarySql).toContain("cash_on_hand = EXCLUDED.cash_on_hand,");
    expect(summarySql).toContain("source_url = EXCLUDED.source_url,");
    expect(summarySql).toContain("outside_support_total = COALESCE(EXCLUDED.outside_support_total, zz_candidate_finance_summaries.outside_support_total)");
    expect(summarySql).toContain("outside_oppose_total = COALESCE(EXCLUDED.outside_oppose_total, zz_candidate_finance_summaries.outside_oppose_total)");
  });

  it("allows breakdowns without groups when validation is none (default)", async () => {
    const { db } = poolWithClient();

    await expect(
      makeWriter().replaceSnapshot({
        db,
        link: linkInput(),
        syncedAt: NOW,
        outsideGroupBreakdowns: [
          {
            committeeId: "PAC-1",
            supportOppose: "support",
            categoryType: "donor",
            categoryName: "Some Donor",
            amount: 500,
          },
        ],
      })
    ).resolves.toMatchObject({ outsideGroupBreakdownsWritten: 1 });
  });

  it("presence validation rejects breakdowns without groups before touching the db", async () => {
    const db = { connect: vi.fn(), query: vi.fn() };

    await expect(
      makeWriter({ outsideGroupValidation: "presence" }).replaceSnapshot({
        db,
        link: linkInput(),
        syncedAt: NOW,
        outsideGroupBreakdowns: [
          {
            committeeId: "PAC-1",
            supportOppose: "support",
            categoryType: "donor",
            categoryName: "Some Donor",
            amount: 500,
          },
        ],
      })
    ).rejects.toThrow("Zetaland outside group breakdowns require outside groups in the same snapshot");
    expect(db.connect).not.toHaveBeenCalled();
    expect(db.query).not.toHaveBeenCalled();
  });

  it("pairing validation rejects a breakdown whose group is not in the snapshot", async () => {
    const db = { connect: vi.fn(), query: vi.fn() };
    const writer = makeWriter({ outsideGroupValidation: "pairing" });

    await expect(
      writer.replaceSnapshot({
        db,
        link: linkInput(),
        syncedAt: NOW,
        outsideGroups: [
          {
            committeeId: "PAC-1",
            committeeName: "Some PAC",
            supportOppose: "support",
            amount: 500,
          },
        ],
        outsideGroupBreakdowns: [
          {
            committeeId: "PAC-1",
            supportOppose: "oppose",
            categoryType: "donor",
            categoryName: "Some Donor",
            amount: 500,
          },
        ],
      })
    ).rejects.toThrow("Zetaland outside group breakdowns must reference outside groups in the same snapshot");
    expect(db.connect).not.toHaveBeenCalled();

    const { db: okDb } = poolWithClient();
    await expect(
      writer.replaceSnapshot({
        db: okDb,
        link: linkInput(),
        syncedAt: NOW,
        outsideGroups: [
          {
            committeeId: "PAC-1",
            committeeName: "Some PAC",
            supportOppose: "support",
            amount: 500,
          },
        ],
        outsideGroupBreakdowns: [
          {
            committeeId: "PAC-1",
            supportOppose: "support",
            categoryType: "donor",
            categoryName: "Some Donor",
            amount: 500,
          },
        ],
      })
    ).resolves.toMatchObject({ outsideGroupsWritten: 1, outsideGroupBreakdownsWritten: 1 });
  });
});
