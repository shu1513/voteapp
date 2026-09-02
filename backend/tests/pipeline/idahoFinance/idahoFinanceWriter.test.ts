import { readFileSync } from "node:fs";

import { describe, expect, it, vi } from "vitest";

import {
  normalizeIdahoCandidateNameForStorage,
  replaceIdahoCandidateFinanceSnapshot,
  upsertIdahoFinanceLink,
} from "../../../src/pipeline/idahoFinance/idahoFinanceWriter.js";
import { GUID_A } from "./idahoTestFixtures.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const LINK_ID = "33333333-3333-4333-8333-333333333333";
const SOURCE_URL = `https://sunshine.voteidaho.gov/public/cf/candidateprofile?guid=${GUID_A}&tabName=CAN&isLegacy=false`;
const VERIFIED_AT = new Date("2026-09-01T00:00:00.000Z");

function baseLink() {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    electionYear: 2026,
    candidateNameNormalized: "TODD ACHILLES",
    officeName: "State Senator",
    district: "16",
    registrationGuid: GUID_A,
    filerName: "Achilles, Todd Baker",
    linkSource: "sunshine_grid" as const,
    sourceUrl: SOURCE_URL,
    lastVerifiedAt: VERIFIED_AT,
  };
}

function successfulQuery(sql: unknown) {
  if (String(sql).includes("INSERT INTO public.id_candidate_finance_links")) {
    return Promise.resolve({ rows: [{ id: LINK_ID }], rowCount: 1 });
  }
  return Promise.resolve({ rows: [], rowCount: 0 });
}

function transactionalDb() {
  const client = { query: vi.fn(successfulQuery), release: vi.fn() };
  const db = { query: vi.fn(), connect: vi.fn().mockResolvedValue(client) };
  return { db, client };
}

describe("idahoFinanceWriter", () => {
  it("writes the registration-guid link identity and protects manual links", async () => {
    const db = { query: vi.fn(successfulQuery) };

    await expect(
      upsertIdahoFinanceLink({ db, link: { ...baseLink(), registrationGuid: GUID_A.toUpperCase() } })
    ).resolves.toEqual({ linkId: LINK_ID });

    expect(db.query).toHaveBeenCalledTimes(2);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("link_source='manual'");
    const insert = db.query.mock.calls[1];
    expect(String(insert?.[0])).toContain("registration_guid");
    expect(String(insert?.[0])).toContain("filer_name");
    expect(String(insert?.[0])).toContain("ON CONFLICT (candidate_id, election_id, registration_guid)");
    expect(insert?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "TODD ACHILLES",
      "State Senator",
      "16",
      GUID_A,
      "Achilles, Todd Baker",
      "active",
      "sunshine_grid",
      SOURCE_URL,
      VERIFIED_AT.toISOString(),
    ]);
  });

  it("replaces a full snapshot transactionally with filer_key outside groups", async () => {
    const { db, client } = transactionalDb();

    await expect(
      replaceIdahoCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        syncedAt: new Date("2026-09-01T12:00:00.000Z"),
        summary: {
          totalReceipts: 1500,
          directContributionTotal: 1500,
          totalDisbursements: 50,
          cashOnHand: -1321.99,
          outsideSupportTotal: 250,
          outsideOpposeTotal: 0,
          sourceUrl: SOURCE_URL,
        },
        directBreakdowns: [
          { categoryType: "contribution_size", categoryName: "$1,000+", amount: 1000, contributorCount: 1 },
          { categoryType: "contributor_source_type", categoryName: "individual", amount: 1500, contributorCount: 3 },
        ],
        outsideGroups: [
          {
            filerKey: "55555555-5555-4555-8555-555555555501",
            filerName: "Sample PAC",
            supportOppose: "support",
            amount: 250,
            sourceUrl: SOURCE_URL,
          },
        ],
      })
    ).resolves.toEqual({
      linkId: LINK_ID,
      summaryWritten: true,
      directBreakdownsWritten: 2,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 0,
    });

    const calls = client.query.mock.calls;
    expect(calls[0]?.[0]).toBe("BEGIN");
    expect(calls.at(-1)?.[0]).toBe("COMMIT");
    expect(client.release).toHaveBeenCalledOnce();

    const summary = calls.find((call) => String(call[0]).includes("id_candidate_finance_summaries"));
    expect(String(summary?.[0])).toContain(
      "total_receipts = COALESCE(EXCLUDED.total_receipts, id_candidate_finance_summaries.total_receipts)"
    );
    expect(summary?.[1]?.slice(2, 9)).toEqual([1500, 1500, 50, -1321.99, 250, 0, SOURCE_URL]);

    const directInserts = calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.id_candidate_finance_direct_breakdowns")
    );
    expect(directInserts.map((call) => call[1]?.[2])).toEqual(["contribution_size", "contributor_source_type"]);

    const outsideInsert = calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.id_candidate_finance_outside_groups")
    );
    expect(String(outsideInsert?.[0])).toContain("filer_key");
    expect(String(outsideInsert?.[0])).toContain("filer_name");
    expect(outsideInsert?.[1]).toContain("55555555-5555-4555-8555-555555555501");

    const supersede = calls.find(
      (call) =>
        String(call[0]).includes("UPDATE public.id_candidate_finance_links") && String(call[0]).includes("id <>")
    );
    expect(String(supersede?.[0])).toContain("link_source = 'sunshine_grid'");
  });

  it("preserves stored rows when sections are unavailable and clears them after an empty fetch", async () => {
    const unavailable = transactionalDb();
    await replaceIdahoCandidateFinanceSnapshot({
      db: unavailable.db,
      link: baseLink(),
      summary: {
        totalReceipts: null,
        directContributionTotal: null,
        totalDisbursements: null,
        cashOnHand: null,
        outsideSupportTotal: null,
        outsideOpposeTotal: null,
        sourceUrl: null,
      },
    });
    expect(
      unavailable.client.query.mock.calls.some((call) => String(call[0]).includes("DELETE FROM public.id_candidate_finance"))
    ).toBe(false);

    const empty = transactionalDb();
    await replaceIdahoCandidateFinanceSnapshot({
      db: empty.db,
      link: baseLink(),
      summary: {
        totalReceipts: 0,
        directContributionTotal: 0,
        totalDisbursements: 0,
        cashOnHand: 0,
        outsideSupportTotal: 0,
        outsideOpposeTotal: 0,
        sourceUrl: SOURCE_URL,
      },
      directBreakdowns: [],
      outsideGroups: [],
    });
    const deletes = empty.client.query.mock.calls.filter((call) => String(call[0]).includes("DELETE FROM public.id_candidate_finance"));
    expect(deletes.length).toBeGreaterThanOrEqual(2);
  });

  it("rejects invalid guids, foreign category types, and pre-2026 years before database writes", async () => {
    const db = { query: vi.fn(), connect: vi.fn() };

    await expect(
      replaceIdahoCandidateFinanceSnapshot({ db: db as never, link: { ...baseLink(), registrationGuid: "257" } })
    ).rejects.toThrow('Invalid Idaho registration guid: "257"');
    await expect(
      replaceIdahoCandidateFinanceSnapshot({
        db: db as never,
        link: baseLink(),
        directBreakdowns: [{ categoryType: "occupation" as never, categoryName: "Retired", amount: 100 }],
      })
    ).rejects.toThrow("Idaho finance direct breakdown category type is not allowed: occupation");
    await expect(
      replaceIdahoCandidateFinanceSnapshot({ db: db as never, link: { ...baseLink(), electionYear: 2024 } })
    ).rejects.toThrow(/2024/);
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("rolls back and releases the client when a snapshot query fails", async () => {
    const client = {
      query: vi.fn((sql: unknown) => {
        if (String(sql).includes("id_candidate_finance_summaries")) {
          return Promise.reject(new Error("summary failed"));
        }
        return successfulQuery(sql);
      }),
      release: vi.fn(),
    };
    const db = { query: vi.fn(), connect: vi.fn().mockResolvedValue(client) };

    await expect(
      replaceIdahoCandidateFinanceSnapshot({
        db,
        link: baseLink(),
        summary: {
          totalReceipts: 1,
          directContributionTotal: 1,
          totalDisbursements: 1,
          cashOnHand: 0,
          outsideSupportTotal: null,
          outsideOpposeTotal: null,
          sourceUrl: SOURCE_URL,
        },
      })
    ).rejects.toThrow("summary failed");

    expect(client.query.mock.calls.at(-1)?.[0]).toBe("ROLLBACK");
    expect(client.release).toHaveBeenCalledOnce();
  });

  it("normalizes roster names for storage", () => {
    expect(normalizeIdahoCandidateNameForStorage("Raúl Labrador")).toBe("RAUL LABRADOR");
    expect(normalizeIdahoCandidateNameForStorage("Rod W. Beck, Jr.")).toBe("ROD W BECK JR");
  });
});

describe("migration 268", () => {
  const sql = readFileSync(
    new URL("../../../../db/migrations/268_add_idaho_campaign_finance_tables.sql", import.meta.url),
    "utf8"
  );

  it("pins the writer's schema contract", () => {
    for (const table of [
      "id_candidate_finance_links",
      "id_candidate_finance_summaries",
      "id_candidate_finance_direct_breakdowns",
      "id_candidate_finance_outside_groups",
      "id_candidate_finance_outside_group_breakdowns",
    ]) {
      expect(sql).toContain(`CREATE TABLE IF NOT EXISTS public.${table} (`);
    }
    expect(sql).toContain("CHECK (link_source IN ('manual', 'sunshine_grid'))");
    expect(sql).toContain("CHECK (category_type IN ('contribution_size', 'contributor_source_type'))");
    expect(sql).toContain("UNIQUE (candidate_id, election_id, registration_guid)");
    expect(sql).toContain("UNIQUE (link_id, election_year, filer_key, support_oppose)");
    expect(sql).toContain("CHECK (election_year BETWEEN 2026 AND 2100)");
    expect(GUID_A).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
    expect(sql).toContain("CHECK (registration_guid ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')");
    // cash_on_hand is signed: it must not appear in the nonnegative amounts guard.
    expect(sql).not.toContain("cash_on_hand IS NULL OR cash_on_hand >= 0");
    expect(sql).toContain("total_disbursements IS NULL OR total_disbursements >= 0");
  });

  it("keeps every identifier within Postgres's 63-character limit", () => {
    const identifiers = [...sql.matchAll(/(?:CONSTRAINT|INDEX IF NOT EXISTS|TRIGGER IF EXISTS|CREATE TRIGGER)\s+([a-z0-9_]+)/g)].map(
      (match) => match[1]!
    );
    expect(identifiers.length).toBeGreaterThan(20);
    for (const identifier of identifiers) {
      expect(identifier.length, identifier).toBeLessThanOrEqual(63);
    }
  });
});
