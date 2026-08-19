import { describe, expect, it, vi } from "vitest";

import {
  listSelectableResearchAreas,
  listUserResearchAreaPreferences,
  replaceUserResearchAreaPreferences,
  UserResearchAreaPreferencesError,
} from "../../../src/pipeline/users/userResearchAreaPreferences.js";

const userId = "11111111-1111-4111-8111-111111111111";
const researchAreaIdA = "22222222-2222-4222-8222-222222222222";
const researchAreaIdB = "33333333-3333-4333-8333-333333333333";
const researchAreaIdC = "44444444-4444-4444-8444-444444444444";

function createMockQueryable() {
  return {
    query: vi.fn(),
  };
}

function createMockTransactionalDb() {
  const client = {
    query: vi.fn(),
    release: vi.fn(),
  };
  const db = {
    connect: vi.fn().mockResolvedValue(client),
  };
  return { db, client };
}

function expectPreferenceError(error: unknown, code: string): void {
  expect(error).toBeInstanceOf(UserResearchAreaPreferencesError);
  expect((error as UserResearchAreaPreferencesError).code).toBe(code);
}

function makeResearchAreaId(index: number): string {
  return `22222222-2222-4222-8222-${index.toString().padStart(12, "0")}`;
}

describe("listSelectableResearchAreas", () => {
  it("returns selectable research areas in catalog order", async () => {
    const db = createMockQueryable();
    db.query.mockResolvedValueOnce({
      rows: [
        {
          id: researchAreaIdA,
          slug: "housing_affordability",
          name: "Housing Affordability",
          description: "Housing policy",
        },
        {
          id: researchAreaIdB,
          slug: "healthcare_affordability",
          name: "Healthcare Affordability",
          description: null,
        },
      ],
    });

    await expect(listSelectableResearchAreas(db)).resolves.toEqual({
      research_areas: [
        {
          id: researchAreaIdA,
          slug: "housing_affordability",
          name: "Housing Affordability",
          description: "Housing policy",
        },
        {
          id: researchAreaIdB,
          slug: "healthcare_affordability",
          name: "Healthcare Affordability",
          description: null,
        },
      ],
    });
    expect(String(db.query.mock.calls[0]?.[0])).toContain("is_user_selectable = true");
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ORDER BY name ASC, slug ASC");
  });
});

describe("listUserResearchAreaPreferences", () => {
  it("rejects invalid user IDs before querying", async () => {
    const db = createMockQueryable();

    await expect(listUserResearchAreaPreferences(db, "not-a-uuid")).rejects.toSatisfy((error) => {
      expectPreferenceError(error, "invalid_user_id");
      return true;
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("rejects missing or deleted users", async () => {
    const db = createMockQueryable();
    db.query.mockResolvedValueOnce({ rows: [] });

    await expect(listUserResearchAreaPreferences(db, userId)).rejects.toSatisfy((error) => {
      expectPreferenceError(error, "user_not_found");
      return true;
    });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("u.deleted_at IS NULL");
    expect(db.query.mock.calls[0]?.[1]).toEqual([userId]);
  });

  it("returns an empty list when the user has no preferences", async () => {
    const db = createMockQueryable();
    db.query.mockResolvedValueOnce({
      rows: [
        {
          user_id: userId,
          research_area_id: null,
          slug: null,
          name: null,
          description: null,
          rank: null,
        },
      ],
    });

    await expect(listUserResearchAreaPreferences(db, userId)).resolves.toEqual({ preferences: [] });
  });

  it("returns ranked and unranked preferences in query order", async () => {
    const db = createMockQueryable();
    db.query.mockResolvedValueOnce({
      rows: [
        {
          user_id: userId,
          research_area_id: researchAreaIdB,
          slug: "housing_affordability",
          name: "Housing Affordability",
          description: "Housing",
          rank: 1,
          direction: "oppose",
          hard_veto: true,
        },
        {
          user_id: userId,
          research_area_id: researchAreaIdA,
          slug: "healthcare_affordability",
          name: "Healthcare Affordability",
          description: null,
          rank: null,
          direction: "support",
          hard_veto: false,
        },
      ],
    });

    await expect(listUserResearchAreaPreferences(db, userId)).resolves.toEqual({
      preferences: [
        {
          research_area_id: researchAreaIdB,
          slug: "housing_affordability",
          name: "Housing Affordability",
          description: "Housing",
          rank: 1,
          direction: "oppose",
          hard_veto: true,
        },
        {
          research_area_id: researchAreaIdA,
          slug: "healthcare_affordability",
          name: "Healthcare Affordability",
          description: null,
          rank: null,
          direction: "support",
          hard_veto: false,
        },
      ],
    });
    expect(String(db.query.mock.calls[0]?.[0])).toContain("ORDER BY preference.rank ASC NULLS LAST");
  });
});

describe("replaceUserResearchAreaPreferences", () => {
  it("rejects invalid user IDs before opening a database connection", async () => {
    const { db } = createMockTransactionalDb();

    await expect(replaceUserResearchAreaPreferences(db, "not-a-uuid", [])).rejects.toSatisfy((error) => {
      expectPreferenceError(error, "invalid_user_id");
      return true;
    });
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("accepts every selectable area ranked 1..n (no count or rank ceiling)", async () => {
    const { db, client } = createMockTransactionalDb();
    const preferences = Array.from({ length: 25 }, (_value, index) => ({
      researchAreaId: makeResearchAreaId(index + 1),
      rank: index + 1,
    }));
    client.query
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: userId }] })
      .mockResolvedValueOnce({ rows: preferences.map((p) => ({ id: p.researchAreaId, is_user_selectable: true })) })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ user_id: userId, research_area_id: null, slug: null, name: null, description: null, rank: null, direction: null, hard_veto: null }] })
      .mockResolvedValueOnce({ rows: [] });

    await expect(replaceUserResearchAreaPreferences(db, userId, preferences)).resolves.toBeDefined();
    expect(client.query.mock.calls[4]?.[1]?.[2]).toEqual(preferences.map((p) => p.rank));
  });

  it("rejects an invalid direction or hard_veto before opening a database connection", async () => {
    const { db } = createMockTransactionalDb();

    await expect(
      replaceUserResearchAreaPreferences(db, userId, [
        { researchAreaId: researchAreaIdA, rank: 1, direction: "against" as never },
      ])
    ).rejects.toSatisfy((error) => {
      expectPreferenceError(error, "invalid_preferences");
      return true;
    });
    await expect(
      replaceUserResearchAreaPreferences(db, userId, [
        { researchAreaId: researchAreaIdA, rank: 1, hardVeto: "yes" as never },
      ])
    ).rejects.toSatisfy((error) => {
      expectPreferenceError(error, "invalid_preferences");
      return true;
    });
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("rejects duplicate research areas before opening a database connection", async () => {
    const { db } = createMockTransactionalDb();

    await expect(
      replaceUserResearchAreaPreferences(db, userId, [
        { researchAreaId: researchAreaIdA, rank: 1 },
        { researchAreaId: researchAreaIdA.toUpperCase(), rank: 2 },
      ])
    ).rejects.toSatisfy((error) => {
      expectPreferenceError(error, "invalid_preferences");
      return true;
    });
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("rejects duplicate non-null ranks before opening a database connection", async () => {
    const { db } = createMockTransactionalDb();

    await expect(
      replaceUserResearchAreaPreferences(db, userId, [
        { researchAreaId: researchAreaIdA, rank: 1 },
        { researchAreaId: researchAreaIdB, rank: 1 },
      ])
    ).rejects.toSatisfy((error) => {
      expectPreferenceError(error, "invalid_preferences");
      return true;
    });
    expect(db.connect).not.toHaveBeenCalled();
  });

  it.each([0, 2, 2147483648])("rejects rank %d for a one-item list before opening a database connection", async (rank) => {
    const { db } = createMockTransactionalDb();

    await expect(
      replaceUserResearchAreaPreferences(db, userId, [{ researchAreaId: researchAreaIdA, rank }])
    ).rejects.toSatisfy((error) => {
      expectPreferenceError(error, "invalid_preferences");
      return true;
    });
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("clears preferences transactionally", async () => {
    const { db, client } = createMockTransactionalDb();
    client.query
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: userId }] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({
        rows: [
          {
            user_id: userId,
            research_area_id: null,
            slug: null,
            name: null,
            description: null,
            rank: null,
            direction: null,
            hard_veto: null,
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [] });

    const result = await replaceUserResearchAreaPreferences(db, userId, []);

    expect(result).toEqual({ preferences: [] });
    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(String(client.query.mock.calls[1]?.[0])).toContain("FOR UPDATE");
    expect(String(client.query.mock.calls[2]?.[0])).toContain("DELETE FROM public.user_research_area_preferences");
    expect(client.query.mock.calls.some((call) => String(call[0]).includes("INSERT INTO public.user_research_area_preferences"))).toBe(
      false
    );
    expect(client.query.mock.calls[4]?.[0]).toBe("COMMIT");
    expect(client.release).toHaveBeenCalledOnce();
  });

  it("replaces preferences with ranked and unranked selections", async () => {
    const { db, client } = createMockTransactionalDb();
    client.query
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: userId }] })
      .mockResolvedValueOnce({
        rows: [
          { id: researchAreaIdA, is_user_selectable: true },
          { id: researchAreaIdB, is_user_selectable: true },
        ],
      })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({
        rows: [
          {
            user_id: userId,
            research_area_id: researchAreaIdA,
            slug: "healthcare_affordability",
            name: "Healthcare Affordability",
            description: null,
            rank: 1,
            direction: "oppose",
            hard_veto: false,
          },
          {
            user_id: userId,
            research_area_id: researchAreaIdB,
            slug: "housing_affordability",
            name: "Housing Affordability",
            description: null,
            rank: null,
            direction: "support",
            hard_veto: false,
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [] });

    const result = await replaceUserResearchAreaPreferences(db, userId, [
      { researchAreaId: researchAreaIdA, rank: 1, direction: "oppose" },
      { researchAreaId: researchAreaIdB, rank: null },
    ]);

    expect(result.preferences.map((preference) => preference.research_area_id)).toEqual([
      researchAreaIdA,
      researchAreaIdB,
    ]);
    expect(String(client.query.mock.calls[2]?.[0])).toContain("is_user_selectable");
    expect(String(client.query.mock.calls[3]?.[0])).toContain("DELETE FROM public.user_research_area_preferences");
    expect(String(client.query.mock.calls[3]?.[0])).toContain("RETURNING");
    expect(String(client.query.mock.calls[4]?.[0])).toContain("INSERT INTO public.user_research_area_preferences");
    // Nothing was stored before: omitted fields take the defaults.
    expect(client.query.mock.calls[4]?.[1]).toEqual([
      userId,
      [researchAreaIdA, researchAreaIdB],
      [1, null],
      ["oppose", "support"],
      [false, false],
    ]);
    expect(client.query.mock.calls[6]?.[0]).toBe("COMMIT");
  });

  it("keeps stored direction/hard_veto for areas re-sent without them, and applies explicit values", async () => {
    const { db, client } = createMockTransactionalDb();
    client.query
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: userId }] })
      .mockResolvedValueOnce({
        rows: [
          { id: researchAreaIdA, is_user_selectable: true },
          { id: researchAreaIdB, is_user_selectable: true },
          { id: researchAreaIdC, is_user_selectable: true },
        ],
      })
      // DELETE ... RETURNING: what the user had before this PUT (C was not
      // stored, A had a veto + oppose, B had a veto). Upper-cased id on B
      // checks the case-insensitive match.
      .mockResolvedValueOnce({
        rows: [
          { research_area_id: researchAreaIdA, direction: "oppose", hard_veto: true },
          { research_area_id: researchAreaIdB.toUpperCase(), direction: "support", hard_veto: true },
        ],
      })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ user_id: userId, research_area_id: null, slug: null, name: null, description: null, rank: null, direction: null, hard_veto: null }] })
      .mockResolvedValueOnce({ rows: [] });

    await replaceUserResearchAreaPreferences(db, userId, [
      // Rank-only (mobile-style) resend: keeps oppose + veto.
      { researchAreaId: researchAreaIdA, rank: 1 },
      // Explicit values win over what was stored.
      { researchAreaId: researchAreaIdB, rank: 2, direction: "oppose", hardVeto: false },
      // New area, nothing stored: defaults.
      { researchAreaId: researchAreaIdC, rank: 3 },
    ]);

    expect(client.query.mock.calls[4]?.[1]).toEqual([
      userId,
      [researchAreaIdA, researchAreaIdB, researchAreaIdC],
      [1, 2, 3],
      ["oppose", "oppose", "support"],
      [true, false, false],
    ]);
  });

  it("rolls back when the user is missing or deleted", async () => {
    const { db, client } = createMockTransactionalDb();
    client.query.mockResolvedValueOnce({ rows: [] }).mockResolvedValueOnce({ rows: [] }).mockResolvedValueOnce({ rows: [] });

    await expect(replaceUserResearchAreaPreferences(db, userId, [])).rejects.toSatisfy((error) => {
      expectPreferenceError(error, "user_not_found");
      return true;
    });

    expect(client.query.mock.calls[2]?.[0]).toBe("ROLLBACK");
    expect(client.release).toHaveBeenCalledOnce();
  });

  it("rolls back and reports unknown research areas without deleting existing preferences", async () => {
    const { db, client } = createMockTransactionalDb();
    client.query
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: userId }] })
      .mockResolvedValueOnce({ rows: [{ id: researchAreaIdA, is_user_selectable: true }] })
      .mockResolvedValueOnce({ rows: [] });

    await expect(
      replaceUserResearchAreaPreferences(db, userId, [
        { researchAreaId: researchAreaIdA, rank: 1 },
        { researchAreaId: researchAreaIdB, rank: null },
      ])
    ).rejects.toSatisfy((error) => {
      expectPreferenceError(error, "unknown_research_area_ids");
      expect((error as UserResearchAreaPreferencesError).details.unknownResearchAreaIds).toEqual([researchAreaIdB]);
      return true;
    });

    expect(client.query.mock.calls.some((call) => String(call[0]).includes("DELETE FROM public.user_research_area_preferences"))).toBe(
      false
    );
    expect(client.query.mock.calls[3]?.[0]).toBe("ROLLBACK");
  });

  it("rolls back and rejects non-selectable research areas such as general or impartiality", async () => {
    const { db, client } = createMockTransactionalDb();
    client.query
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: userId }] })
      .mockResolvedValueOnce({
        rows: [
          { id: researchAreaIdA, is_user_selectable: true },
          { id: researchAreaIdC, is_user_selectable: false },
        ],
      })
      .mockResolvedValueOnce({ rows: [] });

    await expect(
      replaceUserResearchAreaPreferences(db, userId, [
        { researchAreaId: researchAreaIdA, rank: 1 },
        { researchAreaId: researchAreaIdC, rank: null },
      ])
    ).rejects.toSatisfy((error) => {
      expectPreferenceError(error, "unselectable_research_area_ids");
      expect((error as UserResearchAreaPreferencesError).details.unselectableResearchAreaIds).toEqual([researchAreaIdC]);
      return true;
    });

    expect(client.query.mock.calls.some((call) => String(call[0]).includes("DELETE FROM public.user_research_area_preferences"))).toBe(
      false
    );
    expect(client.query.mock.calls[3]?.[0]).toBe("ROLLBACK");
  });
});
