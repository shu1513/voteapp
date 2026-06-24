import { describe, expect, it, vi } from "vitest";

import {
  listUserDistrictIds,
  UserDistrictReaderError,
} from "../../../src/pipeline/users/userDistrictReader.js";

const userId = "11111111-1111-4111-8111-111111111111";
const districtIdA = "22222222-2222-4222-8222-222222222222";
const districtIdB = "33333333-3333-4333-8333-333333333333";

function createMockDb() {
  return {
    query: vi.fn(),
  };
}

function expectReaderError(error: unknown, code: string): void {
  expect(error).toBeInstanceOf(UserDistrictReaderError);
  expect((error as UserDistrictReaderError).code).toBe(code);
}

describe("listUserDistrictIds", () => {
  it("rejects invalid user IDs before querying", async () => {
    const db = createMockDb();

    await expect(listUserDistrictIds(db, "not-a-uuid")).rejects.toSatisfy((error) => {
      expectReaderError(error, "invalid_user_id");
      return true;
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("rejects missing or deleted users", async () => {
    const db = createMockDb();
    db.query.mockResolvedValueOnce({ rows: [] });

    await expect(listUserDistrictIds(db, userId)).rejects.toSatisfy((error) => {
      expectReaderError(error, "user_not_found");
      return true;
    });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("deleted_at IS NULL");
    expect(db.query.mock.calls[0]?.[1]).toEqual([userId]);
  });

  it("returns an empty list when the user has no saved districts", async () => {
    const db = createMockDb();
    db.query.mockResolvedValueOnce({ rows: [{ id: userId }] }).mockResolvedValueOnce({ rows: [] });

    await expect(listUserDistrictIds(db, userId)).resolves.toEqual([]);

    expect(db.query).toHaveBeenCalledTimes(2);
    expect(String(db.query.mock.calls[1]?.[0])).toContain("FROM public.user_districts AS ud");
    expect(String(db.query.mock.calls[1]?.[0])).toContain("JOIN public.districts AS d");
    expect(db.query.mock.calls[1]?.[1]).toEqual([userId]);
  });

  it("returns saved district IDs in stable saved order", async () => {
    const db = createMockDb();
    db.query.mockResolvedValueOnce({ rows: [{ id: userId }] }).mockResolvedValueOnce({
      rows: [{ district_id: districtIdA }, { district_id: districtIdB }],
    });

    await expect(listUserDistrictIds(db, userId)).resolves.toEqual([districtIdA, districtIdB]);

    expect(String(db.query.mock.calls[1]?.[0])).toContain("ORDER BY ud.created_at ASC, ud.id ASC");
  });
});
