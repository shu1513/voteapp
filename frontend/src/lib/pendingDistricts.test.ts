import { beforeEach, describe, expect, it } from "vitest";
import { clearPendingDistrictIds, readPendingDistrictIds, savePendingDistrictIds } from "./pendingDistricts";

beforeEach(() => {
  sessionStorage.clear();
});

describe("pendingDistricts", () => {
  it("round-trips district ids through sessionStorage", () => {
    savePendingDistrictIds(["a", "b"]);
    expect(readPendingDistrictIds()).toEqual(["a", "b"]);
    clearPendingDistrictIds();
    expect(readPendingDistrictIds()).toEqual([]);
  });

  it("returns empty for missing or corrupted values", () => {
    expect(readPendingDistrictIds()).toEqual([]);
    sessionStorage.setItem("voteapp_pending_district_ids", "{not json");
    expect(readPendingDistrictIds()).toEqual([]);
    sessionStorage.setItem("voteapp_pending_district_ids", JSON.stringify({ nope: 1 }));
    expect(readPendingDistrictIds()).toEqual([]);
    sessionStorage.setItem("voteapp_pending_district_ids", JSON.stringify(["ok", 5, null]));
    expect(readPendingDistrictIds()).toEqual(["ok"]);
  });
});
