import { describe, expect, it } from "vitest";
import { getQueryClient } from "./root";

describe("getQueryClient in the browser", () => {
  it("returns a stable singleton so the cache survives re-renders and navigation", () => {
    expect(getQueryClient()).toBe(getQueryClient());
  });
});
