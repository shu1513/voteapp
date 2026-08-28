import { describe, expect, it } from "vitest";

import { parseDelawareRefreshArtifactsArgs } from "../../src/scripts/refreshDelawareCfrsArtifacts.js";

describe("parseDelawareRefreshArtifactsArgs", () => {
  it("parses repeated --cf-id values, deduped, with cache dir and force", () => {
    expect(
      parseDelawareRefreshArtifactsArgs(["--cf-id", "01009999", "--cf-id", "01008888", "--cf-id", "01009999", "--cache-dir", "/tmp/cache", "--force"])
    ).toEqual({ cfIds: ["01009999", "01008888"], cacheDir: "/tmp/cache", force: true });
  });

  it("rejects missing, malformed, and unknown inputs", () => {
    expect(() => parseDelawareRefreshArtifactsArgs([])).toThrow(/At least one --cf-id/);
    expect(() => parseDelawareRefreshArtifactsArgs(["--cf-id", "1234"])).toThrow(/expected 8 digits/);
    expect(() => parseDelawareRefreshArtifactsArgs(["--cfid", "01009999"])).toThrow(/unknown flag/);
  });
});
