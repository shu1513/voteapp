import { describe, expect, it } from "vitest";

import { assertKnownCliFlags } from "../../src/scripts/manualCliFlags.js";

describe("assertKnownCliFlags", () => {
  it("accepts known flags, values, positional words, and the bare -- separator", () => {
    expect(() =>
      assertKnownCliFlags("manual:deferral", ["record", "--", "--district-id", "d-1", "--all"], [
        "--district-id",
        "--all",
      ])
    ).not.toThrow();
  });

  it("rejects an unknown flag and names the known set", () => {
    expect(() =>
      assertKnownCliFlags("manual:candidate-records:write", ["--dry_run"], ["--dry-run"])
    ).toThrow(/unknown flag\(s\) --dry_run.*Known flags: --dry-run/s);
  });

  it("rejects a documented-but-newer flag with the sync hint", () => {
    // The live failure mode: a checkout predating a feature silently ignored
    // its flag; this error is the guard against that.
    expect(() =>
      assertKnownCliFlags("manual:candidate-records:write", ["--evidence-file", "e.json"], [
        "--records-file",
      ])
    ).toThrow(/sync to current main/);
  });

  it("reports all unknown flags at once", () => {
    expect(() => assertKnownCliFlags("x", ["--a", "--b"], [])).toThrow(/--a, --b.*\(none\)/s);
  });
});
