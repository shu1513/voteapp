import { describe, expect, it } from "vitest";

import { assertKnownCliFlags, type CliFlagSpec } from "../../src/scripts/manualCliFlags.js";

const RECORDS_SPECS: readonly CliFlagSpec[] = [
  { name: "--records-file", value: "space" },
  { name: "--evidence-file", value: "space" },
  { name: "--dry-run", value: "none" },
];

describe("assertKnownCliFlags", () => {
  it("accepts known flags, values, positional words, and the bare -- separator", () => {
    expect(() =>
      assertKnownCliFlags(
        "manual:deferral record",
        ["record", "--", "--district-id", "d-1", "--all"],
        [
          { name: "--district-id", value: "space" },
          { name: "--all", value: "none" },
        ]
      )
    ).not.toThrow();
  });

  it("does not mistake a value starting with a single dash for a flag", () => {
    expect(() =>
      assertKnownCliFlags("manual:deferral record", ["--reason", "-blocked by SOS calendar"], [
        { name: "--reason", value: "space" },
      ])
    ).not.toThrow();
  });

  it("rejects an unknown flag and names the known set", () => {
    expect(() => assertKnownCliFlags("manual:candidate-records:write", ["--dry_run"], RECORDS_SPECS)).toThrow(
      /unknown flag --dry_run.*Known flags: --dry-run, --evidence-file, --records-file/s
    );
  });

  it("rejects unknown single-dash tokens", () => {
    expect(() => assertKnownCliFlags("x", ["-typo"], RECORDS_SPECS)).toThrow(/unknown flag -typo/);
  });

  it("rejects a documented-but-newer flag with the sync hint", () => {
    // The live failure mode: a checkout predating a feature silently ignored
    // its flag; this error is the guard against that.
    expect(() =>
      assertKnownCliFlags("manual:candidate-records:write", ["--evidence-file", "e.json"], [
        { name: "--records-file", value: "space" },
      ])
    ).toThrow(/sync to current main/);
  });

  it("reports all unknown flags at once", () => {
    expect(() => assertKnownCliFlags("x", ["--a", "--b"], [])).toThrow(/--a.*--b.*\(none\)/s);
  });

  describe("value styles", () => {
    it("accepts the = form only where the parser reads it", () => {
      // elections-worker integer flags are equals-ONLY; --ingest-key reads both.
      const electionSpecs: readonly CliFlagSpec[] = [
        { name: "--once", value: "none" },
        { name: "--batch-size", value: "equals" },
        { name: "--ingest-key", value: "both" },
      ];
      expect(() =>
        assertKnownCliFlags(
          "elections:validate",
          ["--once", "--batch-size=5000", "--ingest-key=manual:elections:x:2026"],
          electionSpecs
        )
      ).not.toThrow();
      expect(() =>
        assertKnownCliFlags("elections:validate", ["--ingest-key", "manual:elections:x:2026"], electionSpecs)
      ).not.toThrow();
    });

    it("rejects a space-separated value for an equals-only flag (parser would ignore it)", () => {
      expect(() =>
        assertKnownCliFlags("elections:validate", ["--batch-size", "5000"], [
          { name: "--batch-size", value: "equals" },
        ])
      ).toThrow(/--batch-size only accepts the "--batch-size=<value>" form/);
    });

    it("rejects the = form for a space-only flag (parser would ignore it)", () => {
      expect(() =>
        assertKnownCliFlags("manual:candidate-records:write", ["--records-file=r.json"], RECORDS_SPECS)
      ).toThrow(/--records-file takes its value as the next argument/);
    });

    it("rejects a value on a boolean flag", () => {
      expect(() =>
        assertKnownCliFlags("manual:candidate-records:write", ["--dry-run=true"], RECORDS_SPECS)
      ).toThrow(/--dry-run takes no value/);
    });
  });
});
