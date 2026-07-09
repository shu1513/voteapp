import { describe, expect, it } from "vitest";

import {
  parseOptionalStringFlag,
  parsePositiveIntegerFlag,
} from "../../src/scripts/electionsWorkerCliArgs.js";

describe("elections worker CLI args", () => {
  it("parses supported string and positive-integer forms", () => {
    expect(parseOptionalStringFlag(["--ingest-key=manual:test"], "--ingest-key")).toBe(
      "manual:test"
    );
    expect(parseOptionalStringFlag(["--ingest-key", "manual:test"], "--ingest-key")).toBe(
      "manual:test"
    );
    expect(parsePositiveIntegerFlag(["--batch-size=50"], "--batch-size", 25)).toBe(50);
    expect(parsePositiveIntegerFlag(["--batch-size=bad"], "--batch-size", 25)).toBe(25);
  });

  it("fails closed when --ingest-key is present without a value", () => {
    expect(() => parseOptionalStringFlag(["--ingest-key"], "--ingest-key")).toThrow(
      "Missing value for --ingest-key"
    );
    expect(() => parseOptionalStringFlag(["--ingest-key="], "--ingest-key")).toThrow(
      "Missing value for --ingest-key"
    );
    expect(() =>
      parseOptionalStringFlag(["--ingest-key", "--once"], "--ingest-key")
    ).toThrow("Missing value for --ingest-key");
  });
});
