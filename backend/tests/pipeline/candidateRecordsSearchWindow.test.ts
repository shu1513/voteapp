import { afterEach, describe, expect, it } from "vitest";

import {
  computeCandidateRecordsSearchWindow,
  readCandidateRecordsOverlapDaysFromEnv,
} from "../../src/pipeline/candidates/candidateRecordsSearchWindow.js";

describe("computeCandidateRecordsSearchWindow", () => {
  it("returns full mode when no researched-through watermark exists", () => {
    expect(computeCandidateRecordsSearchWindow(null, 45)).toEqual({
      mode: "full",
      sinceDate: null,
    });
  });

  it("returns incremental mode with overlap subtracted from watermark", () => {
    expect(computeCandidateRecordsSearchWindow("2026-05-31", 45)).toEqual({
      mode: "incremental",
      sinceDate: "2026-04-16",
    });
  });

  it("handles month/year rollover when subtracting overlap days", () => {
    expect(computeCandidateRecordsSearchWindow("2026-01-10", 45)).toEqual({
      mode: "incremental",
      sinceDate: "2025-11-26",
    });
  });

  it("throws on invalid overlap days", () => {
    expect(() => computeCandidateRecordsSearchWindow("2026-05-31", 0)).toThrow(
      "Invalid overlapDays"
    );
  });
});

describe("readCandidateRecordsOverlapDaysFromEnv", () => {
  const original = process.env.CANDIDATE_RECORDS_OVERLAP_DAYS;

  afterEach(() => {
    if (original === undefined) {
      delete process.env.CANDIDATE_RECORDS_OVERLAP_DAYS;
    } else {
      process.env.CANDIDATE_RECORDS_OVERLAP_DAYS = original;
    }
  });

  it("uses fallback when env is missing", () => {
    delete process.env.CANDIDATE_RECORDS_OVERLAP_DAYS;
    expect(readCandidateRecordsOverlapDaysFromEnv()).toBe(45);
  });

  it("reads a valid positive integer from env", () => {
    process.env.CANDIDATE_RECORDS_OVERLAP_DAYS = "30";
    expect(readCandidateRecordsOverlapDaysFromEnv()).toBe(30);
  });

  it("throws on malformed env values", () => {
    process.env.CANDIDATE_RECORDS_OVERLAP_DAYS = "30days";
    expect(() => readCandidateRecordsOverlapDaysFromEnv()).toThrow(
      "Invalid positive integer env CANDIDATE_RECORDS_OVERLAP_DAYS"
    );
  });
});
