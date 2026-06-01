import { describe, expect, it } from "vitest";

import { decideCandidateRecordOutcome } from "../../src/pipeline/candidates/candidateRecordOutcomePolicy.js";

describe("decideCandidateRecordOutcome", () => {
  it("does not retry when records were persisted", () => {
    const decision = decideCandidateRecordOutcome({
      discoveredTotalCount: 3,
      persistedCount: 1,
      transientDropCount: 2,
      permanentDropCount: 0,
      repairCallFailedRetryable: false,
    });

    expect(decision).toEqual({ shouldRetry: false, reason: "persisted_records_available" });
  });

  it("does not retry when nothing was discovered", () => {
    const decision = decideCandidateRecordOutcome({
      discoveredTotalCount: 0,
      persistedCount: 0,
      transientDropCount: 0,
      permanentDropCount: 0,
      repairCallFailedRetryable: false,
    });

    expect(decision).toEqual({ shouldRetry: false, reason: "no_records_discovered" });
  });

  it("does not retry zero-persist with permanent failures", () => {
    const decision = decideCandidateRecordOutcome({
      discoveredTotalCount: 2,
      persistedCount: 0,
      transientDropCount: 1,
      permanentDropCount: 1,
      repairCallFailedRetryable: false,
    });

    expect(decision).toEqual({ shouldRetry: false, reason: "permanent_source_failures" });
  });

  it("retries zero-persist when only transient failures remain", () => {
    const decision = decideCandidateRecordOutcome({
      discoveredTotalCount: 2,
      persistedCount: 0,
      transientDropCount: 2,
      permanentDropCount: 0,
      repairCallFailedRetryable: false,
    });

    expect(decision).toEqual({ shouldRetry: true, reason: "transient_source_failures" });
  });

  it("retries zero-persist on retryable repair call failure", () => {
    const decision = decideCandidateRecordOutcome({
      discoveredTotalCount: 2,
      persistedCount: 0,
      transientDropCount: 0,
      permanentDropCount: 0,
      repairCallFailedRetryable: true,
    });

    expect(decision).toEqual({ shouldRetry: true, reason: "repair_call_retryable_failure" });
  });
});
