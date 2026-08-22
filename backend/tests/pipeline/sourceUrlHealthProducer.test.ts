import { describe, expect, it } from "vitest";

import {
  classifyUrlHealthCheckResult,
  computeHardFailureStreakAfterCheck,
  isCleanupEligible,
} from "../../src/pipeline/elections/sourceUrlHealthProducer.js";

describe("sourceUrlHealthProducer helpers", () => {
  it("classifies 404 as hard_fail", () => {
    const classified = classifyUrlHealthCheckResult({
      ok: false,
      reason: "citation fetch returned status 404",
    });
    expect(classified.outcome).toBe("hard_fail");
    expect(classified.statusCode).toBe(404);
  });

  it("parses HTTP-prefixed status codes from reason text", () => {
    const classified = classifyUrlHealthCheckResult({
      ok: false,
      reason: "HTTP 404 Not Found",
    });
    expect(classified.outcome).toBe("hard_fail");
    expect(classified.statusCode).toBe(404);
  });

  it("classifies permanent unresolved hostname as hard_fail (shared contract with candidate producer)", () => {
    const classified = classifyUrlHealthCheckResult({
      ok: false,
      reason: "citation URL hostname could not be resolved",
    });
    expect(classified.outcome).toBe("hard_fail");
    expect(classified.statusCode).toBeNull();
  });

  it("keeps transient DNS resolver failures transient", () => {
    const classified = classifyUrlHealthCheckResult({
      ok: false,
      reason: "citation URL DNS lookup failed transiently: EAI_AGAIN",
    });
    expect(classified.outcome).toBe("transient_fail");
  });

  it("classifies timeout as transient_fail", () => {
    const classified = classifyUrlHealthCheckResult({
      ok: false,
      reason: "citation URL fetch timed out",
    });
    expect(classified.outcome).toBe("transient_fail");
    expect(classified.statusCode).toBeNull();
  });

  it("classifies ok response as healthy", () => {
    const classified = classifyUrlHealthCheckResult({
      ok: true,
      normalizedUrl: "https://example.com",
      finalUrl: "https://example.com",
      status: 200,
    });
    expect(classified.outcome).toBe("healthy");
    expect(classified.statusCode).toBe(200);
    expect(classified.reason).toBeNull();
  });

  it("requires threshold + window + hard status for cleanup eligibility", () => {
    const asOf = new Date("2026-05-25T00:00:00.000Z");
    const eligible = isCleanupEligible({
      consecutiveHardFailures: 3,
      firstHardFailedAt: new Date("2026-05-10T00:00:00.000Z"),
      lastHttpStatus: 404,
      hardFailureThreshold: 3,
      hardFailureWindowDays: 14,
      asOf,
    });
    expect(eligible).toBe(true);

    const notEligibleByWindow = isCleanupEligible({
      consecutiveHardFailures: 3,
      firstHardFailedAt: new Date("2026-05-20T00:00:00.000Z"),
      lastHttpStatus: 404,
      hardFailureThreshold: 3,
      hardFailureWindowDays: 14,
      asOf,
    });
    expect(notEligibleByWindow).toBe(false);

    const notEligibleByStatus = isCleanupEligible({
      consecutiveHardFailures: 3,
      firstHardFailedAt: new Date("2026-05-10T00:00:00.000Z"),
      lastHttpStatus: 500,
      hardFailureThreshold: 3,
      hardFailureWindowDays: 14,
      asOf,
    });
    expect(notEligibleByStatus).toBe(false);
  });

  it("preserves hard-failure streak on transient outcomes and resets only on healthy", () => {
    const priorFirst = new Date("2026-05-01T00:00:00.000Z");
    const priorLast = new Date("2026-05-10T00:00:00.000Z");
    const checkedAt = new Date("2026-05-20T00:00:00.000Z");

    const transient = computeHardFailureStreakAfterCheck({
      priorConsecutiveHardFailures: 2,
      priorFirstHardFailedAt: priorFirst,
      priorLastHardFailedAt: priorLast,
      checkedAt,
      classification: { outcome: "transient_fail", statusCode: null, reason: "timed out" },
    });
    expect(transient.consecutiveHardFailures).toBe(2);
    expect(transient.firstHardFailedAt).toEqual(priorFirst);
    expect(transient.lastHardFailedAt).toEqual(priorLast);

    const healthy = computeHardFailureStreakAfterCheck({
      priorConsecutiveHardFailures: 2,
      priorFirstHardFailedAt: priorFirst,
      priorLastHardFailedAt: priorLast,
      checkedAt,
      classification: { outcome: "healthy", statusCode: 200, reason: null },
    });
    expect(healthy.consecutiveHardFailures).toBe(0);
    expect(healthy.firstHardFailedAt).toBeNull();
    expect(healthy.lastHardFailedAt).toBeNull();
  });

  it("continues the hard-failure streak after a transient interruption", () => {
    const priorFirst = new Date("2026-05-01T00:00:00.000Z");
    const priorLast = new Date("2026-05-10T00:00:00.000Z");
    const checkedAt = new Date("2026-05-20T00:00:00.000Z");

    const continuedHard = computeHardFailureStreakAfterCheck({
      priorConsecutiveHardFailures: 2,
      priorFirstHardFailedAt: priorFirst,
      priorLastHardFailedAt: priorLast,
      checkedAt,
      classification: { outcome: "hard_fail", statusCode: 404, reason: "citation fetch returned status 404" },
    });

    expect(continuedHard.consecutiveHardFailures).toBe(3);
    expect(continuedHard.firstHardFailedAt).toEqual(priorFirst);
    expect(continuedHard.lastHardFailedAt).toEqual(checkedAt);
  });
});
