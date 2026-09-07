import { describe, expect, it } from "vitest";
import { countJobFailures } from "../../src/scripts/runNotificationSchedulerWorkers.js";

// The combined worker only reports BullMQ "failed" events to Sentry. Digest
// and new-election jobs complete even when some recipients failed, so the
// completed handler has to read the failures list itself.
describe("countJobFailures", () => {
  it("counts the failures array on a completed job result", () => {
    expect(
      countJobFailures({
        usersEmailedCount: 3,
        failures: [
          { userId: "a", stage: "send", reason: "boom" },
          { userId: "b", stage: "push_send", reason: "token" },
        ],
      })
    ).toBe(2);
  });

  it("returns 0 for an empty list or any other result shape", () => {
    expect(countJobFailures({ failures: [] })).toBe(0);
    expect(countJobFailures({ lockSkipped: true })).toBe(0);
    expect(countJobFailures({ failures: "not-a-list" })).toBe(0);
    expect(countJobFailures(null)).toBe(0);
    expect(countJobFailures(undefined)).toBe(0);
    expect(countJobFailures("done")).toBe(0);
  });
});
