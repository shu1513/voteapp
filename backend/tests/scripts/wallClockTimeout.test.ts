import { describe, expect, it } from "vitest";

import { withWallClockTimeout } from "../../src/scripts/wallClockTimeout.js";

describe("withWallClockTimeout", () => {
  it("passes through a resolving promise", async () => {
    await expect(withWallClockTimeout(Promise.resolve("ok"), "test phase", 1_000)).resolves.toBe(
      "ok"
    );
  });

  it("passes through a rejecting promise unchanged", async () => {
    await expect(
      withWallClockTimeout(Promise.reject(new Error("inner failure")), "test phase", 1_000)
    ).rejects.toThrow("inner failure");
  });

  it("rejects with a labeled structured error when the promise hangs", async () => {
    const hang = new Promise<never>(() => {});
    await expect(withWallClockTimeout(hang, "candidate profile source validation", 20)).rejects.toThrow(
      /candidate profile source validation exceeded the 20ms wall-clock ceiling/
    );
  });

  it("clears the timer when the promise settles first (no lingering handle)", async () => {
    // If the timer were left armed, the rejection would fire later as an
    // unhandled rejection; settling well inside the window and then waiting
    // past it proves the race cleans up after itself.
    let unhandled: unknown = null;
    const onUnhandled = (reason: unknown): void => {
      unhandled = reason;
    };
    process.on("unhandledRejection", onUnhandled);
    try {
      await withWallClockTimeout(Promise.resolve("fast"), "test phase", 20);
      await new Promise((resolve) => setTimeout(resolve, 40));
      expect(unhandled).toBeNull();
    } finally {
      process.off("unhandledRejection", onUnhandled);
    }
  });
});
