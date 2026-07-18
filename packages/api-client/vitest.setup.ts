import { cleanup } from "@testing-library/react";
import { afterEach } from "vitest";

// With vitest globals disabled, testing-library cannot auto-register its
// cleanup hook; do it explicitly so renderHook mounts don't leak across
// tests. Matches frontend/src/test/setup.ts.
afterEach(() => {
  cleanup();
});
