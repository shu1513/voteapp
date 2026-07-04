import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterEach } from "vitest";

// With vitest globals disabled, testing-library cannot auto-register its
// cleanup hook; do it explicitly so renders don't leak across tests.
afterEach(() => {
  cleanup();
});
