import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    include: ["tests/**/*.test.ts"],
    clearMocks: true,
    restoreMocks: true,
    // Tests mock fetch and never reach real providers; the aiCallGuard
    // default-deny would otherwise short-circuit every provider test.
    env: {
      AI_API_CALLS_ALLOWED: "true",
    },
  },
});

