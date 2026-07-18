import { defineConfig } from "vitest/config";

// CI runs this package's tests through frontend/vitest.config.ts (its
// include list pulls in ../packages/api-client). This config only makes a
// standalone `npx vitest run` inside the package work the same way: the
// hook tests (renderHook) need a DOM, so mirror frontend's jsdom setup.
export default defineConfig({
  test: {
    environment: "jsdom",
    setupFiles: ["./vitest.setup.ts"],
    include: ["src/**/*.test.{ts,tsx}"],
  },
});
