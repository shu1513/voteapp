import path from "node:path";

import { defineConfig } from "vitest/config";

// Kept separate from vite.config.ts: vite 8 (rolldown) plugin types conflict
// with the vite version bundled by vitest. Tests don't need the react
// fast-refresh or tailwind plugins; esbuild handles TSX via tsconfig's
// jsx: react-jsx.
export default defineConfig({
  // Pin the workspace package to THIS checkout's source. Without the alias,
  // node resolution walks up to the repo root's node_modules symlink, which
  // points at the main checkout's packages/ — in a git worktree that can be
  // a different branch, so tests would import stale api-client code.
  resolve: {
    alias: {
      "@voteapp/api-client": path.resolve(__dirname, "../packages/api-client/src/index.ts"),
    },
  },
  test: {
    environment: "jsdom",
    setupFiles: ["./src/test/setup.ts"],
    // Playwright owns e2e/**; vitest must not pick those specs up. The
    // shared api-client package keeps its tests colocated and runs them
    // here, where jsdom and @testing-library are installed.
    include: ["src/**/*.test.{ts,tsx}", "../packages/api-client/src/**/*.test.{ts,tsx}"],
  },
});
