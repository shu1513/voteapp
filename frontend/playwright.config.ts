import { defineConfig } from "@playwright/test";

// Local full-stack smoke tests (plan.md Phase 5): they drive the real dev
// server against the real backend and local database, so they live outside
// vitest and run on demand via `npm run test:e2e`.
//
// The backend is started here with AUTH_MAILER=console and its stdout
// captured to e2e/.api-server.log — the account spec reads verification
// links from that log. Stop any already-running backend on :3001 first
// (reuse is disabled so a live-SES process can never serve the register
// loop).
export default defineConfig({
  testDir: "./e2e",
  fullyParallel: false,
  workers: 1,
  timeout: 60_000,
  use: {
    baseURL: "http://localhost:5173",
  },
  webServer: [
    {
      command:
        "cd ../backend && AUTH_MAILER=console npm run address:api > ../frontend/e2e/.api-server.log 2>&1",
      url: "http://127.0.0.1:3001/api/research-areas",
      reuseExistingServer: false,
      timeout: 60_000,
    },
    {
      command: "npm run dev",
      url: "http://localhost:5173",
      reuseExistingServer: true,
      timeout: 60_000,
    },
  ],
});
