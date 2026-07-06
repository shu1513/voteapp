import { execSync } from "node:child_process";
import path from "node:path";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { sentryVitePlugin } from "@sentry/vite-plugin";

// The Sentry release must match between the SDK init and the source-map
// upload or stacks stay minified. DEPLOY_RELEASE is authoritative at deploy
// time; local builds fall back to the current git SHA.
function resolveRelease(): string | undefined {
  const fromEnv = process.env.DEPLOY_RELEASE?.trim();
  if (fromEnv) {
    return fromEnv;
  }
  try {
    return execSync("git rev-parse --short HEAD", { encoding: "utf8" }).trim();
  } catch {
    return undefined;
  }
}

const release = resolveRelease();
// Source maps upload to Sentry only when a CI/deploy token is present; the
// maps are generated hidden (no sourceMappingURL comment, nothing served
// publicly) and deleted from dist after upload.
const uploadSourceMaps = Boolean(process.env.SENTRY_AUTH_TOKEN?.trim());

// Dev proxy makes the app same-origin with the backend: no CORS config, and
// the SameSite=Lax session cookie just works. Test config lives in
// vitest.config.ts (vite 8 and vitest's bundled vite types conflict).
export default defineConfig({
  plugins: [
    react(),
    tailwindcss(),
    ...(uploadSourceMaps
      ? [
          sentryVitePlugin({
            org: "solo-developer-xv",
            project: "voteapp-frontend",
            authToken: process.env.SENTRY_AUTH_TOKEN,
            release: release ? { name: release } : undefined,
            sourcemaps: { filesToDeleteAfterUpload: ["dist/**/*.map"] },
          }),
        ]
      : []),
  ],
  define: {
    "import.meta.env.VITE_RELEASE": JSON.stringify(release ?? ""),
  },
  build: {
    sourcemap: uploadSourceMaps ? "hidden" : false,
  },
  server: {
    proxy: {
      "/api": {
        target: "http://127.0.0.1:3001",
        // Browsers attach an Origin header to POSTs even same-origin; the
        // backend's CORS allowlist would reject it. The proxy makes the app
        // same-origin in substance, so drop the header to match.
        configure: (proxy) => {
          proxy.on("proxyReq", (proxyReq) => {
            proxyReq.removeHeader("origin");
          });
        },
      },
    },
    fs: {
      // The disclaimer page raw-imports docs/legal/disclaimer.md. Allow ONLY
      // that directory beyond the app root: widening to the repo root would
      // let the dev server serve arbitrary repo files (backend/.env holds
      // real credentials).
      allow: [".", path.resolve(__dirname, "../docs/legal")],
    },
  },
});
