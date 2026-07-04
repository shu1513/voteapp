import path from "node:path";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// Dev proxy makes the app same-origin with the backend: no CORS config, and
// the SameSite=Lax session cookie just works. Test config lives in
// vitest.config.ts (vite 8 and vitest's bundled vite types conflict).
export default defineConfig({
  plugins: [react(), tailwindcss()],
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
