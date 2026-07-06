import type { Config } from "@react-router/dev/config";

export default {
  appDirectory: "src",
  ssr: false,
  prerender: ["/", "/disclaimer", "/terms", "/privacy", "/register", "/login"],
} satisfies Config;
