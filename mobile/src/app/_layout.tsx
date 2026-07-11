// Polyfills must load before anything that can reach the api-client.
import "../lib/polyfills";
import "../global.css";

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Stack } from "expo-router";
import { initApi } from "../lib/api";
import { initErrorMonitoring } from "../lib/errorMonitoring";

initErrorMonitoring();
initApi();

const queryClient = new QueryClient();

export default function RootLayout() {
  return (
    <QueryClientProvider client={queryClient}>
      <Stack />
    </QueryClientProvider>
  );
}
