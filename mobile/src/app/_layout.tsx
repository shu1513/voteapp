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

// A deep link can cold-start the app straight onto a leaf screen (e.g.
// /verify-email from an email). Anchoring the stack on the tab navigator
// puts it beneath that screen, so back works and dismissTo("/") has a
// target instead of an empty stack.
export const unstable_settings = {
  initialRouteName: "(tabs)",
};

export default function RootLayout() {
  return (
    <QueryClientProvider client={queryClient}>
      <Stack>
        {/* The tab navigator draws its own headers; a stack header on top
            would double them. */}
        <Stack.Screen name="(tabs)" options={{ headerShown: false }} />
      </Stack>
    </QueryClientProvider>
  );
}
