import { render } from "@testing-library/react";
import { createMemoryRouter, RouterProvider, type RouteObject } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

/** Renders routes under a fresh QueryClient with retries off (so error
 * states assert immediately instead of after retry backoff). Pass an object
 * entry ({ pathname, state }) to simulate navigation carrying router state. */
export function renderRoutes(
  routes: RouteObject[],
  initialEntry: string | { pathname: string; search?: string; state?: unknown } = "/"
) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  const router = createMemoryRouter(routes, { initialEntries: [initialEntry] });
  return render(
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  );
}
