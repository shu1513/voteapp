import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useEffect } from "react";
import { Link, Outlet } from "react-router";
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";
import { flushUsageEventsForTests, resetUsageForTests, track, useUsageTracking } from "./usage";

type Sent = { name: string; route: string; page_view_id: string | null; props: Record<string, unknown> };

function Layout() {
  useUsageTracking();
  return <Outlet />;
}

// A page whose mount effect records an event — the shape of BallotPage's
// ballot_result on a cached load or an ErrorNotice rendered immediately.
function Home() {
  useEffect(() => {
    track("address_input");
  }, []);
  return (
    <p>
      home <Link to="/ballot">go</Link>
    </p>
  );
}

function Ballot() {
  useEffect(() => {
    track("list_control", { control: "sort", value: "soonest" });
  }, []);
  return <p>ballot</p>;
}

function usageBodies(fetchMock: ReturnType<typeof stubApiRoutes>): Sent[][] {
  return fetchMock.mock.calls
    .filter((call) => String(call[0]).endsWith("/api/usage/events"))
    .map((call) => (JSON.parse((call[1] as RequestInit).body as string) as { events: Sent[] }).events);
}

beforeEach(() => {
  resetUsageForTests();
  sessionStorage.clear();
  localStorage.clear();
  vi.stubEnv("VITE_USAGE_ANALYTICS_ENABLED", "true");
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
});

describe("useUsageTracking", () => {
  it("attributes a child's mount-time event to the page being rendered, not the previous one", async () => {
    const fetchMock = stubApiRoutes({
      "/api/me": apiError(401, "unauthorized", "Not logged in"),
      "/api/usage/events": { status: 204, body: null },
    });
    renderRoutes(
      [
        {
          element: <Layout />,
          children: [
            { id: "pages/HomePage", path: "/", element: <Home /> },
            { id: "pages/BallotPage", path: "/ballot", element: <Ballot /> },
          ],
        },
      ],
      "/"
    );
    await screen.findByText(/home/);
    await userEvent.click(screen.getByRole("link", { name: "go" }));
    await screen.findByText("ballot");
    flushUsageEventsForTests();
    await vi.waitFor(() => expect(usageBodies(fetchMock)).toHaveLength(1));

    const events = usageBodies(fetchMock)[0]!;
    const byName = (name: string) => events.filter((event) => event.name === name);
    // The child's effect ran before the layout's page-view effect; the
    // render-phase stamp still puts it on the right page.
    expect(events.map((event) => event.name).slice(0, 3)).toEqual(["session_start", "page_view", "address_input"]);
    expect(byName("session_start")[0]!.props.landing_route).toBe("home");
    expect(byName("page_view")[0]!.route).toBe("home");
    expect(byName("address_input")[0]).toMatchObject({ route: "home", page_view_id: byName("page_view")[0]!.page_view_id });
    // Navigation: the closing page_time belongs to home, the sort control to ballot.
    expect(byName("page_time")[0]).toMatchObject({ route: "home", page_view_id: byName("page_view")[0]!.page_view_id });
    expect(byName("page_view")[1]!.route).toBe("ballot");
    expect(byName("list_control")[0]).toMatchObject({ route: "ballot", page_view_id: byName("page_view")[1]!.page_view_id });
    // auth_resolved carries the page it settled on, and every event names a route id, never a path.
    expect(byName("auth_resolved")[0]!.props).toEqual({ auth: "guest" });
    for (const event of events) expect(event.route).not.toContain("/");
  });
});
