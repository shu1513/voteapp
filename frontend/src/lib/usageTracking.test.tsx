import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useEffect } from "react";
import { Link, Outlet } from "react-router";
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";
import { flushUsageEventsForTests, resetUsageForTests, track, useSectionExposure, useUsageTracking } from "./usage";

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

// jsdom has no IntersectionObserver; this stub records observed targets and
// lets the test fire an intersection by hand.
type IoCallback = (entries: { isIntersecting: boolean }[]) => void;
function stubIntersectionObserver() {
  const instances: { callback: IoCallback; targets: Element[]; disconnected: boolean }[] = [];
  class FakeIntersectionObserver {
    private record: { callback: IoCallback; targets: Element[]; disconnected: boolean };
    constructor(callback: IoCallback) {
      this.record = { callback, targets: [], disconnected: false };
      instances.push(this.record);
    }
    observe(target: Element) {
      this.record.targets.push(target);
    }
    disconnect() {
      this.record.disconnected = true;
    }
    unobserve() {}
  }
  vi.stubGlobal("IntersectionObserver", FakeIntersectionObserver);
  return instances;
}

function Sectioned({ id }: { id: string }) {
  const ref = useSectionExposure("candidates", id);
  return <h2 ref={ref}>Candidates {id}</h2>;
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

describe("useSectionExposure", () => {
  it("fires section_exposed once when the marker first intersects", async () => {
    const observers = stubIntersectionObserver();
    const fetchMock = stubApiRoutes({
      "/api/me": apiError(401, "unauthorized", "Not logged in"),
      "/api/usage/events": { status: 204, body: null },
    });
    renderRoutes(
      [{ element: <Layout />, children: [{ id: "pages/ElectionPage", path: "/", element: <Sectioned id="a" /> }] }],
      "/"
    );
    await screen.findByText("Candidates a");
    expect(observers).toHaveLength(1);
    expect(observers[0]!.targets[0]!.textContent).toBe("Candidates a");
    // Not yet on screen: nothing. Then two intersections: one event, and
    // the observer is done with this key.
    observers[0]!.callback([{ isIntersecting: false }]);
    observers[0]!.callback([{ isIntersecting: true }]);
    observers[0]!.callback([{ isIntersecting: true }]);
    expect(observers[0]!.disconnected).toBe(true);
    flushUsageEventsForTests();
    await vi.waitFor(() => expect(usageBodies(fetchMock)).toHaveLength(1));
    const exposures = usageBodies(fetchMock)[0]!.filter((event) => event.name === "section_exposed");
    expect(exposures).toHaveLength(1);
    expect(exposures[0]).toMatchObject({ route: "election", props: { section: "candidates" } });
  });
});
