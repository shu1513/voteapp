import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Outlet } from "react-router";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";
import { ME_UNVERIFIED, ME_VERIFIED } from "../test/fixtures";
import { readPendingDistrictIds, savePendingDistrictIds } from "../lib/pendingDistricts";
import {
  resetDistrictHandoffForTests,
  retryDistrictHandoff,
  useDistrictHandoffRunner,
  useDistrictHandoffStatus,
} from "./districtHandoff";

const track = vi.hoisted(() => vi.fn());
vi.mock("./usage", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./usage")>()),
  track,
}));

// App's shape: the runner mounts once on the layout; pages only read status.
function Layout() {
  useDistrictHandoffRunner();
  return <Outlet />;
}

function ElectionPage() {
  const status = useDistrictHandoffStatus();
  return (
    <div>
      <p>election page</p>
      <p>handoff: {status}</p>
      <button type="button" onClick={retryDistrictHandoff}>
        Try again
      </button>
    </div>
  );
}

function renderElectionPage() {
  return renderRoutes(
    [{ element: <Layout />, children: [{ path: "/elections/:electionId", element: <ElectionPage /> }] }],
    "/elections/election-1"
  );
}

beforeEach(() => {
  sessionStorage.clear();
  resetDistrictHandoffForTests();
  track.mockReset();
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("useDistrictHandoffRunner", () => {
  it("initializes districts once from a non-ballot page and invalidates the pick gate's district set", async () => {
    savePendingDistrictIds(["district-1", "district-2"]);
    const initializeCalls: unknown[] = [];
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/districts/initialize": (_url, init) => {
        initializeCalls.push(JSON.parse(String(init?.body)));
        return { body: { ok: true } };
      },
    });
    const { queryClient } = renderElectionPage();
    // Seed the two account queries the handoff must refresh (inactive, so
    // invalidation only flags them — exactly what a later mount will see).
    queryClient.setQueryData(["me", "districts"], { district_ids: [] });
    queryClient.setQueryData(["me", "ballot", ""], { elections: [] });

    expect(await screen.findByText("handoff: done")).toBeInTheDocument();
    expect(initializeCalls).toEqual([{ district_ids: ["district-1", "district-2"] }]);
    expect(readPendingDistrictIds()).toEqual([]);
    expect(queryClient.getQueryState(["me", "districts"])?.isInvalidated).toBe(true);
    expect(queryClient.getQueryState(["me", "ballot", ""])?.isInvalidated).toBe(true);
    expect(track.mock.calls).toEqual([["handoff_result", { outcome: "done" }]]);
  });

  it("stays quiet for unverified sessions and for an empty queue", async () => {
    savePendingDistrictIds(["district-1"]);
    const fetchMock = stubApiRoutes({ "/api/me": { body: ME_UNVERIFIED } });
    renderElectionPage();
    expect(await screen.findByText("handoff: pending")).toBeInTheDocument();
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls.map((call) => new URL(String(call[0]), "http://localhost").pathname)).toEqual([
      "/api/me",
    ]);
    expect(readPendingDistrictIds()).toEqual(["district-1"]);
    expect(track).not.toHaveBeenCalled();
  });

  it("keeps the ids and reports failed on a recoverable error, then retries once on demand", async () => {
    savePendingDistrictIds(["district-1"]);
    let attempts = 0;
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/districts/initialize": () => {
        attempts += 1;
        return attempts === 1 ? apiError(503, "unavailable", "Down") : { body: { ok: true } };
      },
    });
    renderElectionPage();

    expect(await screen.findByText("handoff: failed")).toBeInTheDocument();
    expect(attempts).toBe(1);
    expect(readPendingDistrictIds()).toEqual(["district-1"]);

    await userEvent.click(screen.getByRole("button", { name: "Try again" }));
    expect(await screen.findByText("handoff: done")).toBeInTheDocument();
    expect(attempts).toBe(2);
    expect(readPendingDistrictIds()).toEqual([]);
    // Exactly one event per attempt.
    expect(track.mock.calls).toEqual([
      ["handoff_result", { outcome: "failed" }],
      ["handoff_result", { outcome: "done" }],
    ]);
  });

  it("drops a rejected payload without touching the district set", async () => {
    savePendingDistrictIds(["bogus"]);
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/districts/initialize": apiError(400, "invalid", "Unknown district"),
    });
    const { queryClient } = renderElectionPage();
    queryClient.setQueryData(["me", "districts"], { district_ids: [] });

    expect(await screen.findByText("handoff: done")).toBeInTheDocument();
    expect(readPendingDistrictIds()).toEqual([]);
    expect(queryClient.getQueryState(["me", "districts"])?.isInvalidated).toBe(false);
    expect(track.mock.calls).toEqual([["handoff_result", { outcome: "rejected" }]]);
  });

  it("fires when a guest search lands while the session is already verified", async () => {
    const initializeCalls: unknown[] = [];
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/districts/initialize": (_url, init) => {
        initializeCalls.push(JSON.parse(String(init?.body)));
        return { body: { ok: true } };
      },
    });
    renderElectionPage();
    expect(await screen.findByText("handoff: done")).toBeInTheDocument();
    expect(initializeCalls).toEqual([]);

    act(() => savePendingDistrictIds(["district-9"]));
    await vi.waitFor(() => expect(initializeCalls).toEqual([{ district_ids: ["district-9"] }]));
    expect(await screen.findByText("handoff: done")).toBeInTheDocument();
    expect(readPendingDistrictIds()).toEqual([]);
  });
});
