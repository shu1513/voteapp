import { describe, expect, it } from "vitest";
import { screen } from "@testing-library/react";
import { FollowsPage } from "./FollowsPage";
import { renderRoutes } from "../test/render";

describe("FollowsPage", () => {
  it("redirects the retired follows page to My Picks", async () => {
    renderRoutes(
      [
        { path: "/me/follows", element: <FollowsPage /> },
        { path: "/me/picks", element: <p>Picks placeholder</p> },
      ],
      "/me/follows"
    );
    expect(await screen.findByText("Picks placeholder")).toBeInTheDocument();
  });
});
