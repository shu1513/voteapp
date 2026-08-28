import { describe, expect, it } from "vitest";
import { screen } from "@testing-library/react";
import SupportPage from "./SupportPage";
import { renderRoutes } from "../test/render";
import { stubApiRoutes, apiError } from "../test/mockApi";

describe("SupportPage", () => {
  it("offers the two support paths", async () => {
    stubApiRoutes({ "/api/me": apiError(401, "unauthorized", "Not logged in") });
    renderRoutes(
      [
        { path: "/support", element: <SupportPage /> },
        { path: "/support/member", element: <p /> },
        { path: "/support/once", element: <p /> },
      ],
      "/support"
    );

    expect(
      await screen.findByRole("heading", { name: "Support Elections Simplified" })
    ).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Become an honorary member" })).toHaveAttribute(
      "href",
      "/support/member"
    );
    expect(screen.getByRole("link", { name: "Contribute once" })).toHaveAttribute(
      "href",
      "/support/once"
    );
    // The chooser has no forms — the choice happens first.
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });
});
