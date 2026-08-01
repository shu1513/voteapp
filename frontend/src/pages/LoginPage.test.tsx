import { afterEach, describe, expect, it, vi } from "vitest";
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { LoginPage } from "./LoginPage";
import { renderRoutes } from "../test/render";
import { stubApiRoutes } from "../test/mockApi";
import { ME_VERIFIED } from "../test/fixtures";

function renderLogin(search = "") {
  return renderRoutes(
    [
      { path: "/login", element: <LoginPage /> },
      { path: "/candidates/:candidateId", element: <p>candidate page</p> },
      { path: "/me/ballot", element: <p>ballot page</p> },
      { path: "/register", element: <p>register page</p> },
    ],
    `/login${search}`
  );
}

async function submitLogin() {
  await userEvent.type(screen.getByLabelText("Email"), "voter@example.com");
  await userEvent.type(screen.getByLabelText("Password"), "correct horse battery");
  await userEvent.click(screen.getByRole("button", { name: "Log in" }));
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("LoginPage", () => {
  it("returns to the internal next path after login", async () => {
    stubApiRoutes({
      "/api/auth/login": { body: { status: "ok" } },
      "/api/me": { body: ME_VERIFIED },
    });
    renderLogin("?next=/candidates/c-1");

    await submitLogin();

    expect(await screen.findByText("candidate page")).toBeInTheDocument();
  });

  it("falls back to the ballot when next is missing", async () => {
    stubApiRoutes({
      "/api/auth/login": { body: { status: "ok" } },
      "/api/me": { body: ME_VERIFIED },
    });
    renderLogin();

    await submitLogin();

    expect(await screen.findByText("ballot page")).toBeInTheDocument();
  });

  it("ignores an external next instead of open-redirecting", async () => {
    stubApiRoutes({
      "/api/auth/login": { body: { status: "ok" } },
      "/api/me": { body: ME_VERIFIED },
    });
    // "//evil.example" is protocol-relative — a browser would leave the site.
    renderLogin(`?next=${encodeURIComponent("//evil.example/phish")}`);

    await submitLogin();

    expect(await screen.findByText("ballot page")).toBeInTheDocument();
  });

  it("forwards next to the register link", async () => {
    stubApiRoutes({});
    renderLogin("?next=/candidates/c-1");

    expect(screen.getByRole("link", { name: "Create an account" })).toHaveAttribute(
      "href",
      "/register?next=%2Fcandidates%2Fc-1"
    );
  });
});
