import { afterEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createMemoryRouter, RouterProvider } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { RegisterPage } from "./RegisterPage";
import { TERMS_VERSION } from "@voteapp/api-client";

function renderRegister(search = "") {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  const router = createMemoryRouter(
    [
      { path: "/", element: <RegisterPage /> },
      { path: "/login", element: <p /> },
      { path: "/terms", element: <p /> },
      { path: "/privacy", element: <p /> },
      { path: "/disclaimer", element: <p /> },
      { path: "/me/ballot", element: <p>Saved ballot placeholder</p> },
    ],
    { initialEntries: [`/${search}`] }
  );
  return render(
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
});

// jsdom never loads the GIS script; a stubbed window.google lets the page
// render the button and the test drive its captured credential callback.
function stubGis() {
  const initialize = vi.fn();
  vi.stubGlobal("google", { accounts: { id: { initialize, renderButton: vi.fn() } } });
  return {
    initialize,
    fireCredential(credential: string) {
      const config = initialize.mock.calls.at(-1)?.[0] as
        | { callback: (response: { credential?: string }) => void }
        | undefined;
      config?.callback({ credential });
    },
  };
}

describe("RegisterPage clickwrap", () => {
  it("keeps Create account disabled until the signup box is checked", async () => {
    const user = userEvent.setup();
    renderRegister();

    await user.type(screen.getByLabelText("Email"), "voter@example.com");
    await user.type(screen.getByLabelText("Password"), "correct horse battery staple");
    await user.type(screen.getByLabelText("Confirm password"), "correct horse battery staple");
    expect(screen.getByRole("button", { name: "Create account" })).toBeDisabled();

    await user.click(screen.getByRole("checkbox"));
    expect(screen.getByRole("button", { name: "Create account" })).toBeEnabled();
  });

  it("requires the confirmation to match before Create account enables", async () => {
    const user = userEvent.setup();
    renderRegister();

    await user.type(screen.getByLabelText("Email"), "voter@example.com");
    await user.type(screen.getByLabelText("Password"), "correct horse battery staple");
    await user.type(screen.getByLabelText("Confirm password"), "correct horse battery stapl");
    await user.click(screen.getByRole("checkbox"));

    expect(screen.getByText("Passwords don't match.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Create account" })).toBeDisabled();

    await user.type(screen.getByLabelText("Confirm password"), "e");
    expect(screen.queryByText("Passwords don't match.")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Create account" })).toBeEnabled();
  });

  it("reveals both password fields with the Show password toggle", async () => {
    const user = userEvent.setup();
    renderRegister();

    expect(screen.getByLabelText("Password")).toHaveAttribute("type", "password");
    await user.click(screen.getByRole("button", { name: "Show password" }));
    expect(screen.getByLabelText("Password")).toHaveAttribute("type", "text");
    expect(screen.getByLabelText("Confirm password")).toHaveAttribute("type", "text");
    await user.click(screen.getByRole("button", { name: "Hide password" }));
    expect(screen.getByLabelText("Password")).toHaveAttribute("type", "password");
  });

  it("sends accepted_terms_version with the register payload and shows the check-email state", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers(),
      json: async () => ({ status: "ok" }),
    });
    vi.stubGlobal("fetch", fetchMock);
    const user = userEvent.setup();
    renderRegister();

    await user.type(screen.getByLabelText("Email"), "voter@example.com");
    await user.type(screen.getByLabelText(/First Name/), "Val");
    await user.type(screen.getByLabelText("Password"), "correct horse battery staple");
    await user.type(screen.getByLabelText("Confirm password"), "correct horse battery staple");
    await user.click(screen.getByRole("checkbox"));
    await user.click(screen.getByRole("button", { name: "Create account" }));

    await waitFor(() => {
      expect(screen.getByText("Check your email")).toBeInTheDocument();
    });
    const [path, init] = fetchMock.mock.calls[0] as [string, { body: string }];
    expect(path).toBe("/api/auth/register");
    expect(JSON.parse(init.body)).toEqual({
      email: "voter@example.com",
      password: "correct horse battery staple",
      accepted_terms_version: TERMS_VERSION,
      first_name: "Val",
    });
  });

  it("ignores the Google credential until the signup box is checked, then signs up with terms", async () => {
    vi.stubEnv("VITE_GOOGLE_OAUTH_CLIENT_ID", "test-client-id");
    const gis = stubGis();
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers(),
      json: async () => ({ status: "ok" }),
    });
    vi.stubGlobal("fetch", fetchMock);
    const user = userEvent.setup();
    renderRegister();
    await waitFor(() => expect(gis.initialize).toHaveBeenCalled());

    // Same clickwrap gate as the submit button: an unchecked box means the
    // credential is dropped, and the wrapper is marked disabled.
    expect(screen.getByTestId("google-signin-button").parentElement).toHaveAttribute(
      "aria-disabled",
      "true"
    );
    gis.fireCredential("google-jwt");
    expect(fetchMock).not.toHaveBeenCalled();

    await user.click(screen.getByRole("checkbox"));
    gis.fireCredential("google-jwt");

    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    const [path, init] = fetchMock.mock.calls[0] as [string, { body: string }];
    expect(path).toBe("/api/auth/google");
    expect(JSON.parse(init.body)).toEqual({
      credential: "google-jwt",
      intent: "signup",
      accepted_terms_version: TERMS_VERSION,
    });
  });

  it("explains the greyed-out Google button on click and clears the hint once agreed", async () => {
    vi.stubEnv("VITE_GOOGLE_OAUTH_CLIENT_ID", "test-client-id");
    const gis = stubGis();
    const user = userEvent.setup();
    renderRegister();
    await waitFor(() => expect(gis.initialize).toHaveBeenCalled());

    // pointer-events-none on the disabled wrapper means a real click lands on
    // the hint container; jsdom doesn't apply the class so the click bubbles
    // up through it the same way.
    await user.click(screen.getByTestId("google-signin-button"));
    expect(
      screen.getByText("Check the box above to enable sign-up with Google.")
    ).toBeInTheDocument();

    await user.click(screen.getByRole("checkbox"));
    expect(
      screen.queryByText("Check the box above to enable sign-up with Google.")
    ).not.toBeInTheDocument();
  });

  it("forwards an internal next path to the login links", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers(),
      json: async () => ({ status: "ok" }),
    });
    vi.stubGlobal("fetch", fetchMock);
    const user = userEvent.setup();
    renderRegister("?next=/candidates/c-1");

    expect(screen.getByRole("link", { name: "Log in" })).toHaveAttribute(
      "href",
      "/login?next=%2Fcandidates%2Fc-1"
    );

    // The check-email screen's login button keeps the return path too.
    await user.type(screen.getByLabelText("Email"), "voter@example.com");
    await user.type(screen.getByLabelText("Password"), "correct horse battery staple");
    await user.type(screen.getByLabelText("Confirm password"), "correct horse battery staple");
    await user.click(screen.getByRole("checkbox"));
    await user.click(screen.getByRole("button", { name: "Create account" }));

    await waitFor(() => {
      expect(screen.getByText("Check your email")).toBeInTheDocument();
    });
    expect(screen.getByRole("link", { name: "Go to login" })).toHaveAttribute(
      "href",
      "/login?next=%2Fcandidates%2Fc-1"
    );
  });
});
