import { afterEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import { GoogleSignInButton } from "./GoogleSignInButton";

// jsdom never loads the real GIS script; stubbing window.google up front
// makes the loader short-circuit, so the tests drive the captured callback
// directly — the same seam the real iframe button uses.
function stubGis() {
  const initialize = vi.fn();
  const renderButton = vi.fn();
  vi.stubGlobal("google", { accounts: { id: { initialize, renderButton } } });
  return {
    initialize,
    renderButton,
    fireCredential(credential: string) {
      const config = initialize.mock.calls.at(-1)?.[0] as
        | { callback: (response: { credential?: string }) => void }
        | undefined;
      config?.callback({ credential });
    },
  };
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
});

describe("GoogleSignInButton", () => {
  it("renders nothing when VITE_GOOGLE_OAUTH_CLIENT_ID is not set", () => {
    stubGis();
    const { container } = render(<GoogleSignInButton text="signin_with" onCredential={vi.fn()} />);
    expect(container).toBeEmptyDOMElement();
  });

  it("initializes GIS with the client ID and renders the button", async () => {
    vi.stubEnv("VITE_GOOGLE_OAUTH_CLIENT_ID", "test-client-id");
    const gis = stubGis();
    render(<GoogleSignInButton text="signup_with" onCredential={vi.fn()} />);

    await waitFor(() => expect(gis.renderButton).toHaveBeenCalled());
    expect(gis.initialize).toHaveBeenCalledWith(
      expect.objectContaining({ client_id: "test-client-id" })
    );
    expect(gis.renderButton.mock.calls[0][1]).toMatchObject({ text: "signup_with" });
  });

  it("hands the credential to onCredential and ignores empty responses", async () => {
    vi.stubEnv("VITE_GOOGLE_OAUTH_CLIENT_ID", "test-client-id");
    const gis = stubGis();
    const onCredential = vi.fn();
    render(<GoogleSignInButton text="signin_with" onCredential={onCredential} />);
    await waitFor(() => expect(gis.initialize).toHaveBeenCalled());

    gis.fireCredential("jwt-credential");
    expect(onCredential).toHaveBeenCalledWith("jwt-credential");

    const config = gis.initialize.mock.calls.at(-1)?.[0] as {
      callback: (response: { credential?: string }) => void;
    };
    config.callback({});
    expect(onCredential).toHaveBeenCalledTimes(1);
  });

  it("blocks pointer events while disabled (clickwrap gate)", async () => {
    vi.stubEnv("VITE_GOOGLE_OAUTH_CLIENT_ID", "test-client-id");
    const gis = stubGis();
    render(<GoogleSignInButton text="signup_with" disabled onCredential={vi.fn()} />);
    await waitFor(() => expect(gis.renderButton).toHaveBeenCalled());

    const wrapper = screen.getByTestId("google-signin-button").parentElement;
    expect(wrapper).toHaveAttribute("aria-disabled", "true");
    expect(wrapper?.className).toContain("pointer-events-none");
  });
});
