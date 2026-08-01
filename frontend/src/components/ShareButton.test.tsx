import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ShareButton } from "./ShareButton";

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

const PROPS = {
  path: "/candidates/cand-1",
  shareText: "Jane Doe (Democratic, WA)",
};
const URL = "https://electionssimplified.com/candidates/cand-1";

describe("ShareButton", () => {
  it("uses the native share sheet when navigator.share exists", async () => {
    const share = vi.fn().mockResolvedValue(undefined);
    vi.stubGlobal("navigator", { ...navigator, share });

    render(<ShareButton {...PROPS} />);
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Share" }));

    expect(share).toHaveBeenCalledWith({
      title: PROPS.shareText,
      text: PROPS.shareText,
      url: URL,
    });
    // Native branch is a single button, not a menu.
    expect(screen.queryByRole("menu")).not.toBeInTheDocument();
  });

  it("survives the user dismissing the native sheet", async () => {
    const share = vi.fn().mockRejectedValue(new DOMException("canceled", "AbortError"));
    vi.stubGlobal("navigator", { ...navigator, share });

    render(<ShareButton {...PROPS} />);
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Share" }));
    expect(share).toHaveBeenCalledTimes(1);
  });

  it("falls back to a menu with copy link and intent links", async () => {
    // jsdom has no navigator.share — the fallback branch by default.
    render(<ShareButton {...PROPS} />);
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Share" }));

    const encodedUrl = encodeURIComponent(URL);
    const encodedText = encodeURIComponent(PROPS.shareText);
    expect(screen.getByRole("menuitem", { name: "Share on X" })).toHaveAttribute(
      "href",
      `https://twitter.com/intent/tweet?text=${encodedText}&url=${encodedUrl}`
    );
    expect(screen.getByRole("menuitem", { name: "Share on Facebook" })).toHaveAttribute(
      "href",
      `https://www.facebook.com/sharer/sharer.php?u=${encodedUrl}`
    );
    expect(screen.getByRole("menuitem", { name: "Share on WhatsApp" })).toHaveAttribute(
      "href",
      `https://wa.me/?text=${encodeURIComponent(`${PROPS.shareText} ${URL}`)}`
    );
    expect(screen.getByRole("menuitem", { name: "Email" })).toHaveAttribute(
      "href",
      `mailto:?subject=${encodedText}&body=${encodeURIComponent(`${PROPS.shareText}\n${URL}`)}`
    );
    // Every non-mailto link opens away from the page without an opener.
    for (const name of ["Share on X", "Share on Facebook", "Share on WhatsApp"]) {
      const link = screen.getByRole("menuitem", { name });
      expect(link).toHaveAttribute("target", "_blank");
      expect(link).toHaveAttribute("rel", "noopener noreferrer");
    }
  });

  it("copies the link and confirms", async () => {
    render(<ShareButton {...PROPS} />);
    const user = userEvent.setup();
    // After setup: userEvent installs its own clipboard stub there, which
    // would swallow the component's writeText call.
    const writeText = vi.fn().mockResolvedValue(undefined);
    vi.stubGlobal("navigator", { ...navigator, clipboard: { writeText } });
    await user.click(screen.getByRole("button", { name: "Share" }));
    await user.click(screen.getByRole("menuitem", { name: "Copy link" }));

    expect(writeText).toHaveBeenCalledWith(URL);
    expect(await screen.findByText("Link copied")).toBeInTheDocument();
    // The confirmation is transient.
    await waitFor(() => expect(screen.queryByText("Link copied")).not.toBeInTheDocument(), {
      timeout: 4000,
    });
  });

  it("stays quiet when the clipboard is denied", async () => {
    render(<ShareButton {...PROPS} />);
    const user = userEvent.setup();
    // After setup, same as above.
    const writeText = vi.fn().mockRejectedValue(new Error("denied"));
    vi.stubGlobal("navigator", { ...navigator, clipboard: { writeText } });
    await user.click(screen.getByRole("button", { name: "Share" }));
    await user.click(screen.getByRole("menuitem", { name: "Copy link" }));

    expect(writeText).toHaveBeenCalledTimes(1);
    expect(screen.queryByText("Link copied")).not.toBeInTheDocument();
  });
});
