import { Menu, MenuButton, MenuItem, MenuItems } from "@headlessui/react";
import { useEffect, useRef, useState } from "react";
import { SITE_ORIGIN } from "../lib/pageMeta";

// Share control for candidate and election pages. Two shapes by capability:
//
// - navigator.share available (nearly all mobile browsers, some desktop):
//   one button opening the OS share sheet. This is the only web route into
//   iMessage, Instagram, and every other app the OS knows about — no share
//   URL exists for those.
// - otherwise (most desktop): a menu of copy-link plus the networks that DO
//   have share-intent URLs. Plain prefilled links, no platform scripts —
//   AddThis-style embeds cost tracking and weight for nothing.
//
// The capability check runs in an effect, not at render: SSR has no
// navigator, so the server always renders the menu shape and hydration must
// match it; the effect swaps in the native button after mount.
//
// What the recipient sees when the link lands (the og:*/twitter:* card) is
// pageMeta's job — the pages this button sits on must keep emitting it.

type ShareButtonProps = {
  /** Path with a leading slash; the shared URL is SITE_ORIGIN + path.
   * Built from the route, never window.location — SSR-safe and immune to
   * stray query params ending up in the shared link. */
  path: string;
  /** One line of context sent alongside the link, e.g. the candidate name
   * and race. Platforms that take no text (Facebook) rely on the og card. */
  shareText: string;
};

// Transient "Link copied" confirmation lifetime.
const COPIED_MS = 2000;

const BUTTON_CLASS =
  "rounded-lg border border-line bg-white px-3 py-1.5 text-sm font-medium text-ink transition hover:border-ink";
const ITEM_CLASS = "block px-4 py-2 text-sm text-ink data-[focus]:bg-surface";

export function ShareButton({ path, shareText }: ShareButtonProps) {
  const url = `${SITE_ORIGIN}${path}`;
  const [canNativeShare, setCanNativeShare] = useState(false);
  const [copied, setCopied] = useState(false);
  const copiedTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  useEffect(() => {
    setCanNativeShare(typeof navigator.share === "function");
    return () => clearTimeout(copiedTimer.current);
  }, []);

  async function shareNative() {
    try {
      await navigator.share({ title: shareText, text: shareText, url });
    } catch {
      // Closing the sheet without picking a target rejects (AbortError);
      // that is a normal outcome, not an error to surface.
    }
  }

  async function copyLink() {
    try {
      await navigator.clipboard.writeText(url);
    } catch {
      // Clipboard can be denied (permissions policy, insecure context in
      // dev). Don't claim success.
      return;
    }
    setCopied(true);
    clearTimeout(copiedTimer.current);
    copiedTimer.current = setTimeout(() => setCopied(false), COPIED_MS);
  }

  if (canNativeShare) {
    return (
      <button type="button" onClick={shareNative} className={BUTTON_CLASS}>
        Share
      </button>
    );
  }

  const encodedUrl = encodeURIComponent(url);
  const encodedText = encodeURIComponent(shareText);
  return (
    <Menu as="div" className="relative inline-block">
      <MenuButton className={BUTTON_CLASS}>Share</MenuButton>
      {/* role="status": announce the copy confirmation to screen readers
          without moving focus. Rendered outside MenuItems because the menu
          closes on selection — inside it the message would never be seen. */}
      <span role="status" className={copied ? "absolute right-0 top-full z-20 mt-1 whitespace-nowrap rounded-lg border border-line bg-white px-3 py-1.5 text-xs text-ink shadow-lg" : "sr-only"}>
        {copied ? "Link copied" : null}
      </span>
      <MenuItems className="absolute right-0 z-20 mt-2 w-48 rounded-xl border border-line bg-white py-1 shadow-lg focus:outline-none">
        <MenuItem>
          <button type="button" onClick={copyLink} className={`${ITEM_CLASS} w-full text-left`}>
            Copy link
          </button>
        </MenuItem>
        {/* Share-intent URLs: prefilled compose pages, opened in a new tab so
            the reader doesn't lose their place. Instagram is absent by
            necessity — it has no web share URL; it is reachable only through
            the native-share branch above. */}
        <MenuItem>
          <a
            href={`https://twitter.com/intent/tweet?text=${encodedText}&url=${encodedUrl}`}
            target="_blank"
            rel="noopener noreferrer"
            className={ITEM_CLASS}
          >
            Share on X
          </a>
        </MenuItem>
        <MenuItem>
          <a
            href={`https://www.facebook.com/sharer/sharer.php?u=${encodedUrl}`}
            target="_blank"
            rel="noopener noreferrer"
            className={ITEM_CLASS}
          >
            Share on Facebook
          </a>
        </MenuItem>
        <MenuItem>
          <a
            href={`https://wa.me/?text=${encodeURIComponent(`${shareText} ${url}`)}`}
            target="_blank"
            rel="noopener noreferrer"
            className={ITEM_CLASS}
          >
            Share on WhatsApp
          </a>
        </MenuItem>
        <MenuItem>
          {/* No target="_blank": mailto in a new tab strands a blank tab
              behind the mail client. */}
          <a href={`mailto:?subject=${encodedText}&body=${encodeURIComponent(`${shareText}\n${url}`)}`} className={ITEM_CLASS}>
            Email
          </a>
        </MenuItem>
      </MenuItems>
    </Menu>
  );
}
