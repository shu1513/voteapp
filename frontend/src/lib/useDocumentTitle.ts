import { useEffect } from "react";

const BASE_TITLE = "VoteApp";

// The launch description in index.html is the single source; captured on the
// first hook run (before any page has overwritten it) so routes without
// their own description always fall back to it.
let baseDescription: string | null = null;

/**
 * Sets the tab title to "{title} · VoteApp" and the meta-description for the
 * route. Pass undefined while a page is still loading its subject (election
 * title, candidate name) to show the bare brand / base description until the
 * data arrives. Both reset on unmount so shells without the hook (e.g. the
 * router errorElement) don't inherit the previous page's metadata.
 */
export function useDocumentTitle(title?: string, description?: string) {
  useEffect(() => {
    document.title = title ? `${title} · ${BASE_TITLE}` : BASE_TITLE;
    return () => {
      document.title = BASE_TITLE;
    };
  }, [title]);

  useEffect(() => {
    const meta = document.querySelector('meta[name="description"]');
    if (!(meta instanceof HTMLMetaElement)) {
      return;
    }
    baseDescription ??= meta.content;
    meta.content = description ?? baseDescription;
    return () => {
      meta.content = baseDescription ?? "";
    };
  }, [description]);
}
