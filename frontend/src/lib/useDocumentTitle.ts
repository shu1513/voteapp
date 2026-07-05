import { useEffect } from "react";

const BASE_TITLE = "VoteApp";

/**
 * Sets the tab title to "{title} · VoteApp" and, when given, the
 * meta-description for the route. Pass undefined while a page is still
 * loading its subject (election title, candidate name) to show the bare
 * brand until the data arrives.
 */
export function useDocumentTitle(title?: string, description?: string) {
  useEffect(() => {
    document.title = title ? `${title} · ${BASE_TITLE}` : BASE_TITLE;
  }, [title]);

  useEffect(() => {
    if (!description) {
      return;
    }
    const meta = document.querySelector('meta[name="description"]');
    if (!(meta instanceof HTMLMetaElement)) {
      return;
    }
    const previous = meta.content;
    meta.content = description;
    return () => {
      meta.content = previous;
    };
  }, [description]);
}
