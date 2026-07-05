import { useEffect } from "react";

const BASE_TITLE = "VoteApp";

/**
 * Sets the tab title to "{title} · VoteApp". Pass undefined while a page is
 * still loading its subject (election title, candidate name) to show the
 * bare brand until the data arrives.
 */
export function useDocumentTitle(title?: string) {
  useEffect(() => {
    document.title = title ? `${title} · ${BASE_TITLE}` : BASE_TITLE;
  }, [title]);
}
