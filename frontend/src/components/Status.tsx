import { useEffect } from "react";
import { ApiError } from "@voteapp/api-client";
import { errorCategoryOf, track } from "../lib/usage";

export function LoadingNotice({ text = "Loading…" }: { text?: string }) {
  return <p className="py-8 text-center text-ink-soft">{text}</p>;
}

export function EmptyNotice({ text }: { text: string }) {
  return <p className="py-8 text-center text-ink-soft">{text}</p>;
}

/** Human copy for the API error envelope; special-cases the statuses the
 * anonymous flow can hit (422 bad address, 429 rate limit). */
export function ErrorNotice({ error }: { error: unknown }) {
  // Category only (never the message): which routes show which failures.
  useEffect(() => {
    track("error_shown", { category: errorCategoryOf(error) });
  }, [error]);
  let message = "Something went wrong. Please try again.";
  if (error instanceof ApiError) {
    if (error.status === 422) {
      // Partial-path codes carry user-ready messages ("ZIP code 02861
      // crosses state lines — enter a full street address"); the generic
      // street-address copy would misdiagnose them.
      message =
        error.code.startsWith("zip_") ||
        error.code === "full_address_required" ||
        error.code === "region_unsupported"
          ? error.message
          : "We couldn't find that address. Check the street, city, and state, then try again.";
    } else if (error.status === 429) {
      message = error.retryAfterSeconds
        ? `Too many requests. Please wait ${error.retryAfterSeconds} seconds and try again.`
        : "Too many requests. Please wait a moment and try again.";
    } else if (error.status >= 500) {
      message = "The service is having trouble right now. Please try again shortly.";
    } else {
      message = error.message;
    }
  }
  return (
    <p className="rounded-lg border border-rausch/40 bg-rausch/5 px-3 py-2 text-sm text-rausch-dark">{message}</p>
  );
}
