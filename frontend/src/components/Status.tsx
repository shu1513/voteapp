import { ApiError } from "../api/client";

export function LoadingNotice({ text = "Loading…" }: { text?: string }) {
  return <p className="py-8 text-center text-gray-500">{text}</p>;
}

export function EmptyNotice({ text }: { text: string }) {
  return <p className="py-8 text-center text-gray-500">{text}</p>;
}

/** Human copy for the API error envelope; special-cases the statuses the
 * anonymous flow can hit (422 bad address, 429 rate limit). */
export function ErrorNotice({ error }: { error: unknown }) {
  let message = "Something went wrong. Please try again.";
  if (error instanceof ApiError) {
    if (error.status === 422) {
      message = "We couldn't find that address. Check the street, city, and state, then try again.";
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
  return <p className="rounded border border-red-300 bg-red-50 px-3 py-2 text-sm text-red-800">{message}</p>;
}
