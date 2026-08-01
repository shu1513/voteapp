// Remembers that a user dismissed the post-signup welcome step, so login
// never routes them back to it. Deliberately localStorage, not a backend
// flag: the cost of forgetting (one extra, still skippable screen on a new
// browser) is too small to justify an account field, and the key is scoped
// per email so shared browsers don't leak the skip across accounts.

const KEY_PREFIX = "voteapp:welcome-skipped:";

export function hasSkippedWelcome(email: string): boolean {
  // SSR-safe and private-mode-safe: any storage failure counts as "not
  // skipped", which only risks showing the (skippable) step again.
  try {
    return window.localStorage.getItem(KEY_PREFIX + email) === "1";
  } catch {
    return false;
  }
}

export function markWelcomeSkipped(email: string): void {
  try {
    window.localStorage.setItem(KEY_PREFIX + email, "1");
  } catch {
    // Best effort — see above.
  }
}
