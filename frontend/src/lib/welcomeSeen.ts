// Remembers that a user has been through the post-signup welcome step —
// whether they saved preferences or skipped — so login never routes them
// back to it. Saving alone isn't enough of a record: a user who later
// clears every preference in settings would otherwise look brand-new to
// the login redirect. Deliberately localStorage, not a backend flag: the
// cost of forgetting (one extra, still skippable screen on a new browser)
// is too small to justify an account field, and the key is scoped per
// email so shared browsers don't leak the flag across accounts.

const KEY_PREFIX = "voteapp:welcome-seen:";

export function hasSeenWelcome(email: string): boolean {
  // SSR-safe and private-mode-safe: any storage failure counts as "not
  // seen", which only risks showing the (skippable) step again.
  try {
    return window.localStorage.getItem(KEY_PREFIX + email) === "1";
  } catch {
    return false;
  }
}

export function markWelcomeSeen(email: string): void {
  try {
    window.localStorage.setItem(KEY_PREFIX + email, "1");
  } catch {
    // Best effort — see above.
  }
}
