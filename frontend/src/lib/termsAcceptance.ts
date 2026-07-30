import { TERMS_VERSION } from "@voteapp/api-client";

// Remembers that this browser passed the pre-search clickwrap, so a returning
// visitor is not asked again on every search. Re-prompting teaches people to
// click through without reading, which weakens assent rather than
// strengthening it; the trigger that legally requires fresh consent is a
// change to the terms, which the version check below enforces on its own. The
// 90-day expiry is hygiene on top of that, not a legal requirement: it bounds
// how long a shared or public machine carries one person's acceptance.
//
// Stores a version and a timestamp and nothing else. No identifier, no server
// record, so there is nothing here that identifies anybody and nothing to
// disclose or hand over.
//
// This may only ever decide whether the dialog OPENS. It must never pre-tick
// the checkbox inside it — see the note in packages/api-client/src/legalCopy.ts.

const STORAGE_KEY = "voteapp_terms_acceptance";

export const TERMS_ACCEPTANCE_TTL_MS = 90 * 24 * 60 * 60 * 1000;

type StoredAcceptance = {
  version: string;
  acceptedAt: number;
};

function parse(raw: string | null): StoredAcceptance | null {
  if (!raw) {
    return null;
  }
  try {
    const parsed: unknown = JSON.parse(raw);
    if (typeof parsed !== "object" || parsed === null) {
      return null;
    }
    const { version, acceptedAt } = parsed as { version?: unknown; acceptedAt?: unknown };
    if (typeof version !== "string" || typeof acceptedAt !== "number" || !Number.isFinite(acceptedAt)) {
      return null;
    }
    return { version, acceptedAt };
  } catch {
    return null;
  }
}

/**
 * True only when this browser accepted the CURRENT terms inside the window.
 * Every other outcome — nothing stored, unreadable, a superseded version, an
 * expired or future-dated timestamp, storage disabled — returns false and the
 * visitor sees the dialog. Fails closed: showing the terms one extra time
 * costs a click, skipping them wrongly costs the agreement.
 *
 * Call this from an event handler, never during render. Reading storage while
 * rendering diverges from the server-rendered HTML and breaks hydration.
 */
export function hasCurrentTermsAcceptance(now: number = Date.now()): boolean {
  let stored: StoredAcceptance | null;
  try {
    stored = parse(localStorage.getItem(STORAGE_KEY));
  } catch {
    return false;
  }
  if (!stored || stored.version !== TERMS_VERSION) {
    return false;
  }
  const age = now - stored.acceptedAt;
  // A negative age means a clock change or a tampered value; treat it as
  // unusable rather than as an acceptance that never expires.
  return age >= 0 && age <= TERMS_ACCEPTANCE_TTL_MS;
}

/** Records an acceptance the visitor just made. Never called on their behalf. */
export function rememberTermsAcceptance(now: number = Date.now()): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify({ version: TERMS_VERSION, acceptedAt: now }));
  } catch {
    // Storage unavailable: the visitor is simply asked again next time. Never
    // block the search on being able to remember it.
  }
}
