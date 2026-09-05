// Browser-local "already shown" markers for the draft completion surfaces
// (docs/plans/draft-completion-moment.md): one entry per election date, not
// a single last-date value — a guest switching between ballots with
// different nearest days would otherwise overwrite it and repeat. Two
// scopes, one per surface, so each shows exactly once: the header notice
// ("notice") and the draft pages' milestone box ("milestone"). Shared by
// every account on the browser; that is the guarantee, nothing per-user
// across devices. Storage failures (private mode) fall back to a
// module-level set so each surface still shows at most once per tab life.

export type DraftCompleteSeenScope = "notice" | "milestone";

const STORAGE_KEYS: Record<DraftCompleteSeenScope, string> = {
  notice: "voteapp_draft_complete_seen",
  milestone: "voteapp_draft_milestone_seen",
};

const seenInMemory: Record<DraftCompleteSeenScope, Set<string>> = {
  notice: new Set(),
  milestone: new Set(),
};

function readSeenDates(scope: DraftCompleteSeenScope): string[] | null {
  try {
    const raw = window.localStorage.getItem(STORAGE_KEYS[scope]);
    const parsed: unknown = raw ? JSON.parse(raw) : [];
    return Array.isArray(parsed) ? parsed.filter((date): date is string => typeof date === "string") : [];
  } catch {
    return null;
  }
}

// Memory is consulted first: a browser whose reads work but whose writes
// fail (quota) lands dates only in memory, and checking storage alone
// would let the surface repeat.
export function hasDraftCompleteBeenSeen(date: string, scope: DraftCompleteSeenScope = "notice"): boolean {
  return seenInMemory[scope].has(date) || (readSeenDates(scope)?.includes(date) ?? false);
}

export function markDraftCompleteSeen(date: string, scope: DraftCompleteSeenScope = "notice"): void {
  const dates = readSeenDates(scope);
  if (dates === null) {
    seenInMemory[scope].add(date);
    return;
  }
  if (dates.includes(date)) {
    return;
  }
  try {
    window.localStorage.setItem(STORAGE_KEYS[scope], JSON.stringify([...dates, date]));
  } catch {
    seenInMemory[scope].add(date);
  }
}
