// Browser-local "already congratulated" marker for the draft completion
// notice (docs/plans/draft-completion-moment.md): one entry per election
// date, not a single last-date value — a guest switching between ballots
// with different nearest days would otherwise overwrite it and repeat.
// Shared by every account on the browser; that is the guarantee, nothing
// per-user across devices. Storage failures (private mode) fall back to a
// module-level set so the notice still fires at most once per tab life.

const SEEN_KEY = "voteapp_draft_complete_seen";

const seenInMemory = new Set<string>();

function readSeenDates(): string[] | null {
  try {
    const raw = window.localStorage.getItem(SEEN_KEY);
    const parsed: unknown = raw ? JSON.parse(raw) : [];
    return Array.isArray(parsed) ? parsed.filter((date): date is string => typeof date === "string") : [];
  } catch {
    return null;
  }
}

export function hasDraftCompleteBeenSeen(date: string): boolean {
  const dates = readSeenDates();
  return dates === null ? seenInMemory.has(date) : dates.includes(date);
}

export function markDraftCompleteSeen(date: string): void {
  const dates = readSeenDates();
  if (dates === null) {
    seenInMemory.add(date);
    return;
  }
  if (dates.includes(date)) {
    return;
  }
  try {
    window.localStorage.setItem(SEEN_KEY, JSON.stringify([...dates, date]));
  } catch {
    seenInMemory.add(date);
  }
}
