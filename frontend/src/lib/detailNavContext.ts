// Navigation context the list pages hand to the detail pages via router
// state: where "back" goes, and (later, for the pagers) the sibling order
// the reader just saw. Router state is ephemeral and non-shareable — it can
// survive same-browser reloads and back/forward (React Router copies it
// into window.history.state, see SavedBallotPage's capture-then-clear), but
// an external visit never has it, and an old history entry can hold a shape
// from any past deploy. So every field is validated on read, and the
// optional fields degrade independently: a broken sibling list must not
// take the back link down with it.

import { safeInternalPath } from "./safeInternalPath";

/** A back-link destination. Purely where and what to call it — any state to
 * deliver there travels beside it (CandidateNavState.backState), keeping
 * this type non-recursive. */
export type BackTo = { path: string; label: string };

export type NavContest = { id: string; title: string };
export type NavCandidate = { id: string; name: string };

/** Handed to /elections/:id links. contests = the ballot in displayed
 * order (races + measures + the awaiting-candidates tail). backState
 * restores a candidate page's own context on the back hop (set when the
 * election was reached from a candidate page) — the mirror of
 * CandidateNavState.backState, so a My Picks → candidate → election →
 * back round trip keeps the candidate's original back link. */
export type ElectionNavState = {
  backTo: BackTo;
  backState?: CandidateNavState;
  contests?: NavContest[];
};

/** Handed to /candidates/:id links. backState restores the election page's
 * own ballot context on the back hop; electionId scopes the candidate
 * sequence (a candidate can be in several races at once). */
export type CandidateNavState = {
  backTo: BackTo;
  backState?: ElectionNavState;
  electionId?: string;
  candidates?: NavCandidate[];
};

function readBackTo(value: unknown): BackTo | null {
  if (typeof value !== "object" || value === null) {
    return null;
  }
  const { path, label } = value as Record<string, unknown>;
  // safeInternalPath: a corrupt or ancient history entry must not yield an
  // external or protocol-relative href.
  if (typeof path !== "string" || safeInternalPath(path) === null) {
    return null;
  }
  if (typeof label !== "string" || label.trim() === "") {
    return null;
  }
  return { path, label };
}

// One malformed entry discards the whole list (undefined), not just the
// entry: a partially-valid sequence would page through a list the user
// never saw. Absent stays absent.
function readIdLabelList<Key extends string>(
  value: unknown,
  labelKey: Key
): ({ id: string } & Record<Key, string>)[] | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!Array.isArray(value)) {
    return undefined;
  }
  const entries: ({ id: string } & Record<Key, string>)[] = [];
  for (const entry of value) {
    if (typeof entry !== "object" || entry === null) {
      return undefined;
    }
    const id = (entry as Record<string, unknown>).id;
    const label = (entry as Record<string, unknown>)[labelKey];
    // trim() both: a whitespace-only id would build a broken href, and a
    // whitespace-only label would render an invisible pager link. Values are
    // validated, never normalized — real ids and titles carry no edge spaces.
    if (typeof id !== "string" || id.trim() === "" || typeof label !== "string" || label.trim() === "") {
      return undefined;
    }
    entries.push({ id, [labelKey]: label } as { id: string } & Record<Key, string>);
  }
  return entries;
}

/** null only when there is no usable backTo; a valid backTo with a broken
 * optional field keeps the back link and drops only that field. Mutually
 * recursive with readCandidateNavState via backState — terminates because
 * the stored structure is finite (each hop nests one more layer). */
export function readElectionNavState(state: unknown): ElectionNavState | null {
  if (typeof state !== "object" || state === null) {
    return null;
  }
  const record = state as Record<string, unknown>;
  const backTo = readBackTo(record.backTo);
  if (backTo === null) {
    return null;
  }
  const result: ElectionNavState = { backTo };
  const backState = readCandidateNavState(record.backState);
  if (backState !== null) {
    result.backState = backState;
  }
  const contests = readIdLabelList(record.contests, "title");
  if (contests !== undefined) {
    result.contests = contests;
  }
  return result;
}

/** The pager's neighbors in a validated sibling list, or null when there is
 * nothing to page: no list, fewer than two entries, or the current page
 * missing from it (a stale filtered snapshot must not render a pager the
 * reader never saw). Ends of the sequence yield a null prev/next slot. */
export function pagerNeighbors<Entry extends { id: string }>(
  list: Entry[] | undefined,
  currentId: string
): { prev: Entry | null; next: Entry | null } | null {
  if (list === undefined || list.length < 2) {
    return null;
  }
  const index = list.findIndex((entry) => entry.id === currentId);
  if (index === -1) {
    return null;
  }
  return {
    prev: index > 0 ? list[index - 1] : null,
    next: index < list.length - 1 ? list[index + 1] : null,
  };
}

/** Same layering: backTo is the gate; backState, electionId, and candidates
 * each validate (and degrade) on their own. */
export function readCandidateNavState(state: unknown): CandidateNavState | null {
  if (typeof state !== "object" || state === null) {
    return null;
  }
  const record = state as Record<string, unknown>;
  const backTo = readBackTo(record.backTo);
  if (backTo === null) {
    return null;
  }
  const result: CandidateNavState = { backTo };
  const backState = readElectionNavState(record.backState);
  if (backState !== null) {
    result.backState = backState;
  }
  if (typeof record.electionId === "string" && record.electionId !== "") {
    result.electionId = record.electionId;
  }
  const candidates = readIdLabelList(record.candidates, "name");
  if (candidates !== undefined) {
    result.candidates = candidates;
  }
  return result;
}
