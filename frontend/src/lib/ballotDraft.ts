import { useSyncExternalStore } from "react";
import { ApiError, apiRequest } from "@voteapp/api-client";
import type { ElectionChoice } from "@voteapp/api-client";

// Guest ballot draft: planned votes for visitors with no account, kept in
// localStorage so the draft survives restarts and flushes into
// /api/me/election-choices when they sign up (useFlushBallotDraft in App).
// Rows are stored in ElectionChoice shape — the pick chips, cards, and
// controls all consume that type, so a guest draft plugs into the same
// components as the server truth. localStorage (not sessionStorage, unlike
// pendingDistricts): the draft is the product here — losing 13 picks to a
// closed tab would defeat the point — while pending district ids are a
// one-shot handoff. Server-side caches were rejected outright: an anonymous
// visitor has no durable server identity to key one on.

const STORAGE_KEY = "voteapp_ballot_draft";

export type BallotDraft = {
  v: 1;
  /** District ids of the ballot the draft was built on — the nav badge's
   * link target (/ballot?d=…). */
  district_ids: string[];
  /** Progress denominator: the nearest upcoming election day's race ids,
   * snapshotted on the last /ballot visit. Null until the guest sees a
   * ballot with an upcoming date. */
  target: { election_date: string; election_ids: string[] } | null;
  choices: Record<string, ElectionChoice>;
};

const EMPTY_DRAFT: BallotDraft = { v: 1, district_ids: [], target: null, choices: {} };

const listeners = new Set<() => void>();
let cache: BallotDraft | null = null;

function parseDraft(raw: string | null): BallotDraft {
  if (!raw) {
    return EMPTY_DRAFT;
  }
  try {
    const parsed: unknown = JSON.parse(raw);
    if (
      typeof parsed !== "object" ||
      parsed === null ||
      (parsed as { v?: unknown }).v !== 1 ||
      typeof (parsed as { choices?: unknown }).choices !== "object"
    ) {
      return EMPTY_DRAFT;
    }
    return parsed as BallotDraft;
  } catch {
    return EMPTY_DRAFT;
  }
}

function readStorage(): BallotDraft {
  if (typeof window === "undefined") {
    return EMPTY_DRAFT;
  }
  try {
    return parseDraft(window.localStorage.getItem(STORAGE_KEY));
  } catch {
    // Storage unavailable (private mode): fall back to the in-memory cache
    // so picks at least last the tab's lifetime.
    return cache ?? EMPTY_DRAFT;
  }
}

function emit(): void {
  for (const listener of listeners) {
    listener();
  }
}

function writeDraft(next: BallotDraft): void {
  cache = next;
  try {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
  } catch {
    // Keep the in-memory copy; persistence just won't happen.
  }
  emit();
}

function currentDraft(): BallotDraft {
  cache ??= readStorage();
  return cache;
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

// Cross-tab sync: another tab's write lands here as a storage event; drop
// the cache so the next snapshot re-reads. Module-level and guarded — SSR
// has no window.
if (typeof window !== "undefined") {
  window.addEventListener("storage", (event) => {
    if (event.key === STORAGE_KEY || event.key === null) {
      cache = null;
      emit();
    }
  });
}

export function readBallotDraft(): BallotDraft {
  return currentDraft();
}

export function useBallotDraft(): BallotDraft {
  return useSyncExternalStore(subscribe, currentDraft, () => EMPTY_DRAFT);
}

export function clearBallotDraft(): void {
  cache = EMPTY_DRAFT;
  try {
    window.localStorage.removeItem(STORAGE_KEY);
  } catch {
    // Ignore.
  }
  emit();
}

/** A race counts as decided with at least one candidate pick or a measure
 * position — the same rule as PicksPage's hasRenderablePick. */
export function isDecidedChoice(choice: ElectionChoice | undefined): boolean {
  return choice !== undefined && (choice.picks.length > 0 || choice.measure_position !== null);
}

export function draftChoicesByElectionId(draft: BallotDraft): Map<string, ElectionChoice> {
  return new Map(Object.entries(draft.choices));
}

export function draftProgress(
  draft: BallotDraft
): { picked: number; total: number; complete: boolean } | null {
  if (!draft.target || draft.target.election_ids.length === 0) {
    return null;
  }
  const picked = draft.target.election_ids.filter((id) => isDecidedChoice(draft.choices[id])).length;
  const total = draft.target.election_ids.length;
  return { picked, total, complete: picked === total };
}

/** True when the draft holds anything worth flushing into an account. */
export function hasDraftPicks(draft: BallotDraft): boolean {
  return Object.values(draft.choices).some((choice) => isDecidedChoice(choice));
}

/** Called by the guest ballot page on every successful load, so the badge's
 * link and denominator track the ballot the guest actually looked at last. */
export function setDraftBallotContext(
  districtIds: string[],
  target: { election_date: string; election_ids: string[] } | null
): void {
  const draft = currentDraft();
  writeDraft({ ...draft, district_ids: districtIds, target });
}

type DraftRaceContext = {
  electionId: string;
  raceTitle: string;
  /** ISO YYYY-MM-DD. */
  electionDate: string;
};

function baseRow(
  draft: BallotDraft,
  context: DraftRaceContext,
  raceType: "office" | "ballot_measure",
  seatsToFill: number | null
): ElectionChoice {
  return (
    draft.choices[context.electionId] ?? {
      election_id: context.electionId,
      race_type: raceType,
      official_ballot_title: context.raceTitle,
      election_date: context.electionDate,
      seats_to_fill: seatsToFill,
      picks: [],
      measure_position: null,
      updated_at: new Date().toISOString(),
    }
  );
}

function putRow(draft: BallotDraft, row: ElectionChoice): void {
  const choices = { ...draft.choices };
  if (row.picks.length === 0 && row.measure_position === null) {
    delete choices[row.election_id];
  } else {
    choices[row.election_id] = { ...row, updated_at: new Date().toISOString() };
  }
  writeDraft({ ...draft, choices });
}

/** Same semantics as the server writer: single-seat races replace (radio),
 * multi-seat races append up to the seat cap and no-op past it (the button
 * disables at the cap anyway). */
export function setDraftCandidateChoice(
  input: DraftRaceContext & {
    seatsToFill: number | null;
    candidateId: string;
    candidateName: string;
    chosen: boolean;
  }
): void {
  const draft = currentDraft();
  const row = baseRow(draft, input, "office", input.seatsToFill);
  const seatCap = input.seatsToFill ?? 1;
  const others = row.picks.filter((pick) => pick.candidate_id !== input.candidateId);
  let picks = others;
  if (input.chosen) {
    const pick = { candidate_id: input.candidateId, display_name: input.candidateName, candidacy_status: "active" };
    if (seatCap <= 1) {
      picks = [pick];
    } else if (others.length < seatCap) {
      picks = [...others, pick];
    } else {
      return;
    }
  }
  putRow(draft, { ...row, picks });
}

export function setDraftMeasureChoice(
  input: DraftRaceContext & { position: "yes" | "no" | null }
): void {
  const draft = currentDraft();
  const row = baseRow(draft, input, "ballot_measure", null);
  putRow(draft, { ...row, measure_position: input.position });
}

/**
 * Replays the guest draft into the signed-in account via the normal
 * PUT /api/me/election-choices writer, oldest row first so the newest intent
 * wins any single-seat replace. Business rejections (election closed since,
 * candidate withdrew, multi-seat cap already reached on the account) skip
 * that write and keep going — the server is the authority. A transport
 * failure aborts and KEEPS the draft so a later session can retry; the
 * draft clears only after a complete pass.
 */
export async function flushBallotDraftToAccount(): Promise<void> {
  const draft = currentDraft();
  const rows = Object.values(draft.choices)
    .filter(isDecidedChoice)
    .sort((a, b) => (a.updated_at < b.updated_at ? -1 : 1));
  for (const row of rows) {
    const updates =
      row.measure_position !== null
        ? [{ election_id: row.election_id, measure_position: row.measure_position }]
        : row.picks.map((pick) => ({
            election_id: row.election_id,
            candidate_id: pick.candidate_id,
            chosen: true,
          }));
    for (const update of updates) {
      try {
        await apiRequest("/api/me/election-choices", { method: "PUT", body: update });
      } catch (error) {
        if (!(error instanceof ApiError)) {
          throw error;
        }
      }
    }
  }
  clearBallotDraft();
}
